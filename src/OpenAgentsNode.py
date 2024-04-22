import grpc
from openagents_grpc_proto import rpc_pb2_grpc
from openagents_grpc_proto import rpc_pb2
import time
import os
import traceback
import json
import asyncio
import pickle

class BlobStorage:
    def __init__(self, id, url, node):
        self.id = id
        self.url = url
        self.node = node
        self.closed = False
    
    def list(self, prefix="/"):
        client = self.node.getClient()
        files = client.diskListFiles(rpc_pb2.RpcDiskListFilesRequest(diskId=self.id, path=prefix))
        return files.files
    
    def delete(self, path):
        client = self.node.getClient()
        res = client.diskDeleteFile(rpc_pb2.RpcDiskDeleteFileRequest(diskId=self.id, path=path))
        return res.success

    def writeBytes(self, path, dataBytes):
        client = self.node.getClient()
        CHUNK_SIZE = 1024
        def write_data():
            for j in range(0, len(dataBytes), CHUNK_SIZE):
                chunk = bytes(dataBytes[j:min(j+CHUNK_SIZE, len(dataBytes))])                   
                request = rpc_pb2.RpcDiskWriteFileRequest(diskId=str(self.id), path=path, data=chunk)
                yield request                              
        res=client.diskWriteFile(write_data())
        return res.success

    def readBytes(self, path):
        client = self.node.getClient()
        bytesOut = bytearray()
        for chunk in client.diskReadFile(rpc_pb2.RpcDiskReadFileRequest(diskId=self.id, path=path)):
            bytesOut.extend(chunk.data)
        return bytesOut

    def writeUTF8(self, path, data):
        return self.writeBytes(path, data.encode('utf-8'))

    def readUTF8(self, path):
        return self.readBytes(path).decode('utf-8')

    def close(self):
        if self.closed: return
        client = self.node.getClient()
        client.closeDisk(rpc_pb2.RpcCloseDiskRequest(diskId=self.id))
        self.closed=True

    def getUrl(self):
        return self.url

class JobRunner:
    _filters = None
    _node = None
    _job = None
    _disksByUrl = {}
    _disksById = {}
    _diskByName = {}
    _template = None
    _meta = None
    _sockets = None
    _nextAnnouncementTimestamp = 0
    def __init__(self, filters, meta, template, sockets):
        self._filters = filters
        
        if not isinstance(meta, str):
            self._meta = json.dumps(meta)
        else:
            self._meta = meta
        if not isinstance(template, str):
            self._template = json.dumps(template)
        else:
            self._template = template
        if not isinstance(sockets, str):
            self._sockets = json.dumps(sockets)
        else:
            self._sockets = sockets


    def cacheSet(self, path, value, version=0, expireAt=0):
        try:
            dataBytes = pickle.dumps(value)
            client = self._node.getClient()
            CHUNK_SIZE = 1024
            def write_data():
                for j in range(0, len(dataBytes), CHUNK_SIZE):
                    chunk = bytes(dataBytes[j:min(j+CHUNK_SIZE, len(dataBytes))])                   
                    request = rpc_pb2.RpcCacheSetRequest(
                        key=path, 
                        data=chunk,
                        expireAt=expireAt,
                        version=version
                    )
                    yield request                              
            res=client.cacheSet(write_data())
            return res.success
        except Exception as e:
            print("Error setting cache "+str(e))
            return False
        

    def cacheGet(self, path, lastVersion = 0):
        try:
            client = self._node.getClient()
            bytesOut = bytearray()
            for chunk in client.cacheGet(rpc_pb2.RpcCacheGetRequest(key=path, lastVersion = lastVersion)):
                if not chunk.exists:
                    print("Cache miss")
                    return None
                bytesOut.extend(chunk.data)
            return pickle.loads(bytesOut)
        except Exception as e:
            print("Error getting cache "+str(e))
            return None

    def _setNode(self, node):
        self._node = node

    def _setJob(self, job):
        self._job = job

    def log(self, message):
        if self._job: message+=" for job "+self._job.id
        if self._node: 
            self._node.log(message, self._job.id if self._job else None)
        else: 
            print(message)

    def openStorage(self, url):
        if url in self._disksByUrl:
            return self._disksByUrl[url]
        client = self._node.getClient()
        diskId = client.openDisk(rpc_pb2.RpcOpenDiskRequest(url=url)).diskId
        disk =  BlobStorage(id=diskId, url=url, node=self._node)
        self._disksByUrl[url] = disk
        self._disksById[diskId] = disk
        return disk

    def createStorage(self,name=None,encryptionKey=None,includeEncryptionKeyInUrl=None):
        if name in self._diskByName:
            return self._diskByName[name]
        
        client = self._node.getClient()
        url = client.createDisk(rpc_pb2.RpcCreateDiskRequest(
            name=name,
            encryptionKey=encryptionKey,
            includeEncryptionKeyInUrl=includeEncryptionKeyInUrl
        )).url
        diskId = client.openDisk(rpc_pb2.RpcOpenDiskRequest(url=url)).diskId
        disk = BlobStorage(id=diskId, url=url, node=self._node)
        self._disksByUrl[url] = disk
        self._disksById[diskId] = disk
        self._diskByName[name] = disk
        return disk

    def postRun(self):
        for disk in self._disksById.values():
            disk.close()
        for disk in self._disksByUrl.values():
            disk.close()
        for disk in self._diskByName.values():
            disk.close()
        self._disksById = {}
        self._disksByUrl = {}
        self._diskByName = {}

    def canRun(self,job):
        return True
        
    def preRun(self):
        pass

    async def loop(self):
        pass

    async def run(self, job):
        pass

class OpenAgentsNode:
    nextNodeAnnounce = 0
    nodeName = ""
    nodeIcon = ""
    nodeDescription = ""
    channel = None
    rpcClient = None
    runners=[]
    poolAddress = None
    poolPort = None
    failedJobsTracker = []
    isLooping = False
    def __init__(self, nameOrMeta=None, icon=None, description=None):
        name = ""
        if isinstance(nameOrMeta, str):
            name = nameOrMeta
        else :
            name = nameOrMeta["name"]
            icon = nameOrMeta["picture"]
            description = nameOrMeta["about"]
        self.nodeName = name or os.getenv('NODE_NAME', "OpenAgentsNode")
        self.nodeIcon = icon or os.getenv('NODE_ICON', "")
        self.nodeDescription = description or  os.getenv('NODE_DESCRIPTION', "")
        self.channel = None
        self.rpcClient = None

    def registerRunner(self, runner):
        self.runners.append(runner)


    def getClient(self): 
        if self.channel is None or self.channel._channel.check_connectivity_state(True)  == grpc.ChannelConnectivity.SHUTDOWN:
            if self.channel is not None:
                try:
                    self.channel.close()
                except Exception as e:
                    print("Error closing channel "+str(e))
            print("Connect to "+self.poolAddress+":"+str(self.poolPort)+" with ssl "+str(self.poolSsl))
            if self.poolSsl:
                self.channel = grpc.secure_channel(self.poolAddress+":"+str(self.poolPort), grpc.ssl_channel_credentials())
            else:
                self.channel = grpc.insecure_channel(self.poolAddress+":"+str(self.poolPort))
            self.rpcClient = rpc_pb2_grpc.PoolConnectorStub(self.channel)
        return self.rpcClient


    async def reannounce(self):    
        # Announce node
        time_ms=int(time.time()*1000)
        if time_ms >= self.nextNodeAnnounce:
            try:
                client = self.getClient()
                res=client.announceNode(rpc_pb2.RpcAnnounceNodeRequest(
                    iconUrl = self.nodeIcon,
                    name = self.nodeName,
                    description = self.nodeDescription,
                ))
                self.nextNodeAnnounce = int(time.time()*1000) + res.refreshInterval
                self.log("Node announced, next announcement in "+str(res.refreshInterval)+" ms")
            except Exception as e:
                self.log("Error announcing node "+ str(e), None)
                self.nextNodeAnnounce = int(time.time()*1000) + 5000

        for runner in self.runners:
            try:
                if time_ms >= runner._nextAnnouncementTimestamp:
                    client = self.getClient()
                    res = client.announceEventTemplate(rpc_pb2.RpcAnnounceTemplateRequest(
                        meta=runner._meta,
                        template=runner._template,
                        sockets=runner._sockets
                    ))
                    runner._nextAnnouncementTimestamp = int(time.time()*1000) + res.refreshInterval
                    self.log("Template announced, next announcement in "+str(res.refreshInterval)+" ms")
            except Exception as e:
                self.log("Error announcing template "+ str(e), None)
                runner._nextAnnouncementTimestamp = int(time.time()*1000) + 5000


    async def executePendingJob(self ):
        client = self.getClient()
        for runner in self.runners:
            jobs=[]
            for filter in runner._filters:
                jobs.extend(client.getPendingJobs(rpc_pb2.RpcGetPendingJobs(
                    filterByRunOn =  filter["filterByRunOn"] if "filterByRunOn" in filter else None,
                    filterByCustomer = filter["filterByCustomer"] if "filterByCustomer" in filter else None,
                    filterByDescription = filter["filterByDescription"] if "filterByDescription" in filter else None,
                    filterById = filter["filterById"] if "filterById" in filter else None,
                    filterByKind  = filter["filterByKind"] if "filterByKind" in filter else None
                )).jobs)    
            
            self.failedJobsTracker = [x for x in self.failedJobsTracker if time.time()-x[1] < 60] # Remove older failed jobs  (gives a chance to retry)
            for job in jobs:
                if job.id in [x[0] for x in self.failedJobsTracker]:
                    continue
                if len(jobs)>0 : self.log(str(len(jobs))+" pending jobs")
                wasAccepted=False
                t=time.time()   
                try:
                    client = self.getClient() # Reconnect client for each job
                    if not runner.canRun(job):
                        continue
                    client.acceptJob(rpc_pb2.RpcAcceptJob(jobId=job.id))
                    wasAccepted = True
                    self.log("Job started on node "+self.nodeName, job.id)  
                    runner._setNode(self)
                    runner._setJob(job)
                    runner.preRun()
                    async def task():
                        try:
                            output=await runner.run(job)    
                            runner.postRun()                            
                            self.log("Job completed in "+str(time.time()-t)+" seconds on node "+self.nodeName, job.id)                
                            client.completeJob(rpc_pb2.RpcJobOutput(jobId=job.id, output=output))
                        except Exception as e:
                            self.failedJobsTracker.append([job.id, time.time()])
                            self.log("Job failed in "+str(time.time()-t)+" seconds on node "+self.nodeName+" with error "+str(e), job.id)
                            if wasAccepted:
                                client.cancelJob(rpc_pb2.RpcCancelJob(jobId=job.id, reason=str(e)))
                            traceback.print_exc()
                    asyncio.create_task(task())
                except Exception as e:
                    self.failedJobsTracker.append([job.id, time.time()])
                    self.log("Job failed in "+str(time.time()-t)+" seconds on node "+self.nodeName+" with error "+str(e), job.id)
                    if wasAccepted:
                        client.cancelJob(rpc_pb2.RpcCancelJob(jobId=job.id, reason=str(e)))
                    traceback.print_exc()

    def log(self,message, jobId=None):
        print(message)
        if jobId: 
            self.getClient().logForJob(rpc_pb2.RpcJobLog(jobId=jobId, log=message))

  
    async def loop(self):
        if not self.isLooping:
            self.isLooping = True
            promises = [runner.loop() for runner in self.runners]
            await asyncio.gather(*promises)
            self.isLooping = False
        
    def start(self, poolAddress=None, poolPort=None):
        asyncio.run(self.run(poolAddress, poolPort))

    async def run(self, poolAddress=None, poolPort=None, poolSsl=False):
        self.poolAddress = poolAddress or os.getenv('POOL_ADDRESS', "127.0.0.1")
        self.poolPort = poolPort or int(os.getenv('POOL_PORT', "5000"))
        self.poolSsl = poolSsl or os.getenv('POOL_SSL', "false")== "true"
        while True:
            try:
                asyncio.create_task(self.loop())
                asyncio.create_task(self.reannounce())
                asyncio.create_task(self.executePendingJob())
                # time.sleep(10.0/1000.0)
                await asyncio.sleep(10.0/1000.0)
            except Exception as e:
                self.log("Error in main loop "+str(e))
                traceback.print_exc()
                await asyncio.sleep(5)
                # time.sleep(5)
            except KeyboardInterrupt:
                break