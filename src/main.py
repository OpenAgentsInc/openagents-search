from OpenAgentsNode import OpenAgentsNode
from OpenAgentsNode import JobRunner
import config as NodeConfig
from events import search as SearchEvent
import base64
import json
import numpy as np
import faiss
import hashlib
import asyncio
import time
import os
import gc
class Runner (JobRunner):
    INDEXES={}
    SEARCH_QUEUE = []
    MAX_MEMORY_CACHE_GB = 1

    def __init__(self, filters, meta, template, sockets):
        super().__init__(filters, meta, template, sockets)
        self.MAX_MEMORY_CACHE_GB = float(os.getenv('MAX_MEMORY_CACHE_GB', self.MAX_MEMORY_CACHE_GB))


    async def deserializeFromBlob(self,  url,  out_vectors , out_content):
        blobStorage = await self.openStorage( url)
        files = await blobStorage.list()
        self.log("Reading embeddings from "+url)
        for f in files:
            print(f)
            
        # Find embeddings files
        embeddings_files = [f for f in files if f.endswith(".embeddings")]
        sentences = []
        vectors = []
        dtype = None
        shape = None
        print("Found files "+str(embeddings_files))
        for f in embeddings_files:            
            # Binary read
            sentence_bytes=await blobStorage.readBytes(f)
            vectors_bytes=await blobStorage.readBytes(f+".vectors")
            shape_bytes=await blobStorage.readBytes(f+".shape")
            dtype_bytes=await blobStorage.readBytes(f+".dtype")
            # sentence_marker_bytes=blobStorage.readBytes(f+".kind")
            # Decode
            sentence = sentence_bytes.decode("utf-8")
            dtype = dtype_bytes.decode("utf-8")
            shape = json.loads(shape_bytes.decode("utf-8"))
            embeddings = np.frombuffer(vectors_bytes, dtype=dtype).reshape(shape)         
            out_vectors.append(embeddings)
            out_content.append(sentence)
        await blobStorage.close()
        return [dtype,shape]

    async def deserializeFromJSON( self, data,  out_vectors ,out_content):
        dtype=None
        shape=None
        data=json.loads(data)
        for part in data:
            text = part[0]
            embeddings_b64 = part[1]
            _dtype = part[2]
            _shape = part[3]

            
            # part_marker=part[4] if len(part)>4 else None

            if dtype is None: dtype = _dtype
            elif dtype != _dtype: raise Exception("Data type mismatch")
            if shape is None: shape = _shape
            elif shape != _shape: raise Exception("Shape mismatch")
            # Decode
            embeddings_bytes = base64.b64decode(embeddings_b64)
            embeddings =  np.frombuffer(embeddings_bytes, dtype=dtype).reshape(shape)
            # Append
            out_vectors.append(embeddings)
            out_content.append(text)
        return [dtype,shape]

    async def deserialize( self, jin,out_vectors ,out_content):
        dtype = None
        shape = None
        data = jin.data
        dataType = jin.type
        marker = jin.marker   
        if dataType == "application/hyperdrive+bundle":
            [dtype,shape] = await self.deserializeFromBlob(data, out_vectors, out_content)
        else:
            [dtype,shape] =  await self.deserializeFromJSON(data, out_vectors, out_content)
        return [dtype,shape]

    async def loop(self ):
        
        if len(self.SEARCH_QUEUE) == 0:
            await asyncio.sleep(10.0/1000.0)
            return
        search = self.SEARCH_QUEUE.pop(0)

        faiss_index = search["faiss_index"]
        queue = search["queue"]
        top_k = 1
        
        flattern_queries = []
        flattern_queries_idInQueue = []
        for i in range(len(queue)):
            enqueued = queue[i]
            enqueued_top_k = enqueued[1]
            for query in enqueued[0]:
                flattern_queries.append(query)
                flattern_queries_idInQueue.append(i)
            if enqueued_top_k > top_k:
                top_k = enqueued_top_k
        
        if len(flattern_queries) == 0:
            return
        self.log("Searching "+str(len(flattern_queries))+" queries")
        flattern_queries=np.array(flattern_queries)
        distances, indices = faiss_index.search(flattern_queries, top_k)
        for i in range(len(queue)):
            distances_for_entry = []
            indices_for_entry = []
            for j in range(len(flattern_queries)):
                if flattern_queries_idInQueue[j] == i:
                    distances_for_entry.append(distances[j])
                    indices_for_entry.append(indices[j])
            queue[i][2](distances_for_entry, indices_for_entry)
        await asyncio.sleep(10.0/1000.0)
        
        

    async def run(self,job):
        def getParamValue(key,default=None):
            param = [x for x in job.param if x.key == key]
            return param[0].value[0] if len(param) > 0 else default

        # Extract parameters
        top_k = int(getParamValue("k", "4"))
        normalize = str(getParamValue("normalize", "true"))=="true"
        
        # Deserialize inputs
        indexId=""
        for jin in job.input:
            marker = jin.marker
            if marker != "query":
                indexId += jin.data
        if len(indexId) == 0:
            self.log("No index")
            return json.dumps([])
        indexId=hashlib.sha256(indexId.encode()).hexdigest() 
                
        index = self.INDEXES.get(indexId)
        if not index:
            self.log("Preparing index")
            index_vectors = []
            index_content = []
            dtype = None
            shape = None
            for jin in job.input:
                if jin.marker == "query":
                    continue
                [dtype,shape] = await self.deserialize(jin,index_vectors ,index_content)               

            index_vectors = np.array(index_vectors)
            if normalize and dtype == "float32":
                faiss.normalize_L2(index_vectors)

            # Create faiss index
            self.log("Creating faiss index")
            faiss_index = faiss.IndexFlatL2(shape[0])
            faiss_index.add(index_vectors)
            indexSizeGB = faiss_index.ntotal * shape[0] * 4 / 1024 / 1024 / 1024
            index = [faiss_index, time.time(), index_content, indexSizeGB]
            self.INDEXES[indexId] = index
            
            # drop oldest index if out of memory limit
            totalSize = sum([x[3] for x in self.INDEXES.values()])
            while totalSize > self.MAX_MEMORY_CACHE_GB:
                oldest = min(self.INDEXES.values(), key=lambda x: x[1])
                self.log("Max cache size reached. Dropping oldest index.")
                del self.INDEXES[oldest]
                totalSize -= oldest[3]
            gc.collect()



        else:
            self.log("Index already loaded")
        index[1] = time.time()

        queries = []
        for jin in job.input:
            if jin.marker == "query":
                searches_vectors = []
                searches_content = [] 
                [dtype,shape] = await self.deserialize(jin, searches_vectors, searches_content)
                searches_vectors = np.array(searches_vectors)

                if normalize and dtype == "float32":
                    faiss.normalize_L2(searches_vectors)
                queries=searches_vectors
            
        if len(queries) == 0 :
            self.log("No queries")
            return json.dumps([])
        
        # Search faiss index        
        self.log("Searching")
        search = next((x for x in self.SEARCH_QUEUE if x["indexId"] == indexId), None)
        if not search:
            search = {
                "queue": [],
                "faiss_index": index[0],
                "indexId": indexId
            }
            self.SEARCH_QUEUE.append(search)
        queue = search["queue"]

        future =  asyncio.Future()
        def callback(distances, indices):
            # Get content for each search query and sort by score
            self.log("Retrieving content from index")
            output_per_search = []
            index_content = index[2]
            for i in range(len(indices)):
                output_per_search.append([])
                for j in range(len(indices[i])):
                    content = index_content[indices[i][j]]
                    output_per_search[i].append({"value": content, "score": float(distances[i][j]), "contentId": str(indices[i][j])})
                output_per_search[i] = sorted( output_per_search[i], key=lambda x: x["score"], reverse=False)
                
            # Merge results from all searches 
            output = []
            i=0
            while len(output) < len(output_per_search)*top_k:
                for j in range(len(output_per_search)):
                    if i < len(output_per_search[j]):
                        output.append(output_per_search[j][i])
                i+=1       

            # Remove duplicates
            dedup = []
            dedup_ids=[]
            for o in output:
                if o["contentId"] not in dedup_ids:
                    dedup.append(o)
                    dedup_ids.append(o["contentId"])
            output = dedup
            
            # truncate output
            output = output[:min(top_k, len(output))]
            future.set_result(output)

        queue.append([
            queries,
            top_k,
            callback
        ])
        output = await future
        
        # Serialize output and return
        return json.dumps(output)


node = OpenAgentsNode(NodeConfig.meta)
node.registerRunner(Runner(filters=SearchEvent.filters,sockets=SearchEvent.sockets,meta=SearchEvent.meta,template=SearchEvent.template))
node.start()