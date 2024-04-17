from sentence_transformers import SentenceTransformer
from openagents_grpc_proto import rpc_pb2_grpc
from openagents_grpc_proto import rpc_pb2
import time
import grpc
import json
import hashlib
import pickle
import os
import traceback
import base64
from sentence_transformers.quantization import quantize_embeddings
import faiss
import numpy as np
import math
def log(rpcClient, message, jobId=None):
    print(message)
    if rpcClient and jobId: 
        rpcClient.logForJob(rpc_pb2.RpcJobLog(jobId=jobId, log=message))




    
def completePendingJob(rpcClient ):
    jobs=[]
    jobs.extend(rpcClient.getPendingJobs(rpc_pb2.RpcGetPendingJobs(filterByRunOn="openagents\\/search")).jobs)    
    if len(jobs)>0 : log(rpcClient, str(len(jobs))+" pending jobs")
    for job in jobs:
        wasAccepted=False
        try:
            wasAccepted = True
            rpcClient.acceptJob(rpc_pb2.RpcAcceptJob(jobId=job.id))

            def getParamValue(key,default=None):
                param = [x for x in job.param if x.key == key]
                return param[0].value[0] if len(param) > 0 else default

            top_k = int(getParamValue("k", "4"))
            normalize = str(getParamValue("normalize", "true"))=="true"

            t=time.time()
            log(rpcClient, "Preparing index for job "+job.id)
           

            
            index_vectors = []
            index_content = []
            searches_vectors = []

            dtype = None
            shape = None

            for jin in job.input:
                data = json.loads(jin.data)
                dataType = jin.type
                marker = jin.marker                             
                
                # every data might contain multiple vectors
                for part in data:
                    [text,embeddings_b64,_dtype,_shape] = part
                    if dtype is None: dtype = _dtype
                    elif dtype != _dtype: raise Exception("Data type mismatch")
                    if shape is None: shape = _shape
                    elif shape != _shape: raise Exception("Shape mismatch")
                    embeddings_bytes = base64.b64decode(embeddings_b64)
                    embeddings =  np.frombuffer(embeddings_bytes, dtype=dtype).reshape(shape)
                    if marker == "query":
                        searches_vectors.append(embeddings)
                    else:
                        index_vectors.append(embeddings)
                        index_content.append(text)

            searches_vectors = np.array(searches_vectors)
            index_vectors = np.array(index_vectors)
            if normalize and dtype == "float32":
                faiss.normalize_L2(searches_vectors)
                faiss.normalize_L2(index_vectors)

           
            print("Shape "+str(shape))
            index  = faiss.IndexFlatL2(shape[0])
            index.add(index_vectors)
                

            log(rpcClient, "Index prepared for job "+job.id+" in "+str(time.time()-t)+" seconds")

            t=time.time()
            log(rpcClient, "Searching index for job "+job.id)
            distances, indices = index.search(searches_vectors, top_k)
            log(rpcClient, "Search completed for job "+job.id+" in "+str(time.time()-t)+" seconds")

            log(rpcClient, "Retrieving content for indexes for job "+job.id)
            output_per_search = []
            t=time.time()   
            for i in range(len(indices)):
                output_per_search.append([])
                for j in range(len(indices[i])):
                    content = index_content[indices[i][j]]
                    output_per_search[i].append({"value": content, "score": float(distances[i][j]), "contentId": str(indices[i][j])})
                output_per_search[i] = sorted( output_per_search[i], key=lambda x: x["score"], reverse=False)
            log(rpcClient, "Content retrieved for job "+job.id+" on query " +str(i)+" in "+str(time.time()-t)+" seconds")

            log(rpcClient, "Select top "+str(top_k)+" k for job "+job.id)
            t=time.time()

            output = []
            i=0
            while len(output) < top_k:
                for j in range(len(output_per_search)):
                    if i < len(output_per_search[j]):
                        output.append(output_per_search[j][i])
                i+=1           
           
            output = output[:min(top_k, len(output))]
            # remove duplicates based on content id (keep first occurence)
            dedup = []
            dedup_ids=[]
            for o in output:
                if o["contentId"] not in dedup_ids:
                    dedup.append(o)
                    dedup_ids.append(o["contentId"])
            output = dedup

            log(rpcClient, "Top "+str(top_k)+" selected for job "+job.id+" in "+str(time.time()-t)+" seconds")
         
            rpcClient.completeJob(rpc_pb2.RpcJobOutput(jobId=job.id, output=json.dumps(output)))

        except Exception as e:
            log(rpcClient, "Error processing job "+ str(e), job.id if job else None)
            if wasAccepted:
                rpcClient.cancelJob(rpc_pb2.RpcCancelJob(jobId=job.id, reason=str(e)))
            
            
            traceback.print_exc()



TEMPLATES = [
    {
        "nextAnnouncementTimestamp":0,
        "sockets": json.dumps({
            "in": {
                "k": {
                    "type": "number",
                    "value": 4,
                    "description": "The number of embeddings to return",
                    "name": "Top K"
                },
                "normalize": {
                    "type": "boolean",
                    "value": True,
                    "description": "Normalize index",
                    "name": "Normalize Index"
                },
                "queries":{
                    "type": "object",
                    "description": "The queries",
                    "name": "Queries",
                    "schema": {
                        "value":{
                            "type": "string",
                            "description": "Stringified JSON object with the query embedding from openagents/embeddings",
                            "name": "Query"
                        },
                        "type":{
                            "type": "string",
                            "value" : "text",
                            "description": "The type of the query embedding",
                            "name": "Type"
                    
                        }
                    }
                },
                "indices": {
                    "type": "array",
                    "description": "The index to search on",
                    "name": "Documents",
                    "schema": {
                        "value":{
                            "type": "string",
                            "description": "Stringified JSON object with the document embedding from openagents/embeddings",
                            "name": "Text excerpt"
                        },
                        "type":{
                            "type": "string",
                            "description": "The type of the query embedding",
                            "name": "Type",
                            "value" : "text"
                        }
                    }          
                }
            },
            "out": {
                "output": {
                    "type": "application/json",
                    "description": "The  top K embeddings",
                    "name": "Embeddings"
                }
            }
        }),
        "meta":json.dumps({
            "kind": 5003,
            "name": "Similarity Search",
            "about": "",
            "tos": "",
            "privacy": "",
            "author": "",
            "web": "",
            "picture": "",
            "tags": ["tool"]
        }),
        "template":"""{
            "kind": {{meta.kind}},
            "created_at": {{sys.timestamp_seconds}},
            "tags": [
                ["param","run-on", "openagents/embeddings" ],                             
                ["param", "k", "{{in.k}}"],
                ["param", "normalize", "{{in.normalize}}"],
                {{#in.queries}}
                ["i", "{{value}}", "{{type}}", "",  "query"],
                {{/in.queries}}
                {{#in.indices}}
                ["i", "{{value}}", "{{type}}", "",  "index"],
                {{/in.indices}}
                ["expiration", "{{sys.expiration_timestamp_seconds}}"],
            ],
            "content":""
        }
        """
    }
]
NEXT_NODE_ANNOUNCE=0

def announce(rpcClient):    
    # Announce node
    global NEXT_NODE_ANNOUNCE
    time_ms=int(time.time()*1000)
    if time_ms >= NEXT_NODE_ANNOUNCE:
        ICON = os.getenv('NODE_ICON', "")
        NAME = os.getenv('NODE_NAME', "Embeddings Generator")
        DESCRIPTION = os.getenv('NODE_DESCRIPTION', "Generate embeddings for the input text")
        
        res=rpcClient.announceNode(rpc_pb2.RpcAnnounceNodeRequest(
            iconUrl = ICON,
            name = NAME,
            description = DESCRIPTION,
        ))
        NEXT_NODE_ANNOUNCE = int(time.time()*1000) + res.refreshInterval
    
    # Announce templates
    for template in TEMPLATES:
        if time_ms >= template["nextAnnouncementTimestamp"]:
            res = rpcClient.announceEventTemplate(rpc_pb2.RpcAnnounceTemplateRequest(
                meta=template["meta"],
                template=template["template"],
                sockets=template["sockets"]
            ))
            template["nextAnnouncementTimestamp"] = int(time.time()*1000) + res.refreshInterval



def main():
    POOL_ADDRESS = os.getenv('POOL_ADDRESS', "127.0.0.1")
    POOL_PORT = int(os.getenv('POOL_PORT', "5000"))
    while True:
        try:
            with grpc.insecure_channel(POOL_ADDRESS+":"+str(POOL_PORT)) as channel:
                stub = rpc_pb2_grpc.PoolConnectorStub(channel)
                log(stub, "Connected to "+POOL_ADDRESS+":"+str(POOL_PORT))
                while True:
                    try:
                        announce(stub)
                    except Exception as e:
                        log(stub, "Error announcing node "+ str(e), None)
                    try:
                        completePendingJob(stub)
                    except Exception as e:
                        log(stub, "Error processing pending jobs "+ str(e), None)
                    time.sleep(10.0/1000.0)
        except Exception as e:
            log(None,"Error connecting to grpc server "+ str(e))
            
       

if __name__ == '__main__':
    main()