from OpenAgentsNode import OpenAgentsNode
from OpenAgentsNode import JobRunner
import config as NodeConfig
from events import search as SearchEvent
import base64
import json
import numpy as np
import faiss
class Runner (JobRunner):
    def __init__(self, filters, meta, template, sockets):
        super().__init__(filters, meta, template, sockets)

    def deserializeFromBlob(self,  url, marker, index_vectors ,index_content, searches_vectors):
        blobStorage = self.openStorage( url)
        files = blobStorage.list()
        # Find embeddings files
        embeddings_files = [f for f in files if f.endswith(".embeddings")]
        sentences = []
        vectors = []
        dtype = None
        shape = None
        for f in embeddings_files:
            # Binary read
            sentence_bytes=blobStorage.readBytes(f)
            vectors_bytes=blobStorage.readBytes(f+".vectors")
            shape_bytes=blobStorage.readBytes(f+".shape")
            dtype_bytes=blobStorage.readBytes(f+".dtype")
            sentence_marker_bytes=blobStorage.readBytes(f+".kind")

            # Decode
            sentence = sentence_bytes.decode("utf-8")
            dtype = dtype_bytes.decode("utf-8")
            shape = json.loads(shape_bytes.decode("utf-8"))
            embeddings = np.frombuffer(vectors_bytes, dtype=dtype).reshape(shape)
            sentence_marker = sentence_marker_bytes.decode("utf-8")
            if not marker: marker = sentence_marker
            if not marker: 
                self.log("Marker not specified, use 'passage'")
                marker = "passage"
            # Append
            if marker == "query":
                searches_vectors.append(embeddings)
            else:
                index_vectors.append(embeddings)
                index_content.append(sentence)
        blobStorage.close()
        return [dtype,shape]

    def deserializeFromJSON( self, data, marker, index_vectors ,index_content, searches_vectors):
        dtype=None
        shape=None
        data=json.loads(data)
        for part in data:
            [text,embeddings_b64,_dtype,_shape] = part
            part_marker=part[4] if len(part)>3 else None
            
            if dtype is None: dtype = _dtype
            elif dtype != _dtype: raise Exception("Data type mismatch")
            if shape is None: shape = _shape
            elif shape != _shape: raise Exception("Shape mismatch")
            # Decode
            embeddings_bytes = base64.b64decode(embeddings_b64)
            embeddings =  np.frombuffer(embeddings_bytes, dtype=dtype).reshape(shape)
            if not marker: marker = part_marker
            if not marker: 
                self.log("Marker not specified, use 'passage'")
                marker = "passage"
            # Append
            if marker == "query":
                searches_vectors.append(embeddings)
            else:
                index_vectors.append(embeddings)
                index_content.append(text)
        return [dtype,shape]

    def deserialize( self, jin,index_vectors ,index_content,searches_vectors):
        dtype = None
        shape = None
        data = jin.data
        dataType = jin.type
        marker = jin.marker   
        if dataType == "application/hyperblob":
            [dtype,shape] = self.deserializeFromBlob(data, marker, index_vectors ,index_content, searches_vectors)
        else:
            [dtype,shape] = self.deserializeFromJSON( data, marker, index_vectors ,index_content, searches_vectors)
        return [dtype,shape]

    def run(self,job):
        def getParamValue(key,default=None):
            param = [x for x in job.param if x.key == key]
            return param[0].value[0] if len(param) > 0 else default

        # Extract parameters
        top_k = int(getParamValue("k", "4"))
        normalize = str(getParamValue("normalize", "true"))=="true"
        
        # Deserialize inputs
        self.log("Preparing index")
        index_vectors = []
        index_content = []
        searches_vectors = []
        dtype = None
        shape = None
        for jin in job.input:
            [dtype,shape] = self.deserialize(jin,index_vectors ,index_content,searches_vectors)               

        # Convert to numpy arrays and normalize
        searches_vectors = np.array(searches_vectors)
        index_vectors = np.array(index_vectors)
        if normalize and dtype == "float32":
            faiss.normalize_L2(searches_vectors)
            faiss.normalize_L2(index_vectors)

        # Create faiss index
        index  = faiss.IndexFlatL2(shape[0])
        index.add(index_vectors)

        # Search faiss index        
        self.log("Searching")
        distances, indices = index.search(searches_vectors, top_k)

        # Get content for each search query and sort by score
        self.log("Retrieving content from index")
        output_per_search = []
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

        # Serialize output and return
        return json.dumps(output)


node = OpenAgentsNode(NodeConfig.meta)
node.registerRunner(Runner(filters=SearchEvent.filters,sockets=SearchEvent.sockets,meta=SearchEvent.meta,template=SearchEvent.template))
node.run()