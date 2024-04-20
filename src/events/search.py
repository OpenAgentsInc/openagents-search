
filters=[
    {
        "filterByRunOn":"openagents\\/search"
    }
]

meta = {
    "kind": 5003,
    "name": "Similarity Search",
    "about": "",
    "tos": "",
    "privacy": "",
    "author": "",
    "web": "",
    "picture": "",
    "tags": ["tool"]
}

sockets={
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
                    "description": "The type of the query embedding. 'query' for search queries and 'index' for documents. Empty to auto-detect",
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
}


template = """{
    "kind": {{meta.kind}},
    "created_at": {{sys.timestamp_seconds}},
    "tags": [
        ["param","run-on", "openagents/search" ],                             
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

