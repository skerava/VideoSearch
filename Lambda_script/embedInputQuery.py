from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth
import os
import boto3 
import json
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

srt_index_name = os.environ["QUERY_INDEX_NAME"]
host = os.environ["OPENSEARCH_HOST"]
region = os.environ["AWS_REGION"]

def generate_text_embeddings(event):
    try:
        content = json.loads(event["Body"]["content"][0]["text"])
        query = content["key_words"]
        
        body = json.dumps({
            "texts": [query],
            "input_type": "search_query",  
            "embedding_types": ["float"]
        })
        
        bedrock = boto3.client(service_name='bedrock-runtime')
        response = bedrock.invoke_model(
            body=body,
            modelId="cohere.embed-english-v3",
            accept='*/*',
            contentType='application/json'
        )
        
        response_body = json.loads(response.get('body').read())
        query_embedding = response_body["embeddings"]["float"][0]

        return { 
            "question": query,
            "query_embedding": query_embedding
        }
        
    except ClientError as e:
        logger.error(f"Error invoking the model: {e}")
        return {"error": str(e)}


def query_opensearch(query_embedding, host=host, index_name=srt_index_name):
    credentials = boto3.Session().get_credentials()
    auth = AWSV4SignerAuth(credentials, region, "aoss")
    
    # Initialize OpenSearch client
    search = OpenSearch(
        hosts=[{'host': host, 'port': 443}],
        http_auth=auth,
        use_ssl=True,
        verify_certs=True,
        connection_class=RequestsHttpConnection
    )

    # Main query to retrieve relevant docs
    main_query = {
        "_source": ["Number", "Srt", "Timestamp", "TranslatedSrt", "VideoName"],
        "size": 10,
        "query": {
            "knn": {
                "embeddings": {
                    "vector": query_embedding,
                    "k": 100
                }
            }
        }
    }

    response = search.search(body=main_query, index=index_name)
    retrieves = response["hits"]["hits"]

    related_srts = []

    for retrieve in retrieves:
        retrieve_srt = retrieve["_source"]
        Number = retrieve_srt["Number"]
        VideoName = retrieve_srt["VideoName"]

        # Adjust "Numbers" to a range of IDs
        Numbers = [Number + i for i in range(6)]
        
        # Query to retrieve related SRTs
        related_query = {
            "_source": ["Srt"],
            "query": {
                "bool": {
                    "filter": [
                        {"terms": {"Number": Numbers}},
                        {"term": {"VideoName": VideoName}}
                    ]
                }
            }
        }

        related_response = search.search(body=related_query, index=index_name)
        doc = [srt["_source"].get("Srt", "No Srt found") for srt in related_response.get("hits", {}).get("hits", [])]

        retrieve_srt["doc"] = doc  # Attach related SRTs
        related_srts.append(retrieve_srt)

    return related_srts


def lambda_handler(event, context):
    query_info = generate_text_embeddings(event)
    related_srts = query_opensearch(query_info["query_embedding"])
    
    rerank_info_str = {
        "rerank_info": json.dumps({
            "Question": query_info["question"],
            "Related_srts": related_srts
        })
    }

    return rerank_info_str
