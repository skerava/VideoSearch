from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, exceptions
import os
import json
import boto3
import logging
from botocore.exceptions import ClientError

logger = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO)

def generate_text_embeddings(record):
    srt = json.loads(record["body"])
    srt_value = srt["Srt"]

    body = json.dumps(
        {
            "texts": [
                srt_value
            ],
            "input_type": "search_document",  
            "embedding_types": ["float"]
        }
    )
    
    accept = '*/*'
    content_type = 'application/json'

    logger.info("Generating text embeddings with the Cohere Embed model")


    bedrock = boto3.client(service_name='bedrock-runtime')

    try:
        response = bedrock.invoke_model(
            body=body,
            modelId="cohere.embed-english-v3",
            accept=accept,
            contentType=content_type
        )
    except ClientError as e:
        logger.error(f"Error invoking the model: {e}")
        return {"error": str(e)}

    response_body = json.loads(response.get('body').read())

    srt["embeddings"] = response_body["embeddings"]["float"][0]

    return srt


def insertToOpensearch(srt):

    try:
        host = os.environ['OPENSEARCH_HOST']
        index_name = 'srt_embedding' 
        region = os.environ['AWS_REGION']

        service = 'aoss'
        credentials = boto3.Session().get_credentials()
        auth = AWSV4SignerAuth(credentials, region, service)

        # 配置 OpenSearch 客户端
        client = OpenSearch(
            hosts=[{'host': host, 'port': 443}],
            http_auth=auth,
            use_ssl=True,
            verify_certs=True,
            connection_class=RequestsHttpConnection
        )

        # 执行写入操作
        response = client.index(
            index = index_name,
            body = srt
        )
        print("Document indexed:", response)
        
        return {
            'statusCode': 200,
            'body': json.dumps("Document inserted successfully!")
        }

    # 捕获 OpenSearch 异常
    except exceptions.ConnectionError as e:
        print(f"Connection error: {str(e)}")
        return {
            'statusCode': 503,
            'body': json.dumps("Service unavailable. Please check the OpenSearch connection.")
        }
    except exceptions.RequestError as e:
        print(f"Request error: {str(e)}")
        return {
            'statusCode': 400,
            'body': json.dumps("Bad request. Please check the request payload and parameters.")
        }
    except exceptions.AuthenticationException as e:
        print(f"Authentication error: {str(e)}")
        return {
            'statusCode': 401,
            'body': json.dumps("Unauthorized. Please check your AWS credentials and permissions.")
        }
    except exceptions.TransportError as e:
        print(f"Transport error: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps("Transport error occurred.")
        }
    except Exception as e:
        print(f"An unexpected error occurred: {str(e)}")
        return {
            'statusCode': 500,
            'body': json.dumps("An unexpected error occurred. Please check the logs for more details.")
        }

def lambda_handler(event, context):
    responses = []
    for record in event["Records"]:
        srt  = generate_text_embeddings(record)
        response = insertToOpensearch(srt)
        responses.append(response)
    return responses