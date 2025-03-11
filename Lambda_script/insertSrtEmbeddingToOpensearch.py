from opensearchpy import OpenSearch, RequestsHttpConnection, AWSV4SignerAuth, exceptions
import os
import json
import boto3


def insertToOpensearch(event):

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
            index=index_name,
            body=event
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
    response = insertToOpensearch(event)
    return response
