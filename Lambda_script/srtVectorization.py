import json
import boto3
import os
import logging
from confluent_kafka import Producer



# 设置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def get_s3_object(event):
    s3_info = event['Records'][0]['s3']
    s3 = boto3.client('s3')
    bucket_name = s3_info['bucket']['name']
    object_key = s3_info['object']['key']
    
    logger.info(f"Fetching object '{object_key}' from bucket '{bucket_name}'")
    
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    contents = json.loads(response['Body'].read().decode('utf-8'))
    
    logger.info(f"Successfully fetched object. Contents: {contents[:10]}...")  # 仅显示前10项内容以便调试
    return contents, object_key

def sqs_in_batch(contents, object_key):
    sqs = boto3.client('sqs')
    queue_url = os.environ['QUEUE_URL']
    responses = []

    if not contents:
        logger.warning("No content found in S3 object.")
        return responses

    logger.info(f"Sending contents in batches to SQS queue '{queue_url}'")
    
    for i in range(0, len(contents), 10):
        batch = contents[i:i+10]
        
        # 更新每个消息的 `Number` 字段
        for item in batch:
            for key, value in item.items():
                value["Number"] = int(key)
        
        entries = [
            {
                'Id': f"{key}_{value['VideoName']}",
                'MessageBody': json.dumps(value, ensure_ascii=False)
            }
            for item in batch
            for key, value in item.items()
        ]

        # 检查条目是否超过限制
        if len(entries) > 10:
            logger.error("SQS batch size exceeded limit of 10 messages per batch.")
            raise ValueError("SQS batch size exceeded limit of 10 messages per batch.")
        
        try:
            response = sqs.send_message_batch(QueueUrl=queue_url, Entries=entries)
            responses.append(response)
            logger.info(f"Batch sent successfully. Response: {response}")
        except Exception as e:
            logger.error(f"Error sending batch to SQS: {e}")
            raise

    return responses

def lambda_handler(event, context):
    logger.info("Lambda function invoked.")
    try:
        contents, object_key = get_s3_object(event)
        response = sqs_in_batch(contents, object_key)
        logger.info("Lambda function executed successfully.")
        return response
    except Exception as e:
        logger.error(f"Lambda function failed with error: {e}")
        raise
