import json
import boto3
import logging
from botocore.exceptions import ClientError

# 配置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

def create_presigned_url(bucket: str, key: str, expiration=3600) -> str:
    """生成预签名URL"""
    try:
        s3_client = boto3.client('s3')
        url = s3_client.generate_presigned_url(
            'get_object',
            Params={
                'Bucket': bucket,
                'Key': key
            },
            ExpiresIn=expiration
        )
        logger.info("Generated presigned URL for bucket: %s, key: %s", bucket, key)
        return url
    except Exception as e:
        logger.error("Error generating presigned URL: %s", e)
        return None

def get_json_data(bucket: str, key: str) -> dict:
    """获取json数据"""
    try:
        s3_client = boto3.client('s3')
        response = s3_client.get_object(Bucket=bucket, Key=key)
        content = response['Body'].read().decode('utf-8')
        json_data = json.loads(content)
        logger.info("Retrieved JSON data from bucket: %s, key: %s", bucket, key)
        
        # 从Bedrock响应中提取问卷内容
        if 'content' in json_data:
            for content_item in json_data['content']:
                if content_item['type'] == 'text':
                    parsed_data = json.loads(content_item['text'])
                    logger.info("Extracted questionnaire content from JSON data.")
                    return parsed_data
        
        return {}
    except Exception as e:
        logger.error("Error getting JSON data from bucket: %s, key: %s, error: %s", bucket, key, e)
        return {}

def execute_output(output):
    for item in output['res']:
        # 生成视频预签名URL
        item['video_url'] = create_presigned_url(
            item['bucket_name'], 
            item['video_key']
        )
        
        # 生成字幕文件预签名URL
        item['vtt_url'] = create_presigned_url(
            item['bucket_name'], 
            item['vtt_key']
        )
        
        # 获取视频总结数据
        summary_data = get_json_data(
            item['bucket_name'], 
            item['summary_key']
        )
        item['summary'] = summary_data.get("summary", "")
        item['title'] = summary_data.get("video_title", "")
        logger.info("Summary data for video %s: %s", item['title'], item['summary'])
        
        # 获取问卷数据
        item['checklist'] = get_json_data(
            item['bucket_name'], 
            item['checklist_key']
        )
        logger.info("Checklist data for video %s: %s", item['title'], item['checklist'])
        
        # 删除原始的key和bucket信息
        for key in ['video_key', 'checklist_key', 'bucket_name', 'vtt_key', 'summary_key']:
            item.pop(key, None)
        logger.info("Cleaned up item data for video %s: %s", item['title'], item)
    
    return output

def lambda_handler(event, context):
    try:
        logger.info("Lambda handler invoked with event: %s", event)
        
        # 启动 Step Function
        client = boto3.client('stepfunctions')
        try:
            response = client.start_sync_execution(
                stateMachineArn='arn:aws:states:us-west-2:2:stateMachine:MyStateMachine-tw7xxrw48',
                input=json.dumps(event, ensure_ascii=False)
            )
        except ClientError as e:
            logger.error("Error starting Step Function execution: %s", e)
            raise e
        
        output = json.loads(response["output"])
        logger.info("Received Step Function output: %s", output)
        
        # 处理每个结果项
        output = execute_output(output)
        
        return {
            "statusCode": 200,
            "body": output,
            "headers": {
                "Content-Type": "application/json"
            }
        }
    except Exception as e:
        logger.error("Error in lambda_handler: %s", e)
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)}),
            "headers": {
                "Content-Type": "application/json"
            }
        }
