import json
import boto3


client = boto3.client('stepfunctions')

def lambda_handler(event, context):
    # 从 SQS 消息中获取输入
    for record in event['Records']:
        message_body = record['body']
        
        # 调用 Step Functions 的 StartExecution API 启动一个状态机
        response = client.start_execution(
            stateMachineArn='arn:aws:states:us-west-2:2277:stateMachine:MyStateMachine-sbp378qrj',
            input=json.dumps({"message": message_body})
        )
        
        print(f"Step Functions started with execution ARN: {response['executionArn']}")
