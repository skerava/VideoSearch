import json
import boto3
import logging
import os
import time
from botocore.exceptions import ClientError, BotoCoreError

# 从环境变量中获取 prompt
prompt = '''<persona> You are an expert subtitle editor. You are highly skilled at proofreading and correcting subtitles, using your knowledge and the given context to rectify potential errors. Check the output result and confirm that the output must be an array. The output cannot include any other content. </persona> <srt> {srt} </srt> <srt_instructions> 1. The given <srt> is a JSON format subtitle file containing subtitle numbers, timestamps, and content. 2. Due to limitations in the subtitle generation tools, there are many word errors in the subtitle content. 3. The time segments for splitting subtitles are too short, often dividing a single sentence into multiple fragments. 4. The <srt> example is shown below: { \"129\": { \"Timestamp\": \"00:06:22,850 --> 00:06:24,720\", \"Srt\": \"So let's see if these two models can do it.\" }, \"130\": { \"Timestamp\": \"00:06:24,929 --> 00:06:25,640\", \"Srt\": \"Um\" }, \"131\": { \"Timestamp\": \"00:06:25,799 --> 00:06:27,790\", \"Srt\": \"I was thinking the prompt of\" }, \"132\": { \"Timestamp\": \"00:06:28,640 --> 00:06:31,500\", \"Srt\": \"create a dialogue between a dragon and a knight.\" } } </srt_instructions> <task> Your tasks: 1. Based on the context, merge and re-segment the subtitles to ensure each segment srt contains **one complete sentence**. If a segment's subtitle is an incomplete sentence, merge it with the adjacent subtitle. 2. Correct any errors within these sentences using your knowledge and understanding of the given context. 3. Translate these sentences in Chinese. 4. If segments are merged, merge the corresponding timings accordingly, and renumber them in sequence from the first subtitle numbers in <srt>. 5. Check the output result and confirm that the output must be an array. The output cannot include any other content. </task> <example> Here is an example of how the output should be formatted: [ {\"xxxxx\": { \"Timestamp\": \"00:06:25,799 --> 00:06:31,500\", \"Srt\": \"I was thinking the prompt of creating a dialogue between a dragon and a knight.\", \"TranslatedSrt\": \"我在考虑创作一个龙与骑士之间对话的提示词。\" }} ] </example>
'''

# 重试参数
MAX_RETRIES = 5
RETRY_DELAY = 5  # 每次重试之间的等待时间（秒）

def invoke_bedrock(prompt, chunk):
    logging.info(f"Preparing to invoke Bedrock model with chunk: {chunk}")
    
    prompt = prompt.replace('{srt}', str(chunk))
    client = boto3.client('bedrock-runtime')
    model_id = "anthropic.claude-3-5-sonnet-20240620-v1:0"
    
    request = {
        "anthropic_version": "bedrock-2023-05-31",
        "max_tokens": 4000,
        "temperature": 0.4,
        "top_p": 0.3,
        "top_k": 50,
        "messages": [
            {
                "role": "user",
                "content": [
                    {
                        "type": "text",
                        "text": prompt
                    }
                ]
            },
            {
                "role": "assistant",
                "content": [
                    {
                        "type": "text",
                        "text": "[{"
                    }
                ]
            }
        ]
    }
    request = json.dumps(request)

    # 尝试多次重试
    retries = 0
    while retries < MAX_RETRIES:
        try:
            logging.info("Sending request to Bedrock model")
            # Invoke the model with the request
            response = client.invoke_model(modelId=model_id, body=request)
            logging.info("Model invoked successfully")
            break  # 如果调用成功，跳出重试循环
        except ClientError as e:
            error_code = e.response['Error']['Code']
            if error_code == 'ThrottlingException':
                retries += 1
                logging.warning(f"ThrottlingException: Retrying {retries}/{MAX_RETRIES}...")
                if retries < MAX_RETRIES:
                    time.sleep(RETRY_DELAY)  # 等待一段时间后重试
                    continue
                else:
                    logging.error(f"ERROR: Max retries reached for ThrottlingException.")
                    return None
            else:
                logging.error(f"ERROR: Can't invoke '{model_id}'. Reason: {e}")
                return None
        except (BotoCoreError, Exception) as e:
            logging.error(f"ERROR: Unexpected error invoking '{model_id}'. Reason: {e}")
            return None
    
    # Decode the response body after a successful call
    model_response = json.loads(response["body"].read().decode('utf-8'))
    response_text = model_response["content"][0]["text"]
    logging.info(f"Response from model: {response_text}")
    return response_text

def lambda_handler(event, context):
    chunk = event['chunk']
    
    logging.info(f"Processing chunk: {chunk}")
    opt_chunk = invoke_bedrock(prompt, chunk)
    
    if opt_chunk is None:
        return {
            'opt_chunk': None
        }
    else:
        return {
            'opt_chunk': "[{" + str(opt_chunk)
        }
