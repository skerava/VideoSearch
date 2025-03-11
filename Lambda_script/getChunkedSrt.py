import json
import logging
import re

def get_chunked_srt(event):
    logging.info("Fetching SQS message from event")
    
    # 使用 .get() 来避免 KeyError
    body = event.get("message")
    if not body:
        logging.warning("No 'message' key found in event.")
        return None
    
    logging.info(f"Message body: {body}")
    
    try:
        message_dict = json.loads(body)
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON message body.")
        return None
    
    if "responsePayload" in message_dict:
        response_payload = message_dict["responsePayload"]
        logging.info(f"responsePayload found: {response_payload}")
        return response_payload
    else:
        logging.warning("responsePayload not found in message")
        return None

def get_video_name(event):
    body = event.get("message")
    if not body:
        logging.warning("No 'message' key found in event.")
        return None
    try:
        message_dict = json.loads(body)
    except json.JSONDecodeError:
        logging.error("Failed to decode JSON message body.")
        return None

    srt_key = message_dict["requestPayload"]["Records"][0]["s3"]["object"]["key"]

    match = re.match(
        r"output/(.+?)(\d{4}-\d{2}-\d{2}-\d{2}-\d{2}-\d{2}\.srt)", srt_key
    )

    if match:
        video_name = match.group(1)
        return video_name  # 返回匹配的video_name部分
    else:
        return None


def lambda_handler(event, context):
    print(event)
    logging.info("Lambda handler invoked")
    
    video_name = get_video_name(event)
    chunked_srt = get_chunked_srt(event)
    
    if chunked_srt:
        tasks = [{'chunk': chunk} for chunk in chunked_srt]
        return {
            "video_name": video_name,
            "tasks": tasks
            }
    else:
        logging.warning("No valid chunked_srt found.")
        return {}
