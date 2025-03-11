import json
import boto3
import os
from io import StringIO

def convert_to_vtt(srt):
    """Convert JSON subtitles to VTT format."""
    vtt_content = "WEBVTT\n\n"
    for subtitle in srt:
        for _, value in subtitle.items():
            start_time, end_time = value['Timestamp'].split(' --> ')
            start_time = start_time.replace(',', '.')
            end_time = end_time.replace(',', '.')

            vtt_content += f"{start_time} --> {end_time}\n"
            vtt_content += f"{value['Srt']}\n"
            vtt_content += f"{value['TranslatedSrt']}\n\n"
    
    print(vtt_content)
    return vtt_content

def upload_vtt_to_s3(vtt_content, object_key):
    s3 = boto3.client('s3')
    bucket_name = os.environ['BUCKET_NAME']
    
    # 将内容编码为UTF-8
    vtt_content_bytes = vtt_content.encode('utf-8')
    
    response = s3.put_object(
        Bucket=bucket_name,
        Key=object_key,
        Body=vtt_content_bytes,
        ContentType='text/vtt',
        ContentEncoding='utf-8'  # 添加内容编码说明
    )
    return response

def lambda_handler(event, context):
    srt = event["opted_srts"]
    vtt_content = convert_to_vtt(srt)
    vtt_s3_key = event["vtt_s3_key"]
    response = upload_vtt_to_s3(vtt_content, vtt_s3_key)
    return response