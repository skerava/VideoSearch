import boto3
import time
import requests

#lambda entry point
def lambda_handler(event, context):
    bucket_name = event['Records'][0]['s3']['bucket']['name']
    source_s3_key = event['Records'][0]['s3']['object']['key']
    delimiter = source_s3_key.find('/')
    file_name = source_s3_key[delimiter+1:]
    job_uri = 's3://'+ bucket_name + '/' + source_s3_key
    
    timestamp = time.time()
    local_time = time.localtime(timestamp)
    formatted_time = time.strftime("%Y-%m-%d-%H-%M-%S", local_time)
    job_name = "conv-"+file_name[0:-4]+'-'+formatted_time
    
    transcribe = boto3.client('transcribe')
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        MediaFormat='mp4',
        Media={
            'MediaFileUri': job_uri
        },
        OutputBucketName=bucket_name,
        OutputKey='output/'+file_name[0:-4]+formatted_time,
        Settings={
            'ShowSpeakerLabels': True,
            'MaxSpeakerLabels': 10,
            'ChannelIdentification': False
        },
        IdentifyMultipleLanguages=True,
        Subtitles={
            'Formats': [
                'srt',
            ],
            'OutputStartIndex': 1
        },
        Tags=[
            {
                'Key': 'transcription job',
                'Value': job_name
            },
        ]
    )
    return {
        'statusCode': 200,
        'body': 'Transcription job started'
    }