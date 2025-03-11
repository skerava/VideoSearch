import boto3
import json

def invoke_prompt_flow(document_key, document_size):
    
    client_runtime = boto3.client("bedrock-agent-runtime")
    response = client_runtime.invoke_flow(
        flowIdentifier="JPN09CT3CG",
        flowAliasIdentifier="IVCPQWNXLY",
        inputs=[
            {
                "content": {"document": f"Key: {document_key}, Size: {document_size}"},
                "nodeName": "FlowInputNode",
                "nodeOutputName": "document",
            }
        ],
    )
    result = {}

    for event in response.get("responseStream"):
        result.update(event)

    if result.get("flowCompletionEvent", {}).get("completionReason") == "SUCCESS":
        print(
            "Prompt flow invocation was successful!"
        )
        result_document = result.get("flowOutputEvent", {}).get("content", {}).get("document")
        return result_document
    else:
        print(
            "The prompt flow invocation completed because of the following reason:",
            result.get("flowCompletionEvent", {}).get("completionReason"),
        )
        return None

def lambda_handler(event, context):
    
    # Assuming you want to extract data from the first record in 'Records'
    if "Records" in event and len(event["Records"]) > 0:
        record = event["Records"][0]
        bucket_name = record["s3"]["bucket"]["name"]
        document_key = record["s3"]["object"]["key"]
        document_size = record["s3"]["object"]["size"]

        client_s3 = boto3.client("s3")
        s3_response_object = client_s3.get_object(Bucket=bucket_name, Key=document_key)
        document = s3_response_object["Body"].read().decode("utf-8")
        # Invoke the flow with extracted values
        result = invoke_prompt_flow(document, document_size)
        # Test lambda will not send message to SQS
        return result
    else:
        raise ValueError("No Records found in the event.")