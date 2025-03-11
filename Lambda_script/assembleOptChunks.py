import json
import logging
import datetime


def merge_chunks(event):
    merged_list = []
    opted_chunks = event.get("opted_chunks", [])
    for item in opted_chunks:
        opt_chunk_str = item.get("opt_chunk")  # 避免KeyError
        if opt_chunk_str: 
            try:
                opt_chunk = json.loads(opt_chunk_str)
                merged_list.extend(opt_chunk)
            except json.JSONDecodeError:
                logging.error(f"Failed to decode JSON for chunk: {opt_chunk_str}")
    
    return merged_list


def sort_srt(merged_list, video_name):
    sorted_data = []
    for item in merged_list:
        print(item)
        for key, value in item.items():
            value.update({"VideoName": video_name})
            sorted_data.append((int(key), value))

    sorted_data.sort(key=lambda x: x[0])
    
    renumbered_data = []
    for i, (_, value) in enumerate(sorted_data, start=1):
        renumbered_data.append({str(i): value})
    return renumbered_data


def lambda_handler(event, context):
    merged_list = merge_chunks(event)
    video_name = event.get("video_name")
    response = sort_srt(merged_list, video_name)

    timestamp = datetime.datetime.now().strftime("%Y-%m-%d-%H-%M-%S")
    opted_srts_key = f"optedSRT/opted_{video_name}_{timestamp}.json"
    summary_s3_uri = f"s3://bedrock-videosearch-oregon/Summary/sum_{video_name}.json"
    test_paper_s3_uri = f"s3://bedrock-videosearch-oregon/TestPaper/test_{video_name}.json"
    vtt_s3_key = f"vtt/vtt_{video_name}.vtt"

    return {
        "statusCode": 200,
        "test_paper_s3_uri": test_paper_s3_uri,
        "summary_s3_uri": summary_s3_uri,
        "vtt_s3_key": vtt_s3_key,
        "opted_srts_key": opted_srts_key,
        "opted_srts": response
    }