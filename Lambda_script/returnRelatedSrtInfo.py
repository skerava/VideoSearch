import json
import ast
import os
import logging

# 配置日志记录
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger()

bucket_name = os.environ.get('BUCKET_NAME', '')

def sort_response(event):
    try:
        logger.info("Event received for sorting: %s", event)
        
        # 提取并解析响应内容
        response_text = event['Body']["content"][0].get("text", "")
        
        # 使用 ast.literal_eval 将字符串转换为实际的 Python 数据结构
        model_response = ast.literal_eval(response_text.strip())
        logger.info("Parsed model response: %s", model_response)
        
        # 按 Relevance 排序并提取前3条记录
        sorted_response = sorted(model_response, key=lambda x: x['Relevance'], reverse=True)[:3]
        logger.info("Sorted response: %s", sorted_response)
        
        return sorted_response
    except (KeyError, IndexError, ValueError, SyntaxError) as e:
        logger.error("Error sorting response: %s", e)
        return []

def assemble_retrieved_info(sorted_response):
    logger.info("Assembling information from sorted response")
    res = []
    for item in sorted_response:
        try:
            video_name = item['VideoName']
            start_time = item['Timestamp'].split(' --> ')[0][:8]
            description = item['TranslatedSrt']
            relevance = item['Relevance']

            video_key = f"video-material/{video_name}.mp4"
            checklist_key = f"TestPaper/test_{video_name}.json"
            summary_key = f"Summary/sum_{video_name}.json"
            vtt_key = f"vtt/vtt_{video_name}.vtt"
            relevance = item['Relevance']
            
            result = {
                'video_name': video_name,
                "start_time": start_time,
                "description": description,
                "summary_key": summary_key,
                "vtt_key": vtt_key,
                "video_key": video_key,
                "checklist_key": checklist_key,
                "bucket_name": bucket_name,
                "relevance": relevance
            }
            
            logger.info("Assembled item: %s", result)
            res.append(result)
        except KeyError as e:
            logger.error("Error assembling item: missing key %s in item %s", e, item)
    
    return res

def lambda_handler(event, context):
    logger.info("Lambda handler invoked with event: %s", event)
    
    # 排序响应内容
    sorted_response = sort_response(event)
    logger.info("Sorted response for assembly: %s", sorted_response)
    
    # 组装提取的信息
    response_data = {"res": assemble_retrieved_info(sorted_response)}
    logger.info("Final response data: %s", response_data)
    
    return response_data
