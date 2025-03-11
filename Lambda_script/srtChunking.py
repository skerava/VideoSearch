import json

def extract_elected_fragment_key(event):
    # 获取输入列表
    inputs = event.get("node", {}).get("inputs", [])
    for input_item in inputs:
        # 找到名为 'electedFragment' 的输入项
        if input_item.get("name") == 'electedFragment':
            elected_fragment_value = input_item.get('value', [])
            try:
                # 尝试将 JSON 字符串解析为 Python 对象
                elected_fragment_value = json.loads(elected_fragment_value)
            except (json.JSONDecodeError, TypeError):
                # 如果 JSON 解析失败，返回空列表
                return []  
            # 收集所有键
            keys = []
            for item in elected_fragment_value:
                keys.extend(item.keys())
            return keys
    return []

def extract_structured_fragment(event, chunk_key):
    # 获取输入列表
    inputs = event.get("node", {}).get("inputs", [])
    for input_item in inputs:
        # 找到名为 'structuredFragment' 的输入项，修正了 inputItem 的拼写错误
        if input_item.get("name") == 'structuredFragment':  
            structured_fragment = input_item.get('value', {})
            result = []
            current_segment = {}
            
            # 将 chunk_key 排序，并确保它们是字符串以便与 structured_fragment 的 key 比较
            chunk_key = [str(k) for k in sorted(chunk_key)]  
            
            # 对 structured_fragment 的 key 进行排序，并逐个处理
            for key in sorted(structured_fragment.keys(), key=int):
                str_key = str(key)  # 将 key 转换为字符串以进行比较
                if str_key in chunk_key:
                    if current_segment:
                        result.append(current_segment)
                        current_segment = {}
                current_segment[str_key] = structured_fragment[key]
            
            # 处理最后一个片段
            if current_segment:
                result.append(current_segment)
            
            return result

def lambda_handler(event, context):

    # 提取 chunk_key
    chunk_key = extract_elected_fragment_key(event)
    # 处理 structured_fragment，并返回结果
    function_response = extract_structured_fragment(event, chunk_key)

    return function_response