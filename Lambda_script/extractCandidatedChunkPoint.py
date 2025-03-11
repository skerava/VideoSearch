def extract_keys(data):
    result = []
    for i in range(0, max(map(int, data.keys())), 40):
        group = {}
        if i != 0:
            for j in range(i-3, i+4):
                if str(j) in data:
                    group[str(j)] = data[str(j)]
        if group:
            result.append(group)
    return result

def lambda_handler(event, context):
    data = event["node"]["inputs"][0]["value"]
    functionResponse = extract_keys(data)

    return functionResponse