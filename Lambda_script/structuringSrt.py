import re

# Regex pattern to match the subtitle components
pattern = re.compile(
    r"(\d+)\n"
    r"(\d{2}:\d{2}:\d{2},\d{3} --> \d{2}:\d{2}:\d{2},\d{3})\n"
    r"(.*?)(?=\n\d+\n|$)",
    re.DOTALL
)

# Modified lambda_handler function to handle literal '\n' as real newlines
def lambda_handler(event, context):
    input_name = event["node"]["inputs"][0]["name"]

    # Error check for input name not matching "document"
    if input_name != "document":
        return {
            "error": "No input provided",
            "functionResponse": {}
        }

    srt_string = event["node"]["inputs"][0]["value"]

    # Replace literal '\n' with actual newlines
    srt_string = srt_string.replace('\\n', '\n')

    functionResponse = {}
    matches = pattern.findall(srt_string)

    if not matches:
        return {
            "error": "No matches found",
            "functionResponse": {}
        }

    # Process each match from the regex
    for match in matches:
        num = int(match[0])
        timestamp = match[1].strip()
        srt = match[2].replace("\n", " ").strip()
        functionResponse.update(
            {
                num : {
                    "Timestamp": timestamp, 
                    "Srt": srt
                }
            }
            )

    return functionResponse