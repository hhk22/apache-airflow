import os
import requests
import json
import argparse

parser = argparse.ArgumentParser()
parser.add_argument("--output", type=str, required=True)
args = parser.parse_args()

url = os.environ.get("TARGET_URL", "https://jsonplaceholder.typicode.com/comments")
response: requests.Response = requests.get(url)

data = response.json()
with open(args.output, "w", encoding="utf-8") as f:
    json.dump(data, f, ensure_ascii=False, indent=4)


