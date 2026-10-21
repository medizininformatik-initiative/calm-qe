import json
import os
from pathlib import Path
import urllib3
import requests
from requests.auth import HTTPBasicAuth
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

"""
After extracting the required data from the main FHIR server, this script can be used to upload the generated resource files to another project server.
Requirements:
Below variables needs to be filled:
FHIR_SERVER: URL of the target FHIR server
USERNAME: Username for authentication
PASSWORD: Password for authentication
BASE_FOLDER = Folder path containing the extracted FHIR resource files
"""
FHIR_SERVER = "YOUR_TARGET_SERVER_NAME/fhir"
USERNAME = "YOUR_FHIR_USER_NAME"
PASSWORD = "YOUR_FHIR_PASSWORD"
BASE_FOLDER = Path(__file__).resolve().parent.parent/"data_extraction"/"fhir_results"  #can be changed according to where you extracted data
headers = {
    "Content-Type": "application/fhir+json",
    "Accept": "application/fhir+json",
}

BATCH_SIZE = 500  # Send files to server in smaller chunks to prevent some issues, i.e. memory etc.

def load_bundles_from_file(file_path):
    entries = []

    with open(file_path, encoding="utf-8") as f:
        for line in f:
            if not line.strip():
                continue

            item = json.loads(line)
            resource = item["resource"] if "resource" in item else item

            entries.append({
                "resource": resource,
                "request": {
                    "method": "PUT",
                    "url": f"{resource['resourceType']}/{resource['id']}"
                }
            })

            if len(entries) >= BATCH_SIZE:
                yield {
                    "resourceType": "Bundle",
                    "type": "batch",
                    "entry": entries}
                entries = []

    # Send remaining resources
    if entries:
        yield {
            "resourceType": "Bundle",
            "type": "batch",
            "entry": entries}


def send_bundle(file_path):
    for b, bundle in enumerate(load_bundles_from_file(file_path), start=1):
        try:
            response = requests.post(FHIR_SERVER, headers=headers, json=bundle, auth=HTTPBasicAuth(USERNAME, PASSWORD),
                                     verify=False, timeout=120)
            print(f"{file_path}, Batch {b}, HTTP {response.status_code}")

            if not response.ok:
                print(response.text)
                continue

            result = response.json()
            results = result.get("entry", [])

            if result.get("type") != "batch-response" or len(results) != len(bundle["entry"]):
                print(f"Unexpected response: {response.text}")

            for entry, outcome in zip(bundle["entry"], results):
                status = outcome.get("response", {}).get("status", "")
                if not status.startswith("2"):  #print which resource failed:
                    print(f"FAILED: {entry['request']['url']}, {outcome}")

        except requests.exceptions.RequestException as exc:
            print(f"Batch {b} failed: {exc}")


def main():
    for root, _, files in os.walk(BASE_FOLDER):
        print(f"Folder: {root}\n")
        count = 0
        for filename in files:
            if "metadata.json" in filename:
                continue
            if filename.lower().endswith((".json", ".jsonl")):
                count += 1
                print(f"Processing {count} file: {filename}")
                file_path = os.path.join(root, filename)
                send_bundle(file_path)


if __name__ == "__main__":
    main()
