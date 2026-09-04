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
BASE_FOLDER: Folder containing the extracted FHIR resource files
"""
FHIR_SERVER = "YOUR_TARGET_SERVER_NAME/fhir"
USERNAME = "YOUR_FHIR_USER_NAME"
PASSWORD = "YOUR_FHIR_PASSWORD"
BASE_FOLDER = Path("fhir_results") #can be changed according to where you extracted data
headers = {
    "Content-Type": "application/fhir+json",
    "Accept": "application/fhir+json",
}


def load_bundle_from_file(file_path):
    with open(file_path, encoding="utf-8") as f:
        entries = [json.loads(line) for line in f if line.strip()]

    for entry in entries:
        resource = entry["resource"]
        entry["request"] = {
            "method": "PUT",
            "url": f"{resource['resourceType']}/{resource['id']}"
        }

    return {
        "resourceType": "Bundle",
        "type": "transaction",
        "entry": entries
    }


def send_bundle(file_path):
    bundle = load_bundle_from_file(file_path)
    try:
        response = requests.post(
            FHIR_SERVER,
            headers=headers,
            json=bundle,
            auth=HTTPBasicAuth(USERNAME, PASSWORD),
            verify=False,
            timeout=120
        )
        print(f"HTTP {response.status_code}")
        if not response.ok:
            print(response.text)

    except Exception as exc:
        print(f"Error sending {file_path}: {exc}")


def main():
    for root, _, files in os.walk(BASE_FOLDER):
        print(f"Processing folder: {root}\n")
        count = 0
        for filename in files:
            if "metadata.json" in filename:
                continue
            if filename.lower().endswith(".json"):
                count += 1
                print(f"Processing file: {count}")
                file_path = os.path.join(root, filename)
                send_bundle(file_path)


if __name__ == "__main__":
    main()
