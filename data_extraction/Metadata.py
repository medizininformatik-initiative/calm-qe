import json
import os
from collections import defaultdict
from datetime import datetime

os.makedirs('fhir_results', exist_ok=True)

def gather_metadata(source, count):
    metadata_file_path = 'fhir_results/metadata.json'

    if os.path.exists(metadata_file_path):
        with open(metadata_file_path, 'r') as metadata_file:
            metadata = json.load(metadata_file)
    else:
        metadata = {
            "execution_date": datetime.now().strftime("%Y-%m-%d"),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "total_asthma_or_copd_diagnosed_patients": 0,
            "patient_count_by_age_interval": defaultdict(int),
            "intensive_care_unit_patient_count": 0,
            "asthma_and_copd_patient_count": 0,
            "patient_count_with_observations": 0,
            "patient_count_with_medicationRequests": 0,
            "patient_count_with_medicationAdministrations": 0,
            "patient_count_with_medicationStatements": 0,
            "conditions_counts": defaultdict(int),
            "observations_counts": defaultdict(int),
            "medicationAdministrations_counts": defaultdict(int),
            "medicationRequests_counts": defaultdict(int),
            "medicationStatements_counts": defaultdict(int)
        }

    metadata["execution_date"] = datetime.now().strftime("%Y-%m-%d")
    metadata["execution_time"] = datetime.now().strftime("%H:%M:%S")

    if source in metadata:
        if "patient_count_by_age_interval" in source:
            for key, value in count.items():
                metadata[source][key] = metadata[source].get(key, 0) + value
                print("key-value", key, value)
        else:
            metadata[source] = count
    elif '_counts' in source.lower():
        metadata[source] = defaultdict(int, {count: 0})
        print(f"Source '{source}' was not defined; but it has been created in Metadata.json file.")
    else:
        metadata[source] = count
        print(f"Source '{source}' was not defined; but it has been created in Metadata.json file.")

    with open(metadata_file_path, 'w') as metadata_file:
        json.dump(metadata, metadata_file, indent=4)

    print("Metadata has been saved.")
