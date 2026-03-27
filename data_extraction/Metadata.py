import json
import os
from collections import defaultdict
from datetime import datetime


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
            "main_diagnosis_asthma_or_copd_filter": 0,
            "main_diagnosis_encounter_count": 0,  # Not same as main_diagnosis_asthma_or_copd_filter (might be higher). Sums the total number of main diagnoses a patient has received across all encounters, regardless of the specific diagnosis codes or times.
            "patient_count_by_age_interval": 0,  # todo: modify as dict
            "intensive_care_unit_patient_count": 0,
            "secondary_conditions_patient_count": 0,
            "observations_patient_count": 0,
            "medicationRequests_patient_count": 0,
            "medicationAdministration_patient_count": 0,
            "medicationStatement_patient_count": 0,
            "main_diagnosis_counts": defaultdict(int),
            "secondary_conditions_counts": defaultdict(int),
            "observations_counts": defaultdict(int),
            "medicationAdministrations_counts": defaultdict(int),
            "medicationRequests_counts": defaultdict(int),
            "medicationStatements_counts": defaultdict(int)
        }

    metadata["execution_date"] = datetime.now().strftime("%Y-%m-%d")
    metadata["execution_time"] = datetime.now().strftime("%H:%M:%S")

    if source in metadata:
        metadata[source] = count
    elif '_counts' in source.lower():
        metadata[source] = defaultdict(int, {count: 0})
        print(f"Source '{source}' was not defined; but it has been created in Metadata.json file.")
    else:
        metadata[source] = count
        print(f"Source '{source}' was not defined; but it has been created in Metadata.json file.")

    with open(metadata_file_path, 'w') as metadata_file:
        json.dump(metadata, metadata_file, indent=4)

    print("Metadata has been saved")
