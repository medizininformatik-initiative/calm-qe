import json
import os
from collections import defaultdict
from datetime import datetime


def gather_metadata(source, count):

    if os.path.exists('fhir_results/metadata.json'):
        with open('fhir_results/metadata.json', 'r') as metadata_file:
            metadata = json.load(metadata_file)
    else:
        metadata = {
            "execution_date": datetime.now().strftime("%Y-%m-%d"),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "total_diagnosed_patients_with_asthma_or_copd_count": 0,
            "primary_diagnosed_patients_with_asthma_or_copd": 0,
            "patient_counts_with_age_interval": 0,
            "intensive_care_unit_patient_count": 0,
            "patient_count_with_secondary_conditions": 0,
            "patient_count_with_observations": 0,
            "patient_count_with_medicationRequests": 0,
            "patient_count_with_medicationAdministrations": 0,
            "patient_count_with_medicationStatements": 0,
            "main_diagnosis_count": 0,  # Not same as asthma_and_copd_patients_with_chief_complaint. (might higher)
            # When same patient main diagnosed at different times at different encounter (even for different code) etc.
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
    else:
        print(f"Source {source}, not defined with Metadata.json file.")

    with open('fhir_results/metadata.json', 'w') as metadata_file:
        json.dump(metadata, metadata_file, indent=4)

    print("Metadata has been saved")
