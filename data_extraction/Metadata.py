import json
import os
import logging
import tempfile
from collections import defaultdict
from datetime import datetime
from Constants import CONFIG_EXECUTION

os.makedirs('fhir_results', exist_ok=True)

RESET_FIELDS_BY_EXTRACTION = {

    "conditions": {
        "asthma_and_copd_patient_count": 0,
        "missing_asthma_and_copd_patients": 0,
        "conditions_counts": {}
    },
    "encounters": {
        "patient_count_by_age_interval": 0,
        "patient_count_in_intensive_care": 0,
    },
    "observations": {
        "patient_count_with_observations": 0,
        "observations_counts": {},
        "observations_value_histograms": {}
    },
    "procedures": {
        "patient_count_with_procedures": 0,
        "procedures_counts": {}
    },
    "medication": {
        "patient_count_with_medicationAdministrations": 0,
        "patient_count_with_medicationRequests": 0,
        "patient_count_with_medicationStatements": 0,
        "patient_count_with_medicationList": 0,
        "medicationAdministrations_counts": {},
        "medicationRequests_counts": {},
        "medicationStatements_counts": {},
        "medicationList_counts": {}
    }
}

def reset_values_metadata():
    metadata_file_path = 'fhir_results/metadata.json'

    if os.path.exists(metadata_file_path):
        with open(metadata_file_path, 'r') as metadata_file:
            metadata = json.load(metadata_file)

        if not os.path.exists(CONFIG_EXECUTION):
            raise FileNotFoundError(f"Execution config not found: {CONFIG_EXECUTION}")

        with open(CONFIG_EXECUTION, "r") as json_file:
            config = json.load(json_file)

        extraction = config.get("extract", {})
        enabled_resources = [key for key, value in extraction.items() if value is True]

        filters = config.get("filters", {})
        enabled_filters = [key for key, value in filters.items() if value is True]

        reset_fields = {}
        unknown_resources = []

        if enabled_resources:
            for resource in enabled_resources:
                fields = RESET_FIELDS_BY_EXTRACTION.get(resource)
                if fields is None:
                    unknown_resources.append(resource)
                    continue
                reset_fields.update(fields)

        if enabled_filters:
            for enabled_filter in enabled_filters:
                matched_keys = [meta_key for meta_key in list(metadata.keys()) if enabled_filter in meta_key]
                reset_fields.update({
                    key: defaultdict(int) if isinstance(value, dict) else 0
                    for key, value in metadata.items()
                    if key in matched_keys
                })

        if not reset_fields:
            if unknown_resources:
                logging.warning(
                    "No metadata reset mapping found for enabled extraction(s): %s",
                    ", ".join(unknown_resources),
                )
            return

        metadata.update(reset_fields)
        metadata["execution_date"] = datetime.now().strftime("%Y-%m-%d")
        metadata["execution_time"] = datetime.now().strftime("%H:%M:%S")

        metadata_dir = os.path.dirname(metadata_file_path) or "."
        os.makedirs(metadata_dir, exist_ok=True)

        temp_path = None
        try:
            with tempfile.NamedTemporaryFile(
                    mode="w",
                    encoding="utf-8",
                    dir=metadata_dir,
                    delete=False,
            ) as temp_file:
                json.dump(metadata, temp_file, indent=4)
                temp_file.write("\n")
                temp_path = temp_file.name

            os.replace(temp_path, metadata_file_path)
        finally:
            if temp_path and os.path.exists(temp_path):
                os.remove(temp_path)

        logging.info("Reset metadata fields for enabled extraction(s): %s",", ".join(enabled_resources),)
        if unknown_resources:
            logging.warning("No metadata reset mapping found for enabled extraction(s): %s",", ".join(unknown_resources),)


def gather_metadata(source, count):
    metadata_file_path = 'fhir_results/metadata.json'

    if os.path.exists(metadata_file_path):
        with open(metadata_file_path, 'r') as metadata_file:
            metadata = json.load(metadata_file)
    else:
        metadata = {
            "execution_date": datetime.now().strftime("%Y-%m-%d"),
            "execution_time": datetime.now().strftime("%H:%M:%S"),
            "asthma_and_copd_patient_count": 0,
            "patient_count_by_age_interval": defaultdict(int), # The sum of age count might differ compared with the total count since a patient sometimes have more than one condition at different times.
            "patient_count_in_intensive_care": 0,
            "patient_count_with_observations": 0,
            "patient_count_with_procedures": 0,
            "patient_count_with_medicationRequests": 0,
            "patient_count_with_medicationAdministrations": 0,
            "patient_count_with_medicationStatements": 0,
            "patient_count_with_medicationList": 0,
            "missing_asthma_and_copd_patients": 0,
            "conditions_counts": defaultdict(int),
            "observations_counts": defaultdict(int),
            "procedures_counts": defaultdict(int),
            "medicationAdministrations_counts": defaultdict(int),
            "medicationRequests_counts": defaultdict(int),
            "medicationStatements_counts": defaultdict(int),
            "medicationList_counts": defaultdict(int)
        }

    metadata["execution_date"] = datetime.now().strftime("%Y-%m-%d")
    metadata["execution_time"] = datetime.now().strftime("%H:%M:%S")

    if source in metadata:
        if "patient_count_by_age_interval" in source:
            for key, value in count.items():
                metadata[source][key] = metadata[source].get(key, 0) + value
        else:
            metadata[source] = count
    elif '_counts' in source.lower():
        metadata[source] = defaultdict(int, {count: 0})
        logging.info(f"Source '{source}' was not defined; but it has been created in Metadata.json file.")
    else:
        metadata[source] = count
        logging.info(f"Source '{source}' was not defined; but it has been created in Metadata.json file.")

    with open(metadata_file_path, 'w') as metadata_file:
        json.dump(metadata, metadata_file, indent=4)

    logging.info("Metadata has been saved.")
