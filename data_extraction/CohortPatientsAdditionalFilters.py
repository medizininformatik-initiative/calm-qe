import json
import logging
from pathlib import Path

from Constants import USER_NAME, USER_PASSWORD, PROTOCOL, CONFIG_EXECUTION
from Utils import connect_to_server
from FhirHelpersCohortExtraction import extract_additional_attributes_from_encounters, filter_icu_patients_admission, \
    calculate_los_inpatients, extract_last_three_encounter, get_demographics_patients, filter_patients_by_age_interval

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

""""
This part of the script is designed to complete the extracted main cohort. 
It uses the identified patients with Asthma or COPD and then performs the following tasks:
- Extracts Encounter and Patients bundles.
- Identifies patients based on specific filters.
- Calculates the length of stay for inpatients.
The results are saved as "metadata.json" in the "additional_results" folder, while the extracted bundles are saved in
the "fhir_results" folder.
"""

FHIR_RESULTS = Path('fhir_results')
DIR_RESULTS = Path('additional_results')
DIR_RESULTS.mkdir(exist_ok=True)

def config_filters():
    with open(CONFIG_EXECUTION, "r") as json_file:
        config = json.load(json_file)
        additional_filters = config.get("filter", {})
        return {key: bool(value) for key, value in additional_filters.items() if bool(value)}

def main():
    # Connect to FHIR Server
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=PROTOCOL)

    # Extract attributes from encounters
    diagnoses_filepath = Path(DIR_RESULTS, 'patients_diagnosed_asthma_copd.json')
    encounters_filepath = extract_additional_attributes_from_encounters(diagnoses_filepath)

    # Get configuration of enabled values to execute
    enabled_filter = config_filters()

    # Filter by patients' age  min_age: minimal age in years, max_age: maximal age in years Example: [0-2], [3-5], etc.
    age_interval = {
        'min_age': [0, 6, 12, 18, 25],
        'max_age': [5, 11, 17, 24, 120]
    }
    for min_age, max_age in zip(age_interval['min_age'], age_interval['max_age']):
        filter_patients_by_age_interval(smart, encounters_filepath, min_age=min_age, max_age=max_age, enabled=enabled_filter.get("age_interval"))

    # Filter patients per type of admission (Intensive-Care-Unit)
    filter_icu_patients_admission(FHIR_RESULTS/"Encounters/encounters.jsonl", enabled=enabled_filter.get("intensive_care"))

    # Calculate length-of-stay ('LOS' or 'Aufenthaltsdauer') for inpatients.
    calculate_los_inpatients(FHIR_RESULTS/"Encounters/encounters.jsonl", enabled=enabled_filter.get("inpatient_duration"))

    # Extract last 3 encounter for each patient
    extract_last_three_encounter(encounters_filepath, enabled=enabled_filter.get("last_three_encounters"))

    # Export patient's demographics
    get_demographics_patients(FHIR_RESULTS/"Patients/patients.jsonl", enabled=enabled_filter.get("demographics"))


if __name__ == "__main__":
    main()
