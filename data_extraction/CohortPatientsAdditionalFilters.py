import logging
from pathlib import Path

from Constants import USER_NAME, USER_PASSWORD
from FhirHelpersUtils import connect_to_server
from FhirHelpersCohortExtraction import extract_additional_attributes_from_encounters, filter_icu_patients_admission, \
    calculate_los_inpatients, extract_last_three_encounter, get_demographics_patients, filter_patients_by_age_interval

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

""""
This script is for creating the Cohort Study Data. Study protocol requires the analysis of the patients from HauptDiagnosis with Asthma & COPD.
Here, script first finds the patients with Asthma or Copd diagnosed. And later it filters only for HauptDiagnosis from their Encounter references.
Results are saved in "additional_results.txt"
"""

DIR_RESULTS = Path('additional_results')
DIR_RESULTS.mkdir(exist_ok=True)


def main():
    # Connect to FHIR Server
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)

    # Extract attributes from encounters
    diagnoses_filepath = Path(DIR_RESULTS, 'patients_diagnosed_asthma_copd.json')
    encounters_filepath = extract_additional_attributes_from_encounters(smart, diagnoses_filepath)

    # Filter by patients' age  min_age: minimal age in years, max_age:  maximal age in years Example: [0-2], [3-5], etc.
    age_interval = {
        'min_age': [0, 6, 13, 25],
        'max_age': [5, 12, 24, 120]
    }
    for min_age, max_age in zip(age_interval['min_age'], age_interval['max_age']):
        filter_patients_by_age_interval(smart, encounters_filepath, min_age=min_age, max_age=max_age, enabled=True)

    # Filter patients per type of admission (Intensive-Care-Unit)
    filter_icu_patients_admission(smart, diagnoses_filepath, enabled=True)

    # Calculate length-of-stay ('LOS' or 'Aufenthaltsdauer') for inpatients.
    calculate_los_inpatients(smart, diagnoses_filepath, enabled=True)

    # Extract last 3 encounter for each patient
    extract_last_three_encounter(smart, encounters_filepath, enabled=True)

    # Export patient's demographics
    get_demographics_patients(smart, diagnoses_filepath, enabled=True)


if __name__ == "__main__":
    main()




