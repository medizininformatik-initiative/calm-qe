import json
import logging
from pathlib import Path

from Constants import USER_NAME, USER_PASSWORD
from FhirHelpersUtils import connect_to_server
from FhirHelpersCohortExtraction import patients_with_asthma_copd, filter_main_diagnosis, filter_icu_patients_admission, \
    calculate_los_inpatients, extract_last_three_encounter, get_demographics_patients, filter_patients_by_age_interval

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

""""
This script is for creating the Cohort Study Data. Study protocol requires the analysis of the patients from HauptDiagnosis with Asthma & COPD.
Here, script first finds the patients with Asthma or Copd diagnosed. And later it filters only for HauptDiagnosis from their Encounter references.
HauptDiagnoses are flagged as "CC" (Chief Complaints") in Encounter.Diagnosis.
Results are saved in "patient_results.txt"
"""

DIR_RESULTS = Path('results')
DIR_RESULTS.mkdir(exist_ok=True)


def main():
    # Connect to FHIR Server
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)

    # Get the patients with "ANY TYPE OF DIAGNOSED" Asthma or COPD.
    diagnoses_filepath = patients_with_asthma_copd(smart, DIR_RESULTS)

    # Filter the patients for only "MAIN DIAGNOSED" Asthma or COPD.
    diagnoses_filepath = filter_main_diagnosis(smart, diagnoses_filepath, enabled=True)

    # Filter patients' age  min_age: int minimal age in years, max_age: integer maximal age in years Example: [2-6]
    filter_patients_by_age_interval(smart, diagnoses_filepath, min_age=0, max_age=6, enabled=True)

    # Filter patients per type of admission (Intensive-Care-Unit)
    filter_icu_patients_admission(smart, diagnoses_filepath, enabled=True)

    # Calculate length-of-stay ('LOS' or 'Aufenthaltsdauer') for inpatients.
    calculate_los_inpatients(smart, diagnoses_filepath, enabled=True)

    # Extract last 3 encounter for each patient
    extract_last_three_encounter(smart, diagnoses_filepath, enabled=True)

    # Extracts demographics from patients
    get_demographics_patients(smart, diagnoses_filepath, enabled=True)


if __name__ == "__main__":
    main()
