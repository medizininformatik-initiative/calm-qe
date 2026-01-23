import json
import logging
from pathlib import Path

from Constants import USER_NAME, USER_PASSWORD
from FhirHelpersUtils import connect_to_server
from FhirHelpersCohortExtraction import patients_with_asthma_copd, filter_main_diagnosis, filter_icu_patients_admission, \
    calculate_los_inpatients, extract_last_three_encounter, get_demographics_patients

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

""""
This script is for creating the Cohort Study Data. Study protocol requires the analysis of the patients from HauptDiagnosis with Asthma & COPD.
Here, script first finds the patients with Asthma or Copd diagnosed. And later it filters only for HauptDiagnosis from their Encounter references.
HauptDiagnoses are flagged as "CC" (Chief Complaints") in Encounter.Diagnosis.
Results are saved in "patient_results.txt"
"""

DIR_RESULTS = Path('fhir_results')
DIR_RESULTS.mkdir(exist_ok=True)


def main():
    # Connect to FHIR Server
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)

    # Get the patients with "ANY TYPE OF DIAGNOSED" Asthma or COPD.
    diagnoses_filepath = patients_with_asthma_copd(smart, DIR_RESULTS)
    print("All diagnoses", diagnoses_filepath)

    # Filter the patients for only "MAIN DIAGNOSED" Asthma or COPD.
    diagnoses_filepath = filter_main_diagnosis(smart, diagnoses_filepath, enabled=True)
    print("Filtered diagnoses", diagnoses_filepath)

    # Filter patients per type of admission (Intensive-Care-Unit)
    filter_icu_patients_admission(smart, diagnoses_filepath, enabled=True)
    print("Filtered for icu", diagnoses_filepath)

    # Calculate length-of-stay ('los' or 'Aufenthaltsdauer') for inpatients.
    calculate_los_inpatients(smart, diagnoses_filepath, enabled=True)
    print("LOS", diagnoses_filepath)

    # # Extract last 3 encounter for each patient
    extract_last_three_encounter(smart, diagnoses_filepath, enabled=True)
    print("3 last encounters", diagnoses_filepath)

    # Extracts demographics from patients
    get_demographics_patients(smart, diagnoses_filepath, enabled=True)
    print("Demographics", diagnoses_filepath)


if __name__ == "__main__":
    main()
