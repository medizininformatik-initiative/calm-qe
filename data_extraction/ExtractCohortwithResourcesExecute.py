import json
import logging
from pathlib import Path

from FhirHelpersUtils import connect_to_server

from fhirclient.models.condition import Condition
from fhirclient.models.medicationadministration import MedicationAdministration
from fhirclient.models.medicationrequest import MedicationRequest
from fhirclient.models.medicationstatement import MedicationStatement
from fhirclient.models.observation import Observation

from Constants import USER_NAME, USER_PASSWORD, ICD_CODE_FILE, LOINC_CODE_FILE, ATC_CODE_FILE, PROTOCOL
from FhirHelpersResourceExtraction import (execute_thread_for_fetching, observations, conditions, medications,
                                           observation_frequencies, conditions_frequencies,
                                           medication_frequencies, read_input_code_file, patients_with_asthma_copd)

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

""""
This script is for creating the Cohort Study Data. Study protocol requires the analysis of the patients from with Asthma & COPD.
Here, script first finds the patients with Asthma or Copd diagnosed and put them in the file "patients_diagnosed_asthma_copd.json"

The second part is for connecting to FHIR Server to query information for Cohort Patients. It checks Observations, Conditions and Medications
based on LOINC, ICD, ATC Codes respectively for each Cohort patient. It shows the feasibility of resources given by codes, 
i.e., counting them. It also fetches the resources and save them in output files for each patient separately.
"""

DIR_RESULTS = Path('additional_results')
DIR_RESULTS.mkdir(exist_ok=True)

def main():
    logging.info("Start...")
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= PROTOCOL)

    #Get the patients with "ANY TYPE OF DIAGNOSED" Asthma or COPD.
    diagnosis_path = patients_with_asthma_copd(smart, DIR_RESULTS)

    #Input is the patient list in a text file from Cohort Data Extraction part
    with open(diagnosis_path, "r") as file:
        input_file = json.load(file)
        patients = [patient for patient in input_file.keys()]

    ####Conditions#####
    code_list = read_input_code_file(ICD_CODE_FILE)
    execute_thread_for_fetching(code_list, Condition, patients, "ICD", conditions)

    ####Observations####
    code_list = read_input_code_file(LOINC_CODE_FILE)
    execute_thread_for_fetching(set(code_list), Observation, patients, "LOINC", observations)

    ##Medications####
    medication_profiles = {
        'MedicationAdministration': MedicationAdministration,
        'MedicationRequest': MedicationRequest,
        'MedicationStatement': MedicationStatement,
    }

    for profile in medication_profiles.values():
        code_list = read_input_code_file(ATC_CODE_FILE)
        execute_thread_for_fetching(code_list, profile, patients, "ATC", medications)

    """ Post processing: Analysis """
    conditions_frequencies(ICD_CODE_FILE)
    observation_frequencies(LOINC_CODE_FILE)
    medication_frequencies(ATC_CODE_FILE)

if __name__ == "__main__":
    main()
