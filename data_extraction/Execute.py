import json
import logging
from pathlib import Path
from Utils import connect_to_server, execute_thread_for_fetching
from fhirclient.models.condition import Condition
from fhirclient.models.medicationadministration import MedicationAdministration
from fhirclient.models.medicationrequest import MedicationRequest
from fhirclient.models.medicationstatement import MedicationStatement
from fhirclient.models.list import List as MedicationList
from fhirclient.models.observation import Observation
from fhirclient.models.procedure import Procedure
from Constants import USER_NAME, USER_PASSWORD, ICD_CODE_FILE, LOINC_CODE_FILE, ATC_CODE_FILE,OPS_CODE_FILE, PROTOCOL, CONFIG_EXECUTION
from FhirHelpersResourceExtraction import (observations, conditions, medications,
                                           procedures, conditions_frequencies, medication_frequencies,
                                           procedure_frequencies, read_input_code_file, patients_with_asthma_copd,
                                           fetch_patients, encounters,
                                           observation_frequencies_and_distributions, build_statement_medication_map)

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s', force=True)

""""
This script is for creating the Cohort Study Data. Study protocol requires the analysis of the patients from with Asthma & COPD.
Here, script first finds the patients with Asthma or Copd diagnosed and put them in the file "patients_diagnosed_asthma_copd.json"

The second part is for connecting to FHIR Server to query information for Cohort Patients. It checks Observations, Conditions and Medications
based on LOINC, ICD, ATC Codes respectively for each Cohort patient. It shows the feasibility of resources given by codes, 
i.e., counting them. It also fetches the resources and save them in output files for each patient separately.
"""

DIR_RESULTS = Path('additional_results')
DIR_RESULTS.mkdir(exist_ok=True)

def resources_to_fetch(patient_list, encounters_list, smart):

    with open(CONFIG_EXECUTION, "r") as json_file:
        config = json.load(json_file)
        extraction = config.get("extract", {})

        resources = [key for key, value in extraction.items() if value is True]
        if not resources:
            logging.info("No additional resources were enabled for fetching in `config_execution.json`")

        ####Patients#####
        if extraction.get("patients"):
            logging.info("Fetching patients ...")
            fetch_patients(patient_list, smart)

        ####Conditions####
        if extraction.get("conditions"):
            logging.info("Fetching conditions ...")
            code_list = read_input_code_file(ICD_CODE_FILE)
            execute_thread_for_fetching(code_list, Condition, patient_list, "ICD", conditions)

        ####Observations####
        if extraction.get("observations"):
            logging.info("Fetching observations ...")
            code_list = read_input_code_file(LOINC_CODE_FILE)
            execute_thread_for_fetching(set(code_list), Observation, patient_list, "LOINC", observations)

        ####Encounters####
        if extraction.get("encounters"):
            logging.info("Fetching encounters ...")
            encounters(encounters_list, smart)

        ###Procedures####
        if extraction.get("procedures"):
            logging.info("Fetching procedures ...")
            code_list = read_input_code_file(OPS_CODE_FILE)
            execute_thread_for_fetching(set(code_list), Procedure, patient_list, "OPS", procedures)

        ##Medications####
        if extraction.get("medication"):
            logging.info("Fetching medication ...")

            medication_profiles = [
                MedicationAdministration,
                MedicationRequest,
                MedicationStatement,  # Note: Needs to run before MedicationList
                MedicationList
            ]

            code_list = read_input_code_file(ATC_CODE_FILE)
            for profile in medication_profiles:
                if MedicationList == profile:
                    statement_medication_map = build_statement_medication_map("fhir_results/Medications/Statement")
                    statements_ref = list(statement_medication_map.keys())
                    if statements_ref:
                        execute_thread_for_fetching(code_list, profile, statements_ref, "ATC", medications)
                else:
                    execute_thread_for_fetching(code_list, profile, patient_list, "ATC", medications)


def main():
    logging.info("Start...")

    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=PROTOCOL)

    #Get the patients with "ANY TYPE OF DIAGNOSED" Asthma or COPD.
    diagnosis_path = patients_with_asthma_copd(smart, DIR_RESULTS)
    #Input is the patient list in a text file from Cohort Data Extraction part
    with open(diagnosis_path, "r") as file:
        input_file = json.load(file)
        patients = [patient for patient in input_file.keys()]
        enc_list = list(dict.fromkeys(encounter['encounter'] for encounters_list in input_file.values() for encounter in encounters_list if 'encounter' in encounter))

    # Fetch only resources that are true in the config execution file
    resources_to_fetch(patients, enc_list, smart)

    ###Post processing: Analysis###
    conditions_frequencies(ICD_CODE_FILE)
    observation_frequencies_and_distributions(LOINC_CODE_FILE)
    procedure_frequencies(OPS_CODE_FILE)
    medication_frequencies(ATC_CODE_FILE)
if __name__ == "__main__":
    main()
