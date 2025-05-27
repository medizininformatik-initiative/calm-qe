import json
import logging

from fhirclient.models.condition import Condition
from fhirclient.models.medicationadministration import MedicationAdministration
from fhirclient.models.medicationrequest import MedicationRequest
from fhirclient.models.medicationstatement import MedicationStatement
from fhirclient.models.observation import Observation

from Constants import ICD_CODE_FILE, LOINC_CODE_FILE, ATC_CODE_FILE
from FhirHelpersResourceExtraction import (execute_thread_for_fetching, observations, conditions, medications,
                                           observation_frequencies, secondary_conditions_frequencies,
                                           medication_frequencies)
from Metadata import gather_metadata

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

""""
This script is for connecting to FHIR Server to query information for Cohort Patients. It checks Observations, Conditions and Medications
based on LOINC, ICD, ATC Codes respectively for each Cohort patient. It shows the feasibility of resources given by codes, 
i.e., counting them. It also fetches the resources and save them in output files for each patient separately.
"""


def main():
    logging.info("Start...")
    #Input is the patient list in a text file from Cohort Data Extraction part
    with open("patients_diagnosed_asthma_copd.json", "r") as file:
        input_file = json.load(file)
        patients = [patient for patient in input_file.keys()]

    ####Observations####
    execute_thread_for_fetching(LOINC_CODE_FILE, Observation, patients, "LOINC", observations)
    ####Conditions#####
    execute_thread_for_fetching(ICD_CODE_FILE, Condition, patients, "ICD", conditions)
    ##Medications####
    medication_profiles = {
        'MedicationAdministration': MedicationAdministration,
        'MedicationRequest': MedicationRequest,
        'MedicationStatement': MedicationStatement,
    }

    for profile in medication_profiles.values():
        execute_thread_for_fetching(ATC_CODE_FILE, profile, patients, "ATC", medications)

    """ Post processing: Analysis """

    secondary_conditions_frequencies(ICD_CODE_FILE)
    observation_frequencies(LOINC_CODE_FILE)
    medication_frequencies(ATC_CODE_FILE)

if __name__ == "__main__":
    main()
