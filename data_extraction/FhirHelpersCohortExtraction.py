from collections import defaultdict
import json
import time

from fhirclient.models.condition import Condition
from fhirclient.models.encounter import Encounter

from Constants import USER_NAME, USER_PASSWORD, ICD_SYSTEM_NAME, ASTHMA_COPD_CODES_FILE
from FhirHelpersUtils import fetch_bundle_for_code, connect_to_server
from Metadata import gather_metadata



def patients_with_asthma_copd(smart):
    """
    It reads the ASTHMA or COPD diseases related codes from "ASTHMA_COPD_CODES_FILE" and
    find the patients that have such diagnoses.
    :param smart: Fhir Server Connector
    """
    with open(ASTHMA_COPD_CODES_FILE, 'r') as file:
        main_diagnoses_file = json.load(file)
        main_diagnoses_codes = [item['code'] for item in main_diagnoses_file['codes']]
    print(main_diagnoses_codes)

    patients_conditions_map = defaultdict(list)
    for code in main_diagnoses_codes:
        while True:
            try:
                bundle = Condition.where(struct={'_count': b'1000', 'code': ICD_SYSTEM_NAME + '|' + code}).perform(smart.server)
                break
            except Exception as exc:
                print(f"Generated an exception: {exc} but continue to trying.\n")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                time.sleep(3)

        conditions = fetch_bundle_for_code(smart, bundle)
        if conditions:
            for entry in conditions:
                condition = entry['resource']
                if condition['subject']['reference']:
                    patient_reference = condition['subject']['reference']
                    patients_conditions_map[patient_reference].append({"id": condition['id'], "code": condition['code']})

    print(len(patients_conditions_map))
    gather_metadata("asthma_and_copd_patient_count", len(patients_conditions_map))
    with open('patients_diagnosed_asthma_copd.json', 'w') as file: #Intermediate results, can be deleted later.
        json.dump(patients_conditions_map, file, indent=4)

