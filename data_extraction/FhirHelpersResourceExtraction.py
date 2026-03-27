import os
from collections import defaultdict
import json
import time

from fhirclient.models.medication import Medication
from fhirclient.models.medicationadministration import MedicationAdministration
from fhirclient.models.medicationrequest import MedicationRequest
from fhirclient.models.medicationstatement import MedicationStatement

from Constants import USER_NAME, USER_PASSWORD, ICD_SYSTEM_NAME, LOINC_SYSTEM_NAME, MAX_WORKERS, ATC_SYSTEM_NAME, \
    ASTHMA_COPD_CODES_FILE
from concurrent.futures import ThreadPoolExecutor, as_completed

from FhirHelpersUtils import connect_to_server, fetch_bundle_for_code
from Metadata import gather_metadata

def read_input_code_file(filename):
    """
    :param filename:  input file of code list
    :return:
    """
    with open(filename, "r") as fp:
        lines = json.load(fp)

        if 'loinc_codes' in filename:
            system = LOINC_SYSTEM_NAME
            if not os.path.exists(f"fhir_results/LOINC/"):
                os.makedirs(f"fhir_results/LOINC/")
            code_list = [item['code'] for item in lines['codes']]

        elif 'icd_codes' in filename:
            system = ICD_SYSTEM_NAME
            if not os.path.exists(f"fhir_results/ICD/"):
                os.makedirs(f"fhir_results/ICD/")
            code_list = [code for item in lines['codes'] for code in item['code']]

        elif 'atc_codes' in filename:
            system = ATC_SYSTEM_NAME
            if not os.path.exists(f"fhir_results/ATC/"):
                os.makedirs(f"fhir_results/ATC/")
                os.makedirs(f"fhir_results/ATC/Administrations/")
                os.makedirs(f"fhir_results/ATC/Requests/")
                os.makedirs(f"fhir_results/ATC/Statements/")
            code_list = [code['code'] for code in lines]

    return code_list, system

def write_results(entries, patient_counter, code_type, source):
    """
    It reads all Resources in the bundle and write to output files per patient.
    """
    if code_type == "LOINC":
        whole_path = "fhir_results/LOINC/" + patient_counter + "_patient_observations.json"
    elif code_type == "ICD":
        whole_path = "fhir_results/ICD/" + patient_counter + "_patient_conditions.json"
    elif code_type == "ATC":
        if source is MedicationAdministration:
            whole_path = "fhir_results/ATC/Administrations/" + patient_counter + "_patient_medicationAdministrations.json"
        elif source is MedicationRequest:
            whole_path = "fhir_results/ATC/Requests/" + patient_counter + "_patient_medicationRequests.json"
        elif source is MedicationStatement:
            whole_path = "fhir_results/ATC/Statements/" + patient_counter + "_patient_medicationStatements.json"

    with open(whole_path, 'w') as file:
        json.dump(entries, file, indent=4)



def observations(patient, code_file, source, smart):
    print(f"Creating queries for patient {patient} for observation resources...\n")
    while True:
        try:
            bundle = source.where(struct={'_count': b'1000', 'subject': patient}).perform(smart.server)
            break
        except Exception as exc:
            print(f"Generated an exception: {exc} but continue to trying... \n")
            time.sleep(3)
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)

    observations_bundles = fetch_bundle_for_code(smart, bundle)
    code_list, system = read_input_code_file(code_file)
    filtered_results = []
    for observation in observations_bundles:
        if 'code' in observation['resource'] and 'coding' in observation['resource']['code']:
            for coding in observation['resource']['code']['coding']:
                if LOINC_SYSTEM_NAME == coding['system'] and coding['code'] in code_list:
                    filtered_results.append(observation)
    print(f"Patient {patient} has {len(filtered_results)} observations.")
    return filtered_results

def conditions(patient, code_file, source, smart,):
    code_list, system = read_input_code_file(code_file)
    sub_code_lists = [code_list[i:i + 30] for i in range(0, len(code_list), 30)]  # Smaller chunks of code list
    conditions = []
    print(f"Creating queries for patient {patient} for conditions...\n")
    for sub_code_list in sub_code_lists:
        sub_code_list_str = ','.join([system + '|' + code for code in sub_code_list])
        while True:
            try:
                bundle = source.where(struct={'_count': b'1000', 'subject': patient, 'code': sub_code_list_str}).perform(smart.server)
                break
            except Exception as exc:
                print(f"Generated an exception: {exc} but continue to trying... \n")
                time.sleep(3)
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)

        batch_result = fetch_bundle_for_code(smart, bundle)

        if len(batch_result) > 0:
            conditions.extend(batch_result)

    return conditions

def medications(patient, code_file, source, smart):
    code_list, system = read_input_code_file(code_file)
    code_list_str = ','.join([system + '|' + code for code in code_list])

    print(f"Creating queries for patient {patient}...\n")
    while True:
        try:
            if source == Medication:
                bundle = (source.where(struct={'_count': b'1000', 'subject': patient, 'code': code_list_str})
                          .perform(smart.server))
            else:
                bundle = source.where(
                    struct={'_count': b'1000', 'patient': patient, 'medication.code': code_list_str}).perform(
                    smart.server)
            break
        except Exception as exc:
            print(f"Generated an exception: {exc} but continue to trying... \n")
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
            time.sleep(3)
    medications_bundles = fetch_bundle_for_code(smart, bundle)
    return medications_bundles

def execute_thread_for_fetching(code_file, source, patient_list, code_type, function_to_run):
    """
    Threads for running fetch queries parallel.
    """
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {executor.submit(function_to_run, patient, code_file, source, smart): patient for patient in patient_list}
        counter = 0
        for future in as_completed(future_to_code):
            patient = future_to_code[future]
            try:
                entries = future.result()
                if entries:
                    counter += 1
                    write_results(entries, str(counter), code_type, source)
                print(f"Processed patient {patient} with {len(entries)} entries.\n")
            except Exception as exc:
                print(f"Patient {patient} generated an exception: {exc}.\n")


    ###META DATA COLLECTION###
    '''
    patient_count_with_secondary_conditions: Number of cohort patients that has secondary conditions (non main diagnosis ASTHMA OR COPD) 
    patient_count_with_observations: Number of cohort patients that has at least one observation
    patient_count_with_medications: Number of cohort patients that has at least one medication
    conditions_counts: Frequency of each ICD code 
    observations_counts:Frequency of each LOINC code 
    medication_counts: Frequency of each ATC code 
    '''

    if code_type == "LOINC":
        gather_metadata("observations_patient_count", counter)
    elif code_type == "ATC":
        if source is MedicationAdministration:
            gather_metadata("medicationAdministration_patient_count", counter)
        elif source is MedicationRequest:
            gather_metadata("medicationRequests_patient_count", counter)
        elif source is MedicationStatement:
            gather_metadata("medicationStatement_patient_count", counter)
    else:
        pass
    print("---------------End of Code------------------------")

def observation_frequencies(code_file):
    folder_path = "fhir_results/LOINC"
    observations_counts = defaultdict(int)
    code_list, system = read_input_code_file(code_file)

    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)
                for observation in data:
                    if 'code' in observation['resource'] and 'coding' in observation['resource']['code']:
                        for coding in observation['resource']['code']['coding']:
                            if LOINC_SYSTEM_NAME == coding['system'] and coding['code'] in code_list:
                                observations_counts[coding['code']] += 1

    for code, frequency in observations_counts.items():
        print(f"{code}: {frequency}")
    gather_metadata("observations_counts", observations_counts)

def secondary_conditions_frequencies(code_file):
    folder_path = "fhir_results/ICD"
    code_list, system = read_input_code_file(code_file)
    conditions_counts = defaultdict(int)

    pats = set()
    main_diagnoses_ids = set()

    with open("patients_main_diagnosed_asthma_copd.json", "r") as file:
        patients = json.load(file)
        for conditions in patients.values():
            main_diagnoses_ids.update(condition['id'] for condition in conditions)

    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as json_file:
                data = json.load(json_file)
                for condition in data:
                    if 'code' in condition['resource'] and 'coding' in condition['resource']['code']:
                        for coding in condition['resource']['code']['coding']:
                            if ICD_SYSTEM_NAME == coding['system'] and coding['code'] in code_list:
                                if condition['resource']['id'] not in main_diagnoses_ids:
                                    pats.add(condition['resource']['subject']['reference'])
                                    conditions_counts[coding['code']] += 1

    gather_metadata("secondary_conditions_counts", conditions_counts)
    gather_metadata("secondary_conditions_patient_count", len(pats))


def fetch_atc_codes(resource_ref, system, code_list):
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)

    try:
        source, medication_reference_id = resource_ref.split('/')
        if source:
            medication = Medication.read(medication_reference_id, smart.server)
            if medication.code.coding:
                for coding in medication.code.coding:
                    if system == coding.system and coding.code in code_list:
                        return coding.code

    except Exception as error:
        print(f"Generated an exception:{error} for {resource_ref}")


def medication_frequencies(code_file):
    folder_paths = ["fhir_results/ATC/Administrations", "fhir_results/ATC/Requests", "fhir_results/ATC/Statements"]
    code_list, system = read_input_code_file(code_file)

    for folder_path in folder_paths:
        medication_type_and_med_reference = {}
        resource_structure = defaultdict(lambda: {
            "counting": {
                "total_count": 0,
                "details_count": [],
            }})

        # Gathering, counting and fetching ID-references for "Medication".
        for filename in os.listdir(folder_path):
            if filename.endswith(".json"):
                file_path = os.path.join(folder_path, filename)
                with (open(file_path, 'r') as json_file):
                    data = json.load(json_file)
                    print(f"\nReading {filename}")

                    for medicationReference in data:
                        if 'resource' in medicationReference:
                            resource_type = medicationReference['resource']['resourceType']
                            resource_ref = medicationReference['resource']['medicationReference']['reference']

                            code_name = fetch_atc_codes(resource_ref, system, code_list)
                            print("Fetched code name:", code_name)

                            if resource_type not in medication_type_and_med_reference:
                                medication_type_and_med_reference[resource_type] = {}
                            medication_type_and_med_reference[resource_type][code_name] = (
                                    medication_type_and_med_reference[resource_type].get(code_name, 0) + 1)
                        else:
                            print(f"{filename}  has no 'resource' statement within this file.")

        # Estimates TOTAL counts per medication resource and structures data as outcomes
        for resource_type, num_references in medication_type_and_med_reference.items():
            total_count = sum(num_references.values())
            details_count = [{ref: count} for ref, count in num_references.items()]

            resource_structure[resource_type]["counting"]["total_count"] = total_count
            resource_structure[resource_type]["counting"]["details_count"] = details_count

        print("final resource outcome", resource_structure)
        if "Administrations" in folder_path:
            gather_metadata("medicationAdministrations_counts", resource_structure)
        elif "Requests" in folder_path:
            gather_metadata("medicationRequests_counts", resource_structure)
        elif "Statements" in folder_path:
            gather_metadata("medicationStatements_counts", resource_structure)


