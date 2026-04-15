import os
from collections import defaultdict
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from fhirclient.models.condition import Condition
from fhirclient.models.medication import Medication
from fhirclient.models.medicationadministration import MedicationAdministration
from fhirclient.models.medicationrequest import MedicationRequest
from fhirclient.models.medicationstatement import MedicationStatement

from Constants import USER_NAME, USER_PASSWORD, ICD_SYSTEM_NAME, LOINC_SYSTEM_NAME, MAX_WORKERS, ATC_SYSTEM_NAME, \
    ASTHMA_COPD_CODES_FILE, PROTOCOL
from FhirHelpersUtils import connect_to_server, fetch_bundle_for_code
from Metadata import gather_metadata


def read_input_code_file(filename):
    """
    :param filename:  input file of code list
    :return: code_list
    """
    with open(filename, "r") as fp:
        lines = json.load(fp)

        if 'loinc_codes' in filename:
            if not os.path.exists(f"fhir_results/LOINC/"):
                os.makedirs(f"fhir_results/LOINC/")
            code_list = [item['code'] for item in lines['codes']]

        elif 'icd_codes' in filename:
            if not os.path.exists(f"fhir_results/ICD/"):
                os.makedirs(f"fhir_results/ICD/")
            code_list = [code for item in lines['codes'] for code in item['code']]

        elif 'atc_codes' in filename:
            if not os.path.exists(f"fhir_results/ATC/"):
                os.makedirs(f"fhir_results/ATC/")
                os.makedirs(f"fhir_results/ATC/Administrations/")
                os.makedirs(f"fhir_results/ATC/Requests/")
                os.makedirs(f"fhir_results/ATC/Statements/")
            code_list = [code['code'] for code in lines]

    return code_list


def patients_with_asthma_copd(smart):
    """
    It reads the ASTHMA or COPD diseases related codes from "ASTHMA_COPD_CODES_FILE" and
    find the patients with such diagnoses.
    :param smart: Fhir Server Connector
    """
    protocol = PROTOCOL
    with open(ASTHMA_COPD_CODES_FILE, 'r') as file:
        diagnoses_file = json.load(file)
        diagnoses_codes = [item['code'] for item in diagnoses_file['codes']]

    patients_conditions_map = defaultdict(list)
    for code in diagnoses_codes:
        while True:
            try:
                bundle = smart.server.request_json(
                    Condition.where(struct={'_count': "1000", 'code': ICD_SYSTEM_NAME + '|' + code}).construct())
                break
            except Exception as exc:
                print(f"Generated an exception: {exc} but continue trying.\n")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)
                time.sleep(3)

        for entries in fetch_bundle_for_code(smart, bundle, protocol):
            for entry in entries:
                condition = entry['resource']
                if condition['subject']['reference']:
                    patient_reference = condition['subject']['reference']
                    patients_conditions_map[patient_reference].append(
                        {"id": condition['id'], "code": condition['code']})

    gather_metadata("asthma_and_copd_patient_count", len(patients_conditions_map))
    with open('patients_diagnosed_asthma_copd.json', 'w') as file:  #intermediate results, can be deleted later.
        json.dump(patients_conditions_map, file, indent=4)


def observations(patient, code_set, source, smart):
    patient_id = patient.split("/")[-1]
    whole_path = f"fhir_results/LOINC/{patient_id}_patient_observations.json"
    protocol = PROTOCOL
    while True:
        try:
            bundle = smart.server.request_json(source.where(struct={'_count': '1000', 'subject': patient}).construct())
            break
        except Exception as exc:
            print(f"Generated an exception: {exc} but continue trying.\n")
            time.sleep(3)
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)

    count = 0
    file = None
    try:
        for entries in fetch_bundle_for_code(smart, bundle, protocol):
            for observation in entries:
                resource = observation.get("resource", {})
                codings = resource.get("code", {}).get("coding", [])
                for coding in codings:
                    if LOINC_SYSTEM_NAME == coding['system'] and coding['code'] in code_set:
                        if file is None:
                            file = open(whole_path, "w")
                        json.dump(observation, file, separators=(",", ":"))
                        file.write("\n")
                        count += 1
    finally:
        if file is not None:
            file.close()
    return count


def conditions(patient, code_list, source, smart):
    patient_id = patient.split("/")[-1]
    whole_path = "fhir_results/ICD/" + patient_id + "_patient_conditions.json"
    protocol = PROTOCOL
    sub_code_lists = [code_list[i:i + 30] for i in range(0, len(code_list), 30)]  # smaller chunks of code list

    count = 0
    file = None
    try:
        for sub_code_list in sub_code_lists:
            sub_code_list_str = ','.join([ICD_SYSTEM_NAME + '|' + code for code in sub_code_list])
            while True:
                try:
                    bundle = smart.server.request_json(source.where(
                        struct={'_count': '1000', 'subject': patient, 'code': sub_code_list_str}).construct())
                    break
                except Exception as exc:
                    print(f"Generated an exception: {exc} but continue trying.\n")
                    time.sleep(3)
                    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)

            for entries in fetch_bundle_for_code(smart, bundle, protocol):
                for condition in entries:
                    if file is None:
                        file = open(whole_path, "w")
                    json.dump(condition, file, separators=(",", ":"))
                    file.write("\n")
                    count += 1
    finally:
        if file is not None:
            file.close()
    return count


def medications(patient, code_list, source, smart):
    code_list_str = ','.join([ATC_SYSTEM_NAME + '|' + code for code in code_list])
    patient_id = patient.split("/")[-1]
    protocol = PROTOCOL

    if source is MedicationAdministration:
        whole_path = "fhir_results/ATC/Administrations/" + patient_id + "_patient_medicationAdministrations.json"
    elif source is MedicationRequest:
        whole_path = "fhir_results/ATC/Requests/" + patient_id + "_patient_medicationRequests.json"
    elif source is MedicationStatement:
        whole_path = "fhir_results/ATC/Statements/" + patient_id + "_patient_medicationStatements.json"

    while True:
        try:
            if source == Medication:
                bundle = smart.server.request_json(
                    source.where(struct={'_count': '1000', 'subject': patient, 'code': code_list_str}).construct())
            else:
                bundle = smart.server.request_json(source.where(
                    struct={'_count': '1000', 'patient': patient, 'medication.code': code_list_str}).construct())
            break
        except Exception as exc:
            print(f"Generated an exception: {exc} but continue trying.\n")
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= PROTOCOL)
            time.sleep(3)
    count = 0
    file = None
    try:
        for entries in fetch_bundle_for_code(smart, bundle, protocol):
            for medicationProfile in entries:
                if file is None:
                    file = open(whole_path, "w")
                json.dump(medicationProfile, file, separators=(",", ":"))
                file.write("\n")
                count += 1
    finally:
        if file is not None:
            file.close()
    return count


def execute_thread_for_fetching(code_set, source, patient_list, code_type, function_to_run):
    """
    Threads for running fetch queries parallel.
    """
    protocol = PROTOCOL
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)
    processed = 0
    total_patients = len(patient_list)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        future_to_code = {executor.submit(function_to_run, patient, code_set, source, smart): patient for patient in
                          patient_list}
        patient_counter = 0
        for future in as_completed(future_to_code):
            patient = future_to_code[future]
            processed += 1
            try:
                count = future.result()
                if count > 0:
                    patient_counter += 1
                print(f"[{processed}/{total_patients}] {patient} with {count} {code_type} entries processed")
            except Exception as exc:
                print(f"[{processed}/{total_patients}] [{code_type}] {patient} generated an exception: {exc}")

    ###META DATA COLLECTION###
    '''
    patient_count_with_observations: Number of cohort patients that has at least one observation
    patient_count_with_medications: Number of cohort patients that has at least one medication
    conditions_counts: Frequency of each ICD code 
    observations_counts:Frequency of each LOINC code 
    medication_counts: Frequency of each ATC code 
    '''

    if code_type == "LOINC":
        gather_metadata("patient_count_with_observations", patient_counter)
    elif code_type == "ATC":
        if source is MedicationAdministration:
            gather_metadata("patient_count_with_medicationAdministrations", patient_counter)
        elif source is MedicationRequest:
            gather_metadata("patient_count_with_medicationRequests", patient_counter)
        elif source is MedicationStatement:
            gather_metadata("patient_count_with_medicationStatements", patient_counter)
    else:
        pass
    print("---------------End of Code------------------------")


def observation_frequencies(code_file):
    folder_path = "fhir_results/LOINC"
    observations_counts = defaultdict(int)
    code_list = read_input_code_file(code_file)

    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as json_file:
                for line in json_file:
                    observation = json.loads(line)
                    resource = observation.get("resource", {})
                    codings = resource.get("code", {}).get("coding", [])
                    for coding in codings:
                        if LOINC_SYSTEM_NAME == coding['system'] and coding['code'] in code_list:
                            observations_counts[coding['code']] += 1

    for code, frequency in observations_counts.items():
        print(f"{code}: {frequency}")
    gather_metadata("observations_counts", observations_counts)


def conditions_frequencies(code_file):
    folder_path = "fhir_results/ICD"
    code_list = read_input_code_file(code_file)
    conditions_counts = defaultdict(int)

    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as json_file:
                for line in json_file:
                    condition = json.loads(line)
                    resource = condition.get("resource", {})
                    codings = resource.get("code", {}).get("coding", [])
                    for coding in codings:
                        if ICD_SYSTEM_NAME == coding['system'] and coding['code'] in code_list:
                            conditions_counts[coding['code']] += 1

    gather_metadata("conditions_counts", conditions_counts)


def fetch_atc_codes(resource_ref, code_list, smart):
    system = ATC_SYSTEM_NAME
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
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= PROTOCOL)
    folder_paths = ["fhir_results/ATC/Administrations", "fhir_results/ATC/Requests", "fhir_results/ATC/Statements"]
    code_list = read_input_code_file(code_file)
    system = ATC_SYSTEM_NAME
    protocol = PROTOCOL
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
                    for line in json_file:
                        medicationProfile = json.loads(line)
                        if 'resource' in medicationProfile:
                            resource_type = medicationProfile['resource']['resourceType']
                            resource_ref = medicationProfile['resource']['medicationReference']['reference']

                            try:
                                code_name = fetch_atc_codes(resource_ref, code_list, smart)
                            except Exception as exc:
                                print(f"Generated an exception: {exc} but continue trying.\n")
                                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)
                                time.sleep(3)

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

        if "Administrations" in folder_path:
            gather_metadata("medicationAdministrations_counts", resource_structure)
        elif "Requests" in folder_path:
            gather_metadata("medicationRequests_counts", resource_structure)
        elif "Statements" in folder_path:
            gather_metadata("medicationStatements_counts", resource_structure)
