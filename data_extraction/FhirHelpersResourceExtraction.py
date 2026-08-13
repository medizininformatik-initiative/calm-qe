import os
import logging
from collections import defaultdict
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed

from fhirclient.models.condition import Condition
from fhirclient.models.medication import Medication
from fhirclient.models.medicationadministration import MedicationAdministration
from fhirclient.models.medicationrequest import MedicationRequest
from fhirclient.models.medicationstatement import MedicationStatement
from fhirclient.models.list import List as MedicationList

from Constants import (USER_NAME, USER_PASSWORD, ICD_SYSTEM_NAME, LOINC_SYSTEM_NAME, OPS_SYSTEM_NAME, MAX_WORKERS,
                       ATC_SYSTEM_NAME, ASTHMA_COPD_CODES_FILE, PROTOCOL)
from FhirHelpersUtils import connect_to_server, fetch_bundle_for_code, parse_fhir_datetime
from Metadata import gather_metadata


def read_input_code_file(filename):
    """
    :param filename:  input file of code list
    :return: code_list
    """
    with open(filename, "r") as fp:
        lines = json.load(fp)

        if 'loinc_codes' in filename:
            if not os.path.exists(f"fhir_results/Observations/"):
                os.makedirs(f"fhir_results/Observations/")
            code_list = [item['code'] for item in lines['codes']]

        elif 'icd_codes' in filename:
            if not os.path.exists(f"fhir_results/Conditions/"):
                os.makedirs(f"fhir_results/Conditions/")
            code_list = [code for item in lines['codes'] for code in item['code']]

        elif 'ops_codes' in filename:
            if not os.path.exists(f"fhir_results/Procedures/"):
                os.makedirs(f"fhir_results/Procedures/")
            code_list = [item['code'] for item in lines['codes']]

        elif 'atc_codes' in filename:
            if not os.path.exists(f"fhir_results/Medications/"):
                os.makedirs(f"fhir_results/Medications/")
                os.makedirs(f"fhir_results/Medications/Administration/")
                os.makedirs(f"fhir_results/Medications/Request/")
                os.makedirs(f"fhir_results/Medications/Statement/")
                os.makedirs(f"fhir_results/Medications/List/")
            code_list = [code['code'] for code in lines]

    return code_list


def patients_with_asthma_copd(smart, results_path):
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
                logging.error(f"Generated an exception: {exc} but continue trying.\n")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)
                time.sleep(3)

        for entries in fetch_bundle_for_code(smart, bundle, protocol):
            for entry in entries:
                condition = entry['resource']
                if condition['subject']['reference']:
                    patient_reference = condition['subject']['reference']
                    patient_attributes_map = {'id': condition['id'], 'code': condition['code']}

                    if 'encounter' in condition and "reference" in condition["encounter"]:
                        if condition['encounter']['reference']:  # New: Include encounter reference
                            patient_attributes_map["encounter"] = condition['encounter']['reference'].split("/")[1]
                    else:
                        logging.warning(f"No encounter or encounter-reference found for Condition/{condition['id']}")

                    if condition['recordedDate']:  # New: Include recordedDate from conditions
                        patient_attributes_map["recordedDate"] = condition['recordedDate']
                    patients_conditions_map[patient_reference].append(patient_attributes_map)

    gather_metadata("asthma_and_copd_patient_count", len(patients_conditions_map))

    output_filepath = results_path / f"patients_diagnosed_asthma_copd.json"
    with open(output_filepath, 'w') as file:  # Intermediate results.
        json.dump(patients_conditions_map, file, indent=4)
    logging.info(f"Saved .json file {output_filepath}")
    return output_filepath


def observations(patient, code_set, source, smart):
    patient_id = patient.split("/")[-1]
    whole_path = f"fhir_results/Observations/{patient_id}_patient_observations.json"
    protocol = PROTOCOL
    while True:
        try:
            response = smart.server.post_as_form(url=f"{smart.server.base_uri}/Observation/_search", formdata={'_count': '1000', 'subject': patient})
            bundle = response.json()
            break
        except Exception as exc:
            logging.error(f"Generated an exception: {exc} but continue trying.\n")
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
    whole_path = "fhir_results/Conditions/" + patient_id + "_patient_conditions.json"
    protocol = PROTOCOL
    sub_code_lists = [code_list[i:i + 30] for i in range(0, len(code_list), 30)]  # smaller chunks of code list

    count = 0
    file = None
    try:
        for sub_code_list in sub_code_lists:
            sub_code_list_str = ','.join([ICD_SYSTEM_NAME + '|' + code for code in sub_code_list])
            while True:
                try:
                    response = smart.server.post_as_form(url=f"{smart.server.base_uri}/Condition/_search", formdata={'_count': '1000', 'subject': patient, 'code': sub_code_list_str})
                    bundle = response.json()

                    break
                except Exception as exc:
                    logging.error(f"Generated an exception: {exc} but continue trying.\n")
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
        whole_path = "fhir_results/Medications/Administration/" + patient_id + "_patient_medicationAdministration.json"
    elif source is MedicationRequest:
        whole_path = "fhir_results/Medications/Request/" + patient_id + "_patient_medicationRequest.json"
    elif source is MedicationStatement:
        whole_path = "fhir_results/Medications/Statement/" + patient_id + "_patient_medicationStatement.json"
    elif source is MedicationList:
        whole_path = "fhir_results/Medications/List/" + patient_id + "_patient_medicationList.json"

    while True:
        try:
            if source == Medication:
                bundle = smart.server.request_json(
                    source.where(struct={'_count': '1000', 'subject': patient, 'code': code_list_str}).construct())
            elif source == MedicationList:
                bundle = smart.server.request_json(
                    source.where(struct={'_count': '1000', 'subject': patient, 'code': 'E230'}).construct())
            else:
                bundle = smart.server.request_json(source.where(
                    struct={'_count': '1000', 'patient': patient, 'medication.code': code_list_str}).construct())
            break
        except Exception as exc:
            logging.error(f"Generated an exception: {exc} but continue trying.\n")
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

def procedures(patient, code_set, smart):
    patient_id = patient.split("/")[-1]
    whole_path = f"fhir_results/Procedures/{patient_id}_patient_procedures.json"
    logging.info(f"Fetching patient encounters...{patient_id}")
    protocol = PROTOCOL
    while True:
        try:
            response = smart.server.post_as_form(
                url=f"{smart.server.base_uri}/Procedure/_search",
                formdata={'_count': '1000', 'subject': patient})
            bundle = response.json()
            break
        except Exception as exc:
            logging.error(f"Generated an exception: {exc} but continue trying.\n")
            time.sleep(3)
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)

    count = 0
    file = None
    try:
        for entries in fetch_bundle_for_code(smart, bundle, protocol):
            for procedure in entries:
                resource = procedure.get("resource", {})
                codings = resource.get("code", {}).get("coding", [])
                for coding in codings:
                    if OPS_SYSTEM_NAME == coding['system'] and coding['code'] in code_set:
                        if file is None:
                            file = open(whole_path, "w")
                        json.dump(procedure, file, separators=(",", ":"))
                        file.write("\n")
                        count += 1
    finally:
        if file is not None:
            file.close()
    return count

def encounters(encounter, smart):
    if not os.path.exists(f"fhir_results/Encounters/"):
        os.makedirs(f"fhir_results/Encounters/")

    whole_path = f"fhir_results/Encounters/{encounter}_encounter.json"
    protocol = PROTOCOL
    while True:
        try:
            response = smart.server.post_as_form(
                url=f"{smart.server.base_uri}/Encounter/_search",
                formdata={"_id": encounter})
            bundle = response.json()
            break
        except Exception as exc:
            logging.error(f"Generated an exception: {exc} but continue trying.\n")
            time.sleep(3)
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)

    res_count = 0
    file = None
    try:
        for entries in fetch_bundle_for_code(smart, bundle, protocol):
            if entries:
                for encounter in entries:
                    resource = encounter.get("resource", {})
                    if resource and file is None:
                        file = open(whole_path, "w")
                        json.dump(encounter, file, separators=(",", ":"))
                        file.write("\n")
                        res_count += 1
    finally:
        if file is not None:
            file.close()
    return res_count

def fetch_patients(patient_id, smart):
    if not os.path.exists(f"fhir_results/Patients/"):
        os.makedirs(f"fhir_results/Patients/")

    whole_path = f"fhir_results/Patients/{patient_id}_patient.json"
    protocol = PROTOCOL
    while True:
        try:
            response = smart.server.post_as_form(
                url=f"{smart.server.base_uri}/Patient/_search",
                formdata={"_id": patient_id})
            bundle = response.json()
            break
        except Exception as exc:
            logging.error(f"Generated an exception: {exc} but continue trying.\n")
            time.sleep(3)
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)

    res_count = 0
    file = None
    try:
        for entries in fetch_bundle_for_code(smart, bundle, protocol):
            if entries:
                for patient in entries:
                    resource = patient.get("resource", {})
                    if resource and file is None:
                        file = open(whole_path, "w")
                        json.dump(patient, file, separators=(",", ":"))
                        file.write("\n")
                        res_count += 1
    finally:
        if file is not None:
            file.close()
    return res_count


def execute_thread_for_fetching(code_set, source, patient_list, code_type, function_to_run):
    """
    Threads for running fetch queries parallel.
    """
    protocol = PROTOCOL
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)
    processed = 0
    total_patients = len(patient_list)
    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        if not code_set and code_type is None:
            future_to_code = {executor.submit(function_to_run, patient, smart): patient for patient in
                              patient_list}
        else:
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
                logging.info(f"[{processed}/{total_patients}] {patient} with {count} {code_type} entries processed")
            except Exception as exc:
                logging.error(f"[{processed}/{total_patients}] [{code_type}] {patient} generated an exception: {exc}")

    ###META DATA COLLECTION###
    '''
    patient_count_with_observations: Number of cohort patients that has at least one observation
    patient_count_with_medications: Number of cohort patients that has at least one medication
    conditions_counts: Frequency of each ICD code 
    observations_counts:Frequency of each LOINC code 
    medication_counts: Frequency of each ATC code 
    procedures_counts: Frequency of each OPS code 
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
        elif source is MedicationList:
            gather_metadata("patient_count_with_medicationList", patient_counter)
    elif code_type == "OPS":
        gather_metadata("patient_count_with_procedures", patient_counter)
    else:
        pass
    logging.info("---------------End of Code------------------------")


def observation_frequencies(code_file):
    folder_path = "fhir_results/Observations"
    observations_counts = defaultdict(lambda: defaultdict(int))
    code_list = read_input_code_file(code_file)

    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as json_file:
                for line in json_file:
                    observation = json.loads(line)
                    resource = observation.get("resource", {})
                    codings = resource.get("code", {}).get("coding", [])
                    effective_datetime = resource.get("effectiveDateTime", {})
                    if effective_datetime:
                        try:
                            date = parse_fhir_datetime(effective_datetime)
                            year = date.date().year
                        except ValueError:
                            logging.warning("Invalid year format, skipping...")
                            continue

                        for coding in codings:
                            if LOINC_SYSTEM_NAME == coding['system'] and coding['code'] in code_list:
                                observations_counts[year][coding['code']] += 1

    # Gather metadata
    gather_metadata("observations_counts", dict(sorted(observations_counts.items())))


def conditions_frequencies(code_file):
    folder_path = "fhir_results/Conditions"
    code_list = read_input_code_file(code_file)
    conditions_counts = defaultdict(lambda: defaultdict(int))

    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as json_file:
                for line in json_file:
                    condition = json.loads(line)
                    resource = condition.get("resource", {})
                    codings = resource.get("code", {}).get("coding", [])
                    recorded_date = resource.get("recordedDate", {})
                    if recorded_date:
                        try:
                            year = parse_fhir_datetime(recorded_date).date().year
                        except ValueError:
                            logging.warning("Invalid year format, skipping...")
                            continue

                        for coding in codings:
                            if ICD_SYSTEM_NAME == coding['system'] and coding['code'] in code_list:
                                conditions_counts[year][coding['code']] += 1
    # Gather metadata
    gather_metadata("conditions_counts", dict(sorted(conditions_counts.items())))

def procedure_frequencies(code_file):
    folder_path = "fhir_results/Procedures"
    code_list = read_input_code_file(code_file)
    procedure_counts = defaultdict(lambda: defaultdict(int))

    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as json_file:
                for line in json_file:
                    procedure = json.loads(line)
                    resource = procedure.get("resource", {})
                    codings = resource.get("code", {}).get("coding", [])
                    performed_date = resource.get("performedDateTime", {})
                    if performed_date:
                        try:
                            date = parse_fhir_datetime(performed_date)
                            year = date.date().year
                        except ValueError:
                            logging.warning("Invalid year format, skipping...")
                            continue
                        for coding in codings:
                            if OPS_SYSTEM_NAME == coding['system'] and coding['code'] in code_list:
                                procedure_counts[year][coding['code']] += 1
    # Gather metadata
    gather_metadata("procedures_counts", dict(sorted(procedure_counts.items())))


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
        logging.error(f"Generated an exception:{error} for {resource_ref}")


def medication_frequencies(code_file):
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= PROTOCOL)
    folder_paths = ["fhir_results/Medications/Administration",
                    "fhir_results/Medications/Request",
                    "fhir_results/Medications/Statement",
                    "fhir_results/Medications/List"]
    code_list = read_input_code_file(code_file)
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
                                logging.error(f"Generated an exception: {exc} but continue trying.\n")
                                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)
                                time.sleep(3)

                            if resource_type not in medication_type_and_med_reference:
                                medication_type_and_med_reference[resource_type] = {}
                            medication_type_and_med_reference[resource_type][code_name] = (
                                    medication_type_and_med_reference[resource_type].get(code_name, 0) + 1)
                        else:
                            logging.info(f"{filename}  has no 'resource' statement within this file.")

        # Estimates TOTAL counts per medication resource and structures data as outcomes
        for resource_type, num_references in medication_type_and_med_reference.items():
            total_count = sum(num_references.values())
            details_count = [{ref: count} for ref, count in num_references.items()]

            resource_structure[resource_type]["counting"]["total_count"] = total_count
            resource_structure[resource_type]["counting"]["details_count"] = details_count

        if "Administration" in folder_path:
            gather_metadata("medicationAdministrations_counts", resource_structure)
        elif "Request" in folder_path:
            gather_metadata("medicationRequests_counts", resource_structure)
        elif "Statement" in folder_path:
            gather_metadata("medicationStatements_counts", resource_structure)
        elif "List" in folder_path:
            gather_metadata("medicationList_counts", resource_structure)
