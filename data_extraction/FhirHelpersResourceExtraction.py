import bisect
import os
import logging
from collections import defaultdict
import json
import time

import numpy as np
import requests
from fhirclient.models.condition import Condition
from fhirclient.models.medication import Medication
from fhirclient.models.medicationadministration import MedicationAdministration
from fhirclient.models.medicationrequest import MedicationRequest
from fhirclient.models.medicationstatement import MedicationStatement
from fhirclient.models.list import List as MedicationList
from fhirclient.server import FHIRNotFoundException
from Constants import (USER_NAME, USER_PASSWORD, ICD_SYSTEM_NAME, LOINC_SYSTEM_NAME, OPS_SYSTEM_NAME,
                       ATC_SYSTEM_NAME, ASTHMA_COPD_CODES_FILE, PROTOCOL, DISCHARGE_CODE_CONTEXT_URL)
from Utils import connect_to_server, fetch_bundle_for_code, parse_fhir_datetime
from Metadata import gather_metadata


def read_input_code_file(filename):
    """
    :param filename:  input file of code list
    :return: code_list
    """
    with open(filename, "r") as fp:
        lines = json.load(fp)

        if 'loinc_codes' in filename:
            os.makedirs(f"fhir_results/Observations/", exist_ok=True)
            code_list = [item['code'] for item in lines['codes']]

        elif 'icd_codes' in filename:
            os.makedirs("fhir_results/Conditions", exist_ok=True)
            code_list = [code for item in lines['codes'] for code in item['code']]

        elif 'ops_codes' in filename:
            os.makedirs("fhir_results/Procedures", exist_ok=True)
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
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)
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
    system_name = LOINC_SYSTEM_NAME
    while True:
        try:
            response = smart.server.post_as_form(url=f"{smart.server.base_uri}/Observation/_search",
                                                 formdata={'_count': '1000', 'subject': patient})
            bundle = response.json()
            break
        except Exception as exc:
            logging.error(f"Generated an exception: {exc} but continue trying.\n")
            time.sleep(3)
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)

    count = 0
    file = None
    try:
        for entries in fetch_bundle_for_code(smart, bundle, protocol):
            for observation in entries:
                resource = observation.get("resource", {})
                codings = resource.get("code", {}).get("coding", [])
                for coding in codings:
                    if system_name == coding['system'] and coding['code'] in code_set:
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
                    response = smart.server.post_as_form(url=f"{smart.server.base_uri}/Condition/_search",
                                                         formdata={'_count': '1000', 'subject': patient,
                                                                   'code': sub_code_list_str})
                    bundle = response.json()
                    break
                except Exception as exc:
                    logging.error(f"Generated an exception: {exc} but continue trying.\n")
                    time.sleep(3)
                    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)

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
    discharge_code = DISCHARGE_CODE_CONTEXT_URL
    if source is MedicationAdministration:
        whole_path = "fhir_results/Medications/Administration/" + patient_id + "_patient_medicationAdministration.json"
    elif source is MedicationRequest:
        whole_path = "fhir_results/Medications/Request/" + patient_id + "_patient_medicationRequest.json"
    elif source is MedicationStatement:
        whole_path = "fhir_results/Medications/Statement/" + patient_id + "_patient_medicationStatement.json"
    elif source is MedicationList:
        statement_id = patient_id
        whole_path = "fhir_results/Medications/List/" + statement_id + "_statementRef_medicationList.json"
    while True:
        try:
            if source == MedicationList:
                statement_ref = patient  #  Attention: Here "patient" is not a patient id, but a statement id !!
                bundle = smart.server.request_json(source.where(
                    struct={'_count': '1000', 'item': statement_ref, 'code': discharge_code}).construct())
            else:
                bundle = smart.server.request_json(source.where(
                    struct={'_count': '1000', 'patient': patient, 'medication.code': code_list_str}).construct())
            break
        except FHIRNotFoundException:
            logging.warning(f"Exception. In {source}, resource {patient} missing or deleted. Skipping...")
            return 0
        except Exception as exc:
            logging.error(f"Generated an exception: {exc} but continue trying.\n")
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=PROTOCOL)
            time.sleep(3)
    count = 0
    file = None
    try:
        for entries in fetch_bundle_for_code(smart, bundle, PROTOCOL):
            for medication_profile in entries:
                if file is None:
                    file = open(whole_path, "w")
                json.dump(medication_profile, file, separators=(",", ":"))
                file.write("\n")
                count += 1
    finally:
        if file is not None:
            file.close()
    return count


def build_statement_medication_map(directory):
    if os.path.exists(directory) and len(os.listdir(directory)) > 0:
        statement_to_medication = {}
        for file_name in os.listdir(directory):
            if file_name.endswith(".json"):
                file_path = os.path.join(directory, file_name)
                with open(file_path, "r") as f:
                    for line in f:
                        try:
                            entry = json.loads(line)
                            resource = entry.get("resource", {})
                            statement_id = resource.get("id")
                            medication_ref = (resource.get("medicationReference", {}).get("reference")  )

                            if statement_id and medication_ref:
                                statement_ref = f"MedicationStatement/{statement_id}"
                                statement_to_medication[statement_ref] = medication_ref
                        except json.JSONDecodeError as e:
                            print(f"Error decoding line in file {file_name}: {e}")

        logging.info(f"A total of {len(statement_to_medication)} statements as reference were enlisted.")
        return statement_to_medication
    else:
        logging.warning(f"No medication statements were found in '{directory}'.")
        return {}


def procedures(patient, code_set, source, smart):
    patient_id = patient.split("/")[-1]
    whole_path = f"fhir_results/Procedures/{patient_id}_patient_procedures.json"
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
            smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)

    count = 0
    file = None
    try:
        for entries in fetch_bundle_for_code(smart, bundle, protocol):
            for procedure in entries:
                resource = procedure.get("resource", {})
                codings = resource.get("code", {}).get("coding", [])
                for coding in codings:
                    if coding.get("system") == OPS_SYSTEM_NAME and coding.get("code") in code_set:
                        if file is None:
                            file = open(whole_path, "w")
                        json.dump(resource, file, separators=(",", ":"))
                        file.write("\n")
                        count += 1
                        break
    finally:
        if file is not None:
            file.close()
    return count


def encounters(encounter_ids, smart):
    protocol = PROTOCOL
    os.makedirs("fhir_results/Encounters", exist_ok=True)
    output_file = "fhir_results/Encounters/encounters.jsonl"
    total = len(encounter_ids)
    with open(output_file, "w") as file:
        for count, encounter in enumerate(encounter_ids, start=1):
            logging.info(f"Processing Encounter {count}/{total}")
            while True:
                try:
                    response = smart.server.post_as_form(
                        url=f"{smart.server.base_uri}/Encounter/_search", formdata={"_id": encounter})
                    bundle = response.json()
                    break
                except Exception as exc:
                    logging.error(f"Generated an exception: {exc} but continue trying.\n")
                    time.sleep(3)
                    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)

            entries = bundle.get("entry", [])
            if not entries or not entries[0].get("resource"):
                continue

            resource = entries[0]["resource"]
            file.write(json.dumps(resource, separators=(",", ":")) + "\n")


def fetch_patients(patients_list, smart):
    os.makedirs("fhir_results/Patients", exist_ok=True)
    whole_path = f"fhir_results/Patients/patients.jsonl"
    protocol = PROTOCOL

    with open(whole_path, "w") as file:
        for patient_id in patients_list:
            patient_id = patient_id.split("/")[-1]

            while True:
                try:
                    patient = smart.server.request_json(f"Patient/{patient_id}")
                    break
                except FHIRNotFoundException:
                    logging.warning(f"Patient/{patient_id} not found, skipping.")
                    patient = None
                    break
                except Exception as exc:
                    logging.error(f"Generated an exception: {exc} but continue trying.\n")
                    time.sleep(3)
                    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)

            if patient:
                json.dump(patient, file, separators=(",", ":"))
                file.write("\n")
                logging.info(f"Patient resource for patient ID: {patient_id} extracted.")


def observation_frequencies_and_distributions(code_file):
    folder_path = "fhir_results/Observations"
    observations_counts = defaultdict(lambda: defaultdict(int))
    code_set = set(read_input_code_file(code_file))
    values_by_code = defaultdict(list)
    counter = 0
    for filename in os.listdir(folder_path):
        if filename.endswith(".json"):
            file_path = os.path.join(folder_path, filename)
            with open(file_path, 'r') as json_file:
                counter += 1
                print("Processing " + str(counter) + "\n")
                for line in json_file:
                    observation = json.loads(line)
                    resource = observation.get("resource", {})
                    codings = resource.get("code", {}).get("coding", [])

                    matched_codes = {c['code'] for c in codings if
                                     c.get('system') == LOINC_SYSTEM_NAME and c['code'] in code_set}
                    if not matched_codes:
                        continue
                    value_quantity = resource.get("valueQuantity")
                    if value_quantity is not None:
                        value = value_quantity.get("value")
                        if value is not None:
                            for code in matched_codes:
                                values_by_code[code].append(value)

                    # Counts by year
                    effective_datetime = resource.get("effectiveDateTime")

                    if effective_datetime:
                        try:
                            year = parse_fhir_datetime(effective_datetime).year
                        except ValueError:
                            logging.warning("Invalid effectiveDateTime format: %s", effective_datetime)
                            continue

                        for code in matched_codes:
                            observations_counts[year][code] += 1

    observations_value_histograms = build_histogram_entries_for_observations(values_by_code)
    gather_metadata("observations_value_histograms", observations_value_histograms)
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
                    resource = json.loads(line)
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
                            if OPS_SYSTEM_NAME == coding.get("system") and coding.get("code") in code_list:
                                procedure_counts[year][coding['code']] += 1
    # Gather metadata
    gather_metadata("procedures_counts", dict(sorted(procedure_counts.items())))


def fetch_atc_codes(resource_ref, code_list, smart):
    system = ATC_SYSTEM_NAME
    try:
        source, medication_reference_id = resource_ref.split('/')
        if source:
            medication = Medication.read(medication_reference_id, smart.server)

            if medication.code and medication.code.coding:
                for coding in medication.code.coding:
                    if system == coding.system and coding.code in code_list:
                        return coding.code
            return None
    except requests.exceptions.RequestException:
        raise  # real connection problem: try reconnect
    except FHIRNotFoundException:
        logging.warning(f"Medication not found for reference {resource_ref!r}. Skipping.")
        return None
    except Exception as exc:
        logging.error(f"Unexpected error resolving ATC code for {resource_ref!r}: {exc}")
        return None

def medication_frequencies(code_file):
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=PROTOCOL)
    folder_paths = ["fhir_results/Medications/Administration",
                    "fhir_results/Medications/Request",
                    "fhir_results/Medications/Statement",
                    "fhir_results/Medications/List"]
    code_list = read_input_code_file(code_file)
    protocol = PROTOCOL
    atc_cache = {} # to keep track of Medication ID->ATC mapping to avoid redundant calls to "fetch_atc_code" function
    #build MedicationStatement -> Medication reference map
    statement_medication_map = build_statement_medication_map("fhir_results/Medications/Statement/")
    for folder_path in folder_paths:
        medication_type_and_med_reference = defaultdict(lambda: defaultdict(int))
        resource_structure = defaultdict(lambda: {
            "counting": {
                "total_count": 0,
                "details_count": [],
            }})

        if 'List' in folder_path:
            patient_refs = set()

            for file in os.listdir(folder_path):
                if not file.endswith(".json") or "statementRef" not in file:
                    continue

                reference_id = file.split("_statementRef")[0]
                try:
                    with open(os.path.join(folder_path, file), encoding="utf-8") as f:
                        for line in f:
                            bundle = json.loads(line)
                            #Check the List actually contains this MedicationStatement
                            if not find_medication_statement_ref(bundle, reference_id):
                                continue
                            patient_ref = bundle.get("resource", {}).get("subject", {}).get("reference")
                            if patient_ref:
                                patient_refs.add(patient_ref)

                            statement_ref = f"MedicationStatement/{reference_id}"
                            medication_ref = statement_medication_map.get(statement_ref)
                            if not medication_ref:
                                continue
                            while True:
                                try:
                                    if medication_ref in atc_cache:
                                        code_name = atc_cache[medication_ref]
                                    else:
                                        code_name = fetch_atc_codes(medication_ref, code_list, smart)
                                        atc_cache[medication_ref] = code_name
                                    break
                                except Exception as exc:
                                    logging.error(f"Generated an exception: {exc} but continue trying.\n")
                                    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)
                                    time.sleep(3)
                            if code_name:
                                medication_type_and_med_reference["List"][code_name] += 1

                except Exception as exc:
                    logging.error(f"Error in file {file}: {exc}. Skipping...")
                    continue

            # Get patients count from List
            gather_metadata("patient_count_with_medicationList", len(patient_refs))
        else:
            # Gathering, counting and fetching ID-references for "Medication".
            for filename in os.listdir(folder_path):
                if filename.endswith(".json"):
                    file_path = os.path.join(folder_path, filename)
                    with (open(file_path, 'r') as json_file):
                        for line in json_file:
                            medication_profile = json.loads(line)
                            resource = medication_profile.get("resource", {})

                            if not resource:
                                continue
                            resource_type = resource.get("resourceType")
                            resource_ref = resource.get("medicationReference", {}).get("reference")

                            if not resource_ref:
                                continue
                            while True:
                                try:
                                    if resource_ref in atc_cache:
                                        code_name = atc_cache[resource_ref]
                                    else:
                                        code_name = fetch_atc_codes(resource_ref,code_list,  smart )
                                        atc_cache[resource_ref] = code_name
                                    break
                                except Exception as exc:
                                    logging.error(f"Generated an exception: {exc} but continue trying." )
                                    smart = connect_to_server(user=USER_NAME,   pw=USER_PASSWORD,  protocol=protocol)
                                    time.sleep(3)
                            if code_name:
                                medication_type_and_med_reference[resource_type][code_name] += 1
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


def find_medication_statement_ref(bundle, reference_id):
    resource = bundle.get("resource", {})
    expected_ref = f"MedicationStatement/{reference_id}"

    for item in resource.get("entry", []):
        med_statement_ref = item.get("item", {}).get("reference")

        if med_statement_ref == expected_ref:
            return True

    return False

def find_num_bins(n, min_bins=5, max_bins=20):
    return max(min_bins, min(max_bins, int(np.sqrt(n))))


def build_histogram_entries_for_observations(values_by_code):
    # Build histograms
    observations_value_histograms = {}
    for code, values in values_by_code.items():
        n = len(values)

        if n == 0:
            continue

        lo = min(values)
        hi = max(values)

        if lo == hi:
            hi = lo + 1.0

        num_bins = find_num_bins(n)
        step = (hi - lo) / num_bins
        edges = [lo + i * step for i in range(num_bins + 1)]
        counts = [0] * num_bins

        for value in values:
            bin_index = bisect.bisect_right(edges, value) - 1
            bin_index = max(0, min(bin_index, num_bins - 1))
            counts[bin_index] += 1

        observations_value_histograms[code] = {
            "n": n,
            "bin_edges": [round(edge, 4) for edge in edges],
            "counts": counts,
        }
    return observations_value_histograms
