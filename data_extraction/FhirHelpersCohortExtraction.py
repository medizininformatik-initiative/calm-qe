import os
import logging
from collections import defaultdict
import json
import time
from datetime import datetime
from pathlib import Path

import numpy as np
import pandas as pd
from fhirclient.models import encounter

from fhirclient.models.condition import Condition
from fhirclient.models.encounter import Encounter
from fhirclient.models.patient import Patient
from fhirclient.server import FHIRNotFoundException

from Constants import USER_NAME, USER_PASSWORD, ACT_ENCOUNTER_TYPE_URL
from Utils import fetch_bundle_for_code, connect_to_server
from Utils import parse_fhir_datetime, compute_los
from Metadata import gather_metadata

basis_filename = "patients_diagnosed_asthma_copd"


def generate_output_filename(surfix_filename, directory):
    input_path = Path(directory)
    target_file = input_path.stem

    if basis_filename in target_file:
        return f"patients_{surfix_filename}.json"
    else:
        return f"{surfix_filename}.jsonl"


def process_inpatient_encounter(resource):
    inpatient_types = ["stationaer", "normalstationaer", "intensivstationaer"]

    is_inpatient = False
    for type_entry in resource.get("type", []):
        for coding in type_entry.get("coding", []):
            code_val = coding.get("code", "").lower()
            if code_val.lower() in [inpatient.lower() for inpatient in inpatient_types] or code_val.upper() == "IMP":
                is_inpatient = True
                break
        if is_inpatient:
            break
    if not is_inpatient and "hospitalization" in resource:
        is_inpatient = True

    if not is_inpatient:
        return None

    # Extract period
    period = resource.get("period", {})
    start = parse_fhir_datetime(period.get("start"))
    end = parse_fhir_datetime(period.get("end"))

    # Process LOS
    los_days = compute_los(start, end)

    return {
        "encounter": resource.get("id"),
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "days": round(los_days, 2) if los_days else None
    }


def filter_patients_by_age_interval(smart, input_filepath, min_age, max_age, enabled=True):
    if not enabled:
        return None

    if not isinstance(min_age, int) or not isinstance(max_age, int):
        raise ValueError("'min_age' and 'max_age' must be integers")
    if min_age > max_age:
        raise ValueError("min_age must be <= max_age")

    logging.info(f"\nFiltering patients with age in interval [{min_age}, {max_age}] years...")

    matched_patients = defaultdict(list)
    total_processed = 0

    with open(input_filepath, "r", encoding="utf-8") as f:
        patient_encounters = json.load(f)

    for patient_ref, encounter_attribs in patient_encounters.items():
        total_processed += 1
        patient_id = patient_ref.split("/")[-1]
        logging.info(f"Processing patient {patient_id} by age interval {min_age} to {max_age}...")

        birth_date = None
        while True:
            try:
                patient = Patient.read(patient_id, smart.server)

                if patient.birthDate is None:
                    logging.warning(f"Skipping patient {patient_id} - no birth date available.")
                    break

                birth_iso = getattr(patient.birthDate, 'isostring', None) if patient.birthDate else None
                if not birth_iso:
                    logging.warning(f"Skipping patient {patient_id} - birth date has no attribute isostring.")
                    break

                birth_date = parse_fhir_datetime(birth_iso)
                break
            except Exception as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if 410 or 404 in status:
                    logging.warning(f"Exception {status}. Patient {patient_id} missing or deleted. Skipping..")
                    birth_date = None
                    break
                logging.error(f"Error fetching patient {patient_id}, status {status}: {exc}, but continue to trying...")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                time.sleep(1)

        if not birth_date:
            logging.warning(f"Skipping patient {patient_id} - unable to parse.")
            continue

        for enc in encounter_attribs:
            start = enc.get("start")
            if not start:
                continue

            start_date = parse_fhir_datetime(start)
            if not start_date:
                continue

            try:
                days = (start_date.date() - birth_date.date()).days
                if days < 0:
                    continue
                age_years = round((days / 365), 2) if days < 365 else int(days // 365)

            except Exception as e:
                logging.warning(f"Skipping patient {patient_ref}. {e}.")
                continue

            if min_age <= age_years <= max_age:
                matched_patients[patient_ref].append({
                    "condition": enc["condition"],
                    "start": enc.get("start"),
                    "end": enc.get("end"),
                    "birthdate": birth_date.isoformat() if birth_date else None,
                    "age": age_years
                })

    # gather metadata/counts
    label = f"{min_age}-{max_age}"
    interval_count = len(matched_patients)
    gather_metadata("patient_count_by_age_interval", {label: interval_count})
    logging.info(f"Found {interval_count} patients in interval [{min_age}, {max_age}] out of {total_processed} processed.")

    if interval_count > 0:
        base_path = Path(input_filepath)
        new_filename = generate_output_filename(f"filtered_by_age_interval_{min_age}-{max_age}", input_filepath)

        output_filepath = base_path.with_name(new_filename)
        with open(output_filepath, "w", encoding="utf-8") as out:
            json.dump({pid: entries for pid, entries in matched_patients.items()}, out, indent=4, ensure_ascii=False)
    else:
        logging.warning(f"No count found for patients in interval [{min_age}, {max_age}] ")


def filter_icu_patients_admission(input_filepath, enabled=True):
    """
        From the HauptDiagnosis (Main), filter type of admission, specially ICU patients.
        Reference: https://simplifier.net/guide/mii-ig-modul-fall-2025/
        MIIIGModulFall/TechnischeImplementierung/FHIRProfile/EncounterKontaktGesundheitseinrichtung.page.md?version=current
    """
    if not enabled:
        return None

    logging.info("\nFiltering ICU patients...")
    extracted_encounters_filepath = input_filepath
    icu_encounters = list()
    unique_patient_ids = set()
    mapped_icu_patients_and_encounters = defaultdict(set)

    if os.path.exists(extracted_encounters_filepath):
        with open(extracted_encounters_filepath, "r", encoding="utf-8") as file:
            for counter, line in enumerate(file, start=1):
                line = line.strip()
                if not line:
                    continue
                try:
                    entry = json.loads(line)
                    enc_type = entry.get("type", {})

                    icu_coding = [
                        coding for type_entry in enc_type
                        if "coding" in type_entry
                        for coding in type_entry.get("coding")
                        if coding.get("system") == ACT_ENCOUNTER_TYPE_URL and 'intensiv' in coding.get("code").lower()
                    ]

                    if icu_coding:
                        patient_id = entry.get("subject", {}).get("reference")
                        encounter_id = f"Encounter/{entry.get('id')}"
                        unique_patient_ids.add(patient_id)
                        icu_encounters.append(entry)
                        mapped_icu_patients_and_encounters[patient_id].add(encounter_id)

                except Exception as e:
                    logging.error(f"Error processing ICU for line {counter}: {e}")

    logging.info(f"Unique patients in ICU: {len(unique_patient_ids)} with {len(icu_encounters)} encounters found.")

    # Filter bundles from encounters with those which have an ICU entrance.
    base_path = Path(input_filepath)
    new_filename = generate_output_filename("encounters_with_icu_admission", input_filepath)
    output_filepath = base_path.with_name(new_filename)
    with open(output_filepath, "w", encoding="utf-8") as out:
        for enc_resource in icu_encounters:
            json.dump(enc_resource, out)
            out.write('\n')

    # Export to additional results
    base_path = Path("additional_results")
    output_filepath = base_path / "patients_filtered_by_icu_admission.json"
    icu_patients_json = {pid: list(enc_ids) for pid, enc_ids in mapped_icu_patients_and_encounters.items()}

    with open(output_filepath, "w", encoding="utf-8") as out:
        json.dump(icu_patients_json, out, indent=4)

    gather_metadata("patient_count_in_intensive_care", len(unique_patient_ids))
    return None


def calculate_los_inpatients(smart, input_filepath, enabled=True):
    """
    Aufenthaltsdauer: calculate "Length of Staying", (LOS) from inpatients.
    Reference: https://simplifier.net/guide/mii-ig-modul-fall-2025/
    MIIIGModulFall/TechnischeImplementierung/FHIRProfile/EncounterKontaktGesundheitseinrichtung.page.md?version=current
    """
    if not enabled:
        return None

    logging.info("\nGathering inpatients...")
    extracted_encounters_filepath = input_filepath
    inpatients = defaultdict(list)

    if os.path.exists(extracted_encounters_filepath):
        with open(extracted_encounters_filepath, "r", encoding="utf-8") as file:
            for counter, line in enumerate(file, start=1):
                line = line.strip()
                if not line:
                    continue

                try:
                    entry = json.loads(line)
                    stay_entry = process_inpatient_encounter(entry)
                    if stay_entry:
                        patient_id = entry.get("subject", {}).get("reference")
                        inpatients[patient_id].append(stay_entry)

                except Exception as e:
                    logging.error(f"Error processing LOS in line {counter}: {e}")

    base_path = Path("additional_results")
    output_filepath = base_path / "patients_length_of_stay.json"
    inpatients_json = dict(inpatients)

    with open(output_filepath, "w", encoding="utf-8") as file:
        json.dump(inpatients_json, file, indent=4, ensure_ascii=False)
    logging.info(f"File successfully generated with {len(inpatients)} inpatients")
    return None


def extract_last_three_encounter(input_filepath, enabled=True):
    """
    Extract last three encounter IDs per patient.
    """
    if not enabled:
        return input_filepath

    patients_last_3_encounters = defaultdict(list)
    logging.info("\nFiltering the last three encounters per patient...")

    with open(input_filepath, "r") as file:
        patients = json.load(file)
        for patient in patients.keys():
            attributes_encounter = patients[patient]
            all_encounters_per_patient = []

            for attribute_encounter in attributes_encounter:
                condition = attribute_encounter.get('condition')
                if not condition:
                    logging.warning(f"Missing condition data for encounter. Skipping.")
                    continue
                encounter_id = condition.get('encounter')

                start, end = None, None
                if attribute_encounter.get("start") is not None:
                    parsed_start = parse_fhir_datetime(attribute_encounter.get("start"))
                    start = parsed_start.isoformat() if parsed_start else attribute_encounter.get("start")

                if attribute_encounter.get("end") is not None:
                    parsed_end = parse_fhir_datetime(attribute_encounter["end"])
                    end = parsed_end.isoformat() if parsed_end else attribute_encounter.get("end")

                all_encounters_per_patient.append({
                    'encounter': encounter_id,
                    'start': start,
                    'end': end,
                })

            valid_encounters = [e for e in all_encounters_per_patient if e.get("start")]

            sorted_encounters = sorted(
                valid_encounters,
                key=lambda e: e.get("start"),
                reverse=True
            )

            if sorted_encounters:
                # Keep last 3 encounters
                patients_last_3_encounters[patient] = sorted_encounters[:3]

    base_path = Path(input_filepath)
    new_filename = generate_output_filename("filtered_by_last_3_encounters", input_filepath)
    output_filepath = base_path.with_name(new_filename)
    with open(output_filepath, "w", encoding="utf-8") as file:
        json.dump(patients_last_3_encounters, file, indent=4, ensure_ascii=False)

    logging.info(f"File successfully generated for extracting last three encounters and admission dates for {len(patients_last_3_encounters)} main diagnosed patients")
    return None


def get_demographics_patients(smart, input_filepath, enabled=True):
    '''
    Obtains demographics from patients from selected patient IDs and export results in tabular form.
    Reference: https://www.medizininformatik-initiative.de/Kerndatensatz/
    KDS_Person_V2025/MIIIGModulPerson-TechnischeImplementierung-FHIR-Profile-PatientInPatient.html
    '''
    if not enabled:
        return None

    base_path = Path(input_filepath)
    subdirectory = input_filepath.parent/'csv'
    subdirectory.mkdir(parents=True, exist_ok=True)

    patient_identifiers, patients_demographics = [], []
    non_found_patients = set()

    with open(input_filepath, "r") as file:
        patients = json.load(file)
        for patient in patients.keys():
            logging.info(f"Processing patient with ID: {patient[8:]}...")
            patient_identifiers.append(patient[8:])

    for patient_id in patient_identifiers:
        while True:
            try:
                patient = Patient.read(patient_id, smart.server)

                if patient.birthDate is None:
                    logging.warning(f"Patient {patient_id} has no birthdate available.")
                    break

                birth_iso = getattr(patient.birthDate, 'isostring', None) if patient.birthDate else None
                if not birth_iso:
                    logging.warning(f"Skipping patient {patient_id} - birth date has no attribute isostring.")
                    break
                birth_date = parse_fhir_datetime(birth_iso)

                if patient.gender is None:
                    logging.warning(f"Patient {patient_id} has no gender available.")
                    break
                gender = patient.gender

                patients_demographics.append({
                    "patient": patient_id,
                    "gender": gender,
                    "birthdate": birth_date.isoformat() if birth_date else None,
                })
                break
            except Exception as exc:
                status = getattr(getattr(exc, "response", None), "status_code", None)
                if status in {410, 404}:
                    logging.warning(f"Exception {status}. Patient {patient_id} missing or deleted. Skipping..")
                    non_found_patients.add(f"Patient/{patient_id}")
                    break
                logging.error(f"Generated an exception: {exc} but continue to trying. \n")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                time.sleep(3)

    output_filepath = base_path.parent / "missing_patients.json"
    with open(output_filepath, "w", encoding="utf-8") as file:
        json.dump(list(non_found_patients), file, indent=4, ensure_ascii=False)
    logging.info(f"Saving non-found {len(non_found_patients)} patients as .json {output_filepath}")
    gather_metadata("missing_asthma_and_copd_patients", len(non_found_patients))

    patients_demographics_df = pd.DataFrame(patients_demographics)
    patients_demographics_df.to_csv(os.path.join(subdirectory, "demographics.csv"), index=False, sep=";")
    logging.info(f"Saving extracted demographics as .csv file in {subdirectory}")
    return None


def extract_additional_attributes_from_encounters(smart, input_filepath):
    # Extract interested attributes from encounters (period, fallart, service_department_code)
    contact_system = "http://fhir.de/CodeSystem/kontaktart-de"

    logging.info("Starting additional encounters extraction...")
    encounter_results = defaultdict(list)
    non_found_encounter_results = defaultdict(list)
    base_path = Path(input_filepath)

    with open(input_filepath, "r") as file:
        patients = json.load(file)
        for patient in patients.keys():
            attributes_conditions = patients[patient]
            duplicated_encounter = set()
            for attr_condition in attributes_conditions:
                if 'encounter' not in attr_condition:
                    logging.warning(f'Missing "encounter" in attr_condition for Condition/{attr_condition["id"]}. Skipping.')
                    continue
                encounter_id = attr_condition['encounter'] if isinstance(attr_condition, dict) else attr_condition

                if encounter_id in duplicated_encounter:
                    continue
                duplicated_encounter.add(encounter_id)

                for _ in range(3):
                    try:
                        entry_encounter = Encounter.read(encounter_id, smart.server)
                        enc = {"resource": entry_encounter.as_json()}
                        break
                    except FHIRNotFoundException:
                        logging.warning(f"Encounter {encounter_id} not found. Skipping")
                        non_found_encounter_results[patient].append(encounter_id)
                        enc = None
                        break
                    except Exception as exc:
                        status = getattr(getattr(exc, "response", None), "status_code", None)
                        if status == 410:
                            logging.warning(f"Exception {status}. Encounter {encounter_id} missing or deleted. Skipping")
                            non_found_encounter_results[patient].append(encounter_id)
                            enc = None
                            break

                        logging.warning(f"Generated an exception: {exc} in but continue to trying. \n")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(1)

                if enc is not None:
                    resource = enc.get("resource", {})
                    period = resource.get("period", {})
                    start = period.get("start") if period else None
                    end = period.get("end") if period else None
                    fall_art = resource.get("class", {}).get("code")

                    service_type_code = None
                    service_type_codings = resource.get("serviceType", {}).get("coding", [])
                    if len(service_type_codings) > 0:
                        service_type_code = service_type_codings[0].get("code")

                    type_contact_code = None
                    for type_entry in resource.get("type", []):
                        for coding in type_entry.get("coding", []):
                            if contact_system in coding.get("system", ""):
                                type_contact_code = coding.get("code")

                    encounter_results[patient].append(
                        {
                            "condition": attr_condition,
                            "start": start,
                            "end": end,
                            "case": fall_art,
                            "serviceDepartment": service_type_code,
                            "typeContact": type_contact_code,
                        }
                    )

    # Extended encounters
    encounters_filepath = base_path.with_name(f"{basis_filename}_extended_encounters.json")
    with open(encounters_filepath, 'w') as file:
        json.dump(encounter_results, file, indent=4)

    # Missing encounters
    output_filepath = base_path.parent / f"missing_encounters.json"
    with open(output_filepath, "w", encoding="utf-8") as file:
        json.dump(non_found_encounter_results, file, indent=4, ensure_ascii=False)

    # Export patients summary in csv
    simple_flattening(encounter_results, base_path)

    return encounters_filepath


def simple_flattening(patients_attr_map, path):
    # New: Flatten to export as CSV file
    subdirectory = path.parent / 'csv'
    subdirectory.mkdir(parents=True, exist_ok=True)

    df_rows = []
    for patient_reference, patient_attributes in patients_attr_map.items():
        for attribute in patient_attributes:
            condition_id = attribute.get("condition").get("id")
            attrib_enc = attribute.get("condition")
            code = attribute.get("condition").get("code")
            patient_id = patient_reference.split("/")[1]

            # rename labels to more suitable description of an attribute
            row = {
                'patient': patient_id,
                'condition': condition_id,
                'recordedDiagnosisDate': attrib_enc.get('recordedDate'),
                'encounter': attrib_enc.get('encounter'),
                'admissionDate': attribute.get("start"),
                'dischargeDate': attribute.get("end"),
                'case': attribute.get("case"),
                'serviceDepartment': attribute.get("serviceDepartment"),
                'typeContact': attribute.get("typeContact"),
            }

            # codes from conditions
            if isinstance(code, dict):
                coding_list = code.get('coding', [])
                if coding_list:
                    for code in coding_list:
                        row.update({
                            'code': code.get('code'),
                            'system': code.get('system'),
                            'version': code.get('version')
                        })
            df_rows.append(row)

    # New: reorder columns and export them :)
    if df_rows:
        df = pd.DataFrame(df_rows)
        last_columns = 3
        position_targeted = 2
        cols = df.columns.tolist()
        to_move = cols[-last_columns:]
        new_order = cols[:position_targeted] + to_move + cols[position_targeted:-last_columns]
        df = df[new_order]

        df.to_csv(f"{subdirectory}/main_cohort.csv", sep=";", index=False)
        logging.info(f"Exported {len(df)} patients to main_cohort.csv")
    else:
        logging.warning("No rows to export to CSV")
