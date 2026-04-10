import os
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

from Constants import USER_NAME, USER_PASSWORD, ICD_SYSTEM_NAME, ASTHMA_COPD_CODES_FILE
from FhirHelpersUtils import fetch_bundle_for_code, connect_to_server
from FhirHelpersUtils import parse_fhir_datetime, compute_los
from Metadata import gather_metadata

basis_filename = "patients_diagnosed_asthma_copd"

def generate_output_filename(surfix_filename, directory):
    input_path = Path(directory)
    target_file = input_path.stem

    if basis_filename in target_file:
        return f"patients_{surfix_filename}.json"


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

    print(f"\nFiltering patients with age in interval [{min_age}, {max_age}] years...")

    matched_patients = defaultdict(list)
    pid_not_birthdate = []
    total_processed = 0

    with open(input_filepath, "r", encoding="utf-8") as f:
        patient_encounters = json.load(f)

    for patient_ref, encounter_attribs in patient_encounters.items():
        total_processed += 1
        patient_id = patient_ref.split("/")[-1]
        print(f"\nProcessing patient {patient_id}...")

        while True:
            try:
                patient = Patient.read(patient_id, smart.server)
                birth_iso = patient.birthDate.isostring
                birth_date = parse_fhir_datetime(birth_iso)
                break
            except Exception as exc:
                print(f"Error fetching patient {patient_id}: {exc}, but continue to trying...")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                time.sleep(1)

        if not birth_date:
            pid_not_birthdate.append(patient_ref)
            print(f"Skipping patient {patient_ref} - no birth date available.")
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
                age_years = int(days / 365)
            except Exception:
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
    print(f"Found {interval_count} patients in interval [{min_age}, {max_age}] out of {total_processed} processed.")

    if interval_count > 0:
        base_path = Path(input_filepath)
        new_filename = generate_output_filename(f"filtered_by_age_interval_{min_age}-{max_age}", input_filepath)

        output_filepath = base_path.with_name(new_filename)
        with open(output_filepath, "w", encoding="utf-8") as out:
            json.dump({pid: entries for pid, entries in matched_patients.items()}, out, indent=4, ensure_ascii=False)


def filter_icu_patients_admission(smart, input_filepath, enabled=True):
    """
        From the HauptDiagnosis (Main), filter type of admission, specially ICU patients.
        Reference: https://simplifier.net/guide/mii-ig-modul-fall-2025/
        MIIIGModulFall/TechnischeImplementierung/FHIRProfile/EncounterKontaktGesundheitseinrichtung.page.md?version=current
    """
    if not enabled:
        return None

    print("\nFiltering ICU patients...")
    main_patients_diagnosed = input_filepath
    icu_patients = defaultdict(int)
    if os.path.exists(main_patients_diagnosed):
        with open(main_patients_diagnosed, "r") as file:
            main_patients_conditions = json.load(file)
            for patient_id, condition_ids in main_patients_conditions.items():
                for condition_id in condition_ids:
                    cid = condition_id['id'] if isinstance(condition_id, dict) else condition_id
                    try:
                        bundle = smart.server.request_json(
                            Encounter.where({
                            'subject': f'{patient_id}',
                            'diagnosis.condition': f'Condition/{cid}',
                            '_count': '1000'
                        }).construct())
                    except Exception as e:
                        print(f"Generated an exception for {patient_id} with condition/{condition_id}: {e}, but continue trying...")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(3)

                for entry in fetch_bundle_for_code(smart, bundle):
                    for enc in entry:
                        if "resource" in enc and "type" in enc['resource']:
                            for type_entry in enc["resource"]["type"]:
                                if "coding" not in type_entry:
                                    continue
                                for coding in type_entry["coding"]:
                                    if "code" in coding and "intensiv" in coding["code"].lower():
                                        print(f"ICU encounter found for patient {patient_id}")
                                        encounter_id = enc["resource"].get("id")
                                        icu_patients.setdefault(patient_id, set()).add(encounter_id)

    icu_patients_json = {pid: list(enc_ids) for pid, enc_ids in icu_patients.items()}

    base_path = Path(input_filepath)
    new_filename = generate_output_filename("filtered_by_icu_admission", input_filepath)
    output_filepath = base_path.with_name(new_filename)
    with open(output_filepath, "w", encoding="utf-8") as out:
        json.dump(icu_patients_json, out, indent=4)

    gather_metadata("patient_count_in_intensive_care", len(icu_patients))


def calculate_los_inpatients(smart, input_filepath, enabled=True):
    """
    Aufenthaltsdauer: calculate "Length of Staying", (LOS) from inpatients.
    Reference: https://simplifier.net/guide/mii-ig-modul-fall-2025/
    MIIIGModulFall/TechnischeImplementierung/FHIRProfile/EncounterKontaktGesundheitseinrichtung.page.md?version=current
    """
    if not enabled:
        return None

    print("\nGathering inpatients...")
    main_patients_diagnosed = input_filepath
    inpatients = defaultdict()

    if os.path.exists(main_patients_diagnosed):
        with open(main_patients_diagnosed, "r") as file:
            main_patients_conditions = json.load(file)
            for patient_id, condition_ids in main_patients_conditions.items():
                for condition_id in condition_ids:
                    bundle = None
                    try:
                        bundle = smart.server.request_json(
                            Encounter.where({
                            'subject': f'{patient_id}',
                            'diagnosis.condition': f'Condition/{condition_id['id']}',
                            '_count': '50'
                        }).construct())
                    except Exception as e:
                        print(f" Generated an exception for {patient_id} with condition/{condition_id}: {e}, but continue trying...")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(3)

                    for entry in fetch_bundle_for_code(smart, bundle):
                        for enc in entry:
                            if "resource" in enc:
                                if "type" in enc['resource']:
                                    stay_entry = process_inpatient_encounter(enc['resource'])
                                    if stay_entry:
                                        if patient_id not in inpatients:
                                            inpatients[patient_id] = []
                                        inpatients[patient_id].append(stay_entry)

    base_path = Path(input_filepath)

    new_filename = generate_output_filename("length_of_stay",  input_filepath)
    output_filepath = base_path.with_name(new_filename)
    with open(output_filepath, "w", encoding="utf-8") as file:
        json.dump(inpatients, file, indent=4, ensure_ascii=False)

    print(f"File successfully generated with {len(inpatients)} inpatients")


def extract_last_three_encounter(smart, input_filepath, enabled=True):
    """
    Extract last three encounter IDs per patient.
    """
    if not enabled:
        return input_filepath

    patients_last_3_encounters = defaultdict(list)

    with open(input_filepath, "r") as file:
        patients = json.load(file)
        for patient in patients.keys():
            print(f"Processing patient with ID: {patient[8:]}...")
            all_encounters_per_patient = []
            attributes_encounter = patients[patient]
            for attribute_encounter in attributes_encounter:
                start = parse_fhir_datetime(attribute_encounter["start"]).strftime("%Y-%m-%d")

                while True:  # Connection might get lost sometime, trying to reconnect...
                    try:
                        # Check the patient with the specific condition ID has Encounter reference.
                        bundle = smart.server.request_json(
                            Encounter.where(struct={
                            '_count': b'10',
                            'subject': patient,
                            'date': f"lt{start}"
                        }).construct())
                        break
                    except Exception as exc:
                        print(f"Generated an exception: {exc} but continue to trying. \n")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(3)

                for entry in fetch_bundle_for_code(smart, bundle):
                    for enc in entry:
                        start, end, = None, None

                        resource = enc.get("resource", {})
                        if "period" in enc['resource']:
                            start = enc["resource"]["period"].get("start")
                            end = enc["resource"]["period"].get("end")

                        if not start:
                            continue

                        all_encounters_per_patient.append({
                            "encounter": resource.get("id"),
                            "start": start if start else None,
                            "end": end if end else None,
                        })

            valid_encounters = [e for e in all_encounters_per_patient if e.get("start")]

            sorted_encounters = sorted(
                valid_encounters,
                key=lambda e:e["start"],
                reverse=True
            )

            if len(sorted_encounters) > 0:
                # Keep last 3 encounters
                patients_last_3_encounters[patient] = sorted_encounters[:3]

    base_path = Path(input_filepath)
    new_filename = generate_output_filename("filtered_by_last_3_encounters", input_filepath)
    output_filepath = base_path.with_name(new_filename)
    with open(output_filepath, "w", encoding="utf-8") as file:
        json.dump(patients_last_3_encounters, file, indent=4, ensure_ascii=False)

    print(f"File successfully generated for extracting last three encounters and admission dates for {len(patients_last_3_encounters)} main diagnosed patients")


def get_demographics_patients(smart, input_filepath, enabled=True):
    '''
    Obtains demographics from patients from selected patient IDs and export results in tabular form.
    Reference: https://www.medizininformatik-initiative.de/Kerndatensatz/
    KDS_Person_V2025/MIIIGModulPerson-TechnischeImplementierung-FHIR-Profile-PatientInPatient.html
    '''
    if not enabled:
        return None

    subdirectory = input_filepath.parent/'csv'
    subdirectory.mkdir(parents=True, exist_ok=True)

    patient_identifiers, patients_demographics = [], []

    with open(input_filepath, "r") as file:
        patients = json.load(file)
        for patient in patients.keys():
            print(f"Processing patient with ID: {patient[8:]}...")
            patient_identifiers.append(patient[8:])

    for patient_id in patient_identifiers:
        while True:
            try:
                patient = Patient.read(patient_id, smart.server)
                patients_demographics.append({
                    "patient": patient_id,
                    "gender": patient.gender,
                    "birthdate": patient.birthDate.isostring,
                })
                break
            except Exception as exc:
                print(f"Generated an exception: {exc} but continue to trying. \n")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                time.sleep(3)

    patients_demographics_df = pd.DataFrame(patients_demographics)
    patients_demographics_df.to_csv(os.path.join(subdirectory, "demographics.csv"), index=False, sep=";")
    print(f"Saving extracted demographics as .csv file in {subdirectory}")


def extract_additional_attributes_from_encounters(smart, input_filepath):

    # Extract interested attributes from encounters (period, fallart, service_department_code)
    contact_system = "http://fhir.de/CodeSystem/kontaktart-de"

    print("Starting additional encounters extraction...")
    encounter_results = defaultdict(list)
    non_found_encounter_results = defaultdict(list)
    base_path = Path(input_filepath)

    with open(input_filepath, "r") as file:
        patients = json.load(file)
        for patient in patients.keys():
            attributes_conditions = patients[patient]
            duplicated_encounter = set()
            for count, attr_condition in enumerate(attributes_conditions, start=1):
                encounter_id = attr_condition['encounter'] if isinstance(attr_condition, dict) else attr_condition

                if encounter_id in duplicated_encounter or duplicated_encounter.add(encounter_id):
                    continue

                for _ in range(3):
                    try:
                        entry_encounter = Encounter.read(encounter_id, smart.server)
                        enc = {"resource": entry_encounter.as_json()}
                        break
                    except FHIRNotFoundException:
                        print(f"Encounter {encounter_id} not found. Skipping")
                        non_found_encounter_results[patient].append(encounter_id)
                        enc = None
                        break
                    except Exception as exc:
                        status = getattr(getattr(exc, "response", None), "status_code", None)
                        if status == 410:
                            print(f"Exception {status}. Encounter {encounter_id} missing or deleted. Skipping")
                            non_found_encounter_results[patient].append(encounter_id)
                            enc = None
                            break

                        print(f"Generated an exception: {exc} in but continue to trying. \n")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(1)

                encounters = [enc] if enc else None

                if encounters:
                    for enc in encounters:
                        start, end, fall_art, service_type_code, type_contact_code = None, None, None, None, None

                        resource = enc.get("resource", {})
                        if "period" in resource:
                            start = enc["resource"]["period"].get("start")
                            end = enc["resource"]["period"].get("end")

                        if "class" in enc["resource"]:
                            fall_art = enc["resource"]["class"].get("code")

                        if "serviceType" in enc["resource"]:
                            service_type_code = enc["resource"]["serviceType"].get("coding", [{}])[0].get("code")

                        if "type" in enc["resource"]:
                            for type_entry in enc["resource"]["type"]:
                                if "coding" not in type_entry:
                                    continue
                                for coding in type_entry["coding"]:
                                    if contact_system in coding["system"]:
                                        type_contact_code = coding.get("code")

                        encounter_results[patient].append({
                            "condition": attr_condition,
                            "start": start,
                            "end": end,
                            "case": fall_art,
                            "serviceDepartment": service_type_code,
                            "typeContact": type_contact_code,
                        })

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
            label = list(attribute.keys())
            condition_id = attribute.get("condition").get("id")
            attrib_enc = attribute.get("condition")
            code = attribute.get("condition").get("code")

            row = {
                'patient': patient_reference,
                f'{label[0]}': condition_id if condition_id else None,
                'encounter': attrib_enc.get('encounter') if attrib_enc.get('encounter') else None,
                'onsetDateTime':attrib_enc.get('onsetDateTime') if attrib_enc.get('onsetDateTime') else None,
                f'{label[1]}': attribute.get("start")  if attribute.get("start") else None,
                f'{label[2]}': attribute.get("end")  if attribute.get("end") else None,
                f'{label[3]}': attribute.get("case") if attribute.get("case") else None,
                f'{label[4]}': attribute.get("serviceDepartment") if attribute.get("serviceDepartment") else None,
                f'{label[5]}': attribute.get("typeContact") if attribute.get("typeContact") else None,
            }

            # codes from conditions
            if isinstance(code, dict):
                coding_list = code.get('coding', [])
                if coding_list:
                    for code in coding_list:
                       row.update({
                            'code': code.get('code') if code.get('code') else None,
                            'system': code.get('system') if code.get('system') else None,
                            'version': code.get('version') if code.get('version') else None
                        })


            df_rows.append(row)

    # New: reorder columns and export them :)
    df = pd.DataFrame(df_rows)
    last_columns = 3
    position_targeted = 2
    cols = df.columns.tolist()
    to_move = cols[-last_columns:]
    new_order = cols[:position_targeted] + to_move + cols[position_targeted:-last_columns]
    df = df[new_order]

    df.to_csv(f"{subdirectory}/{basis_filename}_extended_encounters.csv", sep=";", index=False)
    print(f"Exported {len(df)} patients to {basis_filename}.csv")
