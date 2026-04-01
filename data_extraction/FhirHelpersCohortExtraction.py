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

from Constants import USER_NAME, USER_PASSWORD, ICD_SYSTEM_NAME, ASTHMA_COPD_CODES_FILE
from FhirHelpersUtils import fetch_bundle_for_code, connect_to_server
from FhirHelpersUtils import parse_fhir_datetime, compute_los
from Metadata import gather_metadata


def generate_output_filename(surfix_filename, directory):
    input_path = Path(directory)
    target_file = input_path.stem

    if "total_asthma_or_copd_diagnosed_patients" in target_file:
        return f"total_diagnosis_{surfix_filename}.json"


def patients_with_asthma_copd(smart, input_path):
    """
    It reads ASTHMA or COPD diseases related codes from "ASTHMA_COPD_CODES_FILE" and
    find the patients that have such diagnoses.
    :param smart: Fhir Server Connector
    """
    with open(ASTHMA_COPD_CODES_FILE, 'r') as file:
        main_diagnoses_file = json.load(file)
        main_diagnoses_codes = [item['code'] for item in main_diagnoses_file['codes']]
    print('codes main diagnoses:', main_diagnoses_codes)

    basis_filename = "total_asthma_or_copd_diagnosed_patients"
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
                    attributes_condition = {'id': condition['id'], 'code': condition['code']}

                    if condition['encounter']['reference']:  # New: Include encounter reference
                        attributes_condition["encounter"] = condition['encounter']['reference']

                    if condition['onsetDateTime']:  # New: Include onsetDateTime from conditions
                        attributes_condition["onsetDateTime"] = condition['onsetDateTime']
                    patients_conditions_map[patient_reference].append(attributes_condition)

    gather_metadata(basis_filename, len(patients_conditions_map))

    # total json export
    output_filepath = input_path / f"{basis_filename}.json"
    with open(output_filepath, 'w') as file:  # Intermediate results.
        json.dump(patients_conditions_map, file, indent=4)

    base_path = Path(output_filepath)
    encounters_filepath = base_path.with_name(f"{basis_filename}-encounters.json")

    # encounters json export
    additional_encounters = extract_additional_attributes_from_encounters(smart, output_filepath)
    with open(encounters_filepath, 'w') as file:
        json.dump(additional_encounters, file, indent=4)

    return output_filepath


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
        "los_days": round(los_days, 2) if los_days else None
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

        birth_date = None

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
            period_start = enc.get("period_start")
            if not period_start:
                continue

            period_start_date = parse_fhir_datetime(period_start)
            if not period_start_date:
                continue

            try:
                days = (period_start_date.date() - birth_date.date()).days
                if days < 0:
                    continue
                age_years = int(days / 365)
            except Exception:
                continue

            if min_age <= age_years <= max_age:
                matched_patients[patient_ref].append({
                    "condition": enc["condition"],
                    "period_start": enc.get("period_start"),
                    "period_end": enc.get("period_end"),
                    "birth_date": birth_date.isoformat() if birth_date else None,
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
                        bundle = Encounter.where({
                            'subject': f'{patient_id}',
                            'diagnosis.condition': f'Condition/{cid}',
                            '_count': '100'
                        }).perform(smart.server)
                    except Exception as e:
                        print(f"Generated an exception for {patient_id} with condition/{condition_id}: {e}, but continue trying...")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(3)

                if bundle:
                    encounters = fetch_bundle_for_code(smart, bundle)
                    for encounter in encounters:
                        if "resource" in encounter and "type" in encounter['resource']:
                            for type_entry in encounter["resource"]["type"]:
                                if "coding" not in type_entry:
                                    continue
                                for coding in type_entry["coding"]:
                                    if "code" in coding and "intensiv" in coding["code"].lower():
                                        print(f"ICU encounter found for patient {patient_id}")
                                        encounter_id = encounter["resource"].get("id")
                                        icu_patients.setdefault(patient_id, set()).add(encounter_id)
                else:
                    print("Skipping patient, no bundle found")

    icu_patients_json = {pid: list(enc_ids) for pid, enc_ids in icu_patients.items()}

    base_path = Path(input_filepath)
    new_filename = generate_output_filename("icu_admission", input_filepath)
    output_filepath = base_path.with_name(new_filename)
    with open(output_filepath, "w", encoding="utf-8") as out:
        json.dump(icu_patients_json, out, indent=4)

    gather_metadata("intensive_care_unit_patient_count", len(icu_patients))


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
                        bundle = Encounter.where({
                            'subject': f'{patient_id}',
                            'diagnosis.condition': f'Condition/{condition_id['id']}',
                            '_count': '50'
                        }).perform(smart.server)
                    except Exception as e:
                        print(f" Generated an exception for {patient_id} with condition/{condition_id}: {e}, but continue trying...")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(3)

                    if bundle:
                        encounters = fetch_bundle_for_code(smart, bundle)
                        for encounter in encounters:
                            if "resource" in encounter:
                                if "type" in encounter['resource']:
                                    stay_entry = process_inpatient_encounter(encounter['resource'])
                                    if stay_entry:
                                        if patient_id not in inpatients:
                                            inpatients[patient_id] = []
                                        inpatients[patient_id].append(stay_entry)
                    else:
                        print("Skipping patient, no bundle found")
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
                period_start = parse_fhir_datetime(attribute_encounter["period_start"]).strftime("%Y-%m-%d")

                while True:  # Connection might get lost sometime, trying to reconnect...
                    try:
                        # Check the patient with the specific condition ID has Encounter reference.
                        bundle = Encounter.where(struct={
                            '_count': b'10',
                            'subject': patient,
                            'date': f"lt{period_start}"
                        }).perform(smart.server)
                        break
                    except Exception as exc:
                        print(f"Generated an exception: {exc} but continue to trying. \n")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(3)

                encounters = fetch_bundle_for_code(smart, bundle)

                if encounters:
                    for enc in encounters:
                        start, end, = None, None

                        resource = enc.get("resource", {})
                        if "period" in enc['resource']:
                            start = enc["resource"]["period"].get("start")
                            end = enc["resource"]["period"].get("end")

                        if not start:
                            continue

                        all_encounters_per_patient.append({
                            "encounter": resource.get("id"),
                            "period_start": start if start else None,
                            "period_end": end if end else None,
                        })

            valid_encounters = [e for e in all_encounters_per_patient if e.get("period_start")]

            sorted_encounters = sorted(
                valid_encounters,
                key=lambda e:e["period_start"],
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
                    "patient_identifier": patient_id,
                    "gender": patient.gender,
                    "birth_date": patient.birthDate.isostring,
                })
                break
            except Exception as exc:
                print(f"Generated an exception: {exc} but continue to trying. \n")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                time.sleep(3)

    patients_demographics_df = pd.DataFrame(patients_demographics)
    patients_demographics_df.to_csv(os.path.join(subdirectory, "demographics.xlsx"), index=False, sep=";")
    print(f"Saving extracted demographics as .xlsx file in {subdirectory}")


def extract_additional_attributes_from_encounters(smart, input_filepath):

    # Extract interested attributes from encounters (period, fallart, service_department_code)
    contact_system = "http://fhir.de/CodeSystem/kontaktart-de"

    print("Starting encounters extraction...")
    encounter_results = defaultdict(list)

    with open(input_filepath, "r") as file:
        patients = json.load(file)
        for patient in patients.keys():
            conditions_ids = patients[patient]
            for condition in conditions_ids:
                while True:  # Connection might get lost sometime, trying to reconnect...
                    try:
                        bundle = Encounter.where(struct={
                            '_count': b'100',
                            'subject': patient,
                            'diagnosis': 'Condition/' + condition['id']
                        }).perform(smart.server)
                        break
                    except Exception as exc:
                        print(f"Generated an exception: {exc} but continue to trying. \n")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(3)

                encounter = fetch_bundle_for_code(smart, bundle)

                if encounter:
                    for enc in encounter:
                        period_start, period_end, fall_art, service_type_code, type_contact_code = None, None, None, None, None

                        if "period" in enc['resource']:
                            period_start = enc["resource"]["period"].get("start")
                            period_end = enc["resource"]["period"].get("end")

                        if "class" in enc["resource"]:
                            fall_art = enc["resource"]["class"].get("code")

                        if "serviceType" in enc["resource"]:
                            service_type_code = enc["resource"]["serviceType"].get("coding", [{}])[0].get("code")

                        if "type" in enc["resource"]:
                            for type_entry in enc["resource"]["type"]:
                                if "coding" not in type_entry:
                                    continue
                                for coding in type_entry["coding"]:
                                    if not contact_system in coding["system"]:
                                        continue
                                    type_contact_code = coding.get("code")

                        encounter_results[patient].append({
                            "condition": condition,
                            "period_start": period_start,
                            "period_end": period_end,
                            "fall_art": fall_art,
                            "service_department_code": service_type_code,
                            "type_contact_code": type_contact_code,
                        })

    print("encounter_results:", encounter_results)
    return encounter_results
