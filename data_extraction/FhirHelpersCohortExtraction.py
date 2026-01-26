import os
from collections import defaultdict
import json
import time
import pandas as pd

from fhirclient.models.condition import Condition
from fhirclient.models.encounter import Encounter
from fhirclient.models.patient import Patient

from Constants import USER_NAME, USER_PASSWORD, ICD_SYSTEM_NAME, ASTHMA_COPD_CODES_FILE
from FhirHelpersUtils import fetch_bundle_for_code, connect_to_server
from FhirHelpersUtils import parse_fhir_datetime, compute_los
from Metadata import gather_metadata


def generate_output_filename(prefix_filename, directory):
    target_file = directory.stem
    if "primary_diagnosed_patients_with_asthma_or_copd" in target_file:
        return f"primary_diagnosed_patients_asthma_copd-{prefix_filename}.json"
    else:
        return f"total_diagnoses_patients_asthma_copd-{prefix_filename}.json"


def patients_with_asthma_copd(smart, input_path):
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
    gather_metadata("total_diagnosed_patients_with_asthma_or_copd_count", len(patients_conditions_map))
    output_filepath = input_path / "total_diagnosed_patients_with_asthma_or_copd.json"
    with open(output_filepath, 'w') as file:  # Intermediate results.
        json.dump(patients_conditions_map, file, indent=4)

    return output_filepath


def filter_main_diagnosis(smart, input_filepath, enabled=True):
    """
    From the patients diagnosed ASTHMA or COPD, it filters only for HauptDiagnosis(Main) from their Encounter references.
    Put the results into JSON file format.
    :param smart: Fhir Server Connector
    """
    if not enabled:
        return input_filepath

    count_main_diagnose_type = defaultdict(int)
    admission_dates = defaultdict(list)

    patients_with_chief_complaint = defaultdict(list)
    with open(input_filepath, "r") as file:
        patients = json.load(file)
        for patient in patients.keys():
            print(f"Processing patient with ID: {patient[8:]}...")
            conditions_ids = patients[patient]
            for condition in conditions_ids:
                while True:  # Connection might get lost sometime, trying to reconnect...
                    try:
                        #Check the patient with the specific condition ID has Encounter reference.
                        bundle = Encounter.where(struct={'_count': b'10', 'subject': patient, 'diagnosis': 'Condition/' + condition['id']}).perform(smart.server)
                        break
                    except Exception as exc:
                        print(f"Generated an exception: {exc} but continue to trying. \n")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(3)

                encounter = fetch_bundle_for_code(smart, bundle)
                #If the encounter exist, check the diagnosis from this encounter is "MainDiagnose" or not. If so, put it into result.
                if encounter:
                    for enc in encounter:
                        if 'diagnosis' in enc['resource']:
                            for c in enc['resource']['diagnosis']:
                                if c['use']['coding']:
                                    for code in c['use']['coding']:
                                        if code['code'] == "CC" and ('Condition/' + condition['id'] == c['condition']['reference']):  # chief complaint
                                            patients_with_chief_complaint[patient].append(condition)
                                            count_main_diagnose_type[condition['code']['coding'][0]['code']] += 1

                                            # Extract period
                                            period = enc['resource'].get("period", {})
                                            start = parse_fhir_datetime(period.get("start"))  # admission date
                                            admission_dates[patient].append([condition, start])

    gather_metadata("primary_diagnosed_patients_with_asthma_or_copd", len(patients_with_chief_complaint))
    gather_metadata("primary_diagnosis_counts", count_main_diagnose_type)
    gather_metadata("primary_diagnosis_count", sum(count_main_diagnose_type.values()))

    output_filepath = input_filepath.with_name("primary_diagnosed_patients_with_asthma_or_copd-admission_dates.json")
    with open(output_filepath, "w") as out:
        json.dump(admission_dates, out, indent=4)

    output_filepath = input_filepath.with_name("primary_diagnosed_patients_with_asthma_or_copd.json")
    with open(output_filepath, "w") as out:
        json.dump(patients_with_chief_complaint, out, indent=4)

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
        "encounter_id": resource.get("id"),
        "start": start.isoformat() if start else None,
        "end": end.isoformat() if end else None,
        "los_days": round(los_days, 2) if los_days else None
    }


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
                    try:
                        bundle = Encounter.where({
                            'subject': f'{patient_id}',
                            'diagnosis.condition': f'Condition/{condition_id}',
                            '_count': '50'
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
                                    if "code" in coding and coding["code"].lower() == "intensivstationaer":
                                        print(f"ICU encounter found for patient {patient_id}")
                                        cond_id = condition_id["id"] if isinstance(condition_id, dict) else condition_id
                                        icu_patients.setdefault(patient_id, set()).add(cond_id)
                else:
                    print("Skipping patient, no bundle found")

    icu_patients_json = {pid: list(cond_ids) for pid, cond_ids in icu_patients.items()}

    new_filename = generate_output_filename("icu_admission", input_filepath)
    output_filepath = input_filepath.with_name(new_filename)
    with open(output_filepath, "w") as out:
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
                            'diagnosis.condition': f'Condition/{condition_id}',
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
    new_filename = generate_output_filename("length_of_stay",  input_filepath)
    output_filepath = input_filepath.with_name(new_filename)
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
    patients_admission_encounter = defaultdict(list)

    with open(input_filepath, "r") as file:
        patients = json.load(file)
        for patient in patients.keys():
            print(f"Processing patient with ID: {patient[8:]}...")
            all_encounters_per_patient = []
            conditions_ids = patients[patient]
            for condition in conditions_ids:
                while True:  # Connection might get lost sometime, trying to reconnect...
                    try:
                        # Check the patient with the specific condition ID has Encounter reference.
                        bundle = Encounter.where(struct={'_count': b'10', 'subject': patient, 'diagnosis': 'Condition/' + condition['id']}).perform(smart.server)
                        break
                    except Exception as exc:
                        print(f"Generated an exception: {exc} but continue to trying. \n")
                        smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                        time.sleep(3)

                encounters = fetch_bundle_for_code(smart, bundle)

                if encounters:
                    for encounter in encounters:  #Only one encounter should be returned actually since we query by condition ID. #redundant
                        resource = encounter.get("resource", {})
                        period = resource.get("period", {})
                        start = parse_fhir_datetime(period.get("start"))
                        end = parse_fhir_datetime(period.get("end"))

                        if not start and not end:
                            continue
                        all_encounters_per_patient.append({
                            "encounter_id": resource.get("id"),
                            "start": start.isoformat() if start else None,
                            "end": end.isoformat() if end else None,
                        })

            valid_encounters = [e for e in all_encounters_per_patient if e.get("start") or e.get("end")] #Skip if there is no end/start time

            sorted_encounters = sorted(
                valid_encounters,
                key=lambda e: e["end"] or e["start"],
                reverse=True
            )

            # Keep last 3 encounters
            patients_last_3_encounters[patient] = sorted_encounters[:3]
            patients_admission_encounter[patient] = sorted_encounters[0]

    new_filename = generate_output_filename("last_3_encounters", input_filepath)
    output_filepath = input_filepath.with_name(new_filename)
    with open(output_filepath, "w", encoding="utf-8") as file:
        json.dump(patients_last_3_encounters, file, indent=4, ensure_ascii=False)

    new_filename = generate_output_filename("recent_encounter_admission_dates", input_filepath)
    output_filepath = input_filepath.with_name(new_filename)
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
