import json
import os
from collections import defaultdict
import matplotlib.pyplot as plt

def load_json(filepath):
    """Loads json file"""
    if os.path.exists(filepath):
        with open(filepath, 'r') as file:
            return json.load(file)
    return {}

def create_bar_graph(bar_type, keys, values, title, xlabel, ylabel, add_exact_count_labels, filename):
    """Create a bar (or horizontal) graph."""
    plt.figure(figsize=(12, 8))
    if bar_type == 'horizontal':
        bars = plt.barh(keys, values, color='skyblue')
        plt.grid(axis='x', linestyle='--', alpha=0.7)

    else:
        bars = plt.bar(keys, values, color='skyblue')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.xticks(rotation=45, ha='right')

    # Graph titles and labels
    plt.title(title, fontsize=14)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=12)

    # Adds exact count of each bar to view
    if add_exact_count_labels:
        for bar in bars:
            plt.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.05 * bar.get_height(), f'{bar.get_height():,}',  ha='center', va='bottom')

    plt.tight_layout()
    os.makedirs('graphs', exist_ok=True)
    save_path = os.path.join('graphs', filename)
    plt.savefig(save_path, format='png')

meta_data = load_json("fhir_results/metadata.json")

#Add other type of medication resources if you have other sources...
data_overview = {
    "Asthma & COPD Patient Count": meta_data['asthma_and_copd_patient_count'],
    "Patients with Chief Complaint": meta_data['asthma_and_copd_patients_with_chief_complaint'],
    "Patients with Secondary Conditions": meta_data['patient_count_with_secondary_conditions'],
    "Patients with Observations": meta_data['patient_count_with_observations'],
    "Patients with MedicationAdministrations": meta_data['patient_count_with_medicationAdministrations']
}
main_diagnosis_counts = meta_data['main_diagnosis_counts']
secondary_conditions_counts = meta_data['secondary_conditions_counts']
observations_counts = meta_data['observations_counts']

#Only creating medication administration graph, you can add other type of medication as well!
medications_exist = False
if meta_data.get('medicationAdministrations_counts', {}).get('MedicationAdministration', {}).get('counting', {}).get('details_count', []):
    medications_exist = True
    medications_counts = {list(item.keys())[0]: list(item.values())[0] for item in meta_data['medicationAdministrations_counts']['MedicationAdministration']['counting']['details_count']}

print(medications_counts)
group_observation = {"Allergiediagnostik":[
    "23800-6",
    "15234-8",
    "31004-5",
    "51529-6",
    "51861-3",
    "51862-1",
    "15283-5",
    "6265-3",
    "7674-5",
    "6183-8",
    "7258-7",
    "6107-7",
    "6106-9",
    "6082-2",
    "6136-6",
    "6206-7",
    "6061-6",
    "6248-9",
    "6098-8",
    "6833-8",
    "6844-5",
    "40943-3",
    "6096-2",
    "6254-7",
    "6020-2",
    "26951-4",
    "6025-1",
    "6075-6",
    "6182-0",
    "6212-5",
    "6049-1"],
    "klinische chemie": ["1649-3", "1989-3", "1751-7", "6768-6", "1825-9", "9407-8", "1742-6",
    "3174-0", "1920-8", "15152-2", "1975-2", "2339-0", "30934-4", "1986-9",
    "48494-9", "2000-8", "2075-0", "2093-3", "2098-2", "2143-6", "1988-5",
    "30522-7", "2157-6", "32673-6", "2160-0", "41171-0", "48065-7", "30341-2",
    "2276-4", "3255-7", "2284-8", "3051-0", "3024-7", "2324-2", "4542-7",
    "2085-9", "2458-8", "19113-0", "2465-3", "2472-9", "82334-4", "9654-5",
    "98101-9", "26881-3", "33211-4", "2498-4", "13452-8", "14804-9", "3040-3",
    "60344-9", "2089-1", "2601-3", "48146-5", "2639-3", "33762-6", "2692-2",
    "2697-1", "1805-1", "33050-6", "14879-1", "2823-3", "33959-8", "2951-2",
    "3034-6", "2571-8", "10839-9", "21582-2", "27975-2", "3074-2", "3091-6",
    "3084-1", "2923-1", "2132-9"],
    "Liquordiagnostik": ["2342-4", "14744-7", "1746-7", "2464-6", "48669-6", "42207-1", "42206-3",
    "42208-9", "48666-2", "48665-4", "48667-0", "2880-3"],
    "Gerinnungsdiagnostik": ["5894-1"],
    "Hamatologie": ["7789-1", "38892-6", "51583-3", "735-1", "30413-9", "707-0", "706-2",
    "705-4", "26444-0", "707-0", "58410-2", "11274-8", "714-6", "713-8",
    "26449-9", "712-0", "35061-1", "57840-1", "34958-9", "35062-9", "35060-3",
    "714-6", "26453-1", "4548-4", "20570-8", "718-7", "30400-6", "6690-2",
    "26468-9", "53518-7", "26465-5", "26467-1", "26469-7", "737-7", "736-9",
    "732-8", "731-0", "28539-5", "30428-7", "28540-3", "744-3", "5905-5",
    "743-5", "744-3", "23761-0", "753-4", "26499-4", "770-8", "779-9", "10378-8",
    "26453-1", "71693-6", "26515-7", "803-7", "71695-1", "26464-8"],
    "Autoimmundiagnostik":["5128-4", "29953-7","53027-9"]}


# Calculate group total counts for secondary conditions
icd_codes = load_json("input_files/icd_codes.json")
secondary_conditions_groups_sums = defaultdict(int)
for group in icd_codes["codes"]:
    group_sum = sum(secondary_conditions_counts.get(code, 0) for code in group["code"])
    secondary_conditions_groups_sums[group["description"]] = group_sum

#Calculate group and individual total counts of Main Diagnoses COPD vs Asthma
main_diagnosis_group_sums = defaultdict(int)
main_diagnosis_individual_sums = defaultdict(int)
for code, count in main_diagnosis_counts.items():
    main_diagnosis_individual_sums[code] = count
    if code.startswith("J44"):
        main_diagnosis_group_sums["J44.*"] += count
    elif code.startswith("J45"):
        main_diagnosis_group_sums["J45.*"] += count

#Calculate group total for observation counts
observations_groups_sums = defaultdict(int)
for group, codes in group_observation.items():
    observations_groups_sums[group] = sum(observations_counts.get(code, 0) for code in codes)

# Plot the graphs
create_bar_graph('vertical', data_overview.keys(), data_overview.values(), 'Data Overview', '', '', True, "dataOverview.png")
create_bar_graph('horizontal', secondary_conditions_groups_sums.keys(), secondary_conditions_groups_sums.values(), 'Secondary Condition Groups Counts', 'Total Count', 'Condition Groups', False, "secondaryConditions.png")
create_bar_graph('vertical', main_diagnosis_group_sums.keys(), main_diagnosis_group_sums.values(), 'Count of Main Diagnoses COPD vs Asthma', 'Main Diagnosis Groups', 'Total Count', False, "mainDiagnosisGroups.png")
create_bar_graph('vertical', main_diagnosis_individual_sums.keys(), main_diagnosis_individual_sums.values(), 'Main Diagnosis', 'Main Diagnosis', 'Total Count', False, "mainDiagnosis.png")
create_bar_graph('vertical', observations_groups_sums.keys(), observations_groups_sums.values(), 'Observation Group Counts', 'Observation Groups', 'Total Count', False, "observationGroups.png")
if medications_exist:
    create_bar_graph('vertical', medications_counts.keys(), medications_counts.values(), 'MedicationAdministrations', 'Medications', 'Total Count', False, "medicationAdministrations.png")


