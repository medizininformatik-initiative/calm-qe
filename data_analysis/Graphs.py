import json
import os
from collections import defaultdict
import matplotlib
matplotlib.use("Agg")
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


def plot_observation_values_histograms(observations_value_histograms):

    for code in observations_value_histograms:
        histogram = observations_value_histograms.get(code)

        edges = histogram["bin_edges"]
        n = histogram["n"]

        # Left edge of each bin
        x = edges[:-1]

        # Width of each bin
        widths = [edges[i + 1] - edges[i] for i in range(len(edges) - 1)]

        plt.figure(figsize=(10, 6))
        plt.bar(x, histogram["counts"], width=widths, align="edge", edgecolor="black")
        plt.xlabel("Observation value")
        plt.ylabel("Count")
        plt.title(f"LOINC {code} (n={n})")
        plt.tight_layout()
        histograms_folder = os.path.join("graphs", "histograms")
        os.makedirs(histograms_folder, exist_ok=True)
        save_path = os.path.join(histograms_folder, f"{code}_histogram.png")
        plt.savefig(save_path)
        plt.close()

meta_data = load_json("/fhir_results/metadata.json")

#Add other type of medication resources if you have other sources...
data_overview = {
    "Asthma & COPD Patient Count": meta_data['asthma_and_copd_patient_count'],
    "Patients with Observations": meta_data['patient_count_with_observations'],
    "Patients with MedicationAdministrations": meta_data['patient_count_with_medicationAdministrations']
}
conditions_counts = meta_data['conditions_counts']
observations_counts = meta_data['observations_counts']

#Only creating medication administration graph, you can add other type of medication as well!
medications_exist = False
if meta_data.get('medicationAdministrations_counts', {}).get('MedicationAdministration', {}).get('counting', {}).get('details_count', []):
    medications_exist = True
    medications_counts = {list(item.keys())[0]: list(item.values())[0] for item in meta_data['medicationAdministrations_counts']['MedicationAdministration']['counting']['details_count']}

# Read loinc input file and get categories per code
loinc_codes = load_json("input_files/loinc_codes.json")
loinc_codes_per_category = defaultdict(list)

for item in loinc_codes["codes"]:
    loinc_codes_per_category[item["category"]].append(item["code"])
group_observation = dict(loinc_codes_per_category)

# Calculate group total counts for  conditions
icd_codes = load_json("input_files/icd_codes.json")
conditions_groups_sums = defaultdict(int)
for group in icd_codes["codes"]:
    group_sum = sum(conditions_counts.get(code, 0) for code in group["code"])
    conditions_groups_sums[group["description"]] = group_sum

#Calculate group and individual total counts of Diagnoses COPD vs Asthma
diagnosis_group_sums = defaultdict(int)
diagnosis_individual_sums = defaultdict(int)
for code, count in conditions_counts.items():
    diagnosis_individual_sums[code] = count
    if code.startswith("J44"):
        diagnosis_group_sums["J44.*"] += count
    elif code.startswith("J45"):
        diagnosis_group_sums["J45.*"] += count

#Calculate group total for observation counts
observations_groups_sums = defaultdict(int)
for group, codes in group_observation.items():
    observations_groups_sums[group] = sum(observations_counts.get(code, 0) for code in codes)

# Plot the graphs
create_bar_graph('vertical', data_overview.keys(), data_overview.values(), 'Data Overview', '', '', True, "dataOverview.png")
create_bar_graph('horizontal', conditions_groups_sums.keys(), conditions_groups_sums.values(), 'Condition Groups Counts', 'Total Count', 'Condition Groups', False, "Conditions.png")
create_bar_graph('vertical', diagnosis_group_sums.keys(), diagnosis_group_sums.values(), 'Count of Diagnoses COPD vs Asthma', 'Diagnosis Groups', 'Total Count', False, "DiagnosisGroups.png")
create_bar_graph('vertical', diagnosis_individual_sums.keys(), diagnosis_individual_sums.values(), 'Diagnosis', 'Diagnosis', 'Total Count', False, "Diagnosis.png")
create_bar_graph('vertical', observations_groups_sums.keys(), observations_groups_sums.values(), 'Observation Group Counts', 'Observation Groups', 'Total Count', False, "observationGroups.png")
if medications_exist:
    create_bar_graph('vertical', medications_counts.keys(), medications_counts.values(), 'MedicationAdministrations', 'Medications', 'Total Count', False, "medicationAdministrations.png")

# Observation values histograms
plot_observation_values_histograms(meta_data['observations_value_histograms'])