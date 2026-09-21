import json
import logging
import os
import sys
from collections import defaultdict
from pathlib import Path
import numpy as np
from distinctipy import get_colors
from data_extraction.Constants import ICD_CODE_FILE, LOINC_CODE_FILE
import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt

PROJECT_ROOT_DIR = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT_DIR) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT_DIR))

METADATA_DIR = PROJECT_ROOT_DIR / "data_extraction" / "fhir_results" / "metadata.json"

def load_json(filepath):
    """Loads json file"""
    try:
        if os.path.exists(filepath):
            with open(filepath, 'r', encoding='utf-8') as file:
                return json.load(file)
    except FileNotFoundError:
        logging.error(f"File {filepath} not found")
        pass
    return {}

def create_bar_graph(bar_type, keys, values, title, xlabel, ylabel, add_exact_count_labels, filename):
    """Create a bar (or horizontal) graph."""
    plt.figure(figsize=(12, 8))
    if bar_type == 'horizontal':
        bars = plt.barh(keys, values, color='skyblue')
        plt.grid(axis='x', linestyle='--', alpha=0.7)

    else:
        bars = plt.bar(keys, values, color='darkblue')
        plt.grid(axis='y', linestyle='--', alpha=0.7)
        plt.xticks(rotation=45, ha='right')

    # Graph titles and labels
    plt.title(title, fontsize=12)
    plt.xlabel(xlabel, fontsize=12)
    plt.ylabel(ylabel, fontsize=11)

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


def plot_condition_composition(conditions_groups_sums, output_file):
    """
    Plot the relative composition of condition groups per year
    as a 100% stacked horizontal bar chart.
    """
    years = sorted(conditions_groups_sums.keys())
    groups = sorted({group for year in years for group in conditions_groups_sums[year]})

    data = np.array([
        [
            conditions_groups_sums[year].get(group, 0)
            for group in groups
        ]
        for year in years
    ], dtype=float)

    # Convert each year to percentages
    yearly_totals = data.sum(axis=1, keepdims=True)
    percentages = np.divide(data, yearly_totals, out=np.zeros_like(data),where=yearly_totals != 0) * 100

    # assign and distribute color per group
    colors = get_colors(len(groups), pastel_factor=0.7)
    group_colors = {group: colors[i] for i, group in enumerate(groups)}

    # plot figure
    fig, ax = plt.subplots(figsize=(24, max(8, len(years) * 0.45)))

    left = np.zeros(len(years))
    for i, group in enumerate(groups):
        ax.barh(
            years,
            percentages[:, i],
            left=left,
            label=group,
            color=group_colors[group],
            edgecolor="white",
            linewidth=0.3
        )
        left += percentages[:, i]

    ax.set_xlabel("Distribution (%)")
    ax.set_ylabel("Year")
    ax.set_title("Condition Composition by Year")
    ax.set_xlim(0, 100)

    ax.legend(bbox_to_anchor=(1.02, 1), loc="upper left",fontsize=9, ncol=1, frameon=True)
    plt.tight_layout()

    plt.savefig(output_file, dpi=300, bbox_inches="tight")
    plt.close()

    logging.info(f"Saved {output_file}")


meta_data = load_json(METADATA_DIR)

# ---------------- # Data overview # ---------------------------------
#Add other type of medication resources if you have other sources...
data_overview = {
    "Asthma & COPD Patient Count": meta_data['asthma_and_copd_patient_count'],
    "Patients with Observations": meta_data['patient_count_with_observations'],
    "Patients with MedicationAdministrations": meta_data['patient_count_with_medicationAdministrations']
}
conditions_counts = meta_data['conditions_counts']
observations_counts = meta_data['observations_counts']

# ------------------- # MedicationAdministrations # ----------------------------
# Add other medication resource types here if additional sources exist.
medication_details = (
    meta_data .get("medicationAdministrations_counts", {}) .get("MedicationAdministration", {}) .get("counting", {}) .get("details_count", []) )
medications_exist = bool(medication_details)
if medications_exist:
    medications_counts = {list(item.keys())[0]: list(item.values())[0] for item in medication_details}

# ---------------- # Conditions # -----------------------------------------
# Calculate condition group totals per year.
icd_codes = load_json(ICD_CODE_FILE)
conditions_groups_sums = defaultdict(int)
for year, condition_counts in conditions_counts.items():
    year_groups_sums = defaultdict(int)

    for group in icd_codes["codes"]:
        year_groups_sums[group["description"]] = sum(condition_counts.get(code, 0) for code in group["code"])

    conditions_groups_sums[year] = year_groups_sums

#Calculate group and individual total counts of Diagnoses COPD vs Asthma
diagnosis_group_sums = defaultdict(int)
diagnosis_individual_sums = defaultdict(int)

for year, condition_counts in conditions_counts.items():
    year_group_sums = defaultdict(int)
    year_individual_sums = defaultdict(int)

    for code, count in condition_counts.items():
        year_individual_sums[code] = count

        if code.startswith("J44"):
            year_group_sums["J44.*"] += count
        elif code.startswith("J45"):
            year_group_sums["J45.*"] += count

    diagnosis_group_sums[year] = year_group_sums
    diagnosis_individual_sums[year] = year_individual_sums

# ----------------------# Observations (LOINC) # ---------------------------

# Read loinc input file and get categories per code
loinc_codes = load_json(LOINC_CODE_FILE)
loinc_codes_per_category = defaultdict(list)
observations_groups_sums = defaultdict(int)

for item in loinc_codes["codes"]:
    loinc_codes_per_category[item["category"]].append(item["code"])
group_observation = dict(loinc_codes_per_category)

#Calculate group total for observation counts per year
for year, observations in observations_counts.items():
    group_sums = defaultdict(int)
    for group, codes in group_observation.items():
        group_sums[group] = sum(observations.get(code, 0) for code in codes)
    observations_groups_sums[year] = group_sums

# ----------------- # Graphs # -----------------------------------------------

create_bar_graph('vertical', data_overview.keys(), data_overview.values(), 'Data Overview', '', '', True, "dataOverview.png")
# Condition groups - one graph per year
for year, group_sums in conditions_groups_sums.items():
    create_bar_graph( "horizontal", group_sums.keys(), group_sums.values(), f"Condition Groups Counts - {year}", "Total Count", "Condition Groups", False, f"Conditions_{year}.png", )

# COPD vs Asthma - one graph per year
for year, group_sums in diagnosis_group_sums.items(): create_bar_graph( "vertical", group_sums.keys(), group_sums.values(), f"Count of Diagnoses COPD vs Asthma - {year}", "Diagnosis Groups", "Total Count", False, f"DiagnosisGroups_{year}.png", )

# Distribution all conditions per year
plot_condition_composition(conditions_groups_sums,"graphs/ConditionsComposition.png")

if medications_exist:
    create_bar_graph('vertical', medications_counts.keys(), medications_counts.values(), 'MedicationAdministrations', 'Medications', 'Total Count', False, "medicationAdministrations.png")

# Observations per year
for group in group_observation.keys():
    years = list(observations_groups_sums.keys())
    values = [observations_groups_sums[year].get(group, 0)for year in years]
    create_bar_graph('vertical', years, values,f'{group} Observation Counts by Year','Year','Total Count',False,f'observationGroups_{group}.png')

# Observation values histograms
plot_observation_values_histograms(meta_data['observations_value_histograms'])
