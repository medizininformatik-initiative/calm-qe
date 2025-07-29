import logging

from Constants import USER_NAME, USER_PASSWORD
from FhirHelpersUtils import connect_to_server
from FhirHelpersCohortExtraction import patients_with_asthma_copd, filter_main_diagnosis, filter_icu_patients_admission


logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

""""
This script is for creating the Cohort Study Data. Study protocol requires the analysis of the patients from HauptDiagnosis with Asthma & COPD.
Here, script first finds the patients with Asthma or Copd diagnosed. And later it filters only for HauptDiagnosis from their Encounter references.
HauptDiagnoses are flagged as "CC" (Chief Complaints") in Encounter.Diagnosis.
Results are saved in "patient_results.txt"
"""

def main():
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)

    # Get the patients with "ANY TYPE OF DIAGNOSED" Asthma or COPD.
    patients_with_asthma_copd(smart)

    # Filter the patients for only "MAIN DIAGNOSED" Asthma or COPD.
    filter_main_diagnosis(smart)

    # Filter patients per type of admission (Intensive-Care-Unit)
    filter_icu_patients_admission(smart)

if __name__ == "__main__":
    main()




