# CALM-QE

This repository is developed to create "Study Data" for [CALM-QE Project]( https://www.calm-qe.de/).


The purpose of this set of scripts is to determine a cohort dataset specifically for patients whose primary diagnoses are associated with Asthma or Chronic Obstructive Pulmonary Disease (COPD). The scripts identify and extract
locally relevant patient data from FHIR server resources available.
This obtained cohort dataset determines a primary set to continue a further analysis to determine and quantify patients with secondary conditions, observations, and specific medication records related.

To run this project is necessary to cover the following requirements: 
-	Connection to a FHIR Server 
-	Python 3.12 
-	Docker (optional)

The installation can be orchestrated directly by copying this repository locally and following the _**Set up**_ instructions, or run it directly with [**Docker**](#run-using-docker-optional). 

#### Set up 
------------


**1. Install requirements**

Install all the required packages:

```
pip install -r requirements.txt
```
**2. Configure FHIR Server Connection**

Before running the scripts, ensure that FHIR server configurations are added in `data_extraction/Constants.py` file.
You should update the following fields including a _**.env**_ file

- USER_NAME = os.getenv("USER_NAME")
- USER_PASSWORD = os.getenv("USER_PASSWORD")
- SERVER_NAME = os.getenv("SERVER_NAME")

Or for instance by adding credentials directly in these fields:

USER_NAME = os.getenv("USER_NAME", "user1")
USER_PASSWORD = os.getenv("USER_PASSWORD", "pass123")
SERVER_NAME = os.getenv("SERVER_NAME", "server.fhir.diz.uni.de/fhir")

#### Creation of Cohort Patients List and Extraction of the Resources from Cohort Patients
------------------------------------------
This script identifies patients diagnosed with "Asthma" or "COPD".

 `ExtractCohortwithResourcesExecute.py` reads from the input_files folder `asthma_copd_codes.json` automatically. This JSON file includes all the ICD-10 codes available related to "Asthma" and "COPD". Modifications to this code list are possible based on unique needs when required.
The usage of this file is determined in `Constants.py`. 

The script outputs all the patients' IDs and corresponding diagnoses in `patients_diagnosed_asthma_copd.json`.

After the first part is complete, the analysis continues with the fetching, extraction, and count of secondary Conditions, Observations and Medication. 

The script generates separate JSON files for each resource type (e.g., Conditions, Observations, Medications) per patient.

After compiling the script, a `metadata.json` is generated as part of the outcomes to provide a general and quantitative overview of the items generated.

###### Usage:
```
python .\data_extraction\ExtractResourcesForCohortExecute.py
```

#### Applying additional requirements 

After compiling the first part of the script, `CohortPatientsAdditionalFilters.py` generates a summary of participants by extracting interested attributes from encounters.

This section of the script filters patients from `asthma_copd_codes.json` by:
- age intervals [0-5], [6-12], [13-24], and [25, ∞). 
- patients admitted in intensive care.

In addition, the script:

- calculates the length of staying for inpatients
- extracts the last 3 encounters from a patient
- exports demographics from patients

After compiling the script, a metadata.json is generated as part of the outcomes to provide a general and quantitative overview of the items generated.

###### Usage:
```
python .\data_extraction\CohortPatientsAdditionalFilters.py
```

#### Run Using Docker (OPTIONAL)
--------------------------------
Instead of setting up and running the scripts manually, you can run the scripts in a container environment. First, define the necessary credentials to connect to a FHIR Server in `dockerfile` as follows: 
- USER_NAME = "Your User Name"
- USER_PASSWORD = "Your Password"
- SERVER_NAME = "Your Fhir Server Base URL"

After making sure Docker is installed, you can run the following commands.
```
docker build -t fhir-cohort-resources-extraction .

docker run --name calm-qe fhir-cohort-resources-extraction
```




