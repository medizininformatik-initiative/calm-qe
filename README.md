# CALM-QE

This repository is developed to create "Study Data" for [CALM-QE Project]( https://www.calm-qe.de/).

The purpose of this set of scripts is to identify a cohort of patients whose diagnoses are associated with Asthma or Chronic Obstructive Pulmonary Disease (COPD) from a given FHIR server. The scripts extract the relevant patient population (the “cohort”) based on these conditions.

In addition, they retrieve comprehensive clinical data for each patient in the cohort to support further analysis. This includes secondary conditions, observations, and associated medication records.

To run this project, it is necessary to cover the following requirements: 
-	Connection to a FHIR Server 
-	Python 3.12 
-	Docker (optional)

The installation can be orchestrated directly by copying this repository locally and following the _**Set up**_ instructions, or run it directly with [**Docker**](#run-using-docker-optional). 

## Set up
### 1. Install requirements

Install all the required packages:

```bash
pip install -r requirements.txt
```
### 2. Configure FHIR Server Connection

Before running the scripts, ensure that FHIR server configurations are added in `data_extraction/Constants.py` file. 
You should update the following fields including a _**.env**_ file:

```env
USER_NAME=user1
USER_PASSWORD=pass123
SERVER_NAME=server.fhir.diz.uni.de/fhir
PROTOCOL=https
```
An example environment file (`.env.example`) is included in the repository.


Or for instance by adding credentials directly in these fields:
```python
USER_NAME = os.getenv("USER_NAME", "user1")
USER_PASSWORD = os.getenv("USER_PASSWORD", "pass123")
SERVER_NAME = os.getenv("SERVER_NAME", "server.fhir.diz.uni.de/fhir")
PROTOCOL = os.getenv("PROTOCOL", "https")
```
## Creation of Cohort Patients List and Extraction of the Resources from Cohort Patients
This script identifies patients diagnosed with "Asthma" or "COPD".

 `ExtractCohortwithResourcesExecute.py` reads from the input_files folder `asthma_copd_codes.json` automatically. This JSON file includes all the ICD-10 codes available related to "Asthma" and "COPD". Modifications to this code list are possible based on unique needs when required.
The usage of this file is determined in `Constants.py`. 

The script outputs all the patients' IDs and corresponding diagnoses in `patients_diagnosed_asthma_copd.json`.

After the first part is complete, the analysis continues with the fetching, extraction, and counting of secondary Conditions, Observations, and Medication. 

The script generates separate JSON files for each resource type (e.g., Conditions, Observations, Medications) per patient.

After compiling the script, a `metadata.json` is generated as part of the outcomes to provide a general and quantitative overview of the items generated.

### Usage:
```bash
python .\data_extraction\ExtractResourcesForCohortExecute.py
```

## Applying additional requirements 

After compiling the first part of the script, `CohortPatientsAdditionalFilters.py` generates a summary of participants by extracting interested attributes from encounters.

This section of the script filters patients from `asthma_copd_codes.json` by:
- age intervals [0-5], [6-12], [13-24], and [25, ∞). 
- patients admitted in intensive care.

In addition, the script:

- calculates the length of staying for inpatients
- extracts the last 3 encounters from a patient
- exports demographics from patients

After compiling the script, a metadata.json is generated as part of the outcomes to provide a general and quantitative overview of the items generated.

### Usage:
```bash
python .\data_extraction\CohortPatientsAdditionalFilters.py
```
### Additional notes:
Each additional filter have a enable-disable option, in case not all the filters are required to apply. 

Example:
```python
filter_patients_by_age_interval(smart, encounters_filepath, min_age=min_age, max_age=max_age, enabled=False)
```

## Run Using Docker (OPTIONAL)
Instead of setting up and running the scripts manually, you can run them in a containerized environment.
Please refer to the [Set up](#set-up) section for instructions on how to create and configure the `.env` file.

### Build the Docker image
```bash
docker build -t fhir-cohort-resources-extraction .
```
### Run the container
```bash
docker run --rm \
           --env-file .env \
           --name calm-qe \
           -v ./additional_results:/app/additional_results \
           -v ./fhir_results:/app/fhir_results \
           -v ./graphs:/app/graphs \
           fhir-cohort-resources-extraction
```

### Alternative: using docker compose

To avoid plain-text credentials, 
1. Create a `.env`, in the main project's path, and store your credentials: 

   ```dotenv
   USER_NAME=YOUR_USERNAME
   USER_PASSWORD=YOUR_PASSWORD
   SERVER_NAME=YOUR_SERVER_NAME
   PROTOCOL=YOUR_PROTOCOL_TYPE
   ```
2. Run the following command after making sure docker is already installed.

   ```bash
   docker-compose up -d
   ```




