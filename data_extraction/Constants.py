import os
from dotenv import load_dotenv

load_dotenv()
USER_NAME = os.getenv("USER_NAME")
USER_PASSWORD = os.getenv("USER_PASSWORD")
SERVER_NAME = os.getenv("SERVER_NAME")
ICD_CODE_FILE = "../input_files/icd_codes.json"
LOINC_CODE_FILE = "../input_files/loinc_codes.json"
ATC_CODE_FILE = "../input_files/atc_codes.json"
ASTHMA_COPD_CODES_FILE = "../input_files/asthma_copd_codes.json"
ICD_SYSTEM_NAME = 'http://fhir.de/CodeSystem/bfarm/icd-10-gm'
LOINC_SYSTEM_NAME = 'http://loinc.org'
ATC_SYSTEM_NAME = "http://fhir.de/CodeSystem/bfarm/atc"
MAX_WORKERS = min(32, (os.cpu_count() or 1) * 5)
