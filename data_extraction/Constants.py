import os
import logging
from dotenv import load_dotenv

logging.basicConfig(level=logging.INFO, format='%(levelname)s - %(message)s')

load_dotenv()
USER_NAME = os.getenv("USER_NAME")
USER_PASSWORD = os.getenv("USER_PASSWORD")
SERVER_NAME = os.getenv("SERVER_NAME")
PROTOCOL = os.getenv("PROTOCOL")
ICD_CODE_FILE = "input_files/icd_codes.json"
LOINC_CODE_FILE = "input_files/loinc_codes.json"
ATC_CODE_FILE = "input_files/atc_codes.json"
OPS_CODE_FILE = "input_files/ops_codes.json"
ASTHMA_COPD_CODES_FILE = "input_files/asthma_copd_codes.json"
ICD_SYSTEM_NAME = 'http://fhir.de/CodeSystem/bfarm/icd-10-gm'
LOINC_SYSTEM_NAME = 'http://loinc.org'
ATC_SYSTEM_NAME = "http://fhir.de/CodeSystem/bfarm/atc"
OPS_SYSTEM_NAME = "http://fhir.de/CodeSystem/bfarm/ops"
DISCHARGE_CODE_CONTEXT_URL = "http://ihe-d.de/CodeSystems/FallkontextBeiDokumentenerstellung|E230"
ACT_ENCOUNTER_TYPE_URL = "http://fhir.de/CodeSystem/kontaktart-de"
MAX_WORKERS = min(6, (os.cpu_count() or 1) * 5)

raw_fhir_ssl_verify = os.getenv("FHIR_SSL_VERIFY").strip().lower()
if raw_fhir_ssl_verify in ["true", "1", "yes", ""]:
    FHIR_SSL_VERIFY = True
elif raw_fhir_ssl_verify in ["false", "0", "no"]:
    FHIR_SSL_VERIFY = False
    logging.warning("SSL verification is DISABLED (FHIR_SSL_VERIFY=false)")
else:
    raise ValueError(f"Invalid FHIR_SSL_VERIFY value in .env ({raw_fhir_ssl_verify})")