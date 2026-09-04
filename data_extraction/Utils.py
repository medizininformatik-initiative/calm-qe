import time
import logging

from fhirclient.models.medicationadministration import MedicationAdministration
from fhirclient.models.medicationrequest import MedicationRequest
from fhirclient.models.medicationstatement import MedicationStatement
from urllib.parse import quote, urlsplit, urlunsplit
from concurrent.futures import ThreadPoolExecutor, as_completed

import pytz
import urllib3
from fhirclient import client
from Constants import USER_NAME, USER_PASSWORD, SERVER_NAME, PROTOCOL, MAX_WORKERS
from datetime import datetime, timezone

from Metadata import gather_metadata

try:
    from zoneinfo import ZoneInfo
except ImportError:
    ZoneInfo = None


urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def connect_to_server(user, pw, protocol="https"):
    """
    Creates the FhirClient object for requests later.
    :param user: Username for connection to server
    :param pw: Password for connection to server
    """
    user = quote(user, safe="")
    pw = quote(pw, safe="")

    settings = {
        "app_id": "calm_qe",
        "api_base": f"{protocol}://{user}:{pw}@{SERVER_NAME}"}

    smart = client.FHIRClient(settings=settings)
    smart.server.session.verify = False
    return smart

def fetch_bundle_for_code(smart, bundle, protocol="https"):
    """
    Send query request to the Fhir server via Smart,
    return the result in a bundle. If the result bundle is too big (at most 1K entries),
    it returns them in pages separately.
    :param smart: Fhir Server Connector
    :param bundle: Fhir Search Query
    :return: All results in Bundle
    """

    #handle special character
    user = quote(USER_NAME, safe="")
    password = quote(USER_PASSWORD, safe="")

    while True:
        entries = bundle.get("entry", [])
        yield entries

        next_link = next((p for p in bundle.get("link", []) if p.get("relation") == "next"), None)
        if not next_link:
            break

        url_parts = urlsplit(next_link["url"])
        url = urlunsplit((
            url_parts.scheme or protocol,
            f"{user}:{password}@{url_parts.netloc}",
            url_parts.path,
            url_parts.query,
            url_parts.fragment,
        ))

        while True:
            try:
                bundle = smart.server.request_json(url)
                break
            except Exception as exc:
                logging.error(f"Generated an exception: {exc} but continue trying.\n")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol=protocol)
                time.sleep(3)


def execute_thread_for_fetching(code_set, source, item_list, code_type, function_to_run):
    """
    Threads for running fetch queries parallel.
    """
    protocol = PROTOCOL
    smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD, protocol= protocol)
    processed = 0
    patient_counter = 0
    total_items = len(item_list)
    entry_type = code_type or source.__name__

    with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
        if code_set is None and code_type is None:
            future_to_item = {executor.submit(function_to_run, item, smart): item for item in
                              item_list}
        else:
            future_to_item = {executor.submit(function_to_run, item, code_set, source, smart): item for item in
                              item_list}
        for future in as_completed(future_to_item):
            item = future_to_item[future]
            processed += 1
            try:
                count = future.result()
                if count > 0:
                    patient_counter += 1
                logging.info(f"[{processed}/{total_items}] {item} with {count} {entry_type} entries processed")
            except Exception as exc:
                logging.error(f"[{processed}/{total_items}] [{entry_type}] {item} generated an exception: {exc}")

    ###META DATA COLLECTION###
    '''
    patient_count_with_observations: Number of cohort patients that has at least one observation
    patient_count_with_medications: Number of cohort patients that has at least one medication
    conditions_counts: Frequency of each ICD code 
    observations_counts:Frequency of each LOINC code 
    medication_counts: Frequency of each ATC code 
    procedures_counts: Frequency of each OPS code 
    '''

    if code_type == "LOINC":
        gather_metadata("patient_count_with_observations", patient_counter)
    elif code_type == "ATC":
        if source is MedicationAdministration:
            gather_metadata("patient_count_with_medicationAdministrations", patient_counter)
        elif source is MedicationRequest:
            gather_metadata("patient_count_with_medicationRequests", patient_counter)
        elif source is MedicationStatement:
            gather_metadata("patient_count_with_medicationStatements", patient_counter)
    elif code_type == "OPS":
        gather_metadata("patient_count_with_procedures", patient_counter)
    else:
        pass
    logging.info(f"---------------End of {entry_type} Code------------------------")


def parse_fhir_datetime(timestamp):
    if not timestamp:
        return None
    if timestamp.endswith('Z'):
        timestamp = timestamp[:-1] + '+00:00'
    dt = datetime.fromisoformat(timestamp)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_los(start_stamp, end_stamp):
    if not start_stamp or not end_stamp:
        return None
    delta = end_stamp - start_stamp
    return delta.total_seconds() / 86400