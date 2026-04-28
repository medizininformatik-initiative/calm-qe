import time
import logging
from requests.auth import HTTPBasicAuth
from urllib.parse import quote, urlsplit, urlunsplit

import pytz
import urllib3
from fhirclient import client
from Constants import USER_NAME, USER_PASSWORD, SERVER_NAME
from datetime import datetime, timezone
from fhirclient.models.bundle import Bundle


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
        "api_base": f"https://{user}:{pw}@{SERVER_NAME}"}

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


def parse_fhir_datetime(timestamp):
    if not timestamp:
        return None
    if timestamp.endswith('Z'):
        timestamp = timestamp[:-1] + '+00:00'
    dt = datetime.fromisoformat(timestamp)
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return dt.astimezone(timezone.utc)


def compute_los(start, end):
    if not start:
        return None
    if not end:
        tz = pytz.timezone("Europe/Berlin")
        now_local = datetime.now(tz)
        end = now_local.astimezone(pytz.UTC)
    delta = end - start
    return delta.total_seconds() / 86400