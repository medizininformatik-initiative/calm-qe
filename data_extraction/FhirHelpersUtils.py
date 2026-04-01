import time
from urllib.parse import quote

import urllib3
from fhirclient import client
from Constants import USER_NAME, USER_PASSWORD, SERVER_NAME

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

def connect_to_server(user, pw):
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

def fetch_bundle_for_code(smart, bundle):
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

        url = f"https://{user}:{password}@" + next_link["url"].split("://", 1)[1]
        while True:
            try:
                bundle = smart.server.request_json(url)
                break
            except Exception as exc:
                print(f"Generated an exception: {exc} but continue trying.\n")
                smart = connect_to_server(user=USER_NAME, pw=USER_PASSWORD)
                time.sleep(3)

