from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import os
import time
import random
import logging
import requests
import traceback

log = logging.getLogger('sisyphus')


class GSCAPI(object):
    def __init__(self):
        """
        Create a session object, authenticating based on the tantalus user.
        """

        self.request_handle = requests.Session()

        self.headers = {
            "Content-Type":
            "application/json",
            "Accept":
            "application/json",
            "User-Agent":
            'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/56.0.2924.87 Safari/537.36',
        }

        self.gsc_api_url = os.environ.get("GSC_API_URL", "https://sbs.bcgsc.ca:8100/")

        create_session_url = os.path.join(self.gsc_api_url, "session")
        auth_json = {
            "username": os.environ.get("GSC_API_USERNAME"),
            "password": os.environ.get("GSC_API_PASSWORD"),
        }

        # TODO: prompt for username and password if none are provided
        response = self.request_handle.post(create_session_url, json=auth_json, headers=self.headers)

        if response.status_code == 200:
            # Add the authentication token to the headers.
            token = response.json().get("token")
            self.headers.update({"X-Token": token})
        else:
            raise Exception("unable to authenticate GSC API")

    def query(self, query_string):
        """
        Query the gsc api.
        """

        query_url = self.gsc_api_url + query_string
        retries = 5
        for retry in range(retries):
            try:
                if retry != 0:
                    wait_time = random.randint(10, 60)
                    log.info("Waiting {} seconds before connecting to GSC".format(wait_time))
                    time.sleep(wait_time)

                result = self.request_handle.get(query_url, headers=self.headers).json()
                break
            except Exception:
                log.error("Connecting to GSC failed. Retrying.")

                if retry < retries - 1:
                    traceback.print_exc()
                else:
                    log.error("Failed all retry attempts")
                    raise

        if "status" in result and result["status"] == "error":
            raise Exception(result["errors"])

        return result


<<<<<<< HEAD
raw_instrument_map = {"HiSeq": "HiSeq2500", "HiSeqX": "HiSeqX", "NextSeq": "NextSeq550","NovaSeq":"NovaSeq", "NovaXPlus":"NovaXPlus"}
=======
raw_instrument_map = {"HiSeq": "HiSeq2500", "HiSeqX": "HiSeqX", "NextSeq": "NextSeq550","NovaSeq":"NovaSeq","NovaXPlus":"NovaXPlus"}
>>>>>>> 1116d6ec72c91719c3477d3c84f81a118bcab80d


def get_sequencing_instrument(machine):
    """
    Sequencing instrument decode.
    Example machines are HiSeq-27 or HiSeqX-2.
    """

    raw_instrument = machine.split("-")[0]
    return raw_instrument_map[raw_instrument]
