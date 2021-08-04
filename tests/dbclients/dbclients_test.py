import pytest
import requests
from dbclients.utils.dbclients_utils import (
    get_tantalus_base_url,
    get_colossus_base_url,
)

COLOSSUS_BASE_URL = get_colossus_base_url()
TANTALUS_BASE_URL = get_tantalus_base_url()

def test_tantalus_endpoint_exist():
	try:
		request = requests.get(TANTALUS_BASE_URL)
	except:
		raise Exception(f"{TANTALUS_BASE_URL} may not be a valid URL!")

	assert (request.status_code == 200, f"{TANTALUS_BASE_URL} returned status code: {request.status_code}")

def test_colossus_endpoint_exist():
	try:
		request = requests.get(COLOSSUS_BASE_URL)
	except:
		raise Exception(f"{COLOSSUS_BASE_URL} may not be a valid URL!")

	assert (request.status_code == 200, f"{COLOSSUS_BASE_URL} returned status code: {request.status_code}")