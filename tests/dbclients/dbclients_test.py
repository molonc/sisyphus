import unittest
import requests
from dbclients.utils.dbclients_utils import (
    get_tantalus_base_url,
    get_colossus_base_url,
)

COLOSSUS_BASE_URL = get_colossus_base_url()
TANTALUS_BASE_URL = get_tantalus_base_url()

class ApiEndpointTestCase(unittest.TestCase):
	def test_tantalus_endpoint_exist(self):
		try:
			request = requests.get(TANTALUS_BASE_URL)
		except:
			self.fail(f"{TANTALUS_BASE_URL} may not be a valid URL!")

		self.assertTrue(request.status_code == 200, f"{TANTALUS_BASE_URL} returned status code: {request.status_code}")

	def test_colossus_endpoint_exist(self):
		try:
			request = requests.get(COLOSSUS_BASE_URL)
		except:
			self.fail(f"{COLOSSUS_BASE_URL} may not be a valid URL!")

		self.assertTrue(request.status_code == 200, f"{COLOSSUS_BASE_URL} returned status code: {request.status_code}")

if __name__ == '__main__':
	unittest.main()