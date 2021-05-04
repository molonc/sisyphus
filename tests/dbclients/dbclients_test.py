import unittest
import requests
from constants.dbclients_constants import (
	DEFAULT_TANTALUS_BASE_URL,
	DEFAULT_COLOSSUS_BASE_URL,
)

class ApiEndpointTestCase(unittest.TestCase):
	def test_tantalus_endpoint_exist(self):
		try:
			request = requests.get(DEFAULT_TANTALUS_BASE_URL)
		except:
			self.fail(f"{DEFAULT_TANTALUS_BASE_URL} may not be a valid URL!")

		self.assertTrue(request.status_code == 200, f"{DEFAULT_TANTALUS_BASE_URL} returned status code: {request.status_code}")

	def test_colossus_endpoint_exist(self):
		try:
			request = requests.get(DEFAULT_COLOSSUS_BASE_URL)
		except:
			self.fail(f"{DEFAULT_COLOSSUS_BASE_URL} may not be a valid URL!")

		self.assertTrue(request.status_code == 200, f"{DEFAULT_COLOSSUS_BASE_URL} returned status code: {request.status_code}")

if __name__ == '__main__':
	unittest.main()