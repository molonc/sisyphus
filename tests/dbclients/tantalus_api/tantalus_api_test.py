"""
Suite of test cases to make sure Tantalus API returns responses compatible with Sisyphus and scpipeline.

Requires:
	1. Sisyphus conda environment
	2. Tantalus credentials
"""

import unittest
import dbclients
from dbclients.tantalus import TantalusApi

tantalus_api = TantalusApi()

class TantalusApiGetByAnalysisIdAlignTestCase(unittest.TestCase):
	# set up Tantalus API
	def setUp(self):
		# analysis ID associated with align
		align_id = 8235
		self.analysis = tantalus_api.get('analysis', id=align_id)
		self.analysis_type = 'align'

	def test_analysis_type(self):
		expected = self.analysis_type
		observed = self.analysis['analysis_type']
		message = "Expected analysis_type to be {expected}. Got {obs} instead.".format(
			expected = expected,
			obs = observed,
		)

		self.assertTrue(expected == observed, message)

	def test_fields_exist(self):
		# workflows/analysis/base.py requires the following fields to exist in the Tantalus Analysis object
		base_class_properties = [
			'analysis_type',
			'name',
			'jira_ticket',
			'version',
			'args',
			'status',
		]

		for prop in base_class_properties:
			message = 'Property, {prop}, does not exist in {_type} analysis object'.format(prop=prop, _type=self.analysis_type)
			self.assertTrue(prop in self.analysis, message)

class TantalusApiGetByAnalysisIdHmmcopyTestCase(unittest.TestCase):
	# set up Tantalus API
	def setUp(self):
		# analysis ID associated with hmmcopy
		hmmcopy_id = 8242
		self.analysis = tantalus_api.get('analysis', id=hmmcopy_id)
		self.analysis_type = 'hmmcopy'


	def test_analysis_type(self):
		expected = self.analysis_type
		observed = self.analysis['analysis_type']
		message = "Expected analysis_type to be {expected}. Got {obs} instead.".format(
			expected = expected,
			obs = observed,
		)

		self.assertTrue(expected == observed, message)

	def test_fields_exist(self):
		# workflows/analysis/base.py requires the following fields to exist in the Tantalus Analysis object
		base_class_properties = [
			'analysis_type',
			'name',
			'jira_ticket',
			'version',
			'args',
			'status',
		]

		for prop in base_class_properties:
			message = 'Property, {prop}, does not exist in {_type} analysis object'.format(prop=prop, _type=self.analysis_type)
			self.assertTrue(prop in self.analysis, message)

class TantalusApiGetByAnalysisIdAnnotationTestCase(unittest.TestCase):
	# set up Tantalus API
	def setUp(self):
		# analysis ID associated with annotation
		annotation_id = 8251
		self.analysis = tantalus_api.get('analysis', id=annotation_id)
		self.analysis_type = 'annotation'

	def test_analysis_type(self):
		expected = self.analysis_type
		observed = self.analysis['analysis_type']
		message = "Expected analysis_type to be {expected}. Got {obs} instead.".format(
			expected = expected,
			obs = observed,
		)

		self.assertTrue(expected == observed, message)

	def test_fields_exist(self):
		# workflows/analysis/base.py requires the following fields to exist in the Tantalus Analysis object
		base_class_properties = [
			'analysis_type',
			'name',
			'jira_ticket',
			'version',
			'args',
			'status',
		]

		for prop in base_class_properties:
			message = 'Property, {prop}, does not exist in {_type} analysis object'.format(prop=prop, _type=self.analysis_type)
			self.assertTrue(prop in self.analysis, message)

class TantalusApiGetStorageClientTestCase(unittest.TestCase):
	def setUp(self):
		blob_storage = 'singlecellblob'
		results_storage = 'singlecellresults'
		self.blob_storage = blob_storage
		self.results_storage = results_storage

	def test_singlecellblob_exist(self):
		try:
			blob_storage_obj = tantalus_api.get_storage_client(self.blob_storage)
		# API call fails if storage object does not exist
		except dbclients.basicclient.NotFoundError:
			self.fail(f"Storage, {self.blob_storage}, does not exist")

		isBlobStorageClientObject = isinstance(blob_storage_obj, dbclients.tantalus.BlobStorageClient)
		self.assertTrue(isBlobStorageClientObject, f"Storage, {self.blob_storage} is not of type 'dbclients.tantalus.BlobStorageClient'")

	def test_singlecellresults_exist(self):
		try:
			blob_storage_obj = tantalus_api.get_storage_client(self.results_storage)
		# API call fails if storage object does not exist
		except dbclients.basicclient.NotFoundError:
			self.fail(f"Storage, {self.results_storage}, does not exist")

		isBlobStorageClientObject = isinstance(blob_storage_obj, dbclients.tantalus.BlobStorageClient)
		self.assertTrue(isBlobStorageClientObject, f"Storage, {self.results_storage} is not of type 'dbclients.tantalus.BlobStorageClient'")	

		

if __name__ == '__main__':
	unittest.main()