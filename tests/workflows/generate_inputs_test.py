from workflows.generate_inputs import generate_sample_info

import unittest
from mock import patch

import random
import pandas as pd

# helper method to generate mock return value for ColossusApi().list("sublibraries")
def mock_colossus_api_sublibrary_ret_val(size=3):
	conditions = ['A', 'NTC', 'G', 'B', 'TEST']
	primers = ['TAAGGC', 'TGAGTG', 'TTGCGG', 'GGTTTC', 'TCGGGA', 'CTAAGG', 'TTATTC']

	for i in range(1, size+1):
		obj = {}

		obj['pick_met'] = 'C' + str(random.randint(1,10))
		obj['condition'] = conditions[random.randint(0,4)]
		obj['img_col'] = random.randint(1,20)
		obj['row'] = random.randint(1,10)
		obj['column'] = random.randint(1,10)
		obj['primer_i5'] = primers[random.randint(0,6)]
		obj['index_i5'] = 'i5_' + str(random.randint(1,20))
		obj['primer_i7'] = primers[random.randint(0,6)]
		obj['index_i7'] = 'i7_' + str(random.randint(1,20))

		sample = {
			'sample_id': 'sample_' + str(i),
			'sample_type': 'test',
		}
		obj['sample_id'] = sample

		yield obj

# helper method to generate mock return value for ColossusApi().list("sublibraries")
# deliberately drop few columns
def mock_colossus_api_sublibrary_ret_val_missing(size=3):
	conditions = ['A', 'NTC', 'G', 'B', 'TEST']
	primers = ['TAAGGC', 'TGAGTG', 'TTGCGG', 'GGTTTC', 'TCGGGA', 'CTAAGG', 'TTATTC']

	for i in range(1, size+1):
		obj = {}

		obj['pick_met'] = 'C' + str(random.randint(1,10))
		obj['img_col'] = random.randint(1,20)
		obj['row'] = random.randint(1,10)
		obj['column'] = random.randint(1,10)
		obj['primer_i5'] = primers[random.randint(0,6)]
		obj['index_i5'] = 'i5_' + str(random.randint(1,20))
		obj['primer_i7'] = primers[random.randint(0,6)]
		obj['index_i7'] = 'i7_' + str(random.randint(1,20))

		sample = {
			'sample_id': 'sample_' + str(i),
		}
		obj['sample_id'] = sample

		yield obj

class GenerateInputsTestCase(unittest.TestCase):
	def setUp(self):
		self.library_id = "A12345"
		# sample_info created by generate_sample_info should have 14 columns.
		self.expected_numcol = 14

	@patch('workflows.generate_inputs.colossus_api.list')
	def test_generate_sample_info_success_case(self, mock_colossus_api_list):
		numRow = 5
		mock_colossus_api_list.return_value = mock_colossus_api_sublibrary_ret_val(size=numRow)

		sample_info = generate_sample_info(library_id=self.library_id)

		self.assertTrue(isinstance(sample_info, pd.DataFrame))
		self.assertTrue(sample_info.shape[0] == numRow)
		self.assertTrue(sample_info.shape[1] == self.expected_numcol)

	@patch('workflows.generate_inputs.colossus_api.list')
	def test_generate_sample_info_zero_row_case(self, mock_colossus_api_list):
		numRow = 0
		mock_colossus_api_list.return_value = mock_colossus_api_sublibrary_ret_val(size=numRow)

		sample_info = generate_sample_info(library_id=self.library_id)

		self.assertTrue(isinstance(sample_info, pd.DataFrame))
		self.assertTrue(sample_info.shape[0] == numRow)
		self.assertTrue(sample_info.shape[1] == 0)

	@patch('workflows.generate_inputs.colossus_api.list')
	def test_generate_sample_info_api_missing_properties_case(self, mock_colossus_api_list):
		numRow = 5
		mock_colossus_api_list.return_value = mock_colossus_api_sublibrary_ret_val_missing(size=numRow)

		with self.assertRaises(KeyError):
			generate_sample_info(library_id=self.library_id)

if __name__ == '__main__':
	unittest.main()