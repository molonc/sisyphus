import unittest
from mock import patch

from workflows.analysis.dlp.alignment import AlignmentAnalysis

class InputYamlGenerationTestCase(unittest.TestCase):
	# to mock
	# AlignmentAnalysis()
	# generate_sample_info
	# tantalus_api.get_storage_client
	# tantalus_api.get('sequence_dataset', id=dataset_id)
	# get_flowcell_lane(dataset['sequence_lanes'][0])
	# tantalus_api.get_dataset_file_instances(dataset['id'],'sequencedataset',storage_name,)

	@patch()
	def test_input_generation(self):
		pass

if __name__ == '__main__':
	unittest.main()