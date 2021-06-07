import pytest
from datetime import datetime

@pytest.fixture
def i5_sequence_upper():
	return 'AGGTTT'

@pytest.fixture
def i7_sequence_upper():
	return 'GCCTAA'

@pytest.fixture
def i5_sequence_lower():
	return 'aggttt'

@pytest.fixture
def index_sequence(i5_sequence_upper, i7_sequence_upper):
	return i7_sequence_upper + "-" + i5_sequence_upper

@pytest.fixture
def sequencing_instruments():
	instruments = {
		"HiSeqX": "HiSeqX",
		"HiSeq2500": "HiSeq2500",
		"NextSeq550": "NextSeq550",
		"unknown": "unknown",
	}
	return instruments

@pytest.fixture
def rev_comp_overrides():
	rev_comp = {
		"i7,i5": "i7,i5",
		"i7,rev(i5)": "i7,rev(i5)",
		"rev(i7),i5": "rev(i7),i5",
		"rev(i7),rev(i5)": "rev(i7),rev(i5)",
		"unknown": "unknown",
	}
	return rev_comp

@pytest.fixture
def cell_samples():
	cell_samples = {
		'AAAAAA-TTTTTT': 'sample1',
		'CCCCCC-GGGGGG': 'sample2',
		'ACACAC-TGTGTG': 'sample3',
	}

	return cell_samples

@pytest.fixture
def empty_valid_indexes():
	return {}

@pytest.fixture
def empty_invalid_indexes():
	return []

@pytest.fixture
def valid_indexes():
	"""
	Return indexes that match with sample data in 'tests/dbclients/fixtures/colossus_list_sublibraries.pkl'
	"""
	indexes = {
		'ATCAGT-CGCGGC': 'SA1015-A118402B-R45-C04',
		'TATATC-GTCCTT': '10575-CL-A118402B-R51-C44',
		'CTGATC-GGTTTC': '10575-CL-A118402B-R56-C50',
	}

	return indexes

@pytest.fixture
def invalid_indexes():
	indexes = [
		'AAAAAA-AAAAAA',
		'TTTTTT-TTTTTT',
	]

	return indexes

@pytest.fixture
def gsc_internal_id():
	return "IX-11111"

@pytest.fixture
def gsc_external_id():
	return "PX-11111"

@pytest.fixture
def colossus_library_id():
	return "A99999A"

@pytest.fixture
def num_invalid_indexes():
	return 5

@pytest.fixture
def index_errors():
	# {condition: (matching index, unmatching index, total index)}
	errors = {
		'A': (1, 1, 2),
		'gDNA': (1, 2, 3),
		'NTC': (5, 2, 7),
		'A-NCC': (3, 0, 3),
	}

	return errors

@pytest.fixture
def successful_libs():
	return [
		{
			'lane_requested_date': datetime(2021, 5, 1),
			'dlp_library_id': 'A11111',
			'gsc_library_id': 'PX11111',
			'lanes': [
				{'flowcell_id': 'flowcell_1', 'lane_number': 'lane_1', 'sequencing_date': datetime(2021, 5, 5)},
				{'flowcell_id': 'flowcell_1', 'lane_number': 'lane_2', 'sequencing_date': datetime(2021, 5, 5)},
			],
		},
		{
			'lane_requested_date': datetime(2021, 5, 2),
			'dlp_library_id': 'A22222',
			'gsc_library_id': 'PX22222',
			'lanes': [
				{'flowcell_id': 'flowcell_2', 'lane_number': 'lane_1', 'sequencing_date': datetime(2021, 5, 5)},
				{'flowcell_id': 'flowcell_2', 'lane_number': 'lane_2', 'sequencing_date': datetime(2021, 5, 5)},
			],
		},
	]

@pytest.fixture
def failed_libs():
	return [
		{
			'lane_requested_date': datetime(2021, 5, 1),
			'dlp_library_id': 'A11111',
			'gsc_library_id': 'PX11111',
			'error': 'error',
		},
		{
			'lane_requested_date': datetime(2021, 5, 2),
			'dlp_library_id': 'A22222',
			'gsc_library_id': 'PX22222',
			'error': 'error',
		},
		{
			'lane_requested_date': datetime(2021, 5, 3),
			'dlp_library_id': 'A33333',
			'gsc_library_id': 'PX33333',
			'error': 'error',
		},
		{
			'lane_requested_date': datetime(2021, 5, 4),
			'dlp_library_id': 'A44444',
			'gsc_library_id': 'PX44444',
			'error': 'error',
		},
		{
			'lane_requested_date': datetime(2021, 5, 5),
			'dlp_library_id': 'A55555',
			'gsc_library_id': 'PX55555',
			'error': 'error',
		},
	]

@pytest.fixture
def recently_failed_libs():
	return [
		{
			'lane_requested_date': datetime(2021, 5, 20),
			'dlp_library_id': 'A88888',
			'gsc_library_id': 'PX88888',
			'error': 'error',
		},
		{
			'lane_requested_date': datetime(2021, 5, 21),
			'dlp_library_id': 'A99999',
			'gsc_library_id': 'PX99999',
			'error': 'error',
		},
	]