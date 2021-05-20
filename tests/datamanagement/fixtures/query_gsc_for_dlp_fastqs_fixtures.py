import pytest

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
