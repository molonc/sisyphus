import pytest
import pickle
import os

from tests.utils import get_test_root_dir

COLOSSUS_TEST_FIXTURE_DIR = os.path.join(get_test_root_dir(), 'dbclients', 'fixtures')

@pytest.fixture
def colossus_list_sublibraries():
	"""
	Load saved API result and return the result. API result was generated by randomly subsetting the result.
	It's meant to mock ColossusApi().list("sublibraries")
	"""
	colossus_sublibrary_save = os.path.join(COLOSSUS_TEST_FIXTURE_DIR, "colossus_list_sublibraries.pkl")
	with open(colossus_sublibrary_save, 'rb') as fh:
		sublibraries = pickle.load(fh)

	return sublibraries