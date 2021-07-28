import pytest
import numpy as np

@pytest.fixture
def int_sequence():
	return [1,1,1,2,3,3,4]

@pytest.fixture
def nan_sequence():
	return [np.nan, np.nan, np.nan]

@pytest.fixture
def none_sequence():
	return [None, None, None]

@pytest.fixture
def mixed_none_nan_sequence():
	return [np.nan, None, np.nan]

@pytest.fixture
def mixed_sequence():
	return [np.nan, 1, 1, None, np.nan, np.nan, 2]