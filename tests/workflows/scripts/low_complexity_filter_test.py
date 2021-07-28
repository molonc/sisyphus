import pytest
import numpy as np

from workflows.scripts.low_complexity_filter import rle

from tests.workflows.fixtures.low_complexity_filter_fixtures import (
	int_sequence,
	nan_sequence,
	none_sequence,
	mixed_none_nan_sequence,
	mixed_sequence,
)

def test_rle_test_ints(int_sequence):
	"""
	Fixtures
		- int_sequence: [1,1,1,2,3,3,4]
	"""
	obs = rle(int_sequence)
	expected = {
		'lengths': [3,1,2,1],
		'values': [1,2,3,4],
	}

	assert (obs == expected)

def test_rle_test_na(nan_sequence):
	"""
	Fixtures
		- nan_sequence: [np.nan, np.nan, np.nan]
	"""
	obs = rle(nan_sequence)
	expected = {
		'lengths': [1,1,1],
		'values': [np.nan, np.nan, np.nan],
	}

	assert (obs == expected)

def test_rle_test_none(none_sequence):
	"""
	Fixtures
		- none_sequence: [None, None, None]
	"""
	obs = rle(none_sequence)
	expected = {
		'lengths': [1,1,1],
		'values': [None, None, None],
	}

	assert (obs == expected)

def test_rle_test_na_none(mixed_none_nan_sequence):
	"""
	Fixtures
		- mixed_none_nan_sequence: np.nan, None, np.nan
	"""
	obs = rle(mixed_none_nan_sequence)
	expected = {
		'lengths': [1,1,1],
		'values': [np.nan, None, np.nan],
	}

	assert (obs == expected)

def test_rle_test_mixed(mixed_sequence):
	"""
	Fixtures
		- mixed_sequence: [np.nan, 1, 1, None, np.nan, np.nan, 2]
	"""
	obs = rle(mixed_sequence)
	expected = {
		'lengths': [1,2,1,1,1,1],
		'values': [np.nan, 1, None, np.nan, np.nan, 2],
	}

	assert (obs == expected)
