import pytest
import numpy as np

from workflows.scripts.low_complexity_filter import (
	rle,
	get_human_chr_category,
)

from tests.workflows.fixtures.low_complexity_filter_fixtures import (
	int_sequence,
	nan_sequence,
	none_sequence,
	mixed_none_nan_sequence,
	mixed_sequence,
)

class Test_RLE():
	def test_rle_test_ints(self, int_sequence):
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

	def test_rle_test_na(self, nan_sequence):
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

	def test_rle_test_none(self, none_sequence):
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

	def test_rle_test_na_none(self, mixed_none_nan_sequence):
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

	def test_rle_test_mixed(self, mixed_sequence):
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

def test_get_human_chr_category():
	"""
	Should return all possible human chromosomes (i.e. 1, 2, ..., 22, X, Y)
	"""
	expected = [
		'1', '2', '3', '4', '5', '6', '7', '8', '9', '10',
		'11', '12', '13', '14', '15', '16', '17', '18', '19', 
		'20', '21', '22', 'X', 'Y',
	]
	obs = get_human_chr_category()

	assert (obs == expected)

