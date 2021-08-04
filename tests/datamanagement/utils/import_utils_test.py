import pytest
from datetime import datetime

from datamanagement.utils.import_utils import (
	reverse_complement,
	decode_raw_index_sequence,
	map_index_sequence_to_cell_id,
	summarize_index_errors,
	raise_index_error,
	filter_failed_libs_by_date,
)

# import fixtures
from tests.datamanagement.fixtures.import_utils_fixtures import (
	i5_sequence_upper,
	i7_sequence_upper,
	i5_sequence_lower,
	index_sequence,
	sequencing_instruments,
	rev_comp_overrides,
	cell_samples,
	empty_valid_indexes,
	empty_invalid_indexes,
	valid_indexes,
	invalid_indexes,
	gsc_internal_id,
	gsc_external_id,
	colossus_library_id,
	num_invalid_indexes,
	index_errors,
	successful_libs,
	recently_failed_libs,
	failed_libs,
)

from tests.dbclients.fixtures.colossus import (
	colossus_list_sublibraries,
)

class TestStatus():
	def test_filter_failed_libs_by_date(self, failed_libs, mocker):
		mocker.patch('datamanagement.utils.import_utils.get_today', return_value=datetime(2021, 5, 14))

		obs_recent_libs, obs_old_libs = filter_failed_libs_by_date(failed_libs, days=10)

		expected_recent_libs = [
			{
				'lane_requested_date': '2021-05-04',
				'dlp_library_id': 'A44444',
				'gsc_library_id': 'PX44444',
				'error': 'error',
			},
			{
				'lane_requested_date': '2021-05-05',
				'dlp_library_id': 'A55555',
				'gsc_library_id': 'PX55555',
				'error': 'error',
			},
		]

		expected_old_libs = [
			{
				'lane_requested_date': '2021-05-01',
				'dlp_library_id': 'A11111',
				'gsc_library_id': 'PX11111',
				'error': 'error',
			},
			{
				'lane_requested_date': '2021-05-02',
				'dlp_library_id': 'A22222',
				'gsc_library_id': 'PX22222',
				'error': 'error',
			},
			{
				'lane_requested_date': '2021-05-03',
				'dlp_library_id': 'A33333',
				'gsc_library_id': 'PX33333',
				'error': 'error',
			},
		]

		assert (obs_recent_libs == expected_recent_libs)
		assert (obs_old_libs == expected_old_libs)

#	def test_write_import_statuses(self, successful_libs, recently_failed_libs, failed_libs):
#		write_import_statuses(successful_libs, recently_failed_libs, failed_libs)

class TestIndexErrors():
	def test_summarize_index_errors(self, colossus_library_id, valid_indexes, invalid_indexes, colossus_list_sublibraries, mocker):
		mocker.patch('datamanagement.utils.import_utils.get_sublibraries_from_library_id', return_value=colossus_list_sublibraries)

		obs_errors = summarize_index_errors(colossus_library_id, valid_indexes, invalid_indexes)

		# (matching index, unmatching index, total index)
		expected_errors = (2, {'A': (2,2,4), 'gDNA': (1,0,1)})

		assert (obs_errors == expected_errors)

	def test_raise_index_error(self, num_invalid_indexes, index_errors):
		with pytest.raises(Exception) as excinfo:
			raise_index_error(num_invalid_indexes, index_errors)

		assert ("A: 1 / 2 missing." in str(excinfo.value))
		assert ("gDNA: 2 / 3 missing." in str(excinfo.value))
		assert ("NTC: 2 / 7 missing." in str(excinfo.value))
		assert ("A-NCC" not in str(excinfo.value))


class TestMapIndexSequenceToCellId():
	def test_valid_index_matching(self, cell_samples, gsc_external_id, empty_valid_indexes, empty_invalid_indexes):
		obs_valid_index, _, _ = map_index_sequence_to_cell_id(cell_samples, 'AAAAAA-TTTTTT', gsc_external_id, empty_valid_indexes, empty_invalid_indexes)
		expected_valid_index = {
			'AAAAAA-TTTTTT': 'sample1',
		}

		assert (obs_valid_index == expected_valid_index)

	def test_invalid_index_matching(self, cell_samples, gsc_external_id, empty_valid_indexes, empty_invalid_indexes):
		_, obs_invalid_index, _ = map_index_sequence_to_cell_id(cell_samples, 'AAAAAA-TTTTTT', gsc_external_id, empty_valid_indexes, empty_invalid_indexes)
		expected_invalid_index = []

		assert (obs_invalid_index == expected_invalid_index)

	def test_valid_index_unmatching_external_id(self, cell_samples, gsc_external_id,  empty_valid_indexes, empty_invalid_indexes):
		obs_valid_index, _, _ = map_index_sequence_to_cell_id(cell_samples, 'CCCCCC-CCCCCC', gsc_external_id, empty_valid_indexes, empty_invalid_indexes)
		expected_valid_index = {}

		assert (obs_valid_index == expected_valid_index)

	def test_invalid_index_unmatching_external_id(self, cell_samples, gsc_external_id, empty_valid_indexes, empty_invalid_indexes):
		_, obs_invalid_index, _ = map_index_sequence_to_cell_id(cell_samples, 'CCCCCC-CCCCCC', gsc_external_id, empty_valid_indexes, empty_invalid_indexes)
		expected_invalid_index = [
			'CCCCCC-CCCCCC',
		]

		assert (obs_invalid_index == expected_invalid_index)

	def test_invalid_index_unmatching_internal_id(self, cell_samples, gsc_internal_id, empty_valid_indexes, empty_invalid_indexes):
		_, obs_invalid_index, obs_is_internal = map_index_sequence_to_cell_id(cell_samples, 'CCCCCC-CCCCCC', gsc_internal_id, empty_valid_indexes, empty_invalid_indexes)
		expected_invalid_index = []
		expected_is_internal = True

		assert (obs_invalid_index == expected_invalid_index)
		assert (obs_is_internal == expected_is_internal)

	def test_duplicate_index(self, cell_samples, gsc_external_id, empty_valid_indexes, empty_invalid_indexes):
		obs_valid_index, _, _ = map_index_sequence_to_cell_id(cell_samples, 'AAAAAA-TTTTTT', gsc_external_id, empty_valid_indexes, empty_invalid_indexes)

		with pytest.raises(Exception) as excinfo:
			obs_valid_index, _, _ = map_index_sequence_to_cell_id(cell_samples, 'AAAAAA-TTTTTT', gsc_external_id, obs_valid_index, empty_invalid_indexes)

		assert ("Duplicate" in str(excinfo.value))

	def test_multiple_valid_index(self, cell_samples, gsc_external_id, empty_valid_indexes, empty_invalid_indexes):
		obs_valid_index, _, _ = map_index_sequence_to_cell_id(cell_samples, 'AAAAAA-TTTTTT', gsc_external_id, empty_valid_indexes, empty_invalid_indexes)
		obs_valid_index, _, _ = map_index_sequence_to_cell_id(cell_samples, 'CCCCCC-GGGGGG', gsc_external_id, obs_valid_index, empty_invalid_indexes)
		obs_valid_index, _, _ = map_index_sequence_to_cell_id(cell_samples, 'ACACAC-TGTGTG', gsc_external_id, obs_valid_index, empty_invalid_indexes)

		expected_valid_index = {
			'AAAAAA-TTTTTT': 'sample1',
			'CCCCCC-GGGGGG': 'sample2',
			'ACACAC-TGTGTG': 'sample3',
		}

		assert (obs_valid_index == expected_valid_index)

	# test multiple valid and invalid indexes
	def test_multiple_mixed_index(self, cell_samples, gsc_external_id, empty_valid_indexes, empty_invalid_indexes):
		obs_valid_index, obs_invalid_index, _ = map_index_sequence_to_cell_id(cell_samples, 'AAAAAA-TTTTTT', gsc_external_id, empty_valid_indexes, empty_invalid_indexes)
		obs_valid_index, obs_invalid_index, _ = map_index_sequence_to_cell_id(cell_samples, 'CCCCCC-CCCCCC', gsc_external_id, obs_valid_index, obs_invalid_index)
		obs_valid_index, obs_invalid_index, _ = map_index_sequence_to_cell_id(cell_samples, 'ACACAC-TGTGTG', gsc_external_id, obs_valid_index, obs_invalid_index)

		expected_valid_index = {
			'AAAAAA-TTTTTT': 'sample1',
			'ACACAC-TGTGTG': 'sample3',
		}

		expected_invalid_index = [
			'CCCCCC-CCCCCC',
		]

		assert (obs_valid_index == expected_valid_index)
		assert (obs_invalid_index == expected_invalid_index)

class TestDecodeRawIndexSequence():
	def test_reverse_complement_upper(self, i5_sequence_upper):
		obs_rev_comp = reverse_complement(i5_sequence_upper)
		expected_rev_comp = 'AAACCT'

		assert (obs_rev_comp == expected_rev_comp)

	def test_reverse_complement_lower(self, i5_sequence_lower):
		obs_rev_comp = reverse_complement(i5_sequence_lower)
		expected_rev_comp = 'aaacct'

		assert (obs_rev_comp == expected_rev_comp)

	def test_decode_raw_index_sequence_with_rev_comp_override_i7_i5(self, index_sequence, rev_comp_overrides):
		rev_comp_override = rev_comp_overrides["i7,i5"]

		obs_decoded_index_sequence = decode_raw_index_sequence(
			raw_index_sequence=index_sequence,
			instrument=None,
			rev_comp_override=rev_comp_override,
		)
		# for "i7,i5" rev_comp_override, it should return the original sequence
		expected_decoded_index_sequence = index_sequence

		assert (obs_decoded_index_sequence == expected_decoded_index_sequence)

	def test_decode_raw_index_sequence_with_rev_comp_override_i7_rev_i5(self, index_sequence, rev_comp_overrides):
		rev_comp_override = rev_comp_overrides["i7,rev(i5)"]

		obs_decoded_index_sequence = decode_raw_index_sequence(
			raw_index_sequence=index_sequence,
			instrument=None,
			rev_comp_override=rev_comp_override,
		)
		# for "i7,rev(i5)" rev_comp_override, i5 index is reverse complimented
		expected_decoded_index_sequence = "GCCTAA" + "-" + "AAACCT"

		assert (obs_decoded_index_sequence == expected_decoded_index_sequence)

	def test_decode_raw_index_sequence_with_rev_comp_override_rev_i7_i5(self, index_sequence, rev_comp_overrides):
		rev_comp_override = rev_comp_overrides["rev(i7),i5"]

		obs_decoded_index_sequence = decode_raw_index_sequence(
			raw_index_sequence=index_sequence,
			instrument=None,
			rev_comp_override=rev_comp_override,
		)
		# for "rev(i7),i5" rev_comp_override, i7 index is reverse complimented
		expected_decoded_index_sequence = "TTAGGC" + "-" + "AGGTTT"

		assert (obs_decoded_index_sequence == expected_decoded_index_sequence)

	def test_decode_raw_index_sequence_with_rev_comp_override_rev_i7_rev_i5(self, index_sequence, rev_comp_overrides):
		rev_comp_override = rev_comp_overrides["rev(i7),rev(i5)"]

		obs_decoded_index_sequence = decode_raw_index_sequence(
			raw_index_sequence=index_sequence,
			instrument=None,
			rev_comp_override=rev_comp_override,
		)
		# for "rev(i7),rev(i5)" rev_comp_override, i5 index and i7 index are reverse complimented
		expected_decoded_index_sequence = "TTAGGC" + "-" + "AAACCT"

		assert (obs_decoded_index_sequence == expected_decoded_index_sequence)

	def test_decode_raw_index_sequence_with_rev_comp_error(self, index_sequence, rev_comp_overrides):
		rev_comp_override = rev_comp_overrides["unknown"]

		with pytest.raises(Exception) as excinfo:
			obs_decoded_index_sequence = decode_raw_index_sequence(
				raw_index_sequence=index_sequence,
				instrument=None,
				rev_comp_override=rev_comp_override,
			)
		# for all other cases, it should raise an Exception
		assert ("unknown override" in str(excinfo.value))

	def test_decode_raw_index_sequence_with_HiSeqX(self, index_sequence, sequencing_instruments):
		sequencing_instrument = sequencing_instruments["HiSeqX"]

		obs_decoded_index_sequence = decode_raw_index_sequence(
			raw_index_sequence=index_sequence,
			instrument=sequencing_instrument,
			rev_comp_override=None,
		)
		# for HiSeqX, i5 index and i7 index are reverse complimented
		expected_decoded_index_sequence = "TTAGGC" + "-" + "AAACCT"

		assert (obs_decoded_index_sequence == expected_decoded_index_sequence)

	def test_decode_raw_index_sequence_with_HiSeq2500(self, index_sequence, sequencing_instruments):
		sequencing_instrument = sequencing_instruments["HiSeq2500"]

		obs_decoded_index_sequence = decode_raw_index_sequence(
			raw_index_sequence=index_sequence,
			instrument=sequencing_instrument,
			rev_comp_override=None,
		)
		# for HiSeq2500, i7 index is reverse complimented
		expected_decoded_index_sequence = "TTAGGC" + "-" + "AGGTTT"

		assert (obs_decoded_index_sequence == expected_decoded_index_sequence)

	def test_decode_raw_index_sequence_with_NextSeq550(self, index_sequence, sequencing_instruments):
		sequencing_instrument = sequencing_instruments["NextSeq550"]

		obs_decoded_index_sequence = decode_raw_index_sequence(
			raw_index_sequence=index_sequence,
			instrument=sequencing_instrument,
			rev_comp_override=None,
		)
		# for NextSeq550, i5 index and i7 index are reverse complimented
		expected_decoded_index_sequence = "TTAGGC" + "-" + "AAACCT"

		assert (obs_decoded_index_sequence == expected_decoded_index_sequence)

	def test_decode_raw_index_sequence_with_unknown_sequencing(self, index_sequence, sequencing_instruments):
		sequencing_instrument = sequencing_instruments["unknown"]

		with pytest.raises(Exception) as excinfo:
			obs_decoded_index_sequence = decode_raw_index_sequence(
				raw_index_sequence=index_sequence,
				instrument=sequencing_instrument,
				rev_comp_override=None,
			)

		assert ("unsupported sequencing instrument" in str(excinfo.value))

	def test_decode_raw_index_sequence_with_both_rev_comp_override_and_sequencing(self, index_sequence, sequencing_instruments, rev_comp_overrides):
		sequencing_instrument = sequencing_instruments["NextSeq550"]
		rev_comp_override = rev_comp_overrides["i7,rev(i5)"]

		obs_decoded_index_sequence = decode_raw_index_sequence(
			raw_index_sequence=index_sequence,
			instrument=sequencing_instrument,
			rev_comp_override=rev_comp_override,
		)
		# if both sequencing and reverse compliment override arguments are supplied
		# it should return based on rev_comp_override
		expected_decoded_index_sequence = "GCCTAA" + "-" + "AAACCT"

		assert (obs_decoded_index_sequence == expected_decoded_index_sequence)
