import pytest

from datamanagement.query_gsc_for_dlp_fastqs import (
	reverse_complement,
	decode_raw_index_sequence,
)

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
