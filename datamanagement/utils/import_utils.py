import pandas as pd
import datetime
from workflows.utils.colossus_utils import get_sublibraries_from_library_id
from common_utils.utils import (
    get_today,
)

def reverse_complement(sequence):
    return str(sequence[::-1]).translate(str.maketrans("ACTGactg", "TGACtgac"))

def decode_raw_index_sequence(raw_index_sequence, instrument, rev_comp_override):
    i7 = raw_index_sequence.split("-")[0]
    i5 = raw_index_sequence.split("-")[1]

    if rev_comp_override is not None:
        if rev_comp_override == "i7,i5":
            pass
        elif rev_comp_override == "i7,rev(i5)":
            i5 = reverse_complement(i5)
        elif rev_comp_override == "rev(i7),i5":
            i7 = reverse_complement(i7)
        elif rev_comp_override == "rev(i7),rev(i5)":
            i7 = reverse_complement(i7)
            i5 = reverse_complement(i5)
        else:
            raise Exception("unknown override {}".format(rev_comp_override))

        return i7 + "-" + i5

    if instrument == "HiSeqX":
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)
    elif instrument == "HiSeq2500":
        i7 = reverse_complement(i7)
    elif instrument == "NextSeq550":
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)
    elif instrument == "NovaSeq":
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)
    elif instrument == "NovaXPlus":
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)   
    else:
        raise Exception("unsupported sequencing instrument {}".format(instrument))

    return i7 + "-" + i5

def map_index_sequence_to_cell_id(cell_samples, gsc_index_sequence, gsc_library_id, valid_indexes={}, invalid_indexes=[]):
    """
    Map GSC fastq index sequence to Colossus cell ID.
    If GSC index sequence does not match Colossus index sequence add it to error list to be reported.
    Since dict and list are mutable, update and return them.

    Args:
        cell_samples (dict): dictionary with Colossus index sequence as key and cell ID as value.
        gsc_index_sequence (str): GSC fastq index sequence.
        gsc_library_id (str): GSC library ID.
        valid_indexes (dict): dictionary of valid GSC index sequence to Colossus cell ID.
        invalid_indexes (list): list of GSC indexes sequences not matching with Colossus index sequences.

    Return:
        valid_indexes (dict): dictionary of valid GSC index sequence to Colossus cell ID.
        invalid_indexes (list): list of GSC indexes sequences not matching with Colossus index sequences.
        should_skip (bool): True if internal GSC ID is used, False otherwise.
    """
    # get sample id of index
    should_skip = False
    try:
        cell_sample_id = cell_samples[gsc_index_sequence]

        # index sequence is unique across each cell
        # raise error if there is a duplicate index for the same cell?: TODO
        # TEMPORARY: allow duplicaet index for now...
        #if(gsc_index_sequence in valid_indexes):
        #    raise Exception(f"Duplicate index sequence, {gsc_index_sequence}, in library {gsc_library_id}.")
        valid_indexes[gsc_index_sequence] = cell_sample_id
    except KeyError:
        # if index is not found, check if library being imported is an internal library i.e beginning with IX
        # if so, we can skip this fastq
        if gsc_library_id.startswith("IX"):
            should_skip = True
        else:
            invalid_indexes.append(gsc_index_sequence)
            should_skip = True
    finally:
        return (valid_indexes, invalid_indexes, should_skip)

def summarize_index_errors(library_id, valid_indexes, invalid_indexes):
    """
    Generate useful error messages given list of dicts.
    1. number of invalid GSC indexes
    2. association between experiment conditions and "non-matching" indexes
        - this is calculated by subtracting number of matching indexes from total number of Colossus indexes
        - Note: number of 1 and 2 may differ!

    Args:
        library_id (str): Colossus library id
        valid_indexes (dict): dictionary of valid GSC index sequence to Colossus cell ID.
        invalid_indexes (list): list of GSC indexes sequences not matching with Colossus index sequences.

    Return:
        num_index_errors (int): number of invalid indexes
        errors (dict): dictionary with experiment condition as key, and tuple, (matching index, unmatching index, total index), as value
    """
    # Report total number of indexes not found
    num_index_errors = len(invalid_indexes)

    if(num_index_errors == 0):
        return (0, {})    

    # Find experimental conditions indexes are associated with
    sublibraries = get_sublibraries_from_library_id(library_id)

    sublib_df = pd.DataFrame(sublibraries)
    # construct index sequence
    sublib_df['index_sequence'] = sublib_df["primer_i7"] + "-" + sublib_df["primer_i5"]
    
    # dataframe from GSC valid indexes
    gsc_index_df = pd.DataFrame(data={'index_sequence': list(valid_indexes.keys())})

    # join on 'index_sequence' column to find all matching indexes
    # we only want index sequence and condition
    cols_to_filter = ['index_sequence', 'condition']
    merged_df = sublib_df[cols_to_filter].merge(gsc_index_df, on='index_sequence')

    errors = {}
    # iterate experiment conditions
    for condition in merged_df['condition'].unique():
        total = sublib_df[ sublib_df['condition'] == condition ].shape[0]
        matched = merged_df[ merged_df['condition'] == condition ].shape[0]
        unmatched = total - matched

        errors[condition] = (matched, unmatched, total)
    
    return (num_index_errors, errors)

def raise_index_error(num_index_errors, errors):
    """
    Raise exception if there is at least one unmatching GSC index to Colossus index.

    Args:
        num_index_errors (int): number of invalid indexes
        errors (dict): dictionary with experiment condition as key, and tuple, (matching index, unmatching index, total index), as value
    """

    error_messages = []
    for condition in errors:
        matched = errors[condition][0]
        unmatched = errors[condition][1]
        total = errors[condition][2]

        # skip if no unmatching index
        if(unmatched == 0):
            continue

        error_message = f"Experiment condition, {condition}: {unmatched} / {total} missing."
        error_messages.append(error_message)

    index_error_message = f"Number of missing/duplicate indexes: {num_index_errors}"
    error_messages.append(index_error_message)

    raise Exception('\n'.join(error_messages))

def filter_failed_libs_by_date(failed_libs, days=10):
    """
    Import sometimes fails because library has recently been loaded.
    Filter these libraries based on date.

    Args:
        failed_libs (list): list of dicts that have failed libraries
        days: days to filter by; subset failed_libs by libraries older/later than this day

    Return:
        recent_failed_libs (list): recently failed libs as filtered by days arg
        old_failed_libs (list): old failed libs as filtered by days arg
    """
    filter_date = get_today() - datetime.timedelta(days=days)

    recent_failed_libs = [lib for lib in failed_libs if datetime.datetime.strptime(lib['lane_requested_date'], '%Y-%m-%d') >= filter_date]
    old_failed_libs = [lib for lib in failed_libs if datetime.datetime.strptime(lib['lane_requested_date'], '%Y-%m-%d') < filter_date]

    return (recent_failed_libs, old_failed_libs)

