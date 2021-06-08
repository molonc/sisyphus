#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import string
import sys
import time
import collections
import click
import pandas as pd
import datetime
from collections import defaultdict

from datamanagement.utils.constants import LOGGING_FORMAT
from datamanagement.utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
from datamanagement.utils.comment_jira import comment_jira
import datamanagement.templates as templates
from datamanagement.utils.filecopy import rsync_file, try_gzip
from datamanagement.utils.gsc import get_sequencing_instrument, GSCAPI
from datamanagement.utils.runtime_args import parse_runtime_args
from datamanagement.fixups.add_fastq_metadata import add_fastq_metadata_yaml

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from workflows.utils.jira_utils import create_jira_ticket_from_library
from workflows.utils.colossus_utils import create_colossus_analysis
from workflows.utils.file_utils import load_json
from workflows.utils.tantalus_utils import create_qc_analyses_from_library

from dbclients.utils.dbclients_utils import (
    get_colossus_base_url,
)

from common_utils.utils import get_today

COLOSSUS_BASE_URL = get_colossus_base_url()

SOLEXA_RUN_TYPE_MAP = {"Paired": "P"}

# Mapping from filename pattern to read end, pass/fail
FILENAME_PATTERN_MAP = {
    "_1.fastq.gz": (1, True),
    "_1_*.concat_chastity_passed.fastq.gz": (1, True),
    "_1_chastity_passed.fastq.gz": (1, True),
    "_1_chastity_failed.fastq.gz": (1, False),
    "_1_*bp.concat.fastq.gz": (1, True),
    "_1_*bp_*.concat_chastity_passed.fastq.gz": (1, True),
    "_1_*bp.concat_chastity_passed.fastq.gz": (1, True),
    "_2.fastq.gz": (2, True),
    "_2_*.concat_chastity_passed.fastq.gz": (2, True),
    "_2_chastity_passed.fastq.gz": (2, True),
    "_2_chastity_failed.fastq.gz": (2, False),
    "_2_*bp.concat.fastq.gz": (2, True),
    "_2_*bp_*.concat_chastity_passed.fastq.gz": (2, True),
    "_2_*bp.concat_chastity_passed.fastq.gz": (2, True),
    "*_1_*_adapter_trimmed_*.fastq.gz": (1, True),
    "*_2_*_adapter_trimmed_*.fastq.gz": (2, True),
}

def upload_file(fastq_path, tantalus_filename, tantalus_path, storage, update):
    """
    Transfer fastq path to local server or blob storage.

    Args:
        fastq_path (str): path to fastq file
        tantalus_filename: formatted Tantalus compatible filename
        tantalus_path (str): system path to tantalus_filename
        storage: (str) to storage details for transfer
        update (bool): update an existing dataset
    """
    # transfer fastq if destination storage is server type
    if storage['storage_type'] == 'server':
        rsync_file(fastq_path, tantalus_path)

    # create blob if destination storage blob type
    elif storage['storage_type'] == 'blob':
        storage_client = TantalusApi().get_storage_client(storage['name'])
        storage_client.create(tantalus_filename, fastq_path, update=update)

def validate_file_extension(fastq_path):
    """
    Validate fastq extension
    """
    extension = ''
    if fastq_path.endswith('.gz'):
        extension = '.gz'
    elif not fastq_path.endswith('.fastq'):
        raise ValueError(f'unknown extension for filename {fastq_path}')

    return extension


def check_gzipped(fastq_path, check_library):
    """
    Check if a file is gzipped.

    Args:
        fastq_path (str): path to fastq file
        check_library (bool): only check the library, dont load

    Return:
        True if file is gzipped False otherwise
    """
    try:
        try_gzip(fastq_path)
    except Exception as e:
        if check_library:
            logging.warning('failed to gunzip: {}'.format(e))
            return False
        # check if gunzip failed due to fastqs being empty, if so import anyways
        elif os.path.getsize(fastq_path) == 0:
            logging.info(f"{fastq_path} is empty; importing anyways")
            return True
        # gunzip failed, raise error
        else:
            raise Exception(e)
    return True

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
    sublibraries = ColossusApi().list('sublibraries', library__pool_id=library_id)

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

def validate_fastq_from_filename(filename_pattern):
    """
    Check if fastq is valid given its filename pattern

    Args:
        filename_pattern: fastq filename pattern

    Return:
        is_valid: True if valid fastq else False
        debug: True if debugging information needs to be saved else False
    """
    is_valid = True
    debug = False

    read_end, passed = FILENAME_PATTERN_MAP.get(filename_pattern, (None, None))

    if read_end is None:
        is_valid = False
        debug = True

    if not passed:
        is_valid = False

    return (is_valid, debug)

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
    else:
        raise Exception("unsupported sequencing instrument {}".format(instrument))

    return i7 + "-" + i5


def query_colossus_dlp_cell_info(colossus_api, library_id):

    sublibraries = colossus_api.get_colossus_sublibraries_from_library_id(library_id)

    cell_samples = {}
    for sublib in sublibraries:
        index_sequence = sublib["primer_i7"] + "-" + sublib["primer_i5"]
        cell_samples[index_sequence] = sublib["sample_id"]["sample_id"]

    return cell_samples


def query_colossus_dlp_rev_comp_override(colossus_api, library_id):
    library_info = colossus_api.query_libraries_by_library_id(library_id)

    rev_comp_override = {}
    for sequencing in library_info["dlpsequencing_set"]:
        for lane in sequencing["dlplane_set"]:
            rev_comp_override[lane["flow_cell_id"]] = sequencing["rev_comp_override"]

    return rev_comp_override


def get_existing_fastq_data(tantalus_api, dlp_library_id):
    ''' Get the current set of fastq data in tantalus.

    Args:
        dlp_library_id: library id for the dlp run

    Returns:
        existing_data: set of tuples of the form (flowcell_id, lane_number, index_sequence, read_end)
    '''

    existing_flowcell_ids = []

    lanes = tantalus_api.list('sequencing_lane', dna_library__library_id=dlp_library_id)

    for lane in lanes:
        existing_flowcell_ids.append((lane['flowcell_id'], lane['lane_number']))

    logging.info(f"lanes already on tantalus: {existing_flowcell_ids}")
    return set(existing_flowcell_ids)


def get_gsc_library_info(library_infos, external_identifier):

    if len(library_infos) == 0:
        logging.error(f'no libraries with external_identifier {external_identifier} in gsc api')
        return None
    elif len(library_infos) > 1:
        raise Exception(f"multiple libraries with external_identifier {external_identifier} in gsc api")

    # Querying by external_identifier had exactly one result.
    library_info = library_infos[0]

    return library_info


def check_colossus_gsc_library_id(
        colossus_api,
        jira_ticket,
        sequencing,
        colossus_gsc_id,
        gsc_id,
        external_identifier,
        update=False,
):
    """ Update colossus lane and GSC library information.
    """

    if colossus_gsc_id != gsc_id:
        if not update:
            logging.warning(f"""
                GSC library id on Colossus does not match GSC library id at the GSC.\n
                Colossus GSC ID: {colossus_gsc_id}\n
                Actual GSC ID: {gsc_id}\n
                Tip: Run again with --update flag""")
            raise

        logging.info(f"Updating GSC ID on sequencing {sequencing['id']} from {colossus_gsc_id} to {gsc_id}")

        colossus_api.update(
            'sequencing',
            sequencing['id'],
            gsc_library_id=gsc_id,
            external_gsc_id=external_identifier,
        )

        comment = f"Updating GSC ID on Colossus from {colossus_gsc_id} to {gsc_id}"

    return gsc_id


def import_gsc_dlp_paired_fastqs(
        colossus_api,
        tantalus_api,
        sequencing,
        storage,
        internal_id=None,
        tag_name=None,
        update=False,
        check_library=False,
        dry_run=False,
):
    ''' Import dlp fastq data from the GSC.

    Args:
        colossus_api:   (object) Basic client for colossus
        tantalus_api:   (object) Basic client for tantalus
        sequencing:     (dict) sequencing on colossus
        storage:        (str) to storage details for transfer

    Kwargs:
        tag_name: a tag to add to imported data
        update: update an existing dataset
        check_library: only check the library, dont load
        dry_run: check for new lanes, dont import

    '''

    # Get Jira ticket and GSC sequencing id associated with the library in order to comment about import status
    dlp_library_id = sequencing["library"]
    gsc_library_id = sequencing["gsc_library_id"]
    library_info = colossus_api.query_libraries_by_library_id(dlp_library_id)
    jira_ticket = library_info["jira_ticket"]

    sequencing_colossus_path = f"{COLOSSUS_BASE_URL}/dlp/sequencing/{sequencing['id']}"

    if library_info["exclude_from_analysis"]:
        logging.info("{} is excluded from analysis; skipping check for lanes.".format(dlp_library_id))
        return False

    if internal_id is not None:
        # Check if the internal ID passed in matches the GSC ID entered in Colossus
        if gsc_library_id != internal_id:
            raise Exception(
                f"Internal library id {internal_id} given does not match Colossus gsc library id for {dlp_library_id}: {gsc_library_id}"
            )

        gsc_library_id = internal_id

    logging.info(f"Checking library {dlp_library_id} and sequencing {sequencing['id']} for additional lanes")

    # Existing fastqs in tantalus as a set of tuples of
    # the form (flowcell_id, lane_number, index_sequence, read_end)
    # skip if we are checking an existing library
    if not check_library:
        existing_data = get_existing_fastq_data(tantalus_api, dlp_library_id)

    else:
        existing_data = []

    primary_sample_id = library_info['sample']['sample_id']
    cell_samples = query_colossus_dlp_cell_info(colossus_api, dlp_library_id)
    rev_comp_overrides = query_colossus_dlp_rev_comp_override(colossus_api, dlp_library_id)

    external_identifier = f"{primary_sample_id}_{dlp_library_id}"

    gsc_api = GSCAPI()

    # Query GSC for fastqs and library information. Possibilities:
    # (1) fastqs by parent library (gsc id) has results
    # (2) library by external identifier (<primary_sample_id>_<dlp_library_id>) has results

    # The following scenarios can happen:

    # (1) and (2)
    # Check if sequencing on colossus has gsc id that matches the gsc id on gsc database
    # If above is true, import. Else update colossus and import.

    # (2) and not (1)
    # Colossus GSC id likely entered incorrectly.
    # Get correct id off GSC database, update colossus, and import.

    # (1) and not (2)
    # GSC ID is likely an internal id (begins with 'IX'). Import.

    # Not (1) and not (2)
    # No data is available on the GSC. Either database error or sequencing has not finished.

    gsc_fastq_infos = gsc_api.query(f"concat_fastq?parent_library={gsc_library_id}")
    gsc_library_infos = gsc_api.query(f"library?external_identifier={external_identifier}")
    if not gsc_library_infos:
        logging.info("searching by library id")
        gsc_library_infos = gsc_api.query(f"library?external_identifier={dlp_library_id}")

    gsc_library_info = get_gsc_library_info(gsc_library_infos, external_identifier)

    if len(gsc_fastq_infos) == 0:
        logging.warning(f"no fastqs available in gsc database under parent library {gsc_library_id}")

        if gsc_library_info is None:
            return None

    if gsc_library_info is not None:
        external_identifier = gsc_library_info["external_identifier"]
        gsc_library_id = check_colossus_gsc_library_id(
            colossus_api,
            jira_ticket,
            sequencing,
            gsc_library_id,
            gsc_library_info["name"],
            external_identifier,
            update=update,
        )
        gsc_library_id = gsc_library_info["name"]
        gsc_fastq_infos = gsc_api.query(f"concat_fastq?parent_library={gsc_library_id}")

    if check_library:
        logging.info(f'Checking data for {dlp_library_id}, {gsc_library_id}')

    else:
        logging.info(f'Importing data for {dlp_library_id}, {gsc_library_id}')

    fastq_file_info = []

    lanes = []

    gsc_lane_fastq_file_infos = defaultdict(list)

    sequencing_instrument_map = {'HiSeqX': 'HX', 'HiSeq2500': 'H2500'}

    # To avoid multiple calls to fetch primer, create dictionary keyed by primer_id
    # with set of libcore ids as expected value. This way we can make a single batch call
    # to fetch primers
    primer_libcore = collections.defaultdict(set)

    # initialize flowcell mapping where key will be  flowcell id and value is flowcell code
    flowcell_id_mapping = {}
    for fastq_info in gsc_fastq_infos:
        # check if cell condition start with GSC as we do not have permission to these
        if gsc_library_id.startswith("IX") and fastq_info["libcore"]["library"]["cell_condition"].startswith("GSC-"):
            continue

        # get flowcell id
        flowcell_id = str(fastq_info['libcore']['run']['flowcell_id'])

        # check if flowcell id hasnt already been added to mapping
        if flowcell_id not in flowcell_id_mapping:
            # query for flowcell info
            flowcell_info = gsc_api.query("flowcell?id={}".format(flowcell_id))
            # get flowcell code name
            flowcell_code = str(flowcell_info[0]['lims_flowcell_code'])
            # add to flowcell id mapping
            flowcell_id_mapping[flowcell_id] = flowcell_code
            # use flowecell code as flowcell id
            flowcell_id = flowcell_code

        else:
            flowcell_id = flowcell_id_mapping[flowcell_id]

        # get flowcell lane number
        lane_number = str(fastq_info['libcore']['run']['lane_number'])

        # get sequencing date
        sequencing_date = str(fastq_info["libcore"]["run"]["run_datetime"])

        # get sequencing instrument
        sequencing_instrument = get_sequencing_instrument(fastq_info["libcore"]["run"]["machine"])
        sequencing_instrument = sequencing_instrument_map[sequencing_instrument]

        # link libcore ids to primer id
        primer_libcore[str(fastq_info['libcore']['primer_id'])].add(fastq_info['libcore']['id'])

        # add fastq information to dictionary keyed by set of sequencing information collected above
        gsc_lane_fastq_file_infos[(flowcell_id, lane_number, sequencing_date, sequencing_instrument)].append(fastq_info)

    # get primer ids
    primer_ids = list(primer_libcore.keys())
    # query for all primers
    primer_infos = gsc_api.query("primer?id={}".format(",".join(primer_ids)))

    # create dictionary keyed by libcore id with primer info as expected value
    libcore_primers = dict()
    for primer in primer_infos:
        # fetch libcore id from previously constructed primer libcore mapping
        libcore_ids = primer_libcore[str(primer['id'])]

        for libcore_id in libcore_ids:
            # link primer info to libcore id
            libcore_primers[libcore_id] = primer

    valid_indexes = {}
    invalid_indexes = []
    read_end_errors = []
    for (flowcell_id, lane_number, sequencing_date, sequencing_instrument) in gsc_lane_fastq_file_infos.keys():

        # check if lane already imported to tantalus
        if (flowcell_id, lane_number) in existing_data:
            logging.info('Skipping fastqs with flowcell id {}, lane number {}'.format(flowcell_id, lane_number))
            new = False

        else:
            logging.info("Importing lane {}_{}.".format(flowcell_id, lane_number))
            new = True

        lanes.append({
            "flowcell_id": flowcell_id,
            "lane_number": lane_number,
            "sequencing_date": sequencing_date,
            "sequencing_instrument": sequencing_instrument,
            "new": new,
        })

        # move onto next collection of fastqs if lane already imported
        if not new:
            continue

        if dry_run:
            continue

        for fastq_info in gsc_lane_fastq_file_infos[(flowcell_id, lane_number, sequencing_date, sequencing_instrument)]:
            fastq_path = fastq_info["data_path"]

            # skip fastqs that are not yet in production
            if fastq_info["status"] != "production":
                logging.info("skipping file {} marked as {}".format(fastq_info["data_path"], fastq_info["status"]))
                continue

            # skip fastq if set to removed
            if fastq_info['removed'] is not None:
                logging.info('skipping file {} marked as removed {}'.format(
                    fastq_info['data_path'],
                    fastq_info['removed'],
                ))
                continue

            sequencing_instrument = get_sequencing_instrument(fastq_info["libcore"]["run"]["machine"])

            solexa_run_type = fastq_info["libcore"]["run"]["solexarun_type"]
            read_type = SOLEXA_RUN_TYPE_MAP[solexa_run_type]

            # get primer info by using libcore id
            primer_info = libcore_primers[fastq_info['libcore']['id']]

            # get raw index sequence from primer info
            raw_index_sequence = primer_info["adapter_index_sequence"]

            flowcell_lane = flowcell_id
            if lane_number is not None:
                flowcell_lane = flowcell_lane + "_" + str(lane_number)

            rev_comp_override = rev_comp_overrides.get(flowcell_lane)

            # get index sequence
            index_sequence = decode_raw_index_sequence(raw_index_sequence, sequencing_instrument, rev_comp_override)

            filename_pattern = fastq_info["file_type"]["filename_pattern"]

            logging.info(
                "loading fastq %s, raw index %s, index %s, %s",
                fastq_info["id"],
                raw_index_sequence,
                index_sequence,
                fastq_path,
            )

            is_valid, debug = validate_fastq_from_filename(filename_pattern)
            if not(is_valid):
                if(debug):
                    raise Exception("Unrecognized file type: {}".format(filename_pattern))
                    read_end_errors.append(filename_pattern)
                continue

            # map GSC index to Colossus sample ID
            # TODO: forward and reverse fastqs have different IDs but same index sequence. Deal with it.
            valid_indexes, invalid_indexes, should_skip = map_index_sequence_to_cell_id(cell_samples, index_sequence, gsc_library_id, valid_indexes, invalid_indexes)

            # skip if GSC internal id is used or invalid index is found
            if(should_skip):
                continue

            is_gzipped = check_gzipped(fastq_path, check_library)

            # skip sample if not gzipped
            if not(is_gzipped):
                continue

            extension = validate_file_extension(fastq_path)

            # format filename for tantalus
            # get read end of fastq
            read_end, _ = FILENAME_PATTERN_MAP.get(filename_pattern, (None, None))
            tantalus_filename = templates.SC_WGS_FQ_TEMPLATE.format(
                dlp_library_id=dlp_library_id,
                flowcell_id=flowcell_id,
                lane_number=lane_number,
                cell_sample_id=cell_samples[index_sequence],
                index_sequence=index_sequence,
                read_end=read_end,
                extension=extension,
            )
            tantalus_path = os.path.join(storage["prefix"], tantalus_filename)

            # add information needed to track fastq on tantalus
            fastq_file_info.append(
                dict(
                    dataset_type="FQ",
                    sample_id=cell_samples[index_sequence],
                    library_id=dlp_library_id,
                    library_type="SC_WGS",
                    index_format="D",
                    sequence_lanes=[
                        dict(
                            flowcell_id=flowcell_id,
                            lane_number=lane_number,
                            sequencing_centre="GSC",
                            sequencing_instrument=sequencing_instrument,
                            sequencing_library_id=gsc_library_id,
                            read_type=read_type,
                        )
                    ],
                    read_end=read_end,
                    index_sequence=index_sequence,
                    filepath=tantalus_path,
                ))

            # upload fastq file to local server or remote blob storage
            if not (check_library):
                upload_file(
                    fastq_path,
                    tantalus_filename,
                    tantalus_path,
                    storage,
                    update,
                )

    # if there is at least one mismatching index, throw exception
    if(len(invalid_indexes) > 0):
        num_index_errors, errors = summarize_index_errors(colossus_api, dlp_library_id, valid_indexes, invalid_indexes)
        raise_index_error(num_index_errors, errors)

    import_info = dict(
        dlp_library_id=dlp_library_id,
        gsc_library_id=gsc_library_id,
        lanes=lanes,
    )
    if len(fastq_file_info) == 0:
        logging.info("Available data for library {} already imported".format(dlp_library_id))
        return import_info

    # check if there exists paired fastqs for each index sequence
    fastq_paired_end_check(fastq_file_info)

    cell_index_sequences = set(cell_samples.keys())

    fastq_lane_index_sequences = collections.defaultdict(set)

    # Check that all fastq files refer to indices known in colossus
    for info in fastq_file_info:
        if info['index_sequence'] not in cell_index_sequences:
            raise Exception('fastq {} with index {}, flowcell {}, lane {} with index not in colossus'.format(
                info['filepath'], info['index_sequence'], info['sequence_lanes'][0]['flowcell_id'],
                info['sequence_lanes'][0]['lane_number']))

        flowcell_lane = (info['sequence_lanes'][0]['flowcell_id'], info['sequence_lanes'][0]['lane_number'])
        fastq_lane_index_sequences[flowcell_lane].add(info['index_sequence'])
    logging.info('all fastq files refer to indices known in colossus')

    # Check that all index sequences in colossus have fastq files
    for flowcell_lane in fastq_lane_index_sequences:
        for index_sequence in cell_index_sequences:
            if index_sequence not in fastq_lane_index_sequences[flowcell_lane]:
                raise Exception(
                    f'no fastq found for index sequence {index_sequence}, flowcell {flowcell_lane[0]}, lane {flowcell_lane[1]}'
                )

    logging.info('all indices in colossus have fastq files')

    if not check_library:
        # create dataset to track fastqs on tantalus
        dataset_ids = create_sequence_dataset_models(
            fastq_file_info,
            storage["name"],
            tag_name,
            tantalus_api,
            update=update,
        )

        # add metadata
        for dataset_id in dataset_ids:
            add_fastq_metadata_yaml(dataset_id, storage['name'], dry_run=False)

        # notify lab that library has been imported by commenting on jira ticket
        comment_status(jira_ticket, lanes, gsc_library_id, sequencing_colossus_path)

    logging.info("Library {} imported successfully".format(dlp_library_id))

    return import_info


def comment_status(jira_ticket, lanes, gsc_library_id, sequencing_colossus_path):
    """
    Comment on library JIRA ticket about successfully imported lanes

    Args:
        jira_ticket:                (str) jira ticket id ex. SC-1234
        lanes:                      (dict) lane information
        gsc_library_id:             (str) gsc library id (PX id or IX id)
        sequencing_colossus_path:   (str) url to colossus sequencing
    """
    comment = "Import successful: \n"
    # iterate through lanes of sequencing
    for lane in lanes:
        # check if lane is new
        if lane['new'] == True:
            comment += """\nLane: {}_{}
                Sequencing Date: {} """.format(
                lane["flowcell_id"],
                lane["lane_number"],
                lane["sequencing_date"],
            )

    comment += f"""\nGSC library ID: {gsc_library_id}
        {sequencing_colossus_path}"""

    # comment on jira ticket
    comment_jira(jira_ticket, comment)


def update_colossus_lane(colossus_api, sequencing, lanes):
    """ Update the colossus lanes for a sequencing

    Raises an exception if fewer lanes imported than were expected.
    """
    for lane_to_update in lanes:
        # check if lane is new and add to colossus if so
        if lane_to_update['new']:
            flowcell_id = f"{lane_to_update['flowcell_id']}_{lane_to_update['lane_number']}"
            logging.info(f"Adding lane {flowcell_id} to Colossus.")
            lane = colossus_api.get_or_create(
                "lane",
                sequencing=sequencing['id'],
                flow_cell_id=flowcell_id,
            )

            # update sequencing date if date on colossus is incorrect
            if lane['sequencing_date'] != lane_to_update['sequencing_date']:
                colossus_api.update('lane', lane['id'], sequencing_date=lane_to_update['sequencing_date'])


def check_lanes(colossus_api, sequencing, num_lanes):
    """ Check if number_of_lanes_requested is equal to number of lanes.

    Updates number_of_lanes_requested if necessary
    """

    if sequencing['number_of_lanes_requested'] < num_lanes:
        logging.info('Sequencing goal is less than total number of lanes. Updating.')
        colossus_api.update('sequencing', sequencing['id'], number_of_lanes_requested=num_lanes)
    elif sequencing['number_of_lanes_requested'] > num_lanes:
        raise Exception("Expected number of lanes is {} but total lanes imported is {}".format(
            sequencing['number_of_lanes_requested'], num_lanes))


def write_import_statuses(successful_libs, recent_failed_libs, old_failed_libs):
    """
    Writes text file regarding successful and failed imports

    Args:
        successful_libs (list): successful libs
        recent_failed_libs (list): recently failed libs.
        old_failed_libs (list): failed libs that are at least 'x' days old.
    """
    datamanagement_dir = os.environ.get('DATAMANAGEMENT_DIR', '.')
    import_status_path = os.path.join(datamanagement_dir, 'import_statuses.txt')

    if os.path.exists(import_status_path):
        os.remove(import_status_path)

    file = open(import_status_path, 'w+')
    file.write("Date: {}\n\n".format(str(datetime.date.today())))

    file.write("Successful imports: \n")
    for successful_lib in successful_libs:
        file.write(f"{successful_lib['dlp_library_id']}, {successful_lib['gsc_library_id']} \n")

    old_failed_libs.sort(key=lambda x: x['error'], reverse=True)
    file.write("\n\nFailed imports: \n")
    for failed_lib in old_failed_libs:
        file.write(f"{failed_lib['dlp_library_id']}, {failed_lib['gsc_library_id']}: {failed_lib['error']}\n")

    file.write("\n\nLibraries expected from GSC (submitted < 10 days): \n")
    for recent_lib in recent_failed_libs:
        file.write(f"{recent_lib['dlp_library_id']}, {recent_lib['gsc_library_id']}\n")

    file.close()

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

def create_tickets_and_analyses(import_info):
    """
    Creates jira ticket and an align analysis on tantalus if new lanes were imported

    Args:
        import_info (dict): Contains keys dlp_library_id, gsc_library_id, lanes
    """
    # only create tickets and analyses when new lane is imported
    if any([lane["new"] for lane in import_info['lanes']]):
        # load config file
        config = load_json(
            os.path.join(
                os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
                'workflows',
                'config',
                'normal_config.json',
            ))

        # create analysis jira ticket
        jira_ticket = create_jira_ticket_from_library(import_info["dlp_library_id"])

        # create align analysis objects
        create_qc_analyses_from_library(
            import_info["dlp_library_id"],
            jira_ticket,
            config["scp_version"],
            "align",
            aligner=config["default_aligner"],
        )

        # create analysis object on colossus
        create_colossus_analysis(
            import_info["dlp_library_id"],
            jira_ticket,
            config["scp_version"],
            config["default_aligner"],
        )


@click.command()
@click.argument('storage_name', nargs=1)
@click.option('--dlp_library_id', nargs=1)
@click.option('--internal_id')
@click.option('--tag_name')
@click.option('--all', is_flag=True)
@click.option('--update', is_flag=True)
@click.option('--check_library', is_flag=True)
@click.option('--dry_run', is_flag=True)
def main(storage_name,
         dlp_library_id=None,
         internal_id=None,
         tag_name=None,
         all=False,
         update=False,
         check_library=False,
         dry_run=False):

    # Set up the root logger
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    # Connect to the Tantalus API (this requires appropriate environment)
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    # initiate arrays to store successful and failed libraries
    successful_libs = []
    failed_libs = []

    storage = tantalus_api.get("storage", name=storage_name)
    sequencing_list = list()

    if dry_run:
        logging.info("This is a dry run. No lanes will be imported.")

    # Importing a single library
    if dlp_library_id is not None:
        sequencing_list = list(
            colossus_api.list('sequencing', sequencing_center='BCCAGSC', library__pool_id=dlp_library_id))
    # importing all libraries from the gsc
    elif all:
        sequencing_list = list(colossus_api.list('sequencing', sequencing_center='BCCAGSC'))

    # importing only sequencing expecting more lanes
    else:
        sequencing_list = list(colossus_api.list('sequencing', sequencing_center='BCCAGSC'))
        sequencing_list = list(
            filter(lambda s: s['number_of_lanes_requested'] != len(s['dlplane_set']), sequencing_list))

    for sequencing in sequencing_list:
        # import library
        try:
            import_info = import_gsc_dlp_paired_fastqs(
                colossus_api,
                tantalus_api,
                sequencing,
                storage,
                internal_id,
                tag_name,
                update=update,
                check_library=check_library,
                dry_run=dry_run,
            )

            # check if no import information exists, if so, library does not exist on GSC
            if import_info is None:
                lane_requested_date = sequencing["lane_requested_date"]
                failed_libs.append(
                    dict(
                        dlp_library_id=sequencing["library"],
                        gsc_library_id="None",
                        lane_requested_date=lane_requested_date,
                        error="Doesn't exist on GSC",
                    ))
                continue

            # check if library excluded from import
            elif import_info is False:
                continue

            # update lanes in sequencing
            update_colossus_lane(colossus_api, sequencing, import_info['lanes'])
            # get sequencing object again since sequencing may have with new info
            updated_sequencing = colossus_api.get("sequencing", id=sequencing["id"])
            # check if lanes have been imported
            check_lanes(colossus_api, updated_sequencing, len(updated_sequencing["dlplane_set"]))

            # add lane_requested_date to import info for import status report
            import_info['lane_requested_date'] = sequencing['lane_requested_date']

            # add library to list of succesfully imported libraries
            successful_libs.append(import_info)

            # create jira ticket and analyses with new lanes and datasets
            create_tickets_and_analyses(import_info)

        except Exception as e:
            # add lane_requested_date to import info for import status report
            lane_requested_date = sequencing["lane_requested_date"]
            updated_sequencing = colossus_api.get("sequencing", id=sequencing["id"])
            # add library to list of libraries that failed to import
            failed_libs.append(
                dict(
                    dlp_library_id=sequencing["library"],
                    gsc_library_id=updated_sequencing["gsc_library_id"],
                    lane_requested_date=lane_requested_date,
                    error=str(e),
                ))

            logging.exception(f"Library {sequencing['library']} failed to import: {e}")
            continue

    # Sort lists by date in descending order
    successful_libs.sort(
        key=lambda x: datetime.datetime.strptime(x['lane_requested_date'], '%Y-%m-%d'),
        reverse=True,
    )
    failed_libs.sort(
        key=lambda x: datetime.datetime.strptime(x['lane_requested_date'], '%Y-%m-%d'),
        reverse=True,
    )
    recent_failed_libs, old_failed_libs = filter_failed_libs_by_date(failed_libs)
    # write import report
    write_import_statuses(successful_libs, recent_failed_libs, old_failed_libs)


if __name__ == "__main__":
    main()
