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
import re
from collections import defaultdict


# for async blob upload
import asyncio

import settings

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

from common_utils.utils import (
    get_today,
    validate_mode,
)

from datamanagement.utils.import_utils import (
    reverse_complement,
    decode_raw_index_sequence,
    map_index_sequence_to_cell_id,
    summarize_index_errors,
    raise_index_error,
    filter_failed_libs_by_date,
)

import requests
import json
url = "https://monitors.molonc.ca/api/1/flags/"


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

def rev_comp(seq):
    complement = {'A': 'T', 'C': 'G', 'G': 'C', 'T': 'A'}
    return "".join(complement.get(base, base) for base in reversed(seq))

def trim_string(s):
    if len(s) > 16:
        return rev_comp(s)[:16]  # Return the first 16 characters
    return s  # Return the original string if it's 16 characters or less


def check_index(colossus_api, data_path, dlp_library_id):
    indexs={}
    leftover=[]

    obj=colossus_api.get_colossus_sublibraries_from_library_id(dlp_library_id,True)

    for data in obj:
        data['primer_i7'] = trim_string(data['primer_i7'])
        data['primer_i5'] = trim_string(data['primer_i5'])
        index = data['primer_i7']+"-"+data['primer_i5']
        if index not in indexs:
            indexs[index] = 0

    for index in data_path:
        if index in indexs:
            indexs[index]+=1
        else:
            leftover.append(index)

    missing = []
    for key in indexs.keys():
        if indexs[key] == 0:
            missing.append(key)

    logging.info(f"leftover/total: {len(leftover)/len(data_path)}")
    logging.info(f"missing/total: {len(missing)/len(data_path)}")

    if (len(leftover)/len(data_path)>0.5):
        rev_indexs={}
        rev_leftover=[]
        rev_missing=[]
        for data in obj:
#            data['primer_i7'] = trim_string(data['primer_i7'])
#            data['primer_i5'] = trim_string(data['primer_i5'])
            index = rev_comp(data['primer_i7'])+"-"+rev_comp(data['primer_i5'])
            if index not in rev_indexs:
                rev_indexs[index] = 0

        for index in data_path:
            if index in rev_indexs:
                rev_indexs[index]+=1
            else:
                rev_leftover.append(index)

        for key in rev_indexs.keys():
            if rev_indexs[key] == 0:
                rev_missing.append(key)

        #if (len(rev_missing) == 0 and len(rev_leftover) == 0):
        if (len(rev_missing) == 0):
            return 0
        else:
            logging.info("index is reverse complmented")
            #logging.info(f"rev Index: {rev_indexs}")
            logging.info(f"missing Index: {len(rev_missing)}")
            logging.info(f"leftover Index: {len(rev_leftover)}")
            return 1

    if (len(missing) ==0):
        return 0
    else: 
        logging.info(f"missing Index:{len(missing)}")
        logging.info(f"leftover Index: {len(leftover)}")
        return 1

def map_flowcell_to_fastq_info(gsc_api, gsc_library_id, gsc_fastq_infos, external_identifier, dlp_library_id):
    """
    Initialize flowcell mapping where key will be  flowcell id and value is flowcell code

    Args:
        gsc_api (obj): GSC API instance
        gsc_library_id (str): GSC internal library ID
        gsc_fastq_infos (list): raw fastq data as returned by GSC API query

    Return:
        gsc_lane_fastq_file_infos (dict): dictionary with flowcell information as key and fastq info as value
        primer_libcore (dict): dictionary with primer ID as key and libcore ID as value.
    """
    sequencing_instrument_map = {'HiSeqX': 'HX', 'HiSeq2500': 'H2500', 'NovaSeq':'NovaSeq', 'NovaXPlus':'NovaXPlus','NovaXPlus':'NovaXPlus', "NovaXPlus-1":"NovaXPlus"}
    
    flowcell_id_mapping = {}
    gsc_lane_fastq_file_infos = defaultdict(list)
    data_path=[]

    # To avoid multiple calls to fetch primer, create dictionary keyed by primer_id
    # with set of libcore ids as expected value. This way we can make a single batch call
    # to fetch primers
    primer_libcore = defaultdict(set)

    #controls = ['SA1015', 'SA039']

    for fastq_info in gsc_fastq_infos:
        print(len(data_path))
        # check if cell condition start with GSC as we do not have permission to these
        if gsc_library_id.startswith("IX") and fastq_info["libcore"]["library"]["cell_condition"] != None and  fastq_info["libcore"]["library"]["cell_condition"].startswith("GSC-"):
            continue

        #try:
        #    if ((fastq_info["libcore"]['library']['external_identifier'].split("_")[1] != dlp_library_id)):
        #        continue
        #except Exception as e:
        #    #if fastq_info["libcore"]['library']['external_identifier'] not in controls:
        #    continue

        print("still going")
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
        data_path.append(fastq_info["data_path"].split("/")[-1].split("_")[3])
        print(len(data_path))

    return (gsc_lane_fastq_file_infos, primer_libcore, data_path)

def map_libcore_id_to_primer(primer_infos, primer_libcore):
    """
    Map libcore ID to primer.

    Args:
        primer_infos: primers as returned by GSC API given primer IDs
        primer_libcore: dictionary with primer ID as key and libcore ID as value

    Return:
        libcore_primers (dict): dictionary with libcore ID as key and primer as value 
    """
    # create dictionary keyed by libcore id with primer info as expected value
    libcore_primers = {}
    for primer in primer_infos:
        # fetch libcore id from previously constructed primer libcore mapping
        libcore_ids = primer_libcore[str(primer['id'])]

        for libcore_id in libcore_ids:
            # link primer info to libcore id
            libcore_primers[libcore_id] = primer

    return libcore_primers

def upload_blob_async(blob_infos, storage, tantalus_api):
    """
    Asynchronously upload blob to Azure

    Args:
        storage: (str) to storage details for transfer
        blob_infos (list of tuple): list of tuple (fastq_path, tantalus_filename, tantalus_path)
    """
    concurrency = 3
    async_blob_client = tantalus_api.get_storage_client(storage['name'], is_async=True, concurrency=concurrency)

    async_blob_upload_data = []
    for fastq_path, tantalus_filename, tantalus_path in blob_infos:
        if(storage['storage_type'] == 'server'):
            continue
        elif storage['storage_type'] == 'blob':
            async_blob_upload_data.append((tantalus_filename, fastq_path))
        else:
            raise ValueError("Unexpected storage type. Must be one of 'server' or 'blob'!")

    if(storage['storage_type'] == 'blob'):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(async_blob_client.batch_upload_files(async_blob_upload_data))


def get_data_from_GSC(gsc_library_id, dlp_library_id, external_identifier, colossus_api, sequencing, update):
    """
    Query data from GSC and return it
    """
    mode = settings.mode
    validate_mode(mode)

    if(mode == 'production'):
        gsc_api = GSCAPI()
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
                sequencing,
                gsc_library_id,
                gsc_library_info["name"],
                external_identifier,
                update=update,
            )
            gsc_library_id = gsc_library_info["name"]
            gsc_fastq_infos = gsc_api.query(f"concat_fastq?parent_library={gsc_library_id}")

        return gsc_fastq_infos    
    # for non-production usage, import test data
    else:
        pass


def upload_file(fastq_path, tantalus_filename, tantalus_path, storage, storage_client, update):
    """
    Transfer fastq path to local server or blob storage.

    Args:
        fastq_path (str): path to fastq file
        tantalus_filename: formatted Tantalus compatible filename
        tantalus_path (str): system path to tantalus_filename
        storage: (str) to storage details for transfer
        storage_client (BlobStorageClient): BlobStorageClient object 
        update (bool): update an existing dataset
    """
    # transfer fastq if destination storage is server type
    if storage['storage_type'] == 'server':
        rsync_file(fastq_path, tantalus_path)

    # create blob if destination storage blob type
    elif storage['storage_type'] == 'blob':
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

def query_colossus_dlp_cell_info(colossus_api, library_id):

    sublibraries = colossus_api.get_colossus_sublibraries_from_library_id(library_id)

    cell_samples = {}
    for sublib in sublibraries:
        if len(sublib["primer_i7"]) > 16:
            index_sequence = reverse_complement(sublib["primer_i7"])[:16] + "-" + reverse_complement(sublib["primer_i5"])[:16]
        else:
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
        logging.error(f'no libraries with ID {external_identifier} in gsc api')
        return None
    elif len(library_infos) > 1:
        return library_infos[0]

    # Querying by external_identifier had exactly one result.
    library_info = library_infos[0]

    return library_info


def check_colossus_gsc_library_id(
        colossus_api,
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
    gsc_api = GSCAPI()
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

    # This pattern now only matches if the chip ID is at the very end of the string:
    dlp_chip_pattern = r"_A\d+[ABC]$"

    external_id = sequencing['external_gsc_id']

    # If the pattern is found at the end, remove it; otherwise, leave the external_id unchanged.
    if re.search(dlp_chip_pattern, external_id):
        external_identifier = re.sub(dlp_chip_pattern, "", external_id)
    else:
        external_identifier = external_id

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
    if len(gsc_fastq_infos) == 0:
        gsc_library_id = gsc_api.query(f"concat_fastq?external_identifier={external_identifier}")[0]["data_path"].split("/")[4]
        gsc_fastq_infos = gsc_api.query(f"concat_fastq?parent_library={gsc_library_id}")

    gsc_library_infos = gsc_api.query(f"library/info?external_identifier={external_identifier}")
    if not gsc_library_infos:
        logging.info("searching by library id")
        gsc_library_infos = gsc_api.query(f"library/info?external_identifier={dlp_library_id}")
        gsc_library_info = get_gsc_library_info(gsc_library_infos, dlp_library_id)
    else:
        gsc_library_info = get_gsc_library_info(gsc_library_infos, external_identifier)

    

    if len(gsc_fastq_infos) == 0:
        logging.warning(f"no fastqs available in gsc database under parent library {gsc_library_id}")

        if gsc_library_info is None:
            return None


    if check_library:
        logging.info(f'Checking data for {dlp_library_id}, {gsc_library_id}')

    else:
        logging.info(f'Importing data for {dlp_library_id}, {gsc_library_id}')

    fastq_file_info = []
    lanes = []

    gsc_lane_fastq_file_infos, primer_libcore, data_path = map_flowcell_to_fastq_info(gsc_api, gsc_library_id, gsc_fastq_infos, external_identifier, dlp_library_id)

    logging.info(f"number of fastq = {len(data_path)}")

    check = check_index(colossus_api,data_path, dlp_library_id)

    #if (check==0):
    #    logging.info("all fastq index in data path are here")
    #else:
    #    logging.info("some fastq index in data path are missing")
    #    raise Exception("some index in data path are missing")

    # get primer ids
    primer_ids = list(primer_libcore.keys())
    # query for all primers
    primer_infos = gsc_api.query("primer?id={}".format(",".join(primer_ids)))

    # create dictionary keyed by libcore id with primer info as expected value
    libcore_primers = map_libcore_id_to_primer(primer_infos, primer_libcore)

    valid_indexes = {}
    invalid_indexes = []
    read_end_errors = []
    blob_infos = []
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
        if not new and not update:
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

            blob_infos.append(
                (fastq_path, tantalus_filename, tantalus_path)
            )

    # if there is at least one mismatching index, throw exception
    #if(len(invalid_indexes) > 0):
    #    num_index_errors, errors = summarize_index_errors(colossus_api, dlp_library_id, valid_indexes, invalid_indexes)
    #    raise_index_error(num_index_errors, errors)

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
    # Need index sequence from GSC API and index sequences in Colossus
    # Need flowcell id and lane number
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
        # upload file to Azure blob
        upload_blob_async(blob_infos, storage, tantalus_api)

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
            config["default_aligner"],
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
#@click.option('--check_colossus_gsc_library_id', is_flag=True)
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
        sequencing_list = list(colossus_api.list('sequencing', sequencing_center='BCCAGSC', library__pool_id=dlp_library_id))
    elif all:
        sequencing_list = list(colossus_api.list('sequencing', sequencing_center='BCCAGSC'))
    # importing only sequencing expecting more lanes
    else:
        sequencing_list = list(colossus_api.list('sequencing', sequencing_center='BCCAGSC'))
        sequencing_list = list(
            filter(lambda s: s['number_of_lanes_requested'] != len(s['dlplane_set']), sequencing_list))

        payload = json.dumps({
            "sFlagName": "list_sequencings_colossus_new_lanes",
            "sProcess": "list_finish",
            "sDetails": " ",
            "sOutput": " "
            })
        headers = {
            'Content-Type': 'application/json'
            }
        response = requests.request("POST", url, headers=headers, data=payload)
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
            try:
                updated_sequencing = colossus_api.get("sequencing", id=sequencing["id"])
            except:
                time.sleep(30)
                updated_sequencing = colossus_api.get("sequencing", id=sequencing["id"])
            # check if lanes have been imported
            check_lanes(colossus_api, updated_sequencing, len(updated_sequencing["dlplane_set"]))

            # add lane_requested_date to import info for import status report
            import_info['lane_requested_date'] = sequencing['lane_requested_date']

            # add library to list of succesfully imported libraries
            successful_libs.append(import_info)

            logging.info(import_info)
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
    logging.info("Writing import status")
    write_import_statuses(successful_libs, recent_failed_libs, old_failed_libs)


if __name__ == "__main__":
    main()
