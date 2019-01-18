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
import pandas as pd
from collections import defaultdict
from datamanagement.utils.constants import LOGGING_FORMAT
from datamanagement.utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
import datamanagement.templates as templates
from utils.filecopy import rsync_file, try_gzip
from utils.gsc import get_sequencing_instrument, GSCAPI
from utils.runtime_args import parse_runtime_args
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi


solexa_run_type_map = {"Paired": "P"}


def reverse_complement(sequence):
    return str(sequence[::-1]).translate(string.maketrans("ACTGactg", "TGACtgac"))


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
            rev_comp_override[lane["flow_cell_id"]] = sequencing["dlpsequencingdetail"][
                "rev_comp_override"
            ]

    return rev_comp_override


# Mapping from filename pattern to read end, pass/fail
filename_pattern_map = {
    "_1.fastq.gz": (1, True),
    "_1_*.concat_chastity_passed.fastq.gz": (1, True),
    "_1_chastity_passed.fastq.gz": (1, True),
    "_1_chastity_failed.fastq.gz": (1, False),
    "_1_*bp.concat.fastq.gz": (1, True),
    "_2.fastq.gz": (2, True),
    "_2_*.concat_chastity_passed.fastq.gz": (2, True),
    "_2_chastity_passed.fastq.gz": (2, True),
    "_2_chastity_failed.fastq.gz": (2, False),
    "_2_*bp.concat.fastq.gz": (2, True),
}


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

    return set(existing_flowcell_ids)


def import_gsc_dlp_paired_fastqs(colossus_api, tantalus_api, dlp_library_id, storage, tag_name=None, update=False, check_library=False):
    ''' Import dlp fastq data from the GSC.
    
    Args:
        colossus_api: Basic client for colossus
        tantalus_api: Basic client for tantalus
        dlp_library_id: library id for the dlp run
        storage: to storage details for transfer

    Kwargs:
        tag_name: a tag to add to imported data
        update: update an existing dataset
        check_library: only check the library, dont load

    '''


    if check_library:
        logging.info('checking data for {}'.format(dlp_library_id))

    else:
        logging.info('importing data for {}'.format(dlp_library_id))

    # Existing fastqs in tantalus as a set of tuples of
    # the form (flowcell_id, lane_number, index_sequence, read_end)
    # skip if we are checking an existing library
    if not check_library:
        existing_data = get_existing_fastq_data(tantalus_api, dlp_library_id)

    else:
        existing_data = []

    primary_sample_id = colossus_api.query_libraries_by_library_id(dlp_library_id)['sample']['sample_id']
    cell_samples = query_colossus_dlp_cell_info(colossus_api, dlp_library_id)
    rev_comp_overrides = query_colossus_dlp_rev_comp_override(
        colossus_api, dlp_library_id
    )

    external_identifier = "{}_{}".format(primary_sample_id, dlp_library_id)

    gsc_api = GSCAPI()

    library_infos = gsc_api.query(
        "library?external_identifier={}".format(external_identifier)
    )

    if len(library_infos) == 0:
        logging.error('no libraries with external_identifier {} in gsc api'.format(external_identifier))
        return []
    elif len(library_infos) > 1:
        raise Exception(
            "multiple libraries with external_identifier {} in gsc api".format(
                external_identifier
            )
        )

    library_info = library_infos[0]

    gsc_library_id = library_info["name"]

    fastq_infos = gsc_api.query("fastq?parent_library={}".format(gsc_library_id))

    fastq_file_info = []

    flowcells_to_be_created = []

    lane_fastq_file_info = defaultdict(list)

    for fastq_info in fastq_infos:
        flowcell_id = str(fastq_info['libcore']['run']['flowcell']['lims_flowcell_code'])
        lane_number = str(fastq_info['libcore']['run']['lane_number'])
        lane_fastq_file_info[(flowcell_id, str(lane_number))].append(fastq_info)

    for (flowcell_id, lane_number) in lane_fastq_file_info.keys():

        if (flowcell_id, lane_number) in existing_data:
            logging.info('Skipping fastqs with flowcell id {}, lane number {}'.format(
                flowcell_id, lane_number))
            continue

        for fastq_info in lane_fastq_file_info[(flowcell_id, lane_number)]:
            fastq_path = fastq_info["data_path"]
            
            try_gzip(fastq_path)

            if fastq_info["status"] != "production":
                logging.info(
                    "skipping file {} marked as {}".format(
                        fastq_info["data_path"], fastq_info["status"]
                    )
                )
                continue

            if fastq_info['removed_datetime'] is not None:
                logging.info('skipping file {} marked as removed {}'.format(
                    fastq_info['data_path'], fastq_info['removed_datetime']))
                continue

            sequencing_instrument = get_sequencing_instrument(
                fastq_info["libcore"]["run"]["machine"]
            )
            solexa_run_type = fastq_info["libcore"]["run"]["solexarun_type"]
            read_type = solexa_run_type_map[solexa_run_type]

            primer_id = fastq_info["libcore"]["primer_id"]
            primer_info = gsc_api.query("primer/{}".format(primer_id))
            raw_index_sequence = primer_info["adapter_index_sequence"]

            flowcell_lane = flowcell_id
            if lane_number is not None:
                flowcell_lane = flowcell_lane + "_" + str(lane_number)

            rev_comp_override = rev_comp_overrides.get(flowcell_lane)

            index_sequence = decode_raw_index_sequence(
                raw_index_sequence, sequencing_instrument, rev_comp_override
            )

            filename_pattern = fastq_info["file_type"]["filename_pattern"]
            read_end, passed = filename_pattern_map.get(filename_pattern, (None, None))

            logging.info(
                "loading fastq %s, raw index %s, index %s, %s",
                fastq_info["id"],
                raw_index_sequence,
                index_sequence,
                fastq_path,
            )

            if read_end is None:
                raise Exception("Unrecognized file type: {}".format(filename_pattern))

            if not passed:
                continue

            try:
                cell_sample_id = cell_samples[index_sequence]
            except KeyError:
                raise Exception('unable to find index {} for flowcell lane {} for library {}'.format(
                    index_sequence, flowcell_lane, dlp_library_id))

            extension = ''
            compression = 'UNCOMPRESSED'
            if fastq_path.endswith('.gz'):
                extension = '.gz'
                compression = 'GZIP'
            elif not fastq_path.endswith('.fastq'):
                raise ValueError('unknown extension for filename {}'.format(fastq_path))

            tantalus_filename = templates.SC_WGS_FQ_TEMPLATE.format(
                primary_sample_id=primary_sample_id,
                dlp_library_id=dlp_library_id,
                flowcell_id=flowcell_id,
                lane_number=lane_number,
                cell_sample_id=cell_sample_id,
                index_sequence=index_sequence,
                read_end=read_end,
                extension=extension,
            )

            tantalus_path = os.path.join(storage["prefix"], tantalus_filename)

            fastq_file_info.append(
                dict(
                    dataset_type="FQ",
                    sample_id=cell_sample_id,
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
                    file_type="FQ",
                    read_end=read_end,
                    index_sequence=index_sequence,
                    compression=compression,
                    filepath=tantalus_path,
                )
            )

            flowcells_to_be_created.append(flowcell_id + '_' + str(lane_number))

            if not check_library:
                if storage['storage_type'] == 'server': 
                    rsync_file(fastq_path, tantalus_path)

                elif storage['storage_type'] == 'blob':
                    storage_client = tantalus_api.get_storage_client(storage['name'])
                    storage_client.create(tantalus_filename, fastq_path)

    if len(fastq_file_info) == 0:
        return []

    fastq_paired_end_check(fastq_file_info)

    # Check that all index sequences have fastq files
    cell_index_sequences = set(cell_samples.keys())

    fastq_lane_index_sequences = collections.defaultdict(set)
    for info in fastq_file_info:
        if info['index_sequence'] not in cell_index_sequences:
            raise Exception('fastq {} with index {}, flowcell {}, lane {} with index not in colossus'.format(
                info['filepath'], info['index_sequence'], info['sequence_lanes'][0]['flowcell_id'],
                info['sequence_lanes'][0]['lane_number']))
        flowcell_lane = (
            info['sequence_lanes'][0]['flowcell_id'],
            info['sequence_lanes'][0]['lane_number'])
        fastq_lane_index_sequences[flowcell_lane].add(info['index_sequence'])

    for flowcell_lane in fastq_lane_index_sequences:
        for index_sequence in cell_index_sequences:
            if index_sequence not in fastq_lane_index_sequences[flowcell_lane]:
                raise Exception('no fastq found for index sequence {}, flowcell {}, lane {}'.format(
                    index_sequence, flowcell_lane[0], flowcell_lane[1]))

    if not check_library:
        create_sequence_dataset_models(
            fastq_file_info, storage["name"], tag_name, tantalus_api, update=update
        )

    logging.info('import succeeded')

    return flowcells_to_be_created


if __name__ == "__main__":
    # Set up the root logger
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    # Parse the incoming arguments
    args = parse_runtime_args()

    # Connect to the Tantalus API (this requires appropriate environment
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    storage = tantalus_api.get("storage_server", name=args["storage_name"])

    # Get the tag name if it was passed in
    try:
        tag_name = args["tag_name"]
    except KeyError:
        tag_name = None

    # Query GSC for FastQs
    import_gsc_dlp_paired_fastqs(
        colossus_api,
        tantalus_api,
        args["dlp_library_id"],
        storage,
        tag_name,
        update=args.get("update", False),
        check_library=args.get("check_library", False),
    )

