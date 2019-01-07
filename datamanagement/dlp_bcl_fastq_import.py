#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import re
import sys
import time
import subprocess
import pandas as pd
from dbclients.colossus import get_colossus_sublibraries_from_library_id
from dbclients.tantalus import TantalusApi
from utils.constants import LOGGING_FORMAT
from utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
from utils.runtime_args import parse_runtime_args
from utils.filecopy import rsync_file
from utils.utils import make_dirs
import datamanagement.templates as templates


# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stdout, level=logging.INFO)

# Hard coded BRC details
BRC_INSTRUMENT = "NextSeq550"
BRC_INDEX_FORMAT = "D"
BRC_LIBRARY_TYPE = "SC_WGS"
BRC_READ_TYPE = "P"
BRC_SEQ_CENTRE = "BRC"


def query_colossus_dlp_cell_info(library_id):

    sublibraries = get_colossus_sublibraries_from_library_id(library_id)

    row_column_map = {}
    for sublib in sublibraries:
        index_sequence = sublib["primer_i7"] + "-" + sublib["primer_i5"]
        row_column_map[(sublib["row"], sublib["column"])] = {
            "index_sequence": index_sequence,
            "sample_id": sublib["sample_id"]["sample_id"],
        }

    return row_column_map


def load_brc_fastqs(
    flowcell_id,
    output_dir,
    storage_name,
    storage_directory,
    tantalus_api,
    tag_name=None,
):
    if not os.path.isdir(output_dir):
        raise Exception("output directory {} not a directory".format(output_dir))

    fastq_file_info = get_fastq_info(output_dir, flowcell_id, storage_directory)

    fastq_paired_end_check(fastq_file_info)

    create_sequence_dataset_models(
        fastq_file_info, storage_name, tag_name, tantalus_api
    )


def _update_info(info, key, value):
    if key in info:
        if info[key] != value:
            raise ValueError("{} different from {}".format(info[key], value))
    else:
        info[key] = value


def get_fastq_info(output_dir, flowcell_id, storage_directory):
    """ Retrieve fastq filenames and metadata from output directory.
    """
    filenames = os.listdir(output_dir)

    # Filter for gzipped fastq files
    extension = ".gz"
    filenames = filter(lambda x: ".fastq{}".format(extension) in x, filenames)

    # Remove undetermined fastqs
    filenames = filter(lambda x: "Undetermined" not in x, filenames)

    # Check that the path actually has fastq files
    if not filenames:
        raise Exception("no fastq files in output directory {}".format(output_dir))

    # Cell info keyed by dlp library id
    cell_info = {}

    # Fastq filenames and info keyed by fastq id, read end
    fastq_file_info = []

    for filename in filenames:
        match = re.match(
            r"^(\w+)-(\w+)-R(\d+)-C(\d+)_S(\d+)(_L(\d+))?_R([12])_001.fastq.gz$",
            filename,
        )

        if match is None:
            raise Exception(
                "unrecognized fastq filename structure for {}".format(filename)
            )

        filename_fields = match.groups()

        primary_sample_id = filename_fields[0]
        library_id = filename_fields[1]
        row = int(filename_fields[2])
        column = int(filename_fields[3])
        lane_number = filename_fields[6]
        if lane_number is not None:
            lane_number = int(lane_number)
        read_end = int(filename_fields[7])

        if library_id not in cell_info:
            cell_info[library_id] = query_colossus_dlp_cell_info(library_id)

        index_sequence = cell_info[library_id][row, column]["index_sequence"]
        cell_sample_id = cell_info[library_id][row, column]["sample_id"]

        fastq_path = os.path.join(output_dir, filename)

        tantalus_filename = templates.SC_WGS_FQ_TEMPLATE.format(
            primary_sample_id=primary_sample_id,
            dlp_library_id=library_id,
            flowcell_id=flowcell_id,
            lane_number=lane_number,
            cell_sample_id=cell_sample_id,
            index_sequence=index_sequence,
            read_end=read_end,
            extension=extension,
        )

        tantalus_path = os.path.join(storage_directory, tantalus_filename)

        if storage['storage_type'] == 'server': 
            rsync_file(fastq_path, tantalus_path)

        elif storage['storage_type'] == 'blob'
            storage_client.create(tantalus_filename, fastq_path)


        fastq_file_info.append(
            dict(
                dataset_type="FQ",
                sample_id=cell_sample_id,
                library_id=library_id,
                library_type=BRC_LIBRARY_TYPE,
                index_format=BRC_INDEX_FORMAT,
                sequence_lanes=[
                    dict(
                        flowcell_id=flowcell_id,
                        lane_number=lane_number,
                        sequencing_centre=BRC_SEQ_CENTRE,
                        sequencing_instrument=BRC_INSTRUMENT,
                        read_type=BRC_READ_TYPE,
                    )
                ],
                file_type="FQ",
                read_end=read_end,
                index_sequence=index_sequence,
                compression="GZIP",
                filepath=tantalus_path,
            )
        )

    return fastq_file_info


def get_samplesheet(destination, lane_id):
    sheet_url = 'http://colossus.bcgsc.ca/dlp/sequencing/samplesheet/query_download/{lane_id}'
    sheet_url = sheet_url.format(lane_id=lane_id)

    subprocess.check_call(["wget", "-O", destination, sheet_url])


def run_bcl2fastq(flowcell_id, bcl_dir, output_dir):
    """ Download sample sheet and run bcl2fastq
    """

    if len(os.listdir(output_dir)) > 0:
        raise Exception('bcl2fastq output directory {} is not empty'.format(output_dir))

    samplesheet_filename = os.path.join(output_dir, "SampleSheet.csv")

    get_samplesheet(samplesheet_filename, flowcell_id)

    cmd = [
        'bcl2fastq',
        '--runfolder-dir', bcl_dir,
        '--sample-sheet', samplesheet_filename,
        '--output-dir', output_dir]

    subprocess.check_call(cmd)


if __name__ == "__main__":
    # Parse the incoming arguments
    args = parse_runtime_args()

    # variables defined)
    tantalus_api = TantalusApi()

    storage = tantalus_api.get("storage_server", name=args["storage_name"])
    storage_client = tantalus_api.get_storage_client(storage['name'])

    # Get the tag name if it was passed in
    try:
        tag_name = args["tag_name"]
    except KeyError:
        tag_name = None

    make_dirs(args["temp_dir"])

    datasets = list(tantalus_api.list(
        "sequence_dataset",
        sequence_lanes__flowcell_id=args["flowcell_id"],
        dataset_type="FQ"))

    if len(datasets) > 0:
        raise Exception("found dataset {}".format(','.join([str(d["id"]) for d in datasets])))

    # Run bcl to fastq
    run_bcl2fastq(
        args["flowcell_id"],
        args["bcl_dir"],
        args["temp_dir"]
    )

    # Import fastqs
    load_brc_fastqs(
        args["flowcell_id"],
        args["temp_dir"],
        storage["name"],
        storage["storage_directory"],
        tantalus_api,
        tag_name=tag_name
    )

