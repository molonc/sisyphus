#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import re
import sys
import time
import pandas as pd
from utils.colossus import get_colossus_sublibraries_from_library_id
from utils.constants import LOGGING_FORMAT
from utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
from utils.runtime_args import parse_runtime_args
from utils.tantalus import TantalusApi

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
    storage_name,
    storage_directory,
    output_dir,
    tantalus_api,
    tag_name=None,
):
    # Check for .. in file path
    if ".." in output_dir:
        raise Exception("Invalid path for output_dir. '..' detected")

    # Check that output_dir is actually in storage
    if not output_dir.startswith(storage_directory):
        raise Exception(
            "Invalid path for output_dir. {} doesn't seem to be in the specified storage".format(
                output_dir
            )
        )

    # Check that path is valid.
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
    filenames = filter(lambda x: ".fastq.gz" in x, filenames)

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

        # primary_sample_id = filename_fields[0]
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
        sample_id = cell_info[library_id][row, column]["sample_id"]

        fastq_path = os.path.join(output_dir, filename)

        if not fastq_path.startswith(storage_directory):
            raise Exception(
                "file {} expected in directory {}".format(fastq_path, storage_directory)
            )
        fastq_filename = fastq_path.replace(storage_directory, "")
        fastq_filename = filename.lstrip("/")

        fastq_file_info.append(
            dict(
                dataset_type="FQ",
                sample_id=sample_id,
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
                size=os.path.getsize(fastq_path),
                created=pd.Timestamp(
                    time.ctime(os.path.getmtime(fastq_path)), tz="Canada/Pacific"
                ),
                file_type="FQ",
                read_end=read_end,
                index_sequence=index_sequence,
                compression="GZIP",
                filename=fastq_filename,
            )
        )

    return fastq_file_info


if __name__ == "__main__":
    # Parse the incoming arguments
    args = parse_runtime_args()

    # Connect to the Tantalus API (this requires appropriate environment
    # variables defined)
    tantalus_api = TantalusApi()

    # Get the tag name if it was passed in
    try:
        tag_name = args["tag_name"]
    except KeyError:
        tag_name = None

    # Import fastqs
    load_brc_fastqs(
        args["flowcell_id"],
        args["storage_name"],
        args["storage_directory"],
        args["output_dir"],
        tantalus_api,
        tag_name=tag_name,
    )
