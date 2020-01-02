#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import re
import sys
import time
import gzip
import subprocess
import pandas as pd
import click
import requests
from dbclients.colossus import get_colossus_sublibraries_from_library_id
from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from utils.constants import LOGGING_FORMAT
from utils.dlp import create_sequence_dataset_models, fastq_paired_end_check, fastq_dlp_index_check
from utils.runtime_args import parse_runtime_args
from utils.filecopy import rsync_file
from utils.utils import make_dirs
from utils.comment_jira import comment_jira
import datamanagement.templates as templates
from datamanagement.utils.qsub_job_submission import submit_qsub_job
from datamanagement.utils.qsub_jobs import Bcl2FastqJob
from datamanagement.utils.constants import DEFAULT_NATIVESPEC
import datetime

# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

# Hard coded BRC details
BRC_INSTRUMENT = "NextSeq550"
BRC_INDEX_FORMAT = "D"
BRC_LIBRARY_TYPE = "SC_WGS"
BRC_READ_TYPE = "P"
BRC_SEQ_CENTRE = "BRC"

colossus_api = ColossusApi()
tantalus_api = TantalusApi()


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
        storage,
        tantalus_api,
        storage_client,
        tag_name=None,
        update=False,
        threshold=20,
):
    if not os.path.isdir(output_dir):
        raise Exception("output directory {} not a directory".format(output_dir))

    fastq_file_info = get_fastq_info(output_dir, flowcell_id, storage, storage_client, threshold)

    fastq_paired_end_check(fastq_file_info)

    fastq_dlp_index_check(fastq_file_info)

    create_sequence_dataset_models(
        fastq_file_info,
        storage_name,
        tag_name,
        tantalus_api,
        update=update,
    )

    update_ticket(flowcell_id)

    logging.info('import succeeded')


def update_ticket(flowcell_id):
    """
    Given flowcell, query colossus for lane, find corresponding sequencing and get JIRA library 
    ticket associated with the sequencing.

    Args: 
        flowcell_id (str): Lane/Flowcell id
    """

    lane_info = colossus_api.get("lane", flow_cell_id=flowcell_id)
    sequencing_id = lane_info["sequencing"]

    sequencing = colossus_api.get("sequencing", id=sequencing_id)
    library_id = sequencing["library"]

    library = colossus_api.get("library", pool_id=library_id)
    jira_ticket = library["jira_ticket"]

    sequencing_url = "https://colossus.canadacentral.cloudapp.azure.com/dlp/sequencing/{}".format(sequencing_id)
    comment = "Import successful: \n\nLane: {} \n{}".format(
        flowcell_id,
        sequencing_url,
    )

    comment_jira(jira_ticket, comment)


def _update_info(info, key, value):
    if key in info:
        if info[key] != value:
            raise ValueError("{} different from {}".format(info[key], value))
    else:
        info[key] = value


def check_fastqs(library_id, fastq_file_info, threshold):
    logging.info("Checking if BCL2FASTQ generated complete set of fastqs.")

    # Get indices from colossus
    colossus_samples = query_colossus_dlp_cell_info(library_id)
    colossus_index_sequences = set(sample["index_sequence"] for sample in colossus_samples.values())

    # Get indices from given fastqs
    fastq_index_sequences = set([fastq["index_sequence"] for fastq in fastq_file_info])
    index_sequences = fastq_index_sequences

    if len(colossus_index_sequences - fastq_index_sequences) != 0:
        logging.info("BCL2FASTQ skipped indices {}".format(colossus_index_sequences - fastq_index_sequences))
        index_sequences = colossus_index_sequences

    fastqs_to_be_generated = dict()

    for index in index_sequences:
        fastqs_containing_index = [fastq for fastq in fastq_file_info if fastq["index_sequence"] == index]
        fastq_lane_numbers_to_be_generated = set(range(1, 5))
        for fastq in fastqs_containing_index:
            sequence_lane = fastq["sequence_lanes"][0]
            lane_number = sequence_lane["lane_number"]
            fastq_lane_numbers_to_be_generated = fastq_lane_numbers_to_be_generated - set([lane_number])

        fastqs_to_be_generated[index] = list(fastq_lane_numbers_to_be_generated)

    number_of_fastqs_to_generate = sum(len(fastqs_to_be_generated[index]) for index in fastqs_to_be_generated)
    if number_of_fastqs_to_generate > threshold:
        raise Exception("Number of empty fastqs to be generated ({}) exceeded threshold ({})".format(
            number_of_fastqs_to_generate, threshold))

    return fastqs_to_be_generated


def generate_empty_fastqs(output_dir, library_id, fastqs_to_be_generated):
    sublibraries = get_colossus_sublibraries_from_library_id(library_id)

    index_sequence_map = {}
    file_names = []
    reads = [1, 2]

    for sublibrary in sublibraries:
        index_sequence = sublibrary["primer_i7"] + "-" + sublibrary["primer_i5"]
        sample_id = sublibrary["sample_id"]["sample_id"]
        index_sequence_map[index_sequence] = {
            "sample_id": sample_id,
            "row": sublibrary["row"],
            "column": sublibrary["column"]
        }

    for fastq_index in fastqs_to_be_generated.keys():
        row = index_sequence_map[fastq_index]["row"]
        column = index_sequence_map[fastq_index]["column"]
        sample_id = index_sequence_map[fastq_index]["sample_id"]

        samplename = "-".join([sample_id, library_id, "R{}".format(row), "C{}".format(column)])
        for lane_num in fastqs_to_be_generated[fastq_index]:
            for read in reads:
                # Fastq name example: SA992-A90632-R54-C54_S317_L004_R1_001.fastq.gz
                filename = "_".join([samplename, "S0", "L00{}".format(lane_num), "R{}".format(read), "001.fastq.gz"])
                file_names.append(filename)

    for filename in file_names:
        filepath = os.path.join(output_dir, filename)
        if not os.path.exists(filepath):
            logging.info("Creating empty file {} at {}.".format(filename, filepath))
            with gzip.open(filepath, mode='wb') as f:
                pass

    return file_names


def get_fastq_info(output_dir, flowcell_id, storage, storage_client, threshold):
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

    fastq_file_info = transfer_fastq_files(
        cell_info,
        flowcell_id,
        fastq_file_info,
        filenames,
        output_dir,
        storage,
        storage_client,
    )
    library_id = fastq_file_info[0]["library_id"] # TODO: Maybe make library_id an argument

    # Run through list of fastqs and check if bcl2fastqs skipped over indices or skipped lanes/reads
    fastqs_to_be_generated = check_fastqs(library_id, fastq_file_info, threshold)
    number_of_fastqs_to_generate = sum(len(fastqs_to_be_generated[index]) for index in fastqs_to_be_generated)

    if number_of_fastqs_to_generate != 0:
        logging.info("BCL2FASTQ failed to generate complete set of fastqs. Generating missing fastqs.")
        new_filenames = generate_empty_fastqs(output_dir, library_id, fastqs_to_be_generated)

        new_fastq_file_info = transfer_fastq_files(
            cell_info,
            flowcell_id,
            fastq_file_info,
            new_filenames,
            output_dir,
            storage,
            storage_client,
        )
        return new_fastq_file_info

    return fastq_file_info


def transfer_fastq_files(cell_info, flowcell_id, fastq_file_info, filenames, output_dir, storage, storage_client):
    extension = ".gz"
    logging.info("Transferrings fastq to {}.".format(storage["name"]))
    for filename in filenames:
        match = re.match(
            r"^(\w+)-(\w+)-R(\d+)-C(\d+)_S(\d+)(_L(\d+))?_R([12])_001.fastq.gz$",
            filename,
        )

        if match is None:
            raise Exception("unrecognized fastq filename structure for {}".format(filename))

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

        tantalus_path = os.path.join(storage["prefix"], tantalus_filename)

        if storage['storage_type'] == 'server':
            rsync_file(fastq_path, tantalus_path)

        elif storage['storage_type'] == 'blob':
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
            ))

    return fastq_file_info


def get_samplesheet(destination, lane_id):

    r = requests.get(
        'https://colossus.canadacentral.cloudapp.azure.com/api/samplesheet_query/{}'.format(lane_id),
        auth=(
            os.environ["COLOSSUS_API_USERNAME"],
            os.environ["COLOSSUS_API_PASSWORD"],
        ),
    )

    with open(destination, 'w+') as f:
        f.write(r.content.decode("utf-8"))


def run_bcl2fastq(flowcell_id, bcl_dir, output_dir):
    """ Download sample sheet and run bcl2fastq
    """

    if len(os.listdir(output_dir)) > 0:
        raise Exception('bcl2fastq output directory {} is not empty'.format(output_dir))

    samplesheet_filename = os.path.join(output_dir, "SampleSheet.csv")

    get_samplesheet(samplesheet_filename, flowcell_id)

    job = Bcl2FastqJob('16', bcl_dir, samplesheet_filename, output_dir)

    submit_qsub_job(job, DEFAULT_NATIVESPEC, title=flowcell_id)

    logging.info("Job finished successfully.")


def add_lanes(flowcell_id):
    # get lane information
    lane = colossus_api.get("lane", flow_cell_id=flowcell_id)
    sequencing_id = lane["sequencing"]
    sequencing_date = lane["sequencing_date"]
    # add 4 lanes generated by bcl2fastq on colossus in order to be picked up for analysis
    for lane_number in range(1, 5):
        lane = "{}_{}".format(flowcell_id, lane_number)
        logging.info("creating {}".format(lane))
        colossus_api.create(
            "lane",
            sequencing=sequencing_id,
            sequencing_date=sequencing_date,
            flow_cell_id=lane,
        )


@click.command()
@click.argument('storage_name', nargs=1)
@click.argument('temp_output_dir', nargs=1)
@click.argument('flowcell_id', nargs=1)
@click.argument('bcl_dir', nargs=1)
@click.option('--tag_name')
@click.option('--update', is_flag=True)
@click.option('--no_bcl2fastq', is_flag=True)
@click.option('--threshold', type=int, default=20)
def main(
        storage_name,
        temp_output_dir,
        flowcell_id,
        bcl_dir,
        tag_name=None,
        update=False,
        no_bcl2fastq=False,
        threshold=20,
):

    storage = tantalus_api.get("storage", name=storage_name)
    storage_client = tantalus_api.get_storage_client(storage_name)

    make_dirs(temp_output_dir)

    datasets = list(tantalus_api.list(
        "sequence_dataset",
        sequence_lanes__flowcell_id=flowcell_id,
        dataset_type="FQ",
    ))

    if len(datasets) > 0:
        logging.warning("found dataset {}".format(','.join([str(d["id"]) for d in datasets])))

    if not no_bcl2fastq:
        run_bcl2fastq(
            flowcell_id,
            bcl_dir,
            temp_output_dir,
        )

    # Import fastqs
    load_brc_fastqs(
        flowcell_id,
        temp_output_dir,
        storage_name,
        storage,
        tantalus_api,
        storage_client,
        tag_name=tag_name,
        update=update,
        threshold=threshold,
    )

    # add 4 lanes generated by bcl2fastq on colossus in order to be picked up for analysis
    add_lanes(flowcell_id)


if __name__ == "__main__":
    main()
