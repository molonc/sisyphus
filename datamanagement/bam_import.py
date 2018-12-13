#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import datetime
import os
import time
import azure.storage.blob
import pandas as pd
import pysam
import datamanagement.utils.constants
from datamanagement.utils.runtime_args import parse_runtime_args
from datamanagement.utils.utils import get_lanes_hash, get_lane_str
import datamanagement.templates as templates
from dbclients.tantalus import TantalusApi


def get_bam_ref_genome(bam_header):
    sq_as = bam_header["SQ"][0]["AS"]
    return datamanagement.utils.constants.REF_GENOME_MAP[sq_as]


def get_bam_aligner_name(bam_header):
    for pg in bam_header["PG"]:
        if "bwa" in pg["ID"] or "bwa" in pg["CL"]:
            if "sampe" in pg["CL"]:
                return "bwa_aln"
            if "mem" in pg["CL"]:
                return "bwa_mem"
    raise Exception("no aligner name found")


def get_bam_header_info(header):
    sample_ids = set()
    library_ids = set()
    index_sequences = set()
    sequence_lanes = list()

    for read_group in header["RG"]:
        sample_id = read_group["SM"]
        library_id = read_group["LB"]
        flowcell_lane = read_group["PU"]
        index_sequence = read_group.get("KS")

        flowcell_id = flowcell_lane
        lane_number = ""
        if "_" in flowcell_lane:
            flowcell_id, lane_number = flowcell_lane.split("_")
        elif "." in flowcell_lane:
            flowcell_id, lane_number = flowcell_lane.split(".")

        sequence_lane = dict(flowcell_id=flowcell_id, lane_number=lane_number)

        sample_ids.add(sample_id)
        library_ids.add(library_id)
        index_sequences.add(index_sequence)
        sequence_lanes.append(sequence_lane)

    if len(sample_ids) > 1:
        raise Exception("multiple sample ids {}".format(sample_ids))

    if len(library_ids) > 1:
        raise Exception("multiple library ids {}".format(library_ids))

    if len(index_sequences) > 1:
        raise Exception("multiple index_sequences {}".format(index_sequences))

    return {
        "sample_id": sample_ids.pop(),
        "library_id": library_ids.pop(),
        "index_sequence": index_sequences.pop(),
        "sequence_lanes": sequence_lanes,
    }


def import_bam(
    tantalus_api,
    storage_name,
    library_type,
    bam_filename,
    read_type,
    sequencing_centre,
    index_format,
    update=False,
    lane_info=None,
    tag_name=None,
):
    bam_resource, bam_instance = tantalus_api.add_file(
        storage_name, 
        bam_filename, 
        "BAM",
        fields={"compression":"UNCOMPRESSED"},
        update=update
    )
    bai_resource, bai_instance = tantalus_api.add_file(
        storage_name, 
        bam_filename + ".bai", 
        "BAI",
        fields={"compression":"UNCOMPRESSED"},
        update=update
    )

    bam_url = tantalus_api.get_storage_client(storage_name).get_url(bam_resource['filename'])
    bam_header = pysam.AlignmentFile(bam_url).header
    bam_header_info = get_bam_header_info(bam_header)
    ref_genome = get_bam_ref_genome(bam_header)
    aligner_name = get_bam_aligner_name(bam_header)

    sample_pk = tantalus_api.get_or_create("sample", sample_id=bam_header_info["sample_id"])["id"]

    library_pk = tantalus_api.get_or_create(
        "dna_library", 
        library_id=bam_header_info["library_id"],
        library_type=library_type,
        index_format=index_format,
    )["id"]

    if tag_name is not None:
        tag_pk = tantalus_api.get_or_create("tag", name=tag_name)["id"]
        tags = [tag_pk]
    else:
        tags = []

    sequence_lanes = []
    sequence_lane_pks = []

    if not lane_info:
        for lane in bam_header_info["sequence_lanes"]:
            lane = tantalus_api.get_or_create(
                "sequencing_lane",
                flowcell_id=lane["flowcell_id"],
                dna_library=library_pk,
                read_type=read_type,
                lane_number=lane["lane_number"],
                sequencing_centre=sequencing_centre,
            )
            sequence_lanes.append(lane)
            sequence_lane_pks.append(lane["id"])
    else:
        for lane in lane_info:
            lane = tantalus_api.get_or_create(
                "sequencing_lane",
                flowcell_id=lane["flowcell_id"],
                dna_library=library_pk,
                read_type=read_type, 
                lane_number=lane["lane_number"],
                sequencing_centre=sequencing_centre,
                sequencing_instrument=lane["sequencing_instrument"],
            )
            sequence_lanes.append(lane)
            sequence_lane_pks.append(lane["id"])

    file_resource_pks = [bam_resource["id"], bai_resource["id"]]

    dataset_name = templates.SC_WGS_BAM_NAME_TEMPLATE.format(
        dataset_type="BAM",
        sample_id=bam_header_info["sample_id"],
        library_type=library_type,
        library_id=bam_header_info["library_id"],
        lanes_hash=get_lanes_hash(sequence_lanes),
        aligner=aligner_name,
        reference_genome=ref_genome,
    )

    sequence_dataset = tantalus_api.get_or_create(
        "sequence_dataset",
        name=dataset_name,
        dataset_type="BAM",
        sample=sample_pk,
        library=library_pk,
        sequence_lanes=sequence_lane_pks,
        file_resources=file_resource_pks,
        reference_genome=ref_genome,
        aligner=aligner_name,
        tags=tags,
    )

    return sequence_dataset


if __name__ == "__main__":
    # Get arguments
    args = parse_runtime_args()

    # Connect to the Tantalus API (this requires appropriate environment
    # variables defined)
    tantalus_api = TantalusApi()

    # Import BAMs
    dataset = import_bam(
        tantalus_api,
        args["storage_name"],
        args["library_type"],
        args["bam_filename"],
        args["read_type"],
        args["sequencing_centre"],
        args["index_format"],
        tag_name=args["tag_name"],
    )

    print("dataset {}".format(dataset["id"]))
