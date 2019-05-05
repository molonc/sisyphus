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
import re
from datamanagement.utils.constants import REF_GENOME_REGEX_MAP, SEQUENCING_CENTRE_MAP
from datamanagement.utils.utils import get_lanes_hash, get_lane_str
import datamanagement.templates as templates
from dbclients.tantalus import TantalusApi
import click
from dbclients.basicclient import FieldMismatchError, NotFoundError


def add_sequence_dataset(
                tantalus_api,
                storage_name, 
                sample_id, 
                library, 
                dataset_type,
                dataset_name,
                sequence_lanes, 
                file_paths, 
                reference_genome, 
                aligner, 
                tag_name=None,
                update=False):
        """
        Add a sequence dataset, gets or creates the required sample, library, 
        and sequence lanes for the dataset

        Args:
            storage_name (str)
            dataset_name (str)
            dataset_type (str)
            sample_id (str):        internal sample ID   
            library (dict):         contains: library_id, library_type, index_format
            sequence_lanes (list):  contains: flowcell_id, read_type, lane_number, 
                                    sequencing_centre, sequencing_instrument, library_id
            file_paths (list):      list of file paths to data included in dataset
            reference_genome (str)
            aligner (str)
            tags (list)
        Returns:
            sequence_dataset (dict)
        """ 
        # Create the sample
        sample = tantalus_api.get_or_create(
            "sample",
            sample_id=sample_id
        )

        # Create the library
        library = tantalus_api.get_or_create(
            "dna_library",
            library_id=library["library_id"],
            library_type=library["library_type"],
            index_format=library["index_format"]
        )

        # Create the sequence lanes
        sequence_lane_pks = []
        for lane in sequence_lanes:
            # Get library ID associated with each lane
            lane_library_pk = tantalus_api.get_or_create(
                "dna_library",
                library_id=lane["library_id"],
                library_type=library["library_type"],
                index_format=library["index_format"]
            )["id"]

            lane_pk = tantalus_api.get_or_create(
                "sequencing_lane",
                flowcell_id=lane["flowcell_id"],
                dna_library=lane_library_pk,
                read_type=lane["read_type"],
                lane_number=str(lane["lane_number"]),
                sequencing_centre=lane["sequencing_centre"],
                sequencing_instrument=lane["sequencing_instrument"]
            )["id"]

            sequence_lane_pks.append(lane_pk)

        # Create the tag
        if tag_name is not None:
            tag_pk = tantalus_api.get_or_create("tag", name=tag_name)["id"]
            tags = [tag_pk]
        else:
            tags = []

        # Create the file resources
        file_resource_pks = []
        for file_path in file_paths:
            file_resource, file_instance = tantalus_api.add_file(storage_name, file_path, update=update)
            file_resource_pks.append(file_resource["id"])

        # Create the sequence dataset associated with the above data
        try:
            sequence_dataset = tantalus_api.get(
                    "sequence_dataset",
                    name=dataset_name,
                    dataset_type=dataset_type,
                    sample=sample["id"],
                    library=library["id"],
                    sequence_lanes=sequence_lane_pks,
                    reference_genome=reference_genome,
                    aligner=aligner,
            )
    
            # Add the new file resources to existing file resources 
            file_resource_ids = file_resource_pks + sequence_dataset["file_resources"]
            tag_ids = tags + sequence_dataset["tags"]
    
            sequence_dataset = tantalus_api.update(
                    "sequence_dataset",
                    id=sequence_dataset["id"],
                    file_resources=file_resource_ids,
                    tags=tag_ids,
            )
        except NotFoundError:
            sequence_dataset = tantalus_api.create(
                    "sequence_dataset",
                    name=dataset_name,
                    dataset_type=dataset_type,
                    sample=sample["id"],
                    library=library["id"],
                    sequence_lanes=sequence_lane_pks,
                    file_resources=file_resource_pks,
                    reference_genome=reference_genome,
                    aligner=aligner,
                    tags=tags,
            )

        return sequence_dataset


def get_bam_ref_genome(bam_header):
    """
    Parses the reference genome from bam header

    Args:
        bam_header: (dict)

    Returns:
        reference genome (string)
    """
    sq_as = bam_header["SQ"][0]["AS"]
    found_match = False

    for ref, regex_list in REF_GENOME_REGEX_MAP.items():
        for regex in regex_list:
            if re.search(regex, sq_as, flags=re.I):
                # Found a match
                reference_genome = ref
                found_match = True
                break

        if found_match:
            break

    if not found_match:
        raise Exception("Unrecognized reference genome {}".format(sq_as))
    
    return reference_genome


def get_bam_aligner_name(bam_header):
    """
    Parses aligner name from the bam header

    Args:
        bam_header: (dict)

    Returns:
        aligner:    (string)
    """
    for pg in bam_header["PG"]:
        if "bwa" in pg["ID"] or "bwa" in pg["CL"]:
            if "sampe" in pg["CL"]:
                version = pg["VN"].replace(".", "_")
                return "BWA_ALN_" + version
            if "mem" in pg["CL"]:
                try:
                    version = pg["VN"].replace(".", "_")
                except KeyError:
                    #If we get a bad header
                    components = pg["CL"].split("\t")
                    version = components[-1].replace(".", "_").strip("VN:").upper()
                return "BWA_MEM_" + version.upper()
    raise Exception("no aligner name found")


def get_bam_header_info(header):
    """
    Extracts required info from the bam header

    Args:
        header: (dict) bam header

    Returns:
        header info: (dict)
    """
    sample_ids = set()
    library_ids = set()
    index_sequences = set()
    sequence_lanes = list()

    for read_group in header["RG"]:
        sample_id = read_group["SM"]
        library_id = read_group["LB"]
        flowcell_lane = read_group["PU"]
        index_sequence = read_group.get("KS")
        try:
            sequencing_centre = SEQUENCING_CENTRE_MAP[read_group["CN"]]
        except KeyError:
            raise Exception("Unknown sequencing centre {}".format(read_group["CN"]))

        flowcell_id = flowcell_lane
        lane_number = ""
        if "_" in flowcell_lane:
            flowcell_id, lane_number = flowcell_lane.split("_")
        elif "." in flowcell_lane:
            flowcell_id, lane_number = flowcell_lane.split(".")

        sequence_lane = dict(flowcell_id=flowcell_id, lane_number=lane_number, library_id=library_id)

        sample_ids.add(sample_id)
        library_ids.add(library_id)
        index_sequences.add(index_sequence)
        sequence_lanes.append(sequence_lane)

    if len(sample_ids) > 1:
        raise Exception("multiple sample ids {}".format(sample_ids))

    if len(index_sequences) > 1:
        raise Exception("multiple index_sequences {}".format(index_sequences))

    return {
        "sample_id": sample_ids.pop(),
        "library_ids": library_ids,
        "index_sequence": index_sequences.pop(),
        "sequence_lanes": sequence_lanes,
    }


def import_bam(
    storage_name,
    library,
    bam_file_path,
    read_type,
    lane_infos=None,
    tag_name=None,
    update=False):
    """
    Imports bam into tantalus

    Args:
        storage_name:   (string) name of destination storage
        library:        (dict) contains library_id, library_type, index_format
        bam_file_path:  (string) filepath to bam on destination storage
        read_type:      (string) read type for the run
        lane_infos:     (dict) contains flowcell_id, lane_number, 
                        adapter_index_sequence, sequencing_cenre, read_type, 
                        reference_genome, aligner
        tag_name:       (string)
        update:         (boolean)
    Returns:
        sequence_dataset:   (dict) sequence dataset created on tantalus
    """ 
    # Connect to the Tantalus API (this requires appropriate environment
    # variables defined)
    tantalus_api = TantalusApi()

    file_paths = [bam_file_path, bam_file_path + ".bai"]

    bam_header = pysam.AlignmentFile(bam_file_path).header
    bam_header_info = get_bam_header_info(bam_header)
    
    ref_genome = get_bam_ref_genome(bam_header)
    aligner_name = get_bam_aligner_name(bam_header)

    if lane_infos:
        lane_infos = lane_infos
    else:
        lane_infos = []
        for lane in bam_header_info["sequence_lanes"]:

            lane_info = {
                "flowcell_id": lane["flowcell_id"],
                "lane_number": lane["lane_number"],
                "library_id": lane["library_id"],
                "sequencing_centre": lane["sequencing_centre"],
                "read_type": read_type,
                "sequencing_instrument": None,
            }

            lane_infos.append(lane_info)

    dataset_name = templates.SC_WGS_BAM_NAME_TEMPLATE.format(
        dataset_type="BAM",
        sample_id=bam_header_info["sample_id"],
        library_type=library["library_type"],
        library_id=library["library_id"],
        lanes_hash=get_lanes_hash(lane_infos),
        aligner=aligner_name,
        reference_genome=ref_genome,
    )

    # Add the sequence dataset to Tantalus
    sequence_dataset = add_sequence_dataset(
            tantalus_api,
            storage_name=storage_name,
            sample_id=bam_header_info["sample_id"],
            library=library,
            dataset_type="BAM",
            dataset_name=dataset_name,
            sequence_lanes=lane_infos,
            file_paths=file_paths,
            reference_genome=ref_genome,
            aligner=aligner_name,
            tag_name=tag_name,
            update=update
    )

    return sequence_dataset


@click.command()
@click.argument("storage_name")
@click.argument("library")
@click.argument("bam_file_path")
@click.argument("read_type")
@click.option("--update",is_flag=True)
@click.option("--lane_info",default=None)
@click.option("--tag_name",default=None)
def main(**kwargs):
    """
    Imports the bam into tantalus by creating a sequence dataset and 
    file resources 
    """
    #Import bam
    dataset = import_bam(**kwargs)

    print("dataset {}".format(dataset["id"]))


if __name__ == "__main__":
    main()
