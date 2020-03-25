import datetime
import os
import time
import azure.storage.blob
import pandas as pd
import pysam
import re
import logging
import sys

from datamanagement.utils.constants import REF_GENOME_REGEX_MAP, SEQUENCING_CENTRE_MAP
from datamanagement.utils.utils import get_lanes_hash, get_lane_str
import datamanagement.templates as templates
from dbclients.tantalus import TantalusApi
import click
from dbclients.basicclient import FieldMismatchError, NotFoundError
from datamanagement.utils.constants import LOGGING_FORMAT


def add_sequence_dataset(
                tantalus_api,
                storage_name, 
                sample,
                library, 
                dataset_type,
                sequence_lanes, 
                bam_file_path,
                reference_genome,
                aligner,
                bai_file_path=None,
                tag_name=None,
                update=False):
        """
        Add a sequence dataset, gets or creates the required sample, library, 
        and sequence lanes for the dataset

        Args:
            storage_name (str)
            dataset_type (str)
            sample_id (dict):       contains: sample_id
            library (dict):         contains: library_id, library_type, index_format
            sequence_lanes (list):  contains: flowcell_id, read_type, lane_number, 
                                    sequencing_centre, sequencing_instrument, library_id
            bam_file_path (str):    bam file path to data included in dataset
            reference_genome (str)
            aligner (str)
            bai_file_path (str):    bam index file path to data included in dataset (optional)
            tags (list)
        Returns:
            sequence_dataset (dict)
        """ 
        # Create the sample
        sample = tantalus_api.get_or_create(
            "sample",
            sample_id=sample['sample_id'],
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

            lane_fields = dict(
                dna_library=lane_library_pk,
                flowcell_id=lane["flowcell_id"],
                lane_number=str(lane["lane_number"]),
            )

            # Optional fields for create
            for field_name in ("read_type", "sequencing_centre", "sequencing_instrument"):
                if field_name in lane:
                    lane_fields[field_name] = lane[field_name]
                else:
                    logging.warning(f"field {field_name} missing for lane {lane['flowcell_id']}_{lane['lane_number']}")

            lane_pk = tantalus_api.get_or_create(
                "sequencing_lane",
                **lane_fields)["id"]

            sequence_lane_pks.append(lane_pk)

        # Create the tag
        if tag_name is not None:
            tag_pk = tantalus_api.get_or_create("tag", name=tag_name)["id"]
            tags = [tag_pk]
        else:
            tags = []

        # Create the file resources
        file_resource_pks = []
        file_resource, file_instance = tantalus_api.add_file(storage_name, bam_file_path, update=update)
        file_resource_pks.append(file_resource["id"])

        if bai_file_path is not None:
            file_resource, file_instance = tantalus_api.add_file(storage_name, bai_file_path, update=update)
            file_resource_pks.append(file_resource["id"])

        dataset_name = templates.WGS_BAM_NAME_TEMPLATE.format(
            dataset_type="BAM",
            sample_id=sample["sample_id"],
            library_type=library["library_type"],
            library_id=library["library_id"],
            lanes_hash=get_lanes_hash(sequence_lanes),
            aligner=aligner,
            reference_genome=reference_genome,
        )

        # Find all similarly named datasets
        similar_datasets = list(tantalus_api.list(
            "sequence_dataset",
            name=dataset_name,
        ))

        # Filter for a similarly named dataset with the same files
        existing_dataset = None
        for dataset in similar_datasets:
            if set(dataset['file_resources']) == set(file_resource_pks):
                existing_dataset = dataset
                logging.info(f"found existing dataset {dataset['id']} with identical file list")
                break
            elif set(dataset['file_resources']).intersection(set(file_resource_pks)):
                raise ValueError(f"dataset {dataset['id']} has files {dataset['file_resources']} partially intersecting with {list(file_resource_pks)}")

        if existing_dataset is not None:
            # Get or create to check field consistency
            sequence_dataset = tantalus_api.get_or_create(
                "sequence_dataset",
                name=dataset_name,
                version_number=existing_dataset['version_number'],
                dataset_type=dataset_type,
                sample=sample["id"],
                library=library["id"],
                sequence_lanes=sequence_lane_pks,
                file_resources=file_resource_pks,
                reference_genome=reference_genome,
                aligner=aligner,
            )

            # Update the existing dataset tags
            tag_ids = tags + existing_dataset["tags"]
            sequence_dataset = tantalus_api.update(
                "sequence_dataset",
                id=existing_dataset["id"],
                tags=tag_ids,
            )

        else:
            # Find a new version number if necessary
            version_number = 1
            if len(similar_datasets) > 0:
                version_number = max(d['version_number'] for d in similar_datasets) + 1
                logging.info(f"creating new version of dataset {dataset_name} with version number {version_number}")

            fields={
                'name': dataset_name,
                'version_number': version_number,
                'dataset_type': dataset_type,
                'sample': sample["id"],
                'library': library["id"],
                'sequence_lanes': sequence_lane_pks,
                'file_resources': file_resource_pks,
                'reference_genome': reference_genome,
                'aligner': aligner,
                'tags': tags,
            }

            sequence_dataset, is_updated = tantalus_api.create(
                "sequence_dataset", fields, keys=["name", "version_number"])

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
        # FIXUP: sometimes either the header is formed incorrectly or VN does not get
        # correctly parsed by pysam
        if 'CL' in pg and '\tVN:' in pg['CL']:
            pg['VN'] = pg['CL'][pg['CL'].index('\tVN:')+4:]
            assert '\t' not in pg['VN']
            pg['CL'] = pg['CL'][:pg['CL'].index('\tVN:')]
        if "bwa" in pg["ID"] or "bwa" in pg["CL"]:
            bwa_variant = None
            if "sampe" in pg["CL"]:
                bwa_variant = "BWA_ALN"
            elif "mem" in pg["CL"]:
                bwa_variant = "BWA_MEM"
            else:
                raise ValueError(f"unrecognized CL {pg['CL']}")
            version = pg["VN"]
            if "-r" in version:
                version = version[:version.index("-r")]
            version = version.replace(".", "_")
            return bwa_variant + "_" + version
        if ("PN" in pg and pg["PN"] == "JAGuaR") or ("CL" in pg and "JAGuaR" in pg["CL"]):
            version = pg["VN"]
            version = version.replace(".", "_")
            return "JAGuaR".upper() + "_" + version
        if pg["CL"] == "Sambamba":
            pass
        else:
            raise ValueError(f"unrecognized aligner in {pg}")
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

        sequence_lane = dict(
            flowcell_id=flowcell_id,
            lane_number=lane_number,
            library_id=library_id,
            sequencing_centre=sequencing_centre,
        )

        sample_ids.add(sample_id)
        library_ids.add(library_id)
        index_sequences.add(index_sequence)
        sequence_lanes.append(sequence_lane)

    return {
        "sample_ids": sample_ids,
        "library_ids": library_ids,
        "index_sequences": index_sequences,
        "sequence_lanes": sequence_lanes,
    }


def import_bam(
    storage_name,
    bam_file_path,
    sample=None,
    library=None,
    lane_infos=None,
    read_type=None,
    ref_genome=None,
    tag_name=None,
    update=False):
    """
    Imports bam into tantalus

    Args:
        storage_name:   (string) name of destination storage
        bam_file_path:  (string) filepath to bam on destination storage
        sample:         (dict) contains sample_id
        library:        (dict) contains library_id, library_type, index_format
        lane_infos:     (dict) contains flowcell_id, lane_number, 
                        adapter_index_sequence, sequencing_cenre, read_type, 
                        reference_genome, aligner
        read_type:      (string) read type for the run
        tag_name:       (string)
        update:         (boolean)
    Returns:
        sequence_dataset:   (dict) sequence dataset created on tantalus
    """ 
    tantalus_api = TantalusApi()

    # Get a url allowing access regardless of whether the file
    # is in cloud or local storage
    storage_client = tantalus_api.get_storage_client(storage_name)
    bam_filename = tantalus_api.get_file_resource_filename(storage_name, bam_file_path)
    bam_url = storage_client.get_url(bam_filename)

    bam_header = pysam.AlignmentFile(bam_url).header
    bam_header_info = get_bam_header_info(bam_header)

    if ref_genome is None:
        ref_genome = get_bam_ref_genome(bam_header)

    aligner_name = get_bam_aligner_name(bam_header).upper()

    logging.info(f"bam header shows reference genome {ref_genome} and aligner {aligner_name}")

    bai_file_path = None
    if storage_client.exists(bam_filename + ".bai"):
        bai_file_path = bam_file_path + ".bai"
    else:
        logging.info(f"no bam index found at {bam_filename + '.bai'}")

    # If no sample was specified assume it exists in tantalus and
    # search for it based on header info
    if sample is None:
        if len(bam_header_info["sample_ids"]) != 1:
            raise ValueError(f"found sample_ids={bam_header_info['sample_ids']}, please specify override sample id")
        sample_id = list(bam_header_info["sample_ids"])[0]
        sample = tantalus_api.get('sample', sample_id=sample_id)

    # If no library was specified assume it exists in tantalus and
    # search for it based on header info
    if library is None:
        if len(bam_header_info["library_ids"]) != 1:
            raise ValueError(f"found library_ids={bam_header_info['library_ids']}, please specify override library id")
        library_id = list(bam_header_info["library_ids"])[0]
        library = tantalus_api.get('dna_library', library_id=library_id)

    # Default paired end reads
    if read_type is None:
        read_type = 'P'

    # If no lane infos were specified create them from header info
    if lane_infos is None:
        lane_infos = []
        for lane in bam_header_info["sequence_lanes"]:
            lane_info = {
                "flowcell_id": lane["flowcell_id"],
                "lane_number": lane["lane_number"],
                "library_id": lane["library_id"],
                "sequencing_centre": lane["sequencing_centre"],
                "read_type": read_type,
            }
            lane_infos.append(lane_info)

    # Add the sequence dataset to Tantalus
    sequence_dataset = add_sequence_dataset(
        tantalus_api,
        storage_name=storage_name,
        sample=sample,
        library=library,
        dataset_type="BAM",
        sequence_lanes=lane_infos,
        bam_file_path=bam_file_path,
        reference_genome=ref_genome,
        aligner=aligner_name,
        bai_file_path=bai_file_path,
        tag_name=tag_name,
        update=update,
    )

    return sequence_dataset


@click.command()
@click.argument("storage_name")
@click.argument("bam_file_path")
@click.option("--sample_id")
@click.option("--library_id")
@click.option("--library_type")
@click.option("--index_format")
@click.option("--read_type")
@click.option("--ref_genome")
@click.option("--update",is_flag=True)
@click.option("--tag_name",default=None)
def main(storage_name, bam_file_path, **kwargs):
    """
    Imports the bam into tantalus by creating a sequence dataset and 
    file resources 
    """
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    tantalus_api = TantalusApi()

    sample = None
    if kwargs.get('sample_id') is not None:
        sample = tantalus_api.get_or_create(
            'sample',
            sample_id=kwargs['sample_id'],
        )

    library = None
    if kwargs.get('library_id') is not None:
        library = tantalus_api.get_or_create(
            'dna_library',
            library_id=kwargs['library_id'],
            library_type=kwargs['library_type'],
            index_format=kwargs['index_format'],
        )

    dataset = import_bam(
        storage_name,
        bam_file_path,
        sample=sample,
        library=library,
        read_type=kwargs.get('read_type'),
        ref_genome=kwargs.get('ref_genome'),
        update=kwargs.get('update'),
        tag_name=kwargs.get('tag_name'),
    )

    print("dataset {}".format(dataset["id"]))


if __name__ == "__main__":
    main()

