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
from utils.runtime_args import parse_runtime_args
from utils.tantalus import TantalusApi


def get_bam_ref_genome(bam_header):
    # TODO(mwiens91): come up with a heuristic for determining this from
    # bam headers
    return "HG19"  # bam_header['SQ'][0]['AS']

    # TODO(mwiens91): ummm. what's this code doing here...
    for pg in bam_header["PG"]:
        if "bwa" in pg["ID"]:
            if "GRCh37-lite.fa" in pg["CL"]:
                return "grch37"
            if "mm10" in pg["CL"]:
                return "mm10"

    raise Exception("no ref genome found")


def get_bam_aligner_name(bam_header):
    for pg in bam_header["PG"]:
        if "bwa" in pg["ID"] or "bwa" in pg["CL"]:
            if "sampe" in pg["CL"]:
                return "bwa_aln"
            if "mem" in pg["CL"]:
                return "bwa_mem"

    raise Exception("no aligner name found")


def get_bam_header_file(filename):
    return pysam.AlignmentFile(filename).header


def get_bam_header_blob(blob_service, container_name, blob_name):
    sas_token = blob_service.generate_blob_shared_access_signature(
        container_name,
        blob_name,
        permission=azure.storage.blob.BlobPermissions.READ,
        expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=1),
    )

    blob_url = blob_service.make_blob_url(
        container_name=container_name,
        blob_name=blob_name,
        protocol="http",
        sas_token=sas_token,
    )

    return pysam.AlignmentFile(blob_url).header


def get_bam_header_info(header):
    # TODO(mwiens91): What's this for? It isn't being used anywhere.
    index_info = {}

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


def get_size_file(filename):
    return os.path.getsize(filename)


def get_created_time_file(filename):
    return pd.Timestamp(time.ctime(os.path.getmtime(filename)), tz="Canada/Pacific")


def get_size_blob(blob_service, container, blobname):
    properties = blob_service.get_blob_properties(container, blobname)
    blobsize = properties.properties.content_length
    return blobsize


def get_created_time_blob(blob_service, container, blobname):
    properties = blob_service.get_blob_properties(container, blobname)
    created_time = properties.properties.last_modified.isoformat()
    return created_time


def import_bam(
    tantalus_api,
    storage_name,
    dataset_name,
    bam_filename,
    read_type,
    sequencing_centre,
    tag_name=None,
):
    storage = tantalus_api.get("storage", name=storage_name)

    # TODO(mwiens91): What's this for? It isn't being used anywhere.
    metadata = []

    if storage["storage_type"] == "blob":
        bam_header, file_resources = import_bam_blob(
            bam_filename, storage["storage_container"]
        )
    elif storage["storage_type"] == "server":
        bam_header, file_resources = import_bam_server(
            bam_filename, storage["storage_directory"]
        )
    else:
        raise ValueError("unsupported storage type {}".format(storage["storage_type"]))

    ref_genome = get_bam_ref_genome(bam_header)
    aligner_name = get_bam_aligner_name(bam_header)
    bam_header_info = get_bam_header_info(bam_header)

    sample_pk = tantalus_api.get("sample", sample_id=bam_header_info["sample_id"])["id"]

    library_pk = tantalus_api.get(
        "dna_library", library_id=bam_header_info["library_id"]
    )["id"]

    if tag_name is not None:
        tag_pk = tantalus_api.get("sequence_dataset_tag", name=tag_name)["id"]

        tags = [tag_pk]
    else:
        tags = []

    sequence_lane_pks = []

    for lane in bam_header_info["sequence_lanes"]:
        lane_pk = tantalus_api.get_or_create(
            "sequencing_lane",
            flowcell_id=lane["flowcell_id"],
            dna_library=library_pk,
            read_type=read_type,
            lane_number=lane["lane_number"],
            sequencing_centre=sequencing_centre,
        )["id"]
        sequence_lane_pks.append(lane_pk)

    file_resource_pks = []

    for info in file_resources:
        fr_pk = tantalus_api.get(
            "file_resource",
            size=info["size"],
            created=info["created"],
            file_type=info["file_type"],
            compression="UNCOMPRESSED",
            filename=info["filename"],
        )["id"]

        file_resource_pks.append(fr_pk)

        tantalus_api.get_or_create(
            "file_instance", storage=storage["id"], file_resource=fr_pk
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


def import_bam_blob(bam_filename, container_name):
    # Assumption: bam filename is prefixed by container name
    bam_filename = bam_filename.strip("/")

    if not bam_filename.startswith(container_name + "/"):
        raise Exception(
            "expected container name {} as prefix for bam filename {}".format(
                container_name, bam_filename
            )
        )

    bam_filename = bam_filename[len(container_name + "/") :]

    bai_filename = bam_filename + ".bai"

    blob_service = azure.storage.blob.BlockBlobService(
        account_name=os.environ["AZURE_STORAGE_ACCOUNT"],
        account_key=os.environ["AZURE_STORAGE_KEY"],
    )

    bam_header = get_bam_header_blob(blob_service, container_name, bam_filename)

    bam_info = {
        "filename": bam_filename,
        "size": get_size_blob(blob_service, container_name, bam_filename),
        "created": get_created_time_blob(blob_service, container_name, bam_filename),
        "file_type": "BAM",
    }

    bai_info = {
        "filename": bai_filename,
        "size": get_size_blob(blob_service, container_name, bai_filename),
        "created": get_created_time_blob(blob_service, container_name, bai_filename),
        "file_type": "BAI",
    }

    return bam_header, [bam_info, bai_info]


def import_bam_server(bam_filepath, storage_directory):
    if not bam_filepath.startswith(storage_directory):
        raise ValueError(
            "{} not in storage directory {}".format(bam_filepath, storage_directory)
        )

    bam_filename = bam_filepath.replace(storage_directory, "").lstrip("/")

    bam_header = get_bam_header_file(bam_filepath)

    bam_info = {
        "filename": bam_filename,
        "size": get_size_file(bam_filepath),
        "created": get_created_time_file(bam_filepath),
        "file_type": "BAM",
    }

    bai_info = {
        "filename": bam_filename + ".bai",
        "size": get_size_file(bam_filepath + ".bai"),
        "created": get_created_time_file(bam_filepath + ".bai"),
        "file_type": "BAI",
    }

    return bam_header, [bam_info, bai_info]


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
        args["dataset_name"],
        args["bam_filename"],
        args["read_type"],
        args["sequencing_centre"],
        tag_name=args.get("tag_name"),
    )

    print("dataset {}".format(dataset["id"]))
