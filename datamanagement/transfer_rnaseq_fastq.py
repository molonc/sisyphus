from datamanagement.templates import TENX_FASTQ_NAME_TEMPLATE, TENX_SCRNA_DATASET_TEMPLATE
from datamanagement.utils.gsc import get_sequencing_instrument, GSCAPI
from dbclients.tantalus import TantalusApi
from datamanagement.query_gsc_for_wgs_bams import rsync_file, size_match
from datamanagement.utils.constants import LOGGING_FORMAT, SOLEXA_RUN_TYPE_MAP
from datamanagement.add_generic_dataset import add_generic_dataset
from datamanagement.utils.utils import (
        get_lanes_hash, make_dirs,
        convert_time,
        valid_date,
        add_compression_suffix,
        connect_to_client
    )
import click
import socket
import logging
import sys
import os
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
gsc_api = GSCAPI()
SHAHLAB_BASE_PATH = "/shahlab/archive/"
tantalus_api = TantalusApi()
username = os.environ["SERVER_USER_NAME"]

@click.group()
def main():
    pass

def get_sftp():
    if socket.gethostname() != "txshah":
        ssh_client = connect_to_client("10.9.208.161")
        sftp = ssh_client.open_sftp()
    else:
        sftp = None
    return sftp

@main.command("query_gsc_rnqseq_fastq")
@click.argument("library_id", nargs=1)
@click.argument("to_storage", nargs=1)
@click.option("--sample_id")
@click.option("--update_file_resource", is_flag=True)
def query_gsc_rnqseq_fastq(**kwargs):
    '''
    Main function to perform GSC query for the fastq files of RNASEQ datasets.
    Args:
        library_id:           (string) library id
        to_storage:           (string) destination storage
        sample_id:            (string) sample id, optional
        update_file_resource: (boolean) whether to update the existing file resource(s)
    '''
    library_id = kwargs["library_id"]
    to_storage = kwargs["to_storage"]
    sftp = get_sftp()
    if sftp:
        remote_host = "thost:"
    else:
        remote_host = None
    file_resource_ids = []
    #fetch the file information from GSC
    concat_fastq_results = gsc_api.query("concat_fastq?library={}&production=true".format(library_id))
    lane_info_list = []
    file_paths = []
    for concat_fastq in concat_fastq_results:
        #Check the file's chastity condition
        data_path = concat_fastq["data_path"]
        if "chastity_passed" in data_path:
            #transfer the file only with chastity=passed
            lane_info = {}

            #get the sample id from the file information
            sample_id = concat_fastq["libcore"]["library"]["external_identifier"]

            #rename the file according to the file name template for the fastq file
            file_name = TENX_FASTQ_NAME_TEMPLATE.format(
            library_id=library_id,
            sample_id=sample_id,
            fastq="S1_L00{}_R{}_001.fastq.gz".format(
                    concat_fastq["libcore"]["run"]["lane_number"],
                    concat_fastq["file_type"]["filename_pattern"][1]
                    ),
            )

            #get the lane info from the file_information
            lane_info["flowcell_id"] = concat_fastq["libcore"]["run"]["flowcell_id"]
            lane_info["lane_number"] = concat_fastq["libcore"]["run"]["lane_number"]
            file_path = os.path.join(SHAHLAB_BASE_PATH, library_id, str(concat_fastq["libcore"]["run"]["lane_number"]), file_name)
            file_paths.append(file_path)

            #if the file does not exist at the destination location
            if not os.path.exists(file_path):
                rsync_file(
                    from_path=data_path,
                    to_path=file_path,
                    sftp=sftp,
                    remote_host=remote_host
                )

            #if the local file size does not match the remote file size
            size_match_local = size_match(file_path, data_path, "10.9.208.161", username)
            if not size_match_local:
                logging.info("The file exists at the destination location, but the file size doesn't match the local file size or the local file does not exist, start transferring the file.")
                rsync_file(
                    from_path=data_path,
                    to_path=file_path,
                    sftp=sftp,
                    remote_host=remote_host
                )
            #check the local and remote file sizes again
            size_match_local = size_match(file_path, data_path, "10.9.208.161", username)
            #if the file transfer is done, import the file resource and instance into tantalus
            if os.path.exists(file_path) and size_match_local:
                logging.info("The file exists and the file size matches, import the file into tantalus.")
                logging.info("Adding {} into tantalus".format(file_path))
                file_resource, file_instance = tantalus_api.add_file(to_storage, file_path, update=kwargs["update_file_resource"])
                logging.info("The file has been added into tantalus, with file_resource_id = {}".format(file_resource["id"]))
                file_resource_ids.append(file_resource["id"])
            lane_info_list.append(lane_info)
            else:
                logging.warning("The file {} is imcomplete, please transfer it again.".format(file_path))
                continue

        else:
            continue
    logging.info("The file resources added are {}".format(file_resource_ids))

    #if there are file resources added into tantalus, start creating a new dataset
    if file_resource_ids:
        #creating the dataset name
        if kwargs["sample_id"]:
            sample_id = sample_id

        dataset_name = TENX_SCRNA_DATASET_TEMPLATE.format(
            dataset_type="FQ",
            sample_id=sample_id,
            library_type="RNASeq",
            library_id=library_id,
            lanes_hash=get_lanes_hash(lane_info_list),
            )

        logging.info("Creating dataset {}".format(dataset_name))
        import_fastq(
            sample_id=sample_id,
            library_id=library_id,
            concat_fastqs=concat_fastq_results,
            dataset_name=dataset_name,
            file_resource_pks=file_resource_ids,
            )


def import_fastq(**kwargs):
    '''
    Helper function to import a fastq file into a tantalus dataset.
    '''
    #create the sample
    sample = tantalus_api.get_or_create(
        "sample",
        sample_id=kwargs["sample_id"]
        )
    logging.info("sample created with id:{}".format(sample["id"]))
    #create the library
    library = tantalus_api.get_or_create(
        "dna_library",
        library_id=kwargs["library_id"],
        library_type="RNASEQ",
        index_format="N"
        )
    logging.info("library created with id:{}".format(library["id"]))
    sequence_lane_pks = []
    concat_fastq_info = kwargs["concat_fastqs"]
    for info in concat_fastq_info:
        flowcell_info = gsc_api.query("flowcell/{}".format(info["libcore"]["run"]["flowcell_id"]))
        flowcell_id = flowcell_info["lims_flowcell_code"]
        lane_fields = dict(
            dna_library=library["id"],
            flowcell_id=flowcell_id,
            lane_number=str(info["libcore"]["run"]["lane_number"]),
            read_type=SOLEXA_RUN_TYPE_MAP[info["libcore"]["run"]["solexarun_type"]],
            sequencing_instrument=get_sequencing_instrument(info["libcore"]["run"]["machine"]),
            sequencing_centre="GSC",
        )
        lane = tantalus_api.get_or_create(
            "sequencing_lane",
            **lane_fields
            )

        if lane["id"] not in sequence_lane_pks:
            sequence_lane_pks.append(lane["id"])
    logging.info("The sequence lanes {} are being added.".format(sequence_lane_pks))

    #create the dataset
    sequence_dataset = tantalus_api.get_or_create(
        "sequence_dataset",
        name=kwargs["dataset_name"],
        dataset_type="FQ",
        sample=sample["id"],
        library=library["id"],
        sequence_lanes=sequence_lane_pks,
        file_resources=kwargs["file_resource_pks"],
    )
    logging.info("The sequence dataset with id {} was created.".format(sequence_dataset["id"]))


if __name__=="__main__":
    main()
