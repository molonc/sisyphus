#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import io
import sys
import pwd
import gzip
import click
import shutil
import socket
import logging
import paramiko
import subprocess

import pandas as pd
from Bio import SeqIO
from jira import JIRA
from jira.exceptions import JIRAError

from datamanagement.templates import (
    GSC_SCRNA_FASTQ_PATH_TEMPLATE,
    TENX_FASTQ_NAME_TEMPLATE,
    TENX_FASTQ_BLOB_TEMPLATE,
    TENX_SCRNA_DATASET_TEMPLATE
)
from datamanagement.utils.gsc import GSCAPI
from datamanagement.utils.constants import LOGGING_FORMAT
from datamanagement.utils.utils import get_lanes_hash, connect_to_client
from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import FieldMismatchError, NotFoundError


JIRA_USERNAME = os.environ['JIRA_USERNAME']
JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))
tantalus_api = TantalusApi()
colossus_api = ColossusApi()
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

CMDLINE_REGEX = r".*--fastqs=([^\s]*).*"
CMDLINE_FASTQ_REGEX = r".+/((\d{6}_[\w]{8}_[\d]{4}_[\w]+)/.+)"
BIGWIGS_BASE_PATH = "/brcwork/patientdata"
TENX_FASTQ_TMP_DIR = '/shahlab/archive/fastq_tmp'
COLOSSUS_SEQUENCING_MAP = {
    "NextSeq500": "N500",
    "HiSeq-28": "H2500",
    "HiSeq2500": "H2500",
    "NextSeq550":"N550",
    "MiSeq":"MI",
    "other":"O",
}
TANTALUS_SEQUENCING_MAP = {
    "HiSeq-28": "HiSeq2500",
    "NS500668": "NextSeq500"
}
JIRA_MESSAGE = """Successfully imported FASTQs to Azure storage, Tantalus and Colossus. 
Files can be found at:
{{noformat}}
{filepaths}
{{noformat}}
"""

@click.group()
def sequencing_centre():
    pass


def tantalus_import(
    library_id, 
    sample_id, 
    lane_infos, 
    blob_paths,
    sequencing_centre, 
    dataset_type,  
    storage_name, 
    tag_name=None, 
    update=False
):
    """
    Imports tenx sequence dataset and file resources into tantalus

    Args:
        library_id:         (str) internal name for the library
        sample_id:          (str) internal name for the sample
        lane_infos:         (list) a list of dictionaries containing 
                            flowcell ID, lane number, and sequencing
                            instrument
        blob_paths:         (list) a list of filepaths to the FASTQs 
                            on azure storage
        sequencing_centre:  (str) GSC or BRC according to where the 
                            library was sequenced 
        dataset_type:       (str) FQ, BAM, or BCL
        storage_name:       (str) name of the azure storage in tantalus
        tag_name:           (str) name of the tag associated with the 
                            dataset
        update:             (bool) a boolean indicating whether to update
                            any information already in tantalus
    Returns:
        sequence_dataset["id"]: ID of the newly created sequence dataset
    """
    file_resource_ids, file_instance_ids, sequence_lanes, sequence_lanes_pks = [], [], [], []
    
    sample_pk = tantalus_api.get_or_create(
        "sample",
        sample_id=sample_id,
    )["id"]
    library_pk = tantalus_api.get_or_create(
        "dna_library",
        library_id=library_id,
        library_type="SC_RNASEQ",
        index_format="TENX",
    )["id"]

    # Add the file resources to tantalus
    for blob_path in blob_paths:
        file_resource, file_instance = tantalus_api.add_file(storage_name, blob_path, update=update)
        file_resource_ids.append(file_resource["id"])
        file_instance_ids.append(file_instance["id"])
    
    logging.info("Adding lanes to Tantalus")
    for lane_info in lane_infos:
        # Try to find a match for the sequencing instrument
        try:
            sequencing_instrument = TANTALUS_SEQUENCING_MAP[lane_info["sequencing_instrument"]]
        except KeyError:
            sequencing_instrument = lane_info["sequencing_instrument"]
        lane = tantalus_api.get_or_create(
                "sequencing_lane",
                flowcell_id=lane_info["flowcell_id"],
                dna_library=library_pk,
                read_type="TENX",
                lane_number=str(lane_info["lane_number"]),
                sequencing_centre=sequencing_centre,
                sequencing_instrument=lane_info["sequencing_instrument"]
        )
        sequence_lanes.append(lane)
        sequence_lanes_pks.append(lane["id"])
    
    dataset_name = TENX_SCRNA_DATASET_TEMPLATE.format(
        dataset_type=dataset_type,
        sample_id=sample_id,
        library_type="SC_RNASEQ",
        library_id=library_id,
        lanes_hash=get_lanes_hash(sequence_lanes),
    )

    # Create tags
    if tag_name is not None:
        tag_pk = tantalus_api.get_or_create("tag", name=tag_name)["id"]
        tags = [tag_pk]
    else:
        tags = []

    # Add the sequence dataset to tantalus
    sequence_dataset = tantalus_api.get_or_create(
            "sequence_dataset",
            name=dataset_name,
            dataset_type=dataset_type,
            sample=sample_pk,
            library=library_pk,
            sequence_lanes=sequence_lanes_pks,
            file_resources=file_resource_ids,
            tags=tags,
    )
    logging.info("Sequence dataset has ID {}".format(sequence_dataset["id"]))
    return sequence_dataset["id"]

def colossus_import(library_id, lane_infos, pool_id, tantalus_dataset_id, sequencing_centre):
    """
    Updates the sequencing object in Colossus for the associated pool

    Args:
        library_id:         (str) internal name for the library 
        lane_infos:         (list) list of dicitonaries containing lane
                            info for the pool
        pool_id:            (str) name of the pool in Colossus
        tantalus_dataset_id:(int) primary key of the sequence dataset
                            in tantalus
        sequencing_centre:  (str) UBCBRC or BCCAGSC according to where 
                            the pool was sequenced 
    """
    library = colossus_api.get("tenxlibrary", name=library_id)
    pool = colossus_api.get(
        "tenxpool",
        id=pool_id,
        construction_location=sequencing_centre
    )

    logging.info("Updating sequencing object in Colossus")
    try:
        sequencing_instrument = COLOSSUS_SEQUENCING_MAP[lane_infos[0]["sequencing_instrument"]]
    except KeyError:
        logging.warning("Sequencing instrument {} not found in colossus. Using \"other\"".format(
            lane_infos[0]["sequencing_instrument"])
        )
        sequencing_instrument = COLOSSUS_SEQUENCING_MAP["other"]
    sequencing = colossus_api.get_or_create(
        "tenxsequencing",
        tenx_pool=pool["id"],
        sequencing_center=sequencing_centre,
        sequencing_instrument=sequencing_instrument
    )

    logging.info("Adding lanes to Colossus")
    for lane_info in lane_infos:
        flowcell_id = "_".join([lane_info["flowcell_id"], str(lane_info["lane_number"])])
        try:
            lane = colossus_api.get(
                "tenxlane",
                flow_cell_id=flowcell_id,
                sequencing_date=None,
                sequencing=sequencing["id"]
            )
            tantalus_datasets = [tantalus_dataset_id] + lane["tantalus_datasets"]
            lane = colossus_api.update(
                "tenxlane",
                id=lane["id"],
                tantalus_datasets=tantalus_datasets
            )
        except NotFoundError:
            lane = colossus_api.create(
                "tenxlane",
                flow_cell_id=flowcell_id,
                sequencing_date=None,
                sequencing=sequencing["id"],
                tantalus_datasets=[tantalus_dataset_id]
            )


def check_file(filepath, sftp_client=None):
    """
    Checks if the file exists. Raises an exception if
    the file cannot be found

    Args:
        filepath:   (str) the filepath to test
        sftp_client: (object) sftp client if the file is 
                      on a remote server
    """
    if sftp_client:
        try:
            sftp_client.stat(filepath)
        except IOError:
            raise Exception("The filepath {} does not exist".format(filepath))
    else:
        if not os.path.exists(filepath):
            raise Exception("The filepath {} does not exist".format(filepath))



def update_jira(blob_paths, jira_ticket):
    """
    Checks if the jira ticket exists on the gsc. If it does, add a comment 
    regarding the completion of the data import

    Args:
        blob_paths:     (list) blob paths that were imported
        jira_ticket:    (str) the ticket ID on GSC jira
    """
    out_files = ""
    for blob_path in blob_paths:
        out_files += "\n{}".format(blob_path)
    
    msg = JIRA_MESSAGE.format(
        filepaths=output
    )
    try:
        issue = jira_api.issue(jira_ticket)
    except JIRAError:
        logging.error("The jira ticket {} does not exist. Skipping ticket update".format(jira_ticket))
        return

    comment = jira_api.add_comment(jira_ticket, msg)


def upload_to_blob(blob_name, source_path, storage, storage_client):
    """
    Uploads a file to blob storage

    Args:
        blob_name:      (str) name of the blob on azure
        source_path:    (str) path to the file to be uploaded
        storage:        (dict) tantalus storage object of source storage
        storage_client: (object) storage client connected to 
                        the destination storage on azure

    Returns:
        blob_path:  (str) path to the blob on azure
    """
    blob_path = os.path.join(storage["prefix"], blob_name)
    storage_client.create(blob_name, source_path)

    return blob_path


def transfer_fastq(fastq_path_info, output_dir, storage, sequencing_centre, sftp_client=None):
    """
    Transfers FASTQ files from remote server to Azure storage. If the files are on a different
    machine than the local machine, the FASTQs are first transferred to the local and then 
    uploaded to Azure from the tmp directory

    Args:
        fastq_path_info:    (dataframe) holds source path, destination path, and fastq name
                            for each fastq for the library
        output_dir:         (str) output directory for the tmp fastqs
        storage_name:       (str) name of the azure storage in tantalus
        sftp_client:        (object) sftp client if the file is 
                            on a remote server

    Returns:
        blob_paths: (list) list of all the blob names added to azure
    """
    # Make the tmp directory 
    if not os.path.exists(output_dir):
        logging.info("Creating new directory at {}".format(output_dir))
        os.makedirs(output_dir)

    storage_client = tantalus_api.get_storage_client(storage["name"])

    blob_paths = []
    for index, row in fastq_path_info.iterrows():
        # Transfer to tmp dir if the files are not on the local machine
        if sftp_client:
            if sequencing_centre == "BCCAGSC":
                source_path = "thost:" + row["fastq_source_path"]
            elif sequencing_centre == "UBCBRC":
                source_path = "bigwigs:" + row["fastq_source_path"]

            tmp_dest_path = os.path.join(output_dir, row["fastq_name"])

            # Transfer to temp directory
            cmd = [
                "rsync",
                "-avPL",
                source_path,
                tmp_dest_path
            ]
            logging.info("Copying tmp file to {}".format(tmp_dest_path))
            subprocess.check_call(cmd)
        else:
            tmp_dest_path = row["fastq_source_path"]

        logging.info("Uploading {} to Azure storage {}".format(row["fastq_name"], storage["name"]))
        blob_path = upload_to_blob(
            row["fastq_dest_path"], 
            tmp_dest_path, 
            storage, 
            storage_client
        )
        blob_paths.append(blob_path)

    return blob_paths


def get_brc_fastq_info(results_path, library_id, sample_id, sftp_client):
    """
    Gets the FASTQ filepaths for the library. 

    TODO:   Figure out a procedure for Ryan to follow so we can track the 
            results without having to parse the _cmdline file and manually
            find the FASTQs

    Args:
        results_path:   (str) path to the cellranger results on bigwigs
        sftp_client:    (object) sftp client if the file is 
                        on a remote server
    """
    logging.info("Getting BRC FASTQ paths")
    cmd_file = os.path.join(results_path, "_cmdline")

    # Find the fastq directory
    try:
        with sftp_client.open(cmd_file, "r") as file:
            for line in file:
                fastq_dir = re.search(CMDLINE_REGEX, line, re.I).group(1)
                brc_sample_id = fastq_dir.split("/")[-1]
    except IOError:
        raise Exception("The file {} does not exist".format(cmd_file))

    # Parse the fastq subdirectory
    fastq_subdir = re.search(CMDLINE_FASTQ_REGEX, fastq_dir, re.I).group(1)

    # Construct the fastq directory on bigwigs
    bigwigs_dir = os.path.join(BIGWIGS_BASE_PATH, fastq_subdir)

    # Read the FASTQs
    logging.info("Reading FASTQ files")
    check_file(bigwigs_dir, sftp_client)

    fastq_names, fastq_source_paths, fastq_dest_paths = [], [], []
    for file in sftp_client.listdir(bigwigs_dir):
        fastq_source_path = os.path.join(bigwigs_dir, file)
        fastq_split = fastq.split(brc_sample_id)
        fastq_split = fastq_name_split[-1].strip("_")
        fastq_stripped = "".join(fast_split)

        fastq_name = TENX_FASTQ_NAME_TEMPLATE.format(
            library_id=library_id,
            sample_id=sample_id,
            fastq=fastq_stripped
        )
        fastq_dest_path = TENX_FASTQ_BLOB_TEMPLATE.format(
            library_id=library_id,
            fastq_name=fastq_name
        )
        fastq_names.append(fastq_name)
        fastq_source_paths.append(fastq_source_path)
        fastq_dest_paths.append(fastq_dest_path)

    path_info = pd.DataFrame({
        "fastq_names": fastq_names, 
        "fastq_source_paths": fastq_source_paths,
        "fastq_dest_paths": fastq_dest_paths
    })

    return path_info


def get_brc_sequencing_info(fastq_dir):
    """
    Gets lane info for libraries sequenced at the BRC

    Args:
        fastq_dir:  (str) the local directory where the fastqs
                    were transferred to
    Returns:
        lane_infos: (list) a list of dictionaries containing lane
                    numner, sequencing instrument, and flowcell ID
                    for each lane
    """
    if not os.path.exists(fastq_dir):
        raise Exception("The output directory {} does not exist".format(fastq_dir))

    lane_number_map = {
        "1": {"lane_number": "1", "source_fastqs": []},
        "2": {"lane_number": "2", "source_fastqs": []},
        "3": {"lane_number": "3", "source_fastqs": []},
        "4": {"lane_number": "4", "source_fastqs": []}
    }

    # Read each fastq in the directory
    for fastq in os.listdir(fastq_dir):
        fastq_file = os.path.join(fastq_dir, fastq)
        if fastq_file.endswith(".fastq.gz"):
            read_file = gzip.open(fastq_file, "rb")
        elif fastq_file.endswith(".fastq"):
            read_file = fastq_file
        else:
            continue
        records = SeqIO.parse(read_file, "fastq")

        # Get the lane info from the fastq
        split_id = next(records).id.split(":")
        instrument = split_id[0]
        flowcell_id = split_id[2]
        lane_number = split_id[3]

        lane_dict = lane_number_map[str(lane_number)]
        lane_dict["sequencing_instrument"] = instrument
        lane_dict["flowcell_id"] = flowcell_id

    lane_infos = []
    for key, val in LANE_NUMBER_MAP.iteritems():
        lane_infos.append(val)

    return lane_infos


@sequencing_centre.command()
@click.argument('library_id', nargs=1)
# TODO: Add pool name here and remove results_dir
#@click.argument('pool_name', nargs=1)
@click.argument('results_dir', nargs=1)
def brc(**kwargs):
    """
    Uploads FASTQ files from the BRC to Azure and imports metadata into 
    Tantalus and Colossus

    TODO: this script will create a pool with a single library in it. Hopefully in
    the future, the pool will already be created in colossus before the script is
    run, and then we can just use the colossus_api.get() in the colossus_import
    function

    Args:
        library_id:     (str) the internal library name 
        results_dir:    (str) the directory on bigwigs where the cellranger
                        results are located
    """
    # Connect to the BRC server to access files
    ssh_client = connect_to_client('brclogin1.brc.ubc.ca', 'patientdata')
    sftp_client = ssh_client.open_sftp()

    check_file(kwargs["results_dir"], sftp_client)

    # Get colossus details
    library = colossus_api.get(
        "tenxlibrary",
        name=kwargs["library_id"]
    )
    index_used = library["tenxlibraryconstructioninformation"]["index_used"]
    index_used = index_used.split(",")[0]
    sample_id = library["sample"]["sample_id"]
    tmp_output_dir = os.path.join(TENX_FASTQ_TMP_DIR, "_".join([sample_id, kwargs["library_id"]]))

    fastq_paths = get_brc_fastq_info(
        kwargs["results_dir"], 
        library_id, 
        sample_id, 
        sftp_client
    )
    # Transfer FASTQs to Azure
    storage = tantalus_api.get_storage("scrna_fastq")
    tmp_output_dir = os.path.join(TENX_FASTQ_TMP_DIR, "_".join([sample_id, kwargs["library_id"]]))
    blob_paths = transfer_fastq(fastq_path_info_tmp, tmp_output_dir, storage, "UBCBRC", sftp_client)

    # Get the sequencing lane info
    lane_infos = get_brc_sequencing_info(tmp_output_dir)

    # Upload to tantalus
    tantalus_import(
        library_id=kwargs["library_id"],
        sample_id=sample_id,
        lane_infos=lane_infos,
        blob_paths=blob_paths,
        sequencing_centre="BRC",
        dataset_type="FQ",
        storage_name="scrna_fastq",
        update=True
    )

    # TODO: Hopefully remove this in the future???
    pool = colossus_api.get_or_create(
        "tenxpool", 
        libraries=[library["id"]],
        construction_location=sequencing_centre
    )
    pool = colossus_api.update(
        "tenxpool",
        id=pool["id"],
        pool_name="".join(["TENXPOOL", str(pool["id"]).zfill(4)])
    )

    # Upload to colossus
    colossus_import(
        library_id=kwargs["library_id"],
        lane_infos=lane_infos,
        pool_id=pool["id"],
        sequencing_centre="UBCBRC"
    )

    # Add a comment to the jira ticket
    update_jira(blob_paths, library["jira_ticket"])

    # Remove the data from shahlab
    shutil.rmtree(tmp_output_dir)

    ssh_client.close()


def get_gsc_fastq_info(base_dir, library_id, sample_id, library_index, lane_info, sftp_client=None):
    """
    Gets the filepaths to the FASTQ files in the GSC directory on thost. 

    Args:
        base_dir:       (str) 
        library_id:     (str)
        sample_id:      (str)
        library_index:  (str)
        lane_info:      (dict)
        sftp_client:    (object)

    Returns:
        path_info:  (dataframe)

    """
    logging.info("Getting GSC fastq info")
    
    # Check that the remote path exists on thost
    check_file(base_dir, sftp_client)

    logging.info("Preparing FASTQ paths")
    project_dir = "_".join([lane_info["flowcell_id"], str(lane_info["lane_number"])])
    samplesheet_path = os.path.join(
        base_dir, 
        project_dir,
        "outs",
        "input_samplesheet.csv"
    )

    # Read samplesheet to find the directories where the FASTQs are
    samplesheet = sftp_client.file(samplesheet_path)
    samplesheet_df = pd.read_csv(samplesheet)
    try:
        library_df = samplesheet_df[samplesheet_df["index"] == library_index]
    except KeyError:
        raise Exception("The index {} was not found in the cellranger samplesheet".format(library_index))
    fastq_subdir = library_df["sample"].to_string(index=False).strip(" ")
    fastq_dir = os.path.join(
        base_dir, 
        project_dir,
        "outs",
        "fastq_path",
        project_dir,
        fastq_subdir
    )

    # Get the names of all FASTQs associated with this lane, and then 
    # create the destination path on Azure blob storage
    fastq_names, fastq_source_paths, fastq_dest_paths = [], [], []
    for fastq in sftp_client.listdir(fastq_dir):
        illumina_id = fastq.strip(fastq_subdir)
        fastq_name = TENX_FASTQ_NAME_TEMPLATE.format(
            library_id=library_id,
            sample_id=sample_id,
            fastq=illumina_id
        )
        fastq_source_path = os.path.join(fastq_dir, fastq)
        fastq_dest_path = TENX_FASTQ_BLOB_TEMPLATE.format(
            library_id=library_id,
            fastq_name=fastq_name
        )

        fastq_names.append(fastq_name)
        fastq_source_paths.append(fastq_source_path)
        fastq_dest_paths.append(fastq_dest_path)
        
    path_info = pd.DataFrame({
        "fastq_name": fastq_names,
        "fastq_source_path": fastq_source_paths,
        "fastq_dest_path": fastq_dest_paths,
    })

    return path_info


@sequencing_centre.command()
@click.argument('pool_name', nargs=1)
def gsc(**kwargs):
    """
    Uploads FASTQ files from the GSC to Azure and imports metadata into
    Tantalus and Colossus for each library included in the pool

    Args:
        pool_name:    (str) internal identifier for the pool
    """
    # We can only access the GSC FASTQ data if we are on thost
    # Therefore, we need to connect to thost if the script is 
    # being run elsewhere
    hostname = socket.gethostname()
    if hostname != "txshah":
        username = pwd.getpwuid(os.getuid()).pw_name
        ssh_client = connect_to_client('10.9.208.161', username)
        sftp_client = ssh_client.open_sftp()
    else:
        sftp_client = None

    pool_id = int(kwargs["pool_name"].strip("TENXPOOL"))
    pool = colossus_api.get(
        "tenxpool",
        id=pool_id
    )
    gsc_api = GSCAPI()

    # Query the GSC API for sequencing info based on the pool ID
    logging.info("Querying the GSC for pool with ID {}".format(kwargs["pool_id"]))
    library_infos = gsc_api.query("library?external_identifier={}".format(kwargs["pool_id"]))
    if not library_infos:
        logging.error("No results on the GSC")
        raise Exception("No results on GSC for pool {}".format(kwargs["pool_id"]))
    for library_info in library_infos:
        gsc_library_id = library_info["id"]
        gsc_library_name = library_info["name"]
    logging.info("Querying GSC for run info")
    run_infos = gsc_api.query("run?library_id={}".format(gsc_library_id))
    if not run_infos:
        logging.error("No run results for {}".format(kwargs["pool_id"]))
        raise Exception()
    storage = tantalus_api.get_storage("scrna_fastq")

    # Transfer data for each library in the pool
    for library_pk in pool["libraries"]:
        library = colossus_api.get(
            "tenxlibrary",
            id=library_pk
        )
        index_used = library["tenxlibraryconstructioninformation"]["index_used"]
        index_used = index_used.split(",")[0]
        sample_id = library["sample"]["sample_id"]

        lane_infos = []
        fastq_path_info = pd.DataFrame()
        for run_info in run_infos:
            flowcell_infos = gsc_api.query("flowcell?id={}".format(run_info["flowcell_id"]))
            # TODO: In the future (once GSC starts tracking it in their API), get
            # the fastq path from datapath in the run_info object. For now, 
            # we have to parse the directory based on the data from the API
            lane_info = {
                "flowcell_id": flowcell_infos[0]["lims_flowcell_code"],
                "lane_number": run_info["lane_number"],
                "run_directory": run_info["lims_run_directory"],
                "sequencing_instrument": run_info["machine"],
            }
            gsc_results_dir = GSC_SCRNA_FASTQ_PATH_TEMPLATE.format(
                gsc_library_name=gsc_library_name,
                gsc_run_directory=lane_info["run_directory"]
            )
            fastq_path_info_tmp = get_gsc_fastq_info(
                gsc_results_dir, 
                library["name"], 
                sample_id, 
                index_used, 
                lane_info,
                sftp_client
            )
            fastq_path_info = pd.concat([fastq_path_info, fastq_path_info_tmp])
            lane_infos.append(lane_info)

        # Transfer FASTQs to Azure
        tmp_output_dir = os.path.join(TENX_FASTQ_TMP_DIR, "_".join([sample_id, library["name"]]))
        blob_paths = transfer_fastq(fastq_path_info_tmp, tmp_output_dir, storage, "BCCAGSC", sftp_client) 
        
        # Upload to tantalus
        tantalus_dataset_id = tantalus_import(
            library_id=library["name"],
            sample_id=sample_id,
            lane_infos=lane_infos,
            blob_paths=blob_paths,
            sequencing_centre="GSC",
            dataset_type="FQ",
            storage_name="scrna_fastq",
            update=True
        )

        # Upload to colossus
        colossus_import(
            library_id=library["name"],
            lane_infos=lane_infos,
            pool_name=kwargs["pool_id"],
            tantalus_dataset_id=tantalus_dataset_id,
            sequencing_centre="BCCAGSC"
        )

        # Add a comment to the jira ticket
        update_jira(blob_paths, library["jira_ticket"])

        # Remove the data from shahlab
        shutil.rmtree(tmp_output_dir)

    ssh_client.close()


if __name__=='__main__':
    sequencing_centre()