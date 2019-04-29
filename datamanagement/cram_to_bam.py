#!/usr/bin/env python

from __future__ import print_function
import json
import logging
import os
import re
import socket
import subprocess
import sys
import click
import paramiko
import pwd

from utils.qsub_job_submission import submit_qsub_job
from utils.qsub_jobs import CramToBamJob
from datamanagement.utils.utils import parse_ref_genome, connect_to_client
from dbclients.tantalus import TantalusApi
from dbclients import basicclient

from utils.constants import (LOGGING_FORMAT,
                             REF_GENOME_REGEX_MAP,
                             SHAHLAB_TANTALUS_SERVER_NAME,
                             SHAHLAB_HOSTNAME,
                             SHAHLAB_SPEC_TO_BAM_BINARY_PATH,
                             HUMAN_REFERENCE_GENOMES_MAP,
                             STORAGE_PREFIX_MAP,
                             STORAGE_PREFIX_REGEX_MAP,
                             DEFAULT_NATIVESPEC)

# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
tantalus_api = TantalusApi()  

class BadReferenceGenomeError(Exception):
    pass


class BadSpecStorageError(Exception):
    pass


def get_uncompressed_bam_path(cram_path):
    """Get path of corresponding BAM file given a CRAM path.
    Get path of uncompressed BAM: remove '.cram' but path otherwise is
    the same.
    """
    return cram_path[:-5]


def cram_to_bam(cram_path,
                reference_genome,
                output_bam_path,
                library,
                sftp_client=None):
    """Decompresses a CRAM compressed BamFile.
    Registers the new uncompressed file in the database.
    Args:
        cram_path: A string containing the path to the CRAM file on
            Shahlab.
        generic_spec_path: A string containing the path to the CRAM with
            the /archive/shahlab bit not included at the beginning of
            the path.
        raw_reference_genome: A string containing the reference genome,
            which will be interpreted to be either hg18 or hg 19 (or it
            will raise an error).
        aligner: A string containing the BamFile aligner.
        output_spec_path: A string containing the output path to the location of the bam
    Raises:
        A BadReferenceGenomeError if the raw_reference_genome can not be
        interpreted as either hg18 or hg19.
    """
    # Convert the CRAM to a BAM
    logging.info("Converting {} to {}".format(cram_path, output_bam_path))

    # If an sftp client was passed in 
    if sftp_client:
        # Connect to thost via SSH client
        local_cram_path = output_bam_path + ".cram"
        cmd = [
            "rsync",
            "-avPL",
            "thost:" + cram_path,
            local_cram_path
        ]
        remote_file = sftp_client.stat(cram_path)
        
        # Check if the file has been successfully transferred before
        if not os.path.isfile(local_cram_path):
            logging.info("Copying cram to {}".format(local_cram_path))
            subprocess.check_call(cmd)
        elif os.path.getsize(local_cram_path) != remote_file.st_size:
            logging.info("Copying cram to {}".format(local_cram_file))
            subprocess.check_call(cmd)
        cram_path = local_cram_path
        
    # Create the job to perform the spec decompression
    job = CramToBamJob('10', cram_path, HUMAN_REFERENCE_GENOMES_MAP[reference_genome], output_bam_path)

    # Submit the job to the cluster and wait for it to finish
    submit_qsub_job(job, DEFAULT_NATIVESPEC, title=library)

    logging.info("Successfully created bam at {}".format(output_bam_path))


def create_bam( 
    cram_path, 
    raw_reference_genome, 
    output_bam_path,
    to_storage,
    library_id,
    sftp_client=None
):
    """
    Creates decompressed bam and bam index from the given CRAM file

    Args:
        cram_path:              (string) full path to the cram file
        raw_reference_genome:   (string) reference genome used 
        output_bam_path:        (string) destination path for the decompressed bam
        to_storage:             (dict) name of the destination storage for the bam 
        library:                (string) internal library ID the bam is associated with 
        sftp_client:            (sftp object) the sftp client connected to the remote host
    """ 
    output_bam_filename = output_bam_path[len(to_storage["prefix"]) + 1:] 
    
    # Check if a decompressed bam already exists in Tantalus 
    # and get its file size
    try:
        file_resource = tantalus_api.get(
                "file_resource",
                filename=output_bam_filename
        )
        file_size = file_resource["size"]
    except basicclient.NotFoundError as e:
        file_size = None

    #Check if the file exists on the to_storage, and if it exists in Tantalus with the same size
    if os.path.isfile(output_bam_path):
        if os.path.getsize(output_bam_path) == file_size:
            logging.warning("An uncompressed BAM file already exists at {} Skipping decompression of cram file".format(output_bam_path))
            return False

    # Find out what reference genome to use. Currently there are no
    # standardized strings that we can expect, and for reference genomes
    # there are multiple naming standards, so we need to be clever here.
    logging.info("Parsing reference genome %s", raw_reference_genome)
    
    reference_genome = parse_ref_genome(raw_reference_genome)

    # Make the destination directories if they don't exist
    output_path, filename = os.path.split(output_bam_path)
    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Decompress the spec file
    cram_to_bam(
        cram_path=cram_path, 
        reference_genome=reference_genome,
        output_bam_path=output_bam_path,
        library=library_id,
        sftp_client=sftp_client
    )

    # Create the bam index
    logging.info("Creating bam index at {}".format(output_bam_path + '.bai'))
    cmd = [ 'samtools',
            'index',
            output_bam_path,
    ]
    subprocess.check_call(cmd)

    logging.info("Successfully created bam index at {}".format(output_bam_path + ".bai"))


@click.command()
@click.argument("cram_path")
@click.argument("output_bam_path")
@click.argument("to_storage_name")
@click.argument("library_id")
@click.argument("reference_genome")
@click.option("--from_gsc", is_flag=True)
def main(**kwargs):
    """
    Decompresses cram file to bam at the specified location. Creates 
    a new bam index for the bam, and adds both files to Tantalus

    Args:
        cram_path:          (string) full path to the cram file
        output_bam_path:    (string) full path to the output bam
        to_storage_name:    (string) name of the destination storage for the bam 
        library_id:         (string) name of the library associated with the bam
        reference_genome:   (string) reference genome used 
        from_gsc:           (flag) a flag to specify whether the cram is from the GSC
    """
    # If the cram file is from the GSC, check if the script is being run on thost
    hostname = socket.gethostname()
    if kwargs["from_gsc"] and hostname != "txshah":
        ssh_client = connect_to_client("10.9.208.161")       
        sftp_client = ssh_client.open_sftp()

        try:
            sftp_client.stat(kwargs["cram_path"])
        except IOError:
            raise Exception("The cram does not exist at {} -- skipping decompression".format(kwargs["cram_path"]))

    else:
        sftp_client = None
        if not os.path.exists(kwargs["cram_path"]):
            raise Exception("The cram does not exist at {} -- skipping decompression".format(kwargs["cram_path"]))

    # Create the tantalus storage object
    try:
        storage = tantalus_api.get_storage(kwargs["to_storage_name"])
    except basicclient.NotFoundError:
        raise Exception("Storage name {} not found on Tantalus. Please use a valid storage".format(kwargs["to_storage_name"]))

    # Decompress the spec
    create_bam(
        cram_path=kwargs["cram_path"], 
        raw_reference_genome=kwargs["reference_genome"], 
        output_bam_path=kwargs["output_bam_path"],
        to_storage=storage,
        library_id=kwargs["library_id"],
        sftp_client=sftp_client
    )

    bam_resource, bam_instance = tantalus_api.add_file(kwargs["to_storage_name"], kwargs["output_bam_path"], update=True)
    logging.info("File resource with ID {} created for bam {}".format(bam_resource["id"], kwargs["output_bam_path"]))

    bai_resource, bai_instance = tantalus_api.add_file(kwargs["to_storage_name"], kwargs["output_bam_path"] + ".bai", update=True)
    logging.info("File resource with ID {} created for bai {}".format(bam_resource["id"], kwargs["output_bam_path"] + ".bai"))


if __name__=='__main__':
    main()