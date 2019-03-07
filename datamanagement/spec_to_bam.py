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
import datetime
import paramiko
import pwd

import pypeliner.helpers
from pypeliner.execqueue.qsub import AsyncQsubJobQueue
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


class BadReferenceGenomeError(Exception):
    pass


class BadSpecStorageError(Exception):
    pass


class Job(object):
    def __init__(self, thread, spec_path, ref, out_path, binary):
        self.ctx = {}
        self.thread = str(thread)
        self.spec_path = spec_path
        self.ref = ref
        self.out_path = out_path
        self.binary = binary
        self.finished = False
    
    def __call__(self, **kwargs):
        cmd = [
            self.binary,
            "--thread",
            self.thread,
            "--in",
            self.spec_path, 
            "--ref",
            self.ref,
            "--out",
            self.out_path
        ]
        subprocess.check_call(cmd)
        self.finished = True 
   

def get_uncompressed_bam_path(spec_path):
    """Get path of corresponding BAM file given a SpEC path.
    Get path of uncompressed BAM: remove '.spec' but path otherwise is
    the same.
    """
    return spec_path[:-5]


def spec_to_bam(spec_path,
                raw_reference_genome,
                output_bam_path,
                library,
                from_gsc=False):
    """Decompresses a SpEC compressed BamFile.
    Registers the new uncompressed file in the database.
    Args:
        spec_path: A string containing the path to the SpEC file on
            Shahlab.
        generic_spec_path: A string containing the path to the SpEC with
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
    # Find out what reference genome to use. Currently there are no
    # standardized strings that we can expect, and for reference genomes
    # there are multiple naming standards, so we need to be clever here.
    logging.info("Parsing reference genome %s", raw_reference_genome)

    found_match = False

    for ref, regex_list in REF_GENOME_REGEX_MAP.iteritems():
        for regex in regex_list:
            if re.search(regex, raw_reference_genome, flags=re.I):
                # Found a match
                reference_genome = ref
                found_match = True
                break

        if found_match:
            break
    else:
        # No match was found!
        raise BadReferenceGenomeError(
            raw_reference_genome
            + ' is not a recognized or supported reference genome')

    re_bam_path = r"(.+/).+\.bam$"
    if re.match(re_bam_path, output_bam_path, re.IGNORECASE):
        output_path = re.search(re_bam_path, output_bam_path, re.IGNORECASE).group(1)

    if not os.path.exists(output_path):
        os.makedirs(output_path)

    # Convert the SpEC to a BAM
    logging.info("Converting {} to {}".format(spec_path, output_bam_path))

    # Check if the script is not being run on thost. If its not, copy the spec to local
    hostname = socket.gethostname()
    if from_gsc and hostname != "txshah":
        # Connect to thost via SSH client
        ssh_client = paramiko.SSHClient()
        ssh_client.load_system_host_keys()
        
        username = pwd.getpwuid(os.getuid()).pw_name
        ssh_client.connect("10.9.208.161", username=username)
        
        sftp_client = ssh_client.open_sftp()

        spec_name = spec_path.split('/')[-1]
        local_spec_path = os.path.join(output_path, spec_name)
        cmd = [
            "rsync",
            "-avPL",
            "thost:" + spec_path,
            local_spec_path
        ]
        remote_file = sftp_client.stat(spec_path)
        
        # Check if the file has been successfully transferred before
        if not os.path.isfile(local_spec_path):
            logging.info("Copying spec to {}".format(local_spec_path))
            subprocess.check_call(cmd)
        elif os.path.getsize(local_spec_path) != remote_file.st_size:
            logging.info("Copying spec to {}".format(local_spec_file))
            subprocess.check_call(cmd)
        spec_path = local_spec_path
        
    # Create the pypeliner queue object to submit the job
    queue = AsyncQsubJobQueue(modules=(sys.modules[__name__], ), native_spec=DEFAULT_NATIVESPEC)
    
    # Create the job to perform the spec decompression
    job = Job('10', spec_path, HUMAN_REFERENCE_GENOMES_MAP[reference_genome], output_bam_path, SHAHLAB_SPEC_TO_BAM_BINARY_PATH)
    
    current_time = datetime.datetime.now().strftime('%d-%m-%Y_%S-%M-%H')
    dir_name = library + "_" + current_time

    # Create the temp directory for all output logs
    temps_dir = os.path.join('tmp', dir_name)
    pypeliner.helpers.makedirs(temps_dir)
    
    logging.info("Submitting decompression job to the cluster")
    queue.send({'mem': 10}, 'spec_decompression', job, temps_dir)
    job_name = None
    
    # Wait for the job to finish
    while True:
        job_name = queue.wait()
        if job_name is not None:
            break
        os.sleep(10)
    
    result = queue.receive(job_name)
    
    assert result.finished == True

    logging.info("Successfully created bam at {}".format(output_bam_path))

    
def create_bam( spec_path, 
                reference_genome, 
                output_bam_path,
                to_storage,
                library,
                from_gsc):
    tantalus_api = TantalusApi()         
    output_bam_filename = output_bam_path[len(to_storage["prefix"]) + 1:] 
    
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
            logging.warning("An uncompressed BAM file already exists at {} Skipping decompression of spec file".format(output_bam_path))
            return False

    try:
        spec_to_bam(
            spec_path=spec_path, 
            raw_reference_genome=reference_genome,
            output_bam_path=output_bam_path,
            library=library,
            from_gsc=from_gsc
        )
        created = True
    except BadReferenceGenomeError as e:
        logging.exception("Unrecognized reference genome")

    logging.info("Creating bam index at {}".format(output_bam_path + '.bai'))
    cmd = [ 'samtools',
            'index',
            output_bam_path,
    ]
    subprocess.check_call(cmd)

    return created
