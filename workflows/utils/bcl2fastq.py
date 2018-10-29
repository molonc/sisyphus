#!/usr/bin/env python
import argparse
import os.path as path
import logging

import sys
import log_utils
import colossus_utils

BCL2FASTQ = "/gsc/software/linux-x86_64-centos6/bcl2fastq-2.16.0.10/bin/bcl2fastq"
INSTRUMENT = "N550"

log = logging.getLogger('sisyphus')


def get_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("chip_id", help="The in-house chip_id")
    parser.add_argument("lane_id", help="The BRC lane id ex. CAYKUANXX_2 see SC-430")
    parser.add_argument("source", help="Location of raw data")
    parser.add_argument("bcl_dir", help="The directory with the bcl files")
    parser.add_argument("out_dir", help="The output directory in which to dump the files")

    args = parser.parse_args()
    return args


def retrive_bcl_files(source, bcl_dir):
    """Retrive the bcl files from the source and put them in bcl_dir"""
    log.debug("SOURCE: " + source)
    if "http://bigwigs.brc.ubc.ca/sequencing" in source:
        cmd = ["wget", "-m", "-np", "-nH",
                "--cut-dirs=2",
                "--reject=index.html*",
                "-e", "robots=off",
                "--directory-prefix={}".format(bcl_dir),
                source]
    else:
        cmd = ["rsync", "-auvP", source, bcl_dir, "--include", "Data/",
            "--include", "RunInfo.xml", "--include", "InterOp/",
            "--exclude", "Images/", "--exclude", "Thumbnail_Images/",
            "--exclude", "Logs/", "--exclude", "RTALogs/", "--exclude", "Recipe/"]

    log_utils.sync_call("Retrieving bcl files", cmd)


def get_samplesheet(bcl_dir, chip_id, lane_id):
    """Retrive the samplesheet from the lims"""
    dest = path.join(bcl_dir, "SampleSheet.csv")
    colossus_utils.get_samplesheet(dest, chip_id, lane_id)


# TODO if running on shahlab, should make this use SGE, it hits io really hard
def run_bcl2fastq(bcl_dir, output_dir, shahlab_run):
    """run bcl2fastq on shahlab15 with bcl_dir as input and output_dir as output"""
    cmd = [
        BCL2FASTQ,
        '--runfolder-dir', bcl_dir,
        '--output-dir', output_dir]

    if not shahlab_run:
        cmd = ['ssh', 'shahlab15'] + cmd

    log_utils.sync_call('Running bcl2fastq on shahlab15', cmd)
    return output_dir


def get_row_col_primer_pair_mappings(library_id):
    """Query the lims to create mapping from chip locations to primer pairs
    Args:
        library_id: lims library id
    Returns:
        a dictionary mapping from (row,column) tuples
        to (i5,i7) primer tuples
    """
    data = colossus_utils.query_colossus_library(library_id)
    mapping = {}
    for sublib in data["sublibraryinformation_set"]:
        i5_primer = sublib["primer_i5"]
        i7_primer = sublib["primer_i7"]
        row = sublib["row"]
        column = sublib["column"]
        mapping[(row,column)]=i5_primer,i7_primer

    return mapping
