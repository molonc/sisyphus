#!/usr/bin/env python

from __future__ import print_function
import json
import logging
import os
import re
import socket
import subprocess
import sys
from utils.runtime_args import parse_runtime_args
from utils.constants import LOGGING_FORMAT

# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stdout, level=logging.INFO)

# Useful Shahlab-specific variables
SHAHLAB_TANTALUS_SERVER_NAME = 'shahlab'
SHAHLAB_HOSTNAME = 'node0515'
SHAHLAB_SPEC2BAM_BINARY_PATH = r'/gsc/software/linux-x86_64-centos6/spec-1.3.2/spec2bam'

# This dictionary maps a (Tantalus) BamFile's reference genome field to
# the path of the reference genome FASTA files on Shahlab. We only care
# about the reference genomes that BamFile cares about (currently HG18
# and HG19).
HUMAN_REFERENCE_GENOMES_MAP = {
    'hg18': r'/shahlab/pipelines/reference/gsc_hg18.fa',
    'hg19': r'/shahlab/pipelines/reference/gsc_hg19a.fa',}

# These are regular expressions for identifying which human reference
# genome to use. See https://en.wikipedia.org/wiki/Reference_genome for
# more details on the common standards and how they relate to each
# other. All of these should be run with a case-insensitive regex
# matcher.
HUMAN_REFERENCE_GENOMES_REGEX_MAP = {
    'hg18': [r'hg[-_ ]?18',                 # hg18
             r'ncbi[-_ ]?build[-_ ]?36.1',  # NCBI-Build-36.1
            ],
    'hg19': [r'hg[-_ ]?19',                 # hg19
             r'grc[-_ ]?h[-_ ]?37',         # grch37
            ],}


STORAGE_PREFIX_MAP = {
    'shahlab': r'/shahlab/archive',
    'gsc': r'/projects/analysis',
    'singlecell_blob': r'singlecell/data',
    'rocks': r'/share/lustre/archive'
}

STORAGE_PREFIX_REGEX_MAP = {
    'shahlab': r'^/shahlab/archive/(.+)',
    'gsc': r'^/projects/analysis/(.+)',
    'singlecell_blob': r'^singlecell/data/(.+)',
    'rocks': r'^/share/lustre/archive/(.+)'
}

class BadReferenceGenomeError(Exception):
    pass

class BadSpecStorageError(Exception):
    pass


def get_uncompressed_bam_path(spec_path):
    """Get path of corresponding BAM file given a SpEC path.
    Get path of uncompressed BAM: remove '.spec' but path otherwise is
    the same.
    """
    return spec_path[:-5]


def spec_to_bam(spec_path,
                raw_reference_genome,
                output_bam_path):
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
    logging.debug("Parsing reference genome %s", raw_reference_genome)

    found_match = False

    for ref, regex_list in HUMAN_REFERENCE_GENOMES_REGEX_MAP.iteritems():
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
    logging.debug("Converting {} to {}".format(spec_path, output_bam_path))

    command = [SHAHLAB_SPEC2BAM_BINARY_PATH,
               '--in',
               spec_path,
               '--ref',
               HUMAN_REFERENCE_GENOMES_MAP[reference_genome],
               '--out',
               output_bam_path,
              ]

    subprocess.check_call(command)


def get_filepaths(spec_path, to_storage_prefix):
    found_match = False

    for ref, regex in STORAGE_PREFIX_REGEX_MAP.iteritems():
        if re.match(regex, spec_path, re.IGNORECASE):
            filename = re.search(regex, spec_path, re.IGNORECASE).group(1)
            found_match = True
            break

    if not found_match:
        raise BadSpecStorageError(
            'Can not find a matching storage for ' + spec_path)
        
    output_spec_path = os.path.join(to_storage_prefix, filename)
    output_bam_path = get_uncompressed_bam_path(output_spec_path)
    
    return filename, output_bam_path

    
def create_bam( spec_path, 
                reference_genome, 
                output_bam_path):

    if os.path.isfile(output_bam_path):
        logging.warning("An uncompressed BAM file already exists at {}. Skipping decompression of spec file".format(output_bam_path))
        created = False
        return created

    try:
        spec_to_bam(
            spec_path=spec_path, 
            raw_reference_genome=reference_genome,
            output_bam_path=output_bam_path
        )
        created = True
    except BadReferenceGenomeError as e:
        logging.exception("Unrecognized reference genome")

    if not os.path.isfile(get_uncompressed_bam_path(spec_path) + '.bai'):
        logging.info("Creating bam index at {}".format(output_bam_path + '.bai'))
        cmd = [ 'samtools',
                'index',
                output_bam_path,
        ]
        subprocess.check_call(cmd)

    return created


def main():
    '''
        INPUT JSON IS
        json = {
            "spec_path": spec_path,
            "reference_genome": reference_genome,
            "to_storage": to_storage
        }

    '''
    args = parse_runtime_args()

    filename, output_bam_path = get_filepaths(spec_path, STORAGE_PREFIX_MAP[to_storage])

    created = create_bam(
                args['spec_path'],
                args['reference_genome'], 
                output_bam_path)


if __name__ == '__main__':
    # Run the script
    main()