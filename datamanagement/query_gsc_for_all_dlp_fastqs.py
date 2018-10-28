from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import string
import sys
import time
import pandas as pd
from query_gsc_for_dlp_fastqs import import_gsc_dlp_paired_fastqs
from utils.constants import LOGGING_FORMAT
from utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
from utils.filecopy import rsync_file
from utils.gsc import get_sequencing_instrument, GSCAPI
from utils.runtime_args import parse_runtime_args
from utils.colossus import ColossusApi
from utils.tantalus import TantalusApi

if __name__ == "__main__":
    # Set up the root logger
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stdout, level=logging.INFO)

    # Parse the incoming arguments
    args = parse_runtime_args()

    # Connect to the Tantalus API (this requires appropriate environment
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    storage = tantalus_api.get("storage_server", name=args["storage_name"])
    sequencing_list = list(colossus_api.list('sequencing', dlpsequencingdetail__lanes_requested=True))

    for sequence in sequencing_list:

        # Get the tag name if it was passed in
        try:
            tag_name = args["tag_name"]
        except KeyError:
            tag_name = None

        # Query GSC for FastQs
        flowcells_to_be_created = import_gsc_dlp_paired_fastqs(
            colossus_api,
            tantalus_api,
            sequence["library"],
            storage,
            tag_name)

        for flowcell in flowcells_to_be_created:
            colossus_api.get_or_create("lane", sequencing=sequence['id'], flow_cell_id=flowcell, path_to_archive="")

        colossus_api.update('sequencingdetails', sequence['dlpsequencingdetail']['id'], lanes_received=True)


