#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import string
import sys
import time
import collections
import click
import pandas as pd
import datetime
from collections import defaultdict

# for async blob upload
import asyncio

import settings

from datamanagement.utils.constants import LOGGING_FORMAT
from datamanagement.utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
from datamanagement.utils.comment_jira import comment_jira
import datamanagement.templates as templates
from datamanagement.utils.filecopy import rsync_file, try_gzip
from datamanagement.utils.gsc import get_sequencing_instrument, GSCAPI
from datamanagement.utils.runtime_args import parse_runtime_args
from datamanagement.fixups.add_fastq_metadata import add_fastq_metadata_yaml

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from workflows.utils.jira_utils import create_jira_ticket_from_library
from workflows.utils.colossus_utils import create_colossus_analysis
from workflows.utils.file_utils import load_json
from workflows.utils.tantalus_utils import create_qc_analyses_from_library

from dbclients.utils.dbclients_utils import (
    get_colossus_base_url,
)

from common_utils.utils import (
    get_today,
    validate_mode,
)

from datamanagement.utils.import_utils import (
    reverse_complement,
    decode_raw_index_sequence,
    map_index_sequence_to_cell_id,
    summarize_index_errors,
    raise_index_error,
    filter_failed_libs_by_date,
)

COLOSSUS_BASE_URL = get_colossus_base_url()


def create_tickets_and_analyses(dlp_library_id):
    """
    Creates jira ticket and an align analysis on tantalus if new lanes were imported

    Args:
        import_info (dict): Contains keys dlp_library_id, gsc_library_id, lanes
    """
    config = load_json(
        os.path.join(
            os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
            'workflows',
            'config',
            'normal_config.json',
        ))

    # create analysis jira ticket
    jira_ticket = create_jira_ticket_from_library(dlp_library_id)

    # create align analysis objects
    create_qc_analyses_from_library(
        dlp_library_id,
        jira_ticket,
        config["scp_version"],
        "align",
        aligner=config["default_aligner"],
    )

    # create analysis object on colossus
    create_colossus_analysis(
        dlp_library_id,
        jira_ticket,
        config["scp_version"],
        config["default_aligner"],
    )


@click.command()
@click.argument('brc_library_id', nargs=1)
def main(brc_library_id):
# Connect to the Tantalus API (this requires appropriate environment)
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()
    create_tickets_and_analyses(brc_library_id)
if __name__ == "__main__":
    main()
