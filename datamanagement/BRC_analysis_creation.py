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
import datetime as dt

from collections import defaultdict

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

# create align analysis objects

jira_ticket = create_jira_ticket_from_library("A118375A")
lib = "A118375A"
#jira_ticket = "SC-7024"
create_qc_analyses_from_library(
    lib,
    jira_ticket,
    "v0.8.0",
    "align",
    aligner="M",
)

# create analysis object on colossus
create_colossus_analysis(
    lib,
    jira_ticket,
    "v0.8.0",
    "M",
)



def create_analysis_jira_ticket(library_id, sample, library_ticket):
    '''
    Create analysis jira ticket as subtask of library jira ticket

    Args:
        info (dict): Keys: library_id

    Returns:
        analysis_jira_ticket: jira ticket id (ex. SC-1234)
    '''

    JIRA_USER = os.environ['JIRA_USERNAME']
    JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
    jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

    issue = jira_api.issue(library_ticket)

    # In order to search for library on Jira,
    # Jira ticket must include spaces
    sub_task = {
        'project': {
            'key': 'SC'
        },
        'summary': '{} - {} TenX Analysis'.format(sample, library_id),
        'issuetype': {
            'name': 'Sub-task'
        },
        'parent': {
            'id': issue.key
        }
    }

    sub_task_issue = jira_api.create_issue(fields=sub_task)
    analysis_jira_ticket = sub_task_issue.key

    # Add watchers
    jira_api.add_watcher(analysis_jira_ticket, JIRA_USER)

    # Assign task to myself
    analysis_issue = jira_api.issue(analysis_jira_ticket)
    analysis_issue.update(assignee={'name': JIRA_USER})

    logging.info('Created analysis ticket {} for library {}'.format(analysis_jira_ticket, library_id))

    return analysis_jira_ticket

def create_tickets_and_analyses(import_info):

        # create align analysis objects
        create_qc_analyses_from_library(
            import_info["dlp_library_id"],
            jira_ticket,
            config["scp_version"],
            "align",
            aligner=config["default_aligner"],
        )

        # create analysis object on colossus
        create_colossus_analysis(
            import_info["dlp_library_id"],
            jira_ticket,
            config["scp_version"],
            config["default_aligner"],
        )
