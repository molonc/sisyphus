#!/usr/bin/env python
import os
import click
import logging
import traceback
import subprocess
from datetime import datetime, timedelta
from dateutil import parser
from vm_control import start_vm, stop_vm,check_vm_status
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi
from dbclients.slack import SlackClient

from workflows.analysis.dlp import (
    alignment,
    hmmcopy,
    annotation,
)

from workflows import run_pseudobulk

from workflows.utils import saltant_utils, file_utils, tantalus_utils, colossus_utils
from workflows.utils.jira_utils import update_jira_dlp, add_attachment, comment_jira, update_jira_alhena

from common_utils.utils import get_last_n_days

from constants.workflows_constants import ALHENA_VALID_PROJECTS
from workflows.chasm_run import chasmbot_run, post_to_jira
import argparse
parser = argparse.ArgumentParser(description="abc")
parser.add_argument("a",type=int, help="analysis id")
parser.add_argument("t", help="analysis_type")
parser.add_argument("j", help="jira ticket")
parser.add_argument("v", help="version")
parser.add_argument("l", help="library_id")
parser.add_argument("n", help="aligner")
args=parser.parse_args()
config = file_utils.load_json(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'config',
        'normal_config.json',
    ))
saltant_utils.run_analysis(
    args.a,
    args.t,
    args.j,
    args.v,
    args.l,
    args.n,
    config,
  )
