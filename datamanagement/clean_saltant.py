#!/usr/bin/env python
import os
import click
import logging
import traceback
import subprocess
from datetime import datetime, timedelta
from dateutil import parser

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
from workflows.utils.saltant_utils import get_client, get_or_create_task_instance, get_task_queue_id, get_task_instance_status


from common_utils.utils import get_last_n_days

from constants.workflows_constants import ALHENA_VALID_PROJECTS


from saltant.client import Client
from saltant.constants import SUCCESSFUL, FAILED
from workflows.utils import tantalus_utils



get_or_create_task_instance

workflows.utils.saltant_utils.get_or_create_task_instance(
    saltant_user,
    args,
    14, # TODO: by name, cli
    saltant_queue,
)


a = get_or_create_task_instance(name, "sbeatty", args, task_type_id, "prod")

client = get_client()
executable_task_instances = client.executable_task_instances
c = client.updatetaskinstancestatus
task_queue_id = get_task_queue_id("prod")

params = {'user__username': "prod"}
params = {'user__username': "sbeatty", "state__in":"running"}


# Kill all running task instances
task_instance_list = executable_task_instances.list(params)

a = [task_instance.uuid for task_instance in task_instance_list]
b = [task_instance for task_instance in task_instance_list]


task_instance.terminate()

task_instance_list = executable_task_instances.list(params)
for task_instance in task_instance_list:
#    if get_task_instance_status(task_instance.uuid) == 'created':
    task_instance.terminate()

b[0].terminate() 
b[0].uuid

task_instance.terminate()


def killy_the_tasky(task_instance):
    task_instance.delete()

killy_the_tasky(b[0])

b[0].updatetaskinstancestatus_partial_update("27cb5ac0-fe0c-4dd9-b2b2-1449296d2b40")