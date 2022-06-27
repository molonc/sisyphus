import os
import settings
import logging
import io
import yaml
import click
import time
import pandas as pd

import os
import settings
import time
import logging
import contextlib
import sys
from saltant.client import Client
from saltant.constants import SUCCESSFUL, FAILED
from workflows.utils import tantalus_utils

client = None


from datamanagement.utils.constants import LOGGING_FORMAT

def get_client():
    global client
    if client is None:
        client = Client(
            base_api_url=os.environ.get(
                'SALTANT_API_URL',
                'https://saltant.canadacentral.cloudapp.azure.com/api/',
            ),
            auth_token=os.environ['SALTANT_API_TOKEN'],
        )
    return client


params = {}
params = {'name': 'prod', 'user__username': 'prod'}
params = {'user__username': 'prod'}#, 'state__in': 'running'}
client = get_client()
executable_task_instances = client.executable_task_instances

task_instance_list = executable_task_instances.list(params)

task_queue_id = get_task_queue_id('prod')

executable_task_instances.read(params)
    # Kill all running task instances
task_instance_list = executable_task_instances.list(params)
Run Analysis
for task_instance in task_instance_list:
    print(1)
def get_task_instance_status(uuid):
    """
    Get the status of task instance given a unique identifier.
    """
    return get_client().executable_task_instances.get(uuid=uuid).state
get_task_instance_status(task_instance_list[4].uuid)