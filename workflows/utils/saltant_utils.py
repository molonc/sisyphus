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


log = logging.getLogger('sisyphus')


def get_task_instance_status(uuid):
    """
    Get the status of task instance given a unique identifier.
    """
    return get_client().executable_task_instances.get(uuid=uuid).state


def get_task_type_id(task_type_name):
    """
    Get the id of a task type given its name.
    """
    task_type = get_client().executable_task_types.get(name=task_type_name)
    return task_type.id


def get_task_queue_id(task_queue_name):
    """
    Get the id of a task queue given its name.
    """
    task_queue = get_client().task_queues.get(name=task_queue_name)
    return task_queue.id


def wait_for_finish(task_instance_uuid):
    """
    Wait for a task to finish, given its unique identifier.
    """
    while True:
        time.sleep(10)
        status = get_task_instance_status(task_instance_uuid)

        log.debug('Status of task {}: {}'.format(task_instance_uuid, status))
        print('status: ' + status)
        if status == SUCCESSFUL:
            return
        elif status == FAILED:
            raise Exception('Task instance {} failed'.format(task_instance_uuid))


@contextlib.contextmanager
def wait_for_task_instance(task_instance):
    try:
        yield
    finally:
        if get_task_instance_status(task_instance.uuid) in ('running', 'published'):
            log.debug('Terminating task instance {}'.format(task_instance.uuid))
            task_instance.terminate()


def get_or_create_task_instance(name, user, args, task_type_id, task_queue_name, wait=False):
    """
    Create a new task instance in saltant and return its
    unique identifier.
    Args:
        name (str): name of the task instance
        args (dict): arguments for the task type
        task_type_id (int)
        queue_id (int)
    """

    client = get_client()
    executable_task_instances = client.executable_task_instances

    task_queue_id = get_task_queue_id(task_queue_name)

    params = {'name': name, 'user__username': user}

    # Kill all running task instances
    task_instance_list = executable_task_instances.list(params)
    for task_instance in task_instance_list:
        if get_task_instance_status(task_instance.uuid) == 'running':
            task_instance.terminate()

    new_task_instance = executable_task_instances.create(
        name=name,
        arguments=args,
        task_queue_id=task_queue_id,
        task_type_id=task_type_id,
    )

    log.debug('Created task instance {} in saltant in task queue {}'.format(new_task_instance.uuid, task_queue_name))

    if wait == True:
        with wait_for_task_instance(new_task_instance):
            wait_for_finish(new_task_instance.uuid)


def query_gsc_for_dlp_paired_fastqs(jira, config, storage_name, dlp_library_id):
    """
    Query the GSC for DLP paired fastqs.
    """
    user = config['user']
    name = '{}_fastq_query'.format(jira)
    args = {
        'storage_name': storage_name,
        'dlp_library_id': dlp_library_id,
        'tag_name': None,
    }

    task_type_id = get_task_type_id("GSC DLP Paired Fastq Query")
    get_or_create_task_instance(name, user, args, task_type_id, config['thost_task_queue'])


def transfer_files(jira, config, tag_name, from_storage, to_storage):
    """
    Transfer datasets tagged with tag_name.
    """
    storage_type = tantalus_utils.get_storage_type(from_storage)
    if from_storage == 'gsc':
        queue_name = config['thost_task_queue']
    elif storage_type == 'server':
        queue_name = config['shahlab_task_queue']
    else:
        raise Exception('Set up worker on {} for file transfer'.format(from_storage))

    name = '{}_transfer'.format(tag_name)
    args = {
        'tag_name': tag_name,
        'from_storage': from_storage,
        'to_storage': to_storage,
    }

    task_type_id = get_task_type_id("File transfer")
    get_or_create_task_instance(name, config['user'], args, task_type_id, queue_name)


def run_analysis(
        analysis_id,
        analysis_type,
        jira,
        version,
        library_id,
        aligner,
        config,
        skip_missing=False,
        update=False,
):
    name = f"{jira}_{library_id}_{aligner}_{analysis_type}_{analysis_id}"
    queue_name = config['headnode_task_queue']

    args = {
        'jira': jira,
        'analysis_id': analysis_id,
    }

    if skip_missing:
        args['skip_missing'] = skip_missing

    if update:
        args['update'] = update

    mode = settings.mode.lower()
    if (mode == 'production'):
        task_type_id = get_task_type_id("Run Analysis")
    elif (mode in ["development", "testing", "staging"]):
        task_type_id = get_task_type_id("Run Analysis - Test")
    else:
        raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")

    get_or_create_task_instance(name, config['user'], args, task_type_id, queue_name)


def test(name, config):
    queue_name = config['headnode_task_queue']
    args = {}
    task_type_id = get_task_type_id("Test task")

    get_or_create_task_instance(name, config['user'], args, task_type_id, queue_name)
