import sys
sys.path.append('..')
import os
import logging
from azure.storage.blob import BlockBlobService

from tantalus_client import tantalus

from utils import log_utils, saltant_utils
from log_utils import sentinel


log = logging.getLogger('sisyphus')

AZURE_STORAGE_ACCOUNT = os.environ['AZURE_STORAGE_ACCOUNT']
AZURE_STORAGE_KEY = os.environ['AZURE_STORAGE_KEY']


def archive_sftp(filepaths, results_dir, sftp_dir, user):
    """
    Transfer the results to the SFTP server on thost.

    Args:
        filepaths (list): paths to results files relative to the results directory
        results_dir (str)
        sftp_dir (str)
        user (str)
    """
    result_dirs = {os.path.dirname(os.path.join(sftp_dir, f)) for f in filepaths}

    for dirname in result_dirs:
        mkdir_cmd = [
            'ssh',
            '{user}@thost'.format(user=user),
            'mkdir',
            '-p',
            dirname,
        ]

        log_utils.sync_call('Making {} on SFTP server'.format(dirname), mkdir_cmd)

    for filepath in filepaths:
        out_file = os.path.join(sftp_dir, filepath)
        out_path = '{user}@thost:{out_file}'.format(user=user, out_file=out_file)
        rsync_cmd = [
            'rsync',
            '-uvP',
            os.path.join(results_dir, filepath),
            out_path,
        ]
        log_utils.sync_call('Archiving {} to SFTP server'.format(filepath), rsync_cmd)


def archive_blob(filepaths, results_dir, blob_dir, container_name):
    """
    Transfer results to singlecelldata on Azure.

    Args:
        filepaths (list): path to results files relative to the results directory
        results_dir (str)
        blob_dir (str)
        user (str)
    """
    blob_service = BlockBlobService(
        account_name=AZURE_STORAGE_ACCOUNT,
        account_key=AZURE_STORAGE_KEY,
    )

    for filepath in filepaths:
        filepath = os.path.basename(filepath)

        from_storage_file = os.path.join(results_dir, filepath)
        blob_name = os.path.join(blob_dir, filepath)

        log.debug('Uploading {} to {}'.format(filepath, blob_name))
        blob_service.create_blob_from_path(container_name, blob_name, from_storage_file)


def transfer_files(jira, config, from_storage, to_storage, dataset_ids, results=False):
    if from_storage == to_storage:
        log.debug('No files transferred, to and from both {}'.format(from_storage))
        return

    tag_name = '{}_{}'.format(jira, from_storage)
    if results:
        tag_name += '_results'

    sentinel(
        'Tagging {} files'.format(from_storage),
        tantalus.tag_datasets,
        dataset_ids,
        tag_name,
    )

    sentinel(
        'Transferring files from {} to {}'.format(from_storage, to_storage),
        saltant_utils.transfer_files,
        jira,
        config,
        tag_name,
        from_storage,
        to_storage,
    )
