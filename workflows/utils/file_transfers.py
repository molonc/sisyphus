import sys
sys.path.append('..')
import os
import logging
from azure.storage.blob import BlockBlobService

import dbclients.tantalus
from dbclients.basicclient import NotFoundError

from utils import log_utils, saltant_utils
from log_utils import sentinel


tantalus_api = dbclients.tantalus.TantalusApi()

log = logging.getLogger('sisyphus')

AZURE_STORAGE_ACCOUNT = os.environ['AZURE_STORAGE_ACCOUNT']
AZURE_STORAGE_KEY = os.environ['AZURE_STORAGE_KEY']


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

    try:
        dataset_tag = tantalus_api.get('sequence_dataset_tag', name=tag_name)
    except NotFoundError:
        dataset_tag = None

    if dataset_tag is not None:
        log.debug('found existing sequence dataset tag {}'.format(tag_name))
        if set(dataset_tag['sequencedataset_set']) != set(dataset_ids):
            log.warning('sequencedataset_set previously {} now {}'.format(
                dataset_tag['sequencedataset_set'], dataset_ids))

            tantalus_api.update(
                'sequence_dataset_tag',
                id=dataset_tag['id'],
                sequencedataset_set=dataset_ids)

            dataset_tag = tantalus_api.get('sequence_dataset_tag', name=tag_name)

    else:
        log.debug('creating sequence dataset tag {}'.format(tag_name))
        tantalus_api.create('sequence_dataset_tag', name=tag_name, sequencedataset_set=dataset_ids)


    # TODO: check if files already exist in the target storage before starting transfer
    saltant_utils.transfer_files(jira, config, tag_name, from_storage, to_storage)
    

