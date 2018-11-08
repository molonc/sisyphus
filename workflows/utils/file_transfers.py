import sys
sys.path.append('..')
import os
import logging

import dbclients.tantalus
from dbclients.basicclient import NotFoundError

from utils import log_utils, saltant_utils
from log_utils import sentinel

tantalus_api = dbclients.tantalus.TantalusApi()

log = logging.getLogger('sisyphus')


def transfer_files(jira, config, from_storage, to_storage, dataset_ids):
    if from_storage == to_storage:
        log.debug('No files transferred, to and from both {}'.format(from_storage))
        return

    tag_name = '{}_{}'.format(jira, from_storage)

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


