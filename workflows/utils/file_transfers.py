import sys
import os
import logging

import dbclients.tantalus
from dbclients.basicclient import NotFoundError

from workflows.utils import log_utils, saltant_utils
from workflows.utils.log_utils import sentinel

tantalus_api = dbclients.tantalus.TantalusApi()

log = logging.getLogger('sisyphus')


def transfer_files(jira, config, from_storage, to_storage, dataset_ids, results=False):
    if from_storage == to_storage:
        log.debug('No files transferred, to and from both {}'.format(from_storage))
        return

    tag_name = '{}_{}'.format(jira, from_storage)

    dataset_type = "resultsdataset_set" if results else "sequencedataset_set"
    data = {dataset_type: dataset_ids}

    try:
        dataset_tag = tantalus_api.get("tag", name=tag_name)
    except NotFoundError:
        dataset_tag = None

    if dataset_tag is not None:
        log.debug('found existing sequence dataset tag {}'.format(tag_name))

        if set(dataset_tag[dataset_type]) != set(dataset_ids):
            log.warning('{} previously {} now {}'.format(
                dataset_type, dataset_tag[dataset_type], dataset_ids))

            tantalus_api.tag(
                tag_name,
                **data)

            dataset_tag = tantalus_api.get("tag", name=tag_name)

    else:
        log.debug('creating sequence dataset tag {}'.format(tag_name))

        if results:
            data["sequencedataset_set"] = []
        else:
            data["resultsdataset_set"] = []

        create_data = dict(data)
        create_data['name'] = tag_name
        tantalus_api.create("tag", create_data, ['name'])

    # TODO: check if files already exist in the target storage before starting transfer
    saltant_utils.transfer_files(jira, config, tag_name, from_storage, to_storage)


