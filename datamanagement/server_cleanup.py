import sys
import os
import logging
import json
import click
from dbclients.tantalus import TantalusApi, DataError
from dbclients.basicclient import NotFoundError
from utils.constants import LOGGING_FORMAT
import pandas as pd


logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
logging.getLogger('azure.storage').setLevel(logging.ERROR)


@click.command()
@click.argument('storage_name')
@click.argument('dataset_type')
@click.option('--dataset_id', type=int)
@click.option('--tag_name')
@click.option('--check_remote')
@click.option('--dry_run', is_flag=True)
def main(
    storage_name,
    dataset_type,
    dataset_id=None,
    tag_name=None,
    check_remote=None,
    dry_run=False,
):
    logging.info('cleanup up storage {}'.format(storage_name))

    if check_remote:
        logging.info('checking remote {}'.format(check_remote))
    else:
        logging.warning('not checking remote')

    tantalus_api = TantalusApi()

    storage_client = tantalus_api.get_storage_client(storage_name)

    remote_client = None
    if check_remote is not None:
        remote_client = tantalus_api.get_storage_client(check_remote)

    if dataset_id is None and tag_name is None:
        raise ValueError('require either dataset id or tag name')

    if dataset_id is not None and tag_name is not None:
        raise ValueError('require exactly one of dataset id or tag name')

    if dataset_id is not None:
        logging.info('cleanup up dataset {}, {}'.format(dataset_id, dataset_type))
        datasets = tantalus_api.list(dataset_type, id=dataset_id)

    if tag_name is not None:
        logging.info('cleanup up tag {}'.format(tag_name))
        datasets = tantalus_api.list(dataset_type, tags__name=tag_name)

    total_data_size = 0
    file_num_count = 0

    for dataset in datasets:
        logging.info('checking dataset with id {}, name {}'.format(
            dataset['id'], dataset['name']))

        # Optionally skip datasets not present and intact on the remote storage
        if check_remote is not None:
            if not tantalus_api.is_dataset_on_storage(dataset['id'], 'sequencedataset', check_remote):
                logging.warning('not deleting dataset with id {}, not on remote storage '.format(
                    dataset['id'], check_remote))
                continue

            # For each file instance on the remote, check if it exists and has the correct size in tantalus
            remote_file_size_check = True
            for file_instance in tantalus_api.get_dataset_file_instances(dataset['id'], dataset_type, check_remote):
                try:
                    tantalus_api.check_file(file_instance)
                except DataError:
                    logging.exception('check file failed')
                    remote_file_size_check = False

            # Skip this dataset if any files failed
            if not remote_file_size_check:
                logging.warning("skipping dataset {} that failed check on {}".format(
                    dataset['id'], check_remote))
                continue

        # Check consistency with the removal storage
        file_size_check = True
        for file_instance in tantalus_api.get_dataset_file_instances(dataset['id'], dataset_type, storage_name):
            try:
                tantalus_api.check_file(file_instance)
            except DataError:
                logging.exception('check file failed')
                file_size_check = False

        # Skip this dataset if any files failed
        if not file_size_check:
            logging.warning("skipping dataset {} that failed check on {}".format(
                dataset['id'], storage_name))
            continue

        # Delete all files for this dataset
        for file_instance in tantalus_api.get_dataset_file_instances(dataset['id'], dataset_type, storage_name):
            if dry_run:
                logging.info("would delete file instance with id {}, filepath {}".format(
                    file_instance['id'], file_instance['filepath']))
            else:
                logging.info("deleting file instance with id {}, filepath {}".format(
                    file_instance['id'], file_instance['filepath']))
                tantalus_api.update("file_instance", id=file_instance['id'], is_deleted=True)
            total_data_size += file_instance['file_resource']['size']
            file_num_count += 1

    logging.info("deleted a total of {} files with size {} bytes".format(
        file_num_count, total_data_size))


if __name__ == "__main__":
    main()


