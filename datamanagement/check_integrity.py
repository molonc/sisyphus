import sys
import logging
import click
from dbclients.tantalus import TantalusApi, DataCorruptionError
from dbclients.basicclient import NotFoundError
from utils.constants import LOGGING_FORMAT


logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
logging.getLogger('azure.storage').setLevel(logging.ERROR)


def get_dataset_file_instances(tantalus_api, storage_name, dataset_type, dataset_id=None, tag_name=None):
    if dataset_type is None:
        raise ValueError('require dataset type')

    if dataset_id is not None and tag_name is not None:
        raise ValueError('require at most one of dataset id or tag name')

    if dataset_id is not None:
        logging.info('check dataset {}, {}'.format(dataset_id, dataset_type))
        datasets = tantalus_api.list(dataset_type, id=dataset_id)

    elif tag_name is not None:
        logging.info('check tag {}'.format(tag_name))
        datasets = tantalus_api.list(dataset_type, tags__name=tag_name)

    else:
        logging.info('check all datasets of type {}'.format(dataset_type))
        datasets = tantalus_api.list(dataset_type)

    for dataset in datasets:
        logging.info('checking dataset with id {}, name {}'.format(
            dataset['id'], dataset['name']))

        for file_instance in tantalus_api.get_dataset_file_instances(dataset['id'], dataset_type, storage_name):
            yield file_instance


@click.command()
@click.argument('storage_name')
@click.option('--dataset_type')
@click.option('--dataset_id', type=int)
@click.option('--tag_name')
@click.option('--all_file_instances', is_flag=True)
@click.option('--dry_run', is_flag=True)
@click.option('--fix', is_flag=True)
def main(
    storage_name,
    dataset_type=None,
    dataset_id=None,
    tag_name=None,
    all_file_instances=False,
    dry_run=False,
    fix=False
):
    logging.info('checking integrity of storage {}'.format(storage_name))

    tantalus_api = TantalusApi()

    if all_file_instances:
        file_instances = tantalus_api.list('file_instance', storage__name=storage_name)

    else:
        file_instances = get_dataset_file_instances(
            tantalus_api, storage_name, dataset_type, dataset_id=dataset_id, tag_name=tag_name)

    for file_instance in file_instances:
        logging.warning('checking file instance {} with path {}'.format(
            file_instance['id'], file_instance['filepath']))

        file_corrupt = False
        try:
            tantalus_api.check_file(file_instance)
        except DataCorruptionError:
            file_corrupt = True
            logging.exception('check file failed')

        if file_corrupt and fix:
            logging.warning('updating file instance {} with path {}'.format(
                file_instance['id'], file_instance['filepath']))

            if not dry_run:
                tantalus_api.update_file(file_instance)


if __name__ == "__main__":
    main()

