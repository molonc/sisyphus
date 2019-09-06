import sys
import logging
import click
from dbclients.tantalus import TantalusApi
from datamanagement.utils.constants import LOGGING_FORMAT
from datamanagement.transfer_files import transfer_dataset


@click.command()
@click.argument('results_type')
@click.argument('from_storage_name')
@click.argument('to_storage_name')
def download_datasets(results_type, from_storage_name, to_storage_name):
    ''' Download a set of datasets by type.
    '''

    tantalus_api = TantalusApi()

    dataset_ids = list()
    for dataset in tantalus_api.list('results', results_type=results_type):
        dataset_ids.append(dataset['id'])

    # Download most recent first
    dataset_ids = reversed(sorted(dataset_ids))

    for dataset_id in dataset_ids:
        try:
            transfer_dataset(tantalus_api, dataset_id, 'resultsdataset', from_storage_name, to_storage_name)
        except:
            logging.exception(f'failed to download {dataset_id}')


if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    download_datasets()
