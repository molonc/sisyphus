import os
import click
import sys
import logging
import dbclients.tantalus
from dbclients.tantalus import TantalusApi
from datamanagement.utils.constants import LOGGING_FORMAT


SC_WGS_FQ_TEMPLATE = os.path.join(
    "single_cell_indexing",
    "fastq",
    "{dlp_library_id}",
    "{flowcell_id}_{lane_number}",
    "{cell_sample_id}",
    "{cell_filename}",
)


def rename_fastqs(dataset_id, storage_name, dry_run=False):
    logging.info(f'dataset: {dataset_id}')
    logging.info(f'dry run: {dry_run}')

    tantalus_api = TantalusApi()
    storage_client = tantalus_api.get_storage_client(storage_name)

    dataset = tantalus_api.get('sequencedataset', id=dataset_id)

    file_instances = tantalus_api.get_dataset_file_instances(
        dataset['id'],
        'sequencedataset',
        storage_name,
    )

    for file_instance in file_instances:
        filename = file_instance['file_resource']['filename']

        if os.path.basename(filename) == 'metadata.yaml':
            continue

        assert len(dataset['sequence_lanes']) == 1

        parts = filename.split('/')
        assert parts[0] == 'single_cell_indexing'
        assert parts[1] == 'fastq'
        assert parts[3] == dataset['library']['library_id']
        assert parts[4].split('_')[0] == dataset['sequence_lanes'][0]['flowcell_id']
        assert parts[4].split('_')[1] == dataset['sequence_lanes'][0]['lane_number']
        assert parts[5].split('_')[0] == dataset['sample']['sample_id']
        assert parts[5].split('_')[1] == dataset['library']['library_id']

        new_filename = SC_WGS_FQ_TEMPLATE.format(
            dlp_library_id=dataset['library']['library_id'],
            flowcell_id=dataset['sequence_lanes'][0]['flowcell_id'],
            lane_number=dataset['sequence_lanes'][0]['lane_number'],
            cell_sample_id=dataset['sample']['sample_id'],
            cell_filename=parts[5],
        )

        logging.info(f'renaming {filename} to {new_filename} on {storage_name}')

        if not dry_run:
            if not storage_client.exists(new_filename):
                storage_client.copy(filename, new_filename, wait=True)
            tantalus_api.swap_file(file_instance, new_filename)
            storage_client.delete(filename)


@click.command()
@click.argument('storage_name')
@click.argument('dataset_id', type=int, nargs=-1)
@click.option('--dry_run', is_flag=True, default=False)
def rename_fastq_dataset(storage_name, dataset_id, dry_run=False):
    for id_ in dataset_id:
        rename_fastqs(id_, storage_name, dry_run=dry_run)


if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    rename_fastq_dataset()
