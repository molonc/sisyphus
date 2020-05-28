import yaml
import os
import json
import click
import logging
import collections
import io

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from workflows import generate_inputs
import dbclients.tantalus
from datamanagement.utils.constants import LOGGING_FORMAT


DATASET_TYPE = 'dlpfastqs'
DATASET_VERSION = 'v.0.0.1'


def create_lane_fastq_metadata(tantalus_api, dataset_id):
    """
    Get meatadata per lane of sequencing for a given dataset.
    """
    colossus_api = ColossusApi()

    dataset = tantalus_api.get( "sequencedataset", id=dataset_id)
    library_id = dataset['library']['library_id']
    sample_id = dataset['sample']['sample_id']
    assert len(dataset['sequence_lanes']) == 1
    flowcell_id = dataset['sequence_lanes'][0]['flowcell_id']
    lane_number = dataset['sequence_lanes'][0]['lane_number']

    sample_info = generate_inputs.generate_sample_info(library_id)
    index_sequence_cell_id = sample_info.set_index('index_sequence')['cell_id'].to_dict()

    metadata = {'files': {}, 'meta': {}}

    metadata['meta']['type'] = DATASET_TYPE
    metadata['meta']['version'] = DATASET_VERSION

    metadata['meta']['sample_id'] = sample_id
    metadata['meta']['library_id'] = library_id

    base_dirs = set()
    cell_ids = set()

    file_resources = list(tantalus_api.list('file_resource', sequencedataset__id=dataset['id']))

    for file_resource in file_resources:
        filename = os.path.basename(file_resource['filename'])
        dirname = os.path.dirname(file_resource['filename'])

        if filename.endswith('metadata.yaml'):
            continue

        index_sequence = file_resource['sequencefileinfo']['index_sequence']
        cell_id = index_sequence_cell_id[index_sequence]
        read_end = file_resource['sequencefileinfo']['read_end']

        if filename in metadata['files']:
            raise ValueError(f'duplicate filename {filename}')

        metadata['files'][filename] = {
            'cell_id': cell_id,
            'read_end': read_end,
            'flowcell_id': flowcell_id,
            'lane_number': lane_number,
        }

        base_dirs.add(dirname)
        cell_ids.add(cell_id)

    if len(base_dirs) != 1:
        raise ValueError(f'found files in zero or multiple directories {base_dirs}')

    assert not sample_info['cell_id'].duplicated().any()

    metadata['meta']['cells'] = {}
    for idx, row in sample_info.iterrows():
        cell_id = row['cell_id']
        
        if cell_id not in cell_ids:
            continue

        metadata['meta']['cells'][cell_id] = {
            'library_id': row['library_id'],
            'sample_id': row['sample_id'],
            'pick_met': row['pick_met'],
            'condition': row['condition'],
            'sample_type': row['sample_type'],
            'img_col': row['img_col'],
            'row': row['row'],
            'column': row['column'],
            'primer_i5': row['primer_i5'],
            'index_i5': row['index_i5'],
            'primer_i7': row['primer_i7'],
            'index_i7': row['index_i7'],
            'index_sequence': row['index_sequence'],
        }

    metadata['meta']['lanes'] = {
        flowcell_id: {
            lane_number: {
                'sequencing_centre': dataset['sequence_lanes'][0]['sequencing_centre'],
                'sequencing_instrument': dataset['sequence_lanes'][0]['sequencing_instrument'],
                'sequencing_library_id': dataset['sequence_lanes'][0]['sequencing_library_id'],
                'read_type': dataset['sequence_lanes'][0]['read_type'],
            }
        }
    }

    return metadata, base_dirs.pop()


def add_fastq_metadata_yaml(dataset_id, storage_name, dry_run=False):
    """
    Create a metadata.yaml file for a dataset and add to tantalus.
    """
    tantalus_api = TantalusApi()

    storage = tantalus_api.get_storage(storage_name)
    client = tantalus_api.get_storage_client(storage_name)

    metadata, base_dir = create_lane_fastq_metadata(tantalus_api, dataset_id)

    metadata_filename = os.path.join(base_dir, 'metadata.yaml')
    metadata_filepath = tantalus_api.get_filepath(storage_name, metadata_filename)

    metadata_io = io.BytesIO()
    metadata_io.write(yaml.dump(metadata, default_flow_style=False).encode())

    logging.info(f'writing metadata to file {metadata_filepath}')
    client.write_data(metadata_filename, metadata_io)

    logging.info(f'adding {metadata_filepath} to tantalus')

    if not dry_run:
        file_resource, file_instance = tantalus_api.add_file(storage_name, metadata_filepath, update=True)

        dataset = tantalus_api.get('sequencedataset', id=dataset_id)

        new_file_resources = set(dataset['file_resources'])
        new_file_resources.add(file_resource['id'])

        tantalus_api.update('sequencedataset', id=dataset_id, file_resources=list(new_file_resources))


@click.command()
@click.argument('storage_name')
@click.argument('dataset_id', type=int, nargs=-1)
@click.option('--dry_run', is_flag=True, default=False)
def add_fastq_metadata_yamls(storage_name, dataset_id, dry_run=False):
    for id_ in dataset_id:
        add_fastq_metadata_yaml(id_, storage_name, dry_run=dry_run)


if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    add_fastq_metadata_yamls()
