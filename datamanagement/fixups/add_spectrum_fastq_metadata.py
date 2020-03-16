import yaml
import os
import json
import click

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from workflows import generate_inputs
import dbclients.tantalus
from datamanagement.utils.constants import LOGGING_FORMAT


DATASET_TYPE = 'dlpfastqs'
DATASET_VERSION = 'v.0.0.1'


def create_lane_fastq_metadata(tantalus_api, library_id):
    """
    Get meatadata per lane of sequencing for a given library.
    """
    colossus_api = ColossusApi()

    sample_info = generate_inputs.generate_sample_info(library_id)
    index_sequence_cell_id = sample_info.set_index('index_sequence')['cell_id'].to_dict()

    datasets = list(tantalus_api.list(
        "sequencedataset", dataset_type='FQ', library__library_id=library_id))

    datasets_by_lane = collections.defaultdict(list)

    for dataset in datasets:
        assert len(dataset['sequence_lanes']) == 1
        flowcell_id = dataset['sequence_lanes'][0]['flowcell_id']
        lane_number = dataset['sequence_lanes'][0]['lane_number']
        datasets_by_lane[(flowcell_id, lane_number)].append(dataset)


    for (flowcell_id, lane_number), lane_datasets in datasets_by_lane.items():
        metadata = {'files': {}, 'meta': {}}

        metadata['meta']['type'] = DATASET_TYPE
        metadata['meta']['version'] = DATASET_VERSION

        dataset_ids = set()
        base_dirs = set()
        sequence_lane_ids = set()
        for dataset in lane_datasets:
            file_resources = list(tantalus_api.list('file_resource', sequencedataset__id=dataset['id']))

            dataset_ids.add(dataset['id'])
            sequence_lane_ids.add(dataset['sequence_lanes'][0]['id'])

            for file_resource in file_resources:
                filename = os.path.basename(file_resource['filename'])

                # Find common directory as subdirectory ending with flowcell/lane
                flowcell_lane = f'{flowcell_id}_{lane_number}'
                flowcell_idx = file_resource['filename'].index(flowcell_lane + '/')
                flowcell_idx += len(flowcell_lane)
                base_dir = file_resource['filename'][:flowcell_idx]
                filename = file_resource['filename'][flowcell_idx+1:]
                base_dirs.add(base_dir)

                index_sequence = file_resource['sequencefileinfo']['index_sequence']
                cell_id = index_sequence_cell_id[index_sequence]
                read_end = file_resource['sequencefileinfo']['read_end']

                if filename in metadata:
                    raise ValueError(f'duplicate filename {filename}')

                metadata['files'][filename] = {
                    'cell_id': cell_id,
                    'read_end': read_end,
                    'flowcell_id': flowcell_id,
                    'lane_number': lane_number,
                }

        if len(base_dirs) != 1:
            raise ValueError(f'found files in zero or multiple directories {base_dirs}')

        if len(sequence_lane_ids) != 1:
            raise ValueError(f'found zero or multiple lanes {sequence_lane_ids}')

        assert not sample_info['cell_id'].duplicated().any()

        metadata['meta']['cells'] = {}
        for idx, row in sample_info.iterrows():
            metadata['meta']['cells'][row['cell_id']] = {
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
                    'sequencing_centre': lane_datasets[0]['sequence_lanes'][0]['sequencing_centre'],
                    'sequencing_instrument': lane_datasets[0]['sequence_lanes'][0]['sequencing_instrument'],
                    'sequencing_library_id': lane_datasets[0]['sequence_lanes'][0]['sequencing_library_id'],
                    'read_type': lane_datasets[0]['sequence_lanes'][0]['read_type'],
                }
            }
        }

        dataset_info = {
            'dataset_ids': dataset_ids,
            'flowcell_id': flowcell_id,
            'lane_number': lane_number,
            'base_dir': list(base_dirs)[0],            
        }

        yield dataset_info, metadata


@click.command()
@click.argument('library_id')
@click.argument('storage_name')
@click.option('--dry_run', is_flag=True)
def create_fastq_metadata_yaml(library_id, storage_name, dry_run=False):
    """
    Create a metadata.yaml file for a all FQ datasets for a library id.
    """
    tantalus_api = TantalusApi()

    storage = tantalus_api.get_storage(storage_name)
    client = tantalus_api.get_storage_client(storage_name)

    for dataset_info, metadata in create_lane_fastq_metadata(tantalus_api, library_id):
        metadata_filename = os.path.join(dataset_info['base_dir'], 'metadata.yaml')
        metadata_filepath = tantalus_api.get_filepath('singlecellresults', metadata_filename)

        metadata_io = io.BytesIO()
        metadata_io.write(yaml.dump(metadata, default_flow_style=False).encode())

        logging.info(f'writing metadata to file {metadata_filepath}')
        client.write_data(metadata_filename, metadata_io)

        logging.info(f'adding {metadata_filepath} to tantalus')

        if not dry_run:
            file_resource, file_instance = tantalus_api.add_file('singlecellblob', metadata_filepath, update=True)

            for dataset_id in dataset_info['dataset_ids']:
                new_file_resources = set(results['file_resources'])
                new_file_resources.add(file_resource['id'])

                tantalus_api.update('sequencedataset', id=dataset_id, file_resources=list(new_file_resources))


if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    create_fastq_metadata_yaml()
