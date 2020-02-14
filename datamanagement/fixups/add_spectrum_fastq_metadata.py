from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from workflows import generate_inputs
from datetime import datetime
import yaml
import os
import json
import dbclients.tantalus

# os.environ['TANTALUS_API_USERNAME'] =
# os.environ['TANTALUS_API_PASSWORD'] =
# os.environ['COLOSSUS_API_USERNAME'] =
# os.environ['COLOSSUS_API_PASSWORD'] =


# os.environ["CLIENT_ID"] =
# os.environ["SECRET_KEY"] =
# os.environ["TENANT_ID"] =
# os.environ['AZURE_KEYVAULT_ACCOUNT'] =


tantalus_api = TantalusApi()
colossus_api = ColossusApi()


def define_metadata_yaml():
    return {
        'filenames': [],
        'meta': {
            'type': '',
            'version': '',
            'cell_ids': set(),  # converted to list once populated
            'lane_ids': set(),  # converted to list once populated
            'sequencing_centre': '',
            'sequencing_instrument': '',
            'fastqs': {
                'template': '',
                'instances': []
            }
        }
    }


def construct_instance_info(index_sequence, sample_info, filename):
    instance_info = sample_info[sample_info['index_sequence'] == index_sequence]

    return {
        'sample_id':      instance_info['sample_id'].values[0],
        'library_id':     instance_info['library_id'].values[0],
        'cell_id':        instance_info['cell_id'].values[0],
        'read_end':       int(filename[filename.rfind('_') + 1:filename.find('.')]),
        'img_col':        int(instance_info['img_col'].values[0]),
        'index_i5':       instance_info['index_i5'].values[0],
        'index_i7':       instance_info['index_i7'].values[0],
        'pick_met':       instance_info['pick_met'].values[0],
        'primer_i5':      instance_info['primer_i5'].values[0],
        'primer_i7':      instance_info['primer_i7'].values[0],
        'index_sequence': instance_info['index_sequence'].values[0],
        'row':            int(instance_info['row'].values[0]),
        'column':         int(instance_info['column'].values[0]),
    }


def determine_fastq_template(filename, index_sequence):
    filename_templates = [
        '{sample_id}_{library_id}_{index_sequence}_{read_end}.fastq.gz',
        '{cell_id}_R{read_end}_001.fastq.gz'
    ]
    filename_parts = filename.split('_')

    if filename_parts[2] == index_sequence:
        return filename_templates[0]
    else:
        return filename_templates[1]


def construct_lane_info(lanes):
    """
    Creates dictionary of lane related info for metadata.
    FQ datasets should only have one lane.

    :param lanes:
    :return:
    """
    number_of_seq_lanes = len(lanes)
    assert number_of_seq_lanes == 1, f"expected 1 sequence lane, but got {number_of_seq_lanes}"

    lane_info = {'lane_ids': set(), 'sequencing_centre': '', 'sequencing_instrument': ''}
    centre_and_instrument_determined = False

    for lane in lanes:
        lane_info['lane_ids'].add(lane['flowcell_id'])
        if not centre_and_instrument_determined:
            lane_info['sequencing_centre']     = lane['sequencing_centre']
            lane_info['sequencing_instrument'] = lane['sequencing_instrument']
            centre_and_instrument_determined = True

    return lane_info


def create_fastq_metadata_yaml(library_id, storage_name, dry_run=False):
    """
    Create a metadata.yaml file for a all FQ datasets for a library id
    on blob storage.

    Assumes a dataset is its own directory.

    :param library_id:
    :return:
    """

    storage = tantalus_api.get_storage(storage_name)
    client = tantalus_api.get_storage_client(storage_name)

    datasets = tantalus_api.list("sequencedataset", dataset_type='FQ', library__library_id=library_id)
    for dataset in datasets:
        metadata = create_fastq_dataset_metadata_yaml(dataset)

        # TODO: 
        metadata_save_path = ''

        if dry_run:
            print(f"For dataset id '{dataset['id']}', the following metadata.yaml would have been prodcued:")
            print(metadata)
            continue

        metadata_as_bytes = yaml.dump(metadata, default_flow_style=False, sort_keys=False).encode()
        metadata_save_path = os.path.join(metadata_save_path, 'metadata.yaml')
        client.blob_service.create_blob_from_bytes(storage['storage_container'], metadata_save_path, metadata_as_bytes)


def create_fastq_dataset_metadata_yaml(dataset):
    """
    Create metadata from a dataset
    """
    library_id = dataset['library']['library_id']

    sample_info = generate_inputs.generate_sample_info(library_id)
    metadata = define_metadata_yaml()
    file_resources = tantalus_api.list('file_resource', sequencedataset__id=dataset['id'])

    for file_resource in file_resources:
        index_sequence = file_resource['sequencefileinfo']['index_sequence']
        filename = os.path.basename(file_resource['filename'])
        lane_info = construct_lane_info(dataset['sequence_lanes'])

        metadata['meta']['type'] = dataset['library']['library_type']
        metadata['meta']['version'] = 'v.0.0.1'  # hardcoded for now
        metadata['meta']['sequencing_centre'] = lane_info['sequencing_centre']
        metadata['meta']['sequencing_instrument'] = lane_info['sequencing_instrument']
        metadata['meta']['fastqs']['template'] = determine_fastq_template(filename, index_sequence)
        metadata['meta']['lane_ids'] = metadata['meta']['lane_ids'].union(lane_info['lane_ids'])
        instance_info = construct_instance_info(index_sequence, sample_info, filename)
        metadata['meta']['fastqs']['instances'].append(instance_info)

        #TODO: here you should check whether the template and instance info creates the relevant filename

        metadata['meta']['cell_ids'].add(instance_info['cell_id'])
        metadata['filenames'].append(filename)

        print(f"{library_id} - {dataset['id']} - {index_sequence} - {file_resource['filename']}")

    metadata['meta']['lane_ids'] = list(metadata['meta']['lane_ids'])
    metadata['meta']['cell_ids'] = list(metadata['meta']['cell_ids'])

    return metadata


if __name__ == "__main__":

    dataset = list(tantalus_api.list("sequencedataset", dataset_type='FQ', library__library_id='A96213A', sample__sample_id='SA1090'))[0]
    metadata = create_fastq_dataset_metadata_yaml(dataset)

    with open('test.yaml', 'w') as meta_yaml:
        yaml.safe_dump(metadata, meta_yaml, default_flow_style=False)

    spectrum_libs = ['A108851A',
                     'A108833A',
                     'A108832A',
                     'A108867A',
                     'A96185B',
                     'A96167A',
                     'A96167B',
                     'A96123A',
                     'A98245B',
                     'A98177A',
                     'A98179A',
                     'A98177B',
                     'A96121B',
                     'A96253']

    storage_name = 'singlecellblob'

    for library in spectrum_libs:
        create_fastq_metadata_yaml(library, storage_name)
        # create_fastq_metadata_yaml(library, storage_name, dry_run=True)

