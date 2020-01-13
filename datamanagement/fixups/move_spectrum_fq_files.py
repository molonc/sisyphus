from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.tantalus import BlobStorageClient
from workflows import generate_inputs
from datetime import datetime
import shutil
import os
import time


tantalus_api = TantalusApi()
colossus_api = ColossusApi()

# os.environ["CLIENT_ID"] =
# os.environ["SECRET_KEY"] =
# os.environ["TENANT_ID"] =
# os.environ['AZURE_KEYVAULT_ACCOUNT'] =


def construct_instance_info(index_sequence, sample_info, filename):
    instance_info = sample_info[sample_info['index_sequence'] == index_sequence]

    return {
        'sample_id':      instance_info['sample_id'].values[0],  # sample of cell, NOT primary sample
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


def get_new_fastq_filepath(sample_info, flowcell_id, lane_number, index_sequence, filename):
    instance_info = construct_instance_info(index_sequence, sample_info, filename)

    return os.path.join(
        'single_cell_indexing',
        'fastq',
        instance_info['library_id'],
        flowcell_id + '_' + lane_number,
        instance_info['sample_id'],
        filename
    )


def copy_blob(storage_name, from_path, to_path):
    storage = tantalus_api.get_storage(storage_name)
    blob_storage_client = tantalus_api.get_storage_client(storage_name)

    from_path = os.path.join(storage['prefix'], from_path)
    to_path = os.path.join(storage['prefix'], to_path)

    if blob_storage_client.exists(from_path):
        print(f"{datetime.now()}: copying blob from '{from_path}' to '{to_path}'... ", end='')
        from_url = blob_storage_client.get_url(from_path)
        copied_blob = blob_storage_client.blob_service.copy_blob(storage['storage_container'], to_path, from_url)

        while copied_blob.status != 'success':
            if copied_blob.status in ('aborted', 'failed'):
                print("aborted / failed.")
                return
            time.sleep(0.5)

        print("complete.")


def move_fq_files(library_id, storage_name):
    """
    For a given SPECTRUM library id & BLOB storage name, get all FQ datasets and copy files
    in every dataset such that all FQ files for a given dataset are
    in their own directory on a given storage.

    Destination directory template can be viewed in 'get_new_fastq_filepath'.

    :param library_id: SPECTRUM library id
    :param storage_name: name of storage
    :return:
    """

    sample_info = generate_inputs.generate_sample_info(library_id)
    datasets = tantalus_api.list("sequencedataset", dataset_type='FQ', library__library_id=library_id)
    for dataset in datasets:

        # make sure there's only one sequence lane
        number_of_seq_lanes = len(dataset['sequence_lanes'])
        assert number_of_seq_lanes == 1, f"expected 1 sequence lane, but got {number_of_seq_lanes}"

        flowcell_id = dataset['sequence_lanes'][0]['flowcell_id']
        lane_number = dataset['sequence_lanes'][0]['lane_number']
        updated_file_resources = dataset['file_resources']

        file_instances = tantalus_api.get_dataset_file_instances(dataset['id'], 'sequencedataset', storage_name)
        for file_instance in file_instances:

            index_sequence = file_instance['file_resource']['sequencefileinfo']['index_sequence']
            original_filepath = file_instance['file_resource']['filename']
            filename = os.path.basename(original_filepath)
            new_filepath = get_new_fastq_filepath(sample_info, flowcell_id, lane_number, index_sequence, filename)

            # copy blob file to new location (tantalus will take care of deletion)
            copy_blob(storage_name, original_filepath, new_filepath)

            # create new file resource + instance for newly copied file
            new_file_resource, new_file_instance = tantalus_api.add_file(file_instance['storage'], new_filepath)

            # add new resource id & remove old resource id
            updated_file_resources.append(new_file_resource['id'])
            updated_file_resources.remove(file_instance['file_resource']['id'])

            # update dataset to contain updated file resource
            # can really be done once after all file resources have been created, but repeating in case of failure
            # verify: does update append a file resource to exisitng resource, or takes a new list?
            tantalus_api.update('sequencedataset', dataset['id'], file_resources=updated_file_resources)

            # delete file resource, instances and remove from all datasets
            tantalus_api.delete_file(file_instance['file_resource'])


if __name__ == "__main__":

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

    for library_id in spectrum_libs:
        move_fq_files(library_id, 'singlecellblob')


