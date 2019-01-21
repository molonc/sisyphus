import sys
import os
import logging
import json
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError
import pandas as pd
from sets import Set


tags_to_keep = [
    'SC-1635'
    'SC-1293',
    'SC-1294',
    'shahlab_pdx_bams_to_keep',
]


if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.WARNING)

    tantalus_api = TantalusApi()

    file_instances_to_delete = []

    datasets_to_keep = set()

    for tag_name in tags_to_keep:
        datasets_to_keep.update(tantalus_api.get('tag', name=tag_name)['sequencedataset_set'])

    logging.warning('Keeping {} datasets'.format(len(datasets_to_keep)))

    blob_storage = tantalus_api.get_storage_client('singlecellblob')
    shahlab_storage = tantalus_api.get_storage_client('shahlab')

    all_bam_files = tantalus_api.list('sequence_dataset', dataset_type='BAM', library__library_type__name='WGS')

    total_data_size = 0
    file_num_count = 0

    for dataset in all_bam_files:
        is_on_blob = tantalus_api.is_sequence_dataset_on_storage(dataset, 'singlecellblob')
        is_on_shahlab = tantalus_api.is_sequence_dataset_on_storage(dataset, 'shahlab')

        if not is_on_shahlab:
            continue

        if not is_on_blob:
            logging.warning("Dataset {} has no file instances stored in blob. Skipping...".format(dataset['name']))
            continue

        if dataset['id'] in datasets_to_keep:
            logging.warning("Dataset {} is required. Skipping...".format(dataset['name']))
            continue

        file_size_check = True
        for file_instance in tantalus_api.get_sequence_dataset_file_instances(dataset, 'singlecellblob'):
            if not blob_storage.exists(file_instance['file_resource']['filename']):
                logging.warning("File {} doesnt exist on blob".format(file_instance['filepath']))
                file_size_check = False
                continue
            if blob_storage.get_size(file_instance['file_resource']['filename']) != file_instance['file_resource']['size']:
                logging.warning("File {} has a different size in blob. Skipping...".format(file_instance['filepath']))
                file_size_check = False
                continue

        for file_instance in tantalus_api.get_sequence_dataset_file_instances(dataset, 'shahlab'):
            if not shahlab_storage.exists(file_instance['file_resource']['filename']):
                logging.warning("File {} doesnt exist on shahlab".format(file_instance['filepath']))
                file_size_check = False
                continue
            if shahlab_storage.get_size(file_instance['file_resource']['filename']) != file_instance['file_resource']['size']:
                logging.warning("File {} has a different size in shahlab. Skipping...".format(file_instance['filepath']))
                file_size_check = False
                continue

        if not file_size_check:
            logging.warning("Dataset {} failed file size check in blob. Skipping...".format(dataset['name']))
            continue

        for file_instance in tantalus_api.get_sequence_dataset_file_instances(dataset, 'shahlab'):
            file_instances_to_delete.append(file_instance)
            total_data_size += file_instance['file_resource']['size']
            file_num_count += 1

    logging.warning("Total size of the {} files is {} bytes".format(
        file_num_count, total_data_size))

    with open("file_paths.txt", "w") as f:
        for file_instance in file_instances_to_delete:
            f.write(file_instance['filepath'] +'\n')

    for file_instance in file_instances_to_delete:
        tantalus_api.update("file_instance", id=file_instance['id'], is_deleted=True)


