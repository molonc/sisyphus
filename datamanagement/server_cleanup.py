import sys
import os
import logging
import json
from dbclients.tantalus import TantalusApi, FileInstanceNotFoundException
from datamanagement.utils.runtime_args import parse_runtime_args
import pandas as pd
from sets import Set

if __name__ == "__main__":
    logging.basicConfig(format='%(levelname)s: %(message)s', level=logging.INFO)
    tantalus_api = TantalusApi()
    args = parse_runtime_args()
    files_to_delete = []
    shahlab_prefix = "/shahlab/archive/"

    brittany_files_to_keep_df = pd.read_csv("WGS Inventory - Analysis.csv")
    data_to_keep_list = brittany_files_to_keep_df['Shahlab Filepath.1'].tolist()
    data_to_keep_list.extend(brittany_files_to_keep_df['Shahlab Filepath'].tolist())


    daniel_files_to_keep_df = pd.read_csv("sample_info.csv")
    data_to_keep_list.extend(daniel_files_to_keep_df['ABSOLUTE_FILE_PATH'].tolist())

    data_to_keep_set = Set(data_to_keep_list)

    blob_storage = tantalus_api.get_storage_client('singlecellblob')
    shahlab_storage = tantalus_api.get_storage_client('shahlab')

    all_bam_files = tantalus_api.list('sequence_dataset', dataset_type="BAM")
    total_data_size = 0
    file_num_count = 0
    #Currently Tantalus doesn't have the library_type implemented, so have to filter via iteration
    #Will change once Andrew implements the new filter
    for dataset in all_bam_files:
        brittany_needs_file = False
        if(dataset['library']['library_type'] == "WGS"):
            try:
                logging.info("Getting blob storage for Dataset {}".format(dataset['id']))
                file_instances_blob = tantalus_api.get_sequence_dataset_file_instances(dataset, 'singlecellblob')
                logging.info("Got blob storage for Dataset {}".format(dataset['id']))
            except FileInstanceNotFoundException:
                logging.info("Dataset {} has no file instances stored in blob. Skipping...".format(dataset['id']))
                continue
            for file_instance in file_instances_blob:
                if(file_instance['filepath'] in data_to_keep_set):
                    logging.info("Can't delete. Brittany/Daniel needs this file")
                    brittany_needs_file = True
                    break
            if(brittany_needs_file):
                continue
            for file_instance in file_instances_blob:
                try:
                    shahlab_file_instance = tantalus_api.get_file_instance(file_instance['file_resource'], 'shahlab')
                except FileInstanceNotFoundException:
                    logging.info("File Resource {} doesn't exist in Shahlab".format(file_instance['file_resource']['id']))
                    continue
                if(shahlab_storage.exists(file_instance['file_resource']['filename'])):
                    files_to_delete.append(shahlab_file_instance['filepath'])
                    #shahlab_storage.delete(file_instance['file_resource']['filename'])
                    total_data_size += file_instance['file_resource']['size']
                    file_num_count += 1
                    logging.info("Total size of the {} files has been recorded: {} bytes; {} kilobytes; {} megabytes; {} gigabytes; {} terabytes".format(
                        file_num_count,
                        total_data_size, 
                        total_data_size/1024,
                        total_data_size/(1024*1024),
                        total_data_size/(1024*1024*1024),
                        total_data_size/(1024*1024*1024*1024)))
                else:
                    logging.info("Tantalus says file exists but it doesn't...")
                #tantalus_api.delete("file_instance", file_instance['id'])

    with open("file_paths.txt", "w") as f:
        for path in files_to_delete:
            f.write(path +'\n')

    '''data_to_check_list = []
    shahlab_prefix = "/shahlab/archive"

    blob_storage = tantalus_api.get_storage_client('singlecellblob')
    shahlab_storage = tantalus_api.get_storage_client('shahlab')

    files_to_keep_df = pd.read_csv("WGS Inventory - Analysis.csv")

    data_to_keep_list = files_to_keep_df['Shahlab Filepath.1'].tolist()
    data_to_keep_list.extend(files_to_keep_df['Shahlab Filepath'].tolist())
    data_to_keep_set = Set(data_to_keep_list)

    #Change in future to accept command line args for different dataset types
    all_bam_files = tantalus_api.list('sequence_dataset', dataset_type="BAM")
    total_data_size = 0
    file_num_count = 0
    #Currently Tantalus doesn't have the library_type implemented, so have to filter via iteration
    #Will change once Andrew implements the new filter
    for dataset in all_bam_files:
        brittany_needs_file = False
        if(dataset['library']["library_type"] == "SC_WGS" or dataset['library']['library_type'] == "WGS"):
            try:
                logging.info("Getting blob storage for Dataset {}".format(dataset['id']))
                file_instances_blob = tantalus_api.get_sequence_dataset_file_instances(dataset, 'singlecellblob')
                logging.info("Got blob storage for Dataset {}".format(dataset['id']))
            except FileInstanceNotFoundException:
                logging.info("Dataset {} has no file instances stored in blob. Skipping...".format(dataset['id']))
                continue
            for file_instance in file_instances_blob:
                if(shahlab_prefix + file_instance['file_resource']['filename'] in data_to_keep_set):
                    logging.info("Can't delete. Brittany needs this file")
                    brittany_needs_file = True
                    break
            for file_instance in file_instances_blob:
                if not brittany_needs_file:
                    try:
                        tantalus_api.get_file_instance(file_instance['file_resource'], 'shahlab')
                        total_data_size += file_instance['file_resource']['size']
                        file_num_count += 1
                        logging.info("Total size of the {} files that can be deleted: {} bytes; {} kilobytes; {} megabytes; {} gigabytes; {} terabytes".format(
                            file_num_count,
                            total_data_size, 
                            total_data_size/1024,
                            total_data_size/(1024*1024),
                            total_data_size/(1024*1024*1024),
                            total_data_size/(1024*1024*1024*1024)))
                    except FileInstanceNotFoundException:
                        logging.info("File Resource {} doesn't exist in Shahlab".format(file_instance['file_resource']['id']))
                        continue
                else:
                    break

        


    print(total_data_size)'''

