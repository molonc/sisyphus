import sys
import os
import json
from dbclients.tantalus import TantalusApi
from datamanagement.utils.runtime_args import parse_runtime_args
from sets import Set


def check_files_for_given_dataset(dataset, library_id_set):
	print("Checking Dataset {}".format(dataset['id']))
	temp = tantalus_api.get_dataset_file_instances(dataset['id'], 'sequencedataset', 'shahlab')
	for file_instance in temp:
		storage_client = tantalus_api.get_storage_client(file_instance['storage']['name'])

		if(storage_client.exists(file_instance['file_resource']['filename'])):
			if(storage_client.get_size(file_instance['file_resource']['filename']) != file_instance['file_resource']['size']):
				library_id_set.add(dataset['library']['library_id'])
				print("File Instance ID {}, part of File Resource ID {}, associated with Sequence Dataset ID {}, does not match file size. Deleting...".format(
					file_instance['id'], file_instance['file_resource']['id'], dataset['id']))
				tantalus_api.delete('file_instance', file_instance['id'])
		else:
			library_id_set.add(dataset['library']['library_id'])
			print("File Instance ID {}, part of File Resource ID {}, associated with Sequence Dataset ID {}, does not exist in Shahlab. Deleting...".format(
				file_instance['id'], file_instance['file_resource']['id'], dataset['id']))
			tantalus_api.delete('file_instance', file_instance['id'])

		file_resource = tantalus_api.get("file_resource", id=file_instance['file_resource']['id'])
		#Checks whether the file instance was the last one associated with it's file resource
		if(len(file_resource['file_instances']) == 0):
			tantalus_api.delete("file_resource", file_resource['id'])


	'''for file_id in dataset['file_resources']:
		file_resource = tantalus_api.get('file_resource', id=file_id)
		storage_client = tantalus_api.get_storage_client(file_resource['file_instances'][0]['storage']['name'])
		if(storage_client.exists(file_resource['filename'])):
			if(storage_client.get_size(file_resource['filename']) != file_resource['size']):
				library_id_set.add(dataset['library']['library_id'])
				print("File Resource with ID {} associated with Dataset ID {} with the specified path of {} in storage directory {} does not match file sizes".format(
					file_resource['id'], dataset['id'], file_resource['filename'], storage_client.storage_directory))
		else:
			library_id_set.add(dataset['library']['library_id'])
			print("File Resource with ID {} associated with Dataset ID {} does not exist with the specified path of {} in storage directory {}".format(
					file_resource['id'], dataset['id'], file_resource['filename'], storage_client.storage_directory))'''

#Accepts args 'library_id' and 'id' for library id and sequence dataset id
if __name__ == '__main__':
	library_id_set = Set()
	tantalus_api = TantalusApi()
	args = parse_runtime_args()
	dataset_set_list = []
	multiple_libraries = False

	if('file_path' in args.keys()):
		multiple_libraries = True
		with open(args['file_path'], 'r') as f:
			for line in f:
				dataset_set_list.append(tantalus_api.list('sequence_dataset', library__library_id=line.strip()))
	elif('library_id' in args.keys() and 'id' in args.keys()):
		dataset_set = tantalus_api.list('sequence_dataset', library__library_id=args['library_id'], id=int(args['id']))
	elif('library_id' in args.keys() and 'id' not in args.keys()):
		dataset_set = tantalus_api.list('sequence_dataset', library__library_id=args['library_id'])
	elif('library_id' not in args.keys() and 'id' in args.keys()):
		dataset_set = tantalus_api.list('sequence_dataset', id=int(args['id']))
	else:
		print("ERROR: Invalid search parameters provided")
		quit()


	if(multiple_libraries):
		for dataset_set in dataset_set_list:
			for dataset in dataset_set:
				check_files_for_given_dataset(dataset, library_id_set)
				updated_dataset = tantalus_api.get('sequence_dataset', id=dataset['id'])
				if(len(updated_dataset['file_resources']) == 0):
					print("Deleting Sequence_Dataset {}".format(updated_dataset['id']))
					tantalus_api.delete('sequence_dataset', id=updated_dataset['id'])

	else:
		for dataset in dataset_set:
			check_files_for_given_dataset(dataset, library_id_set)
			updated_dataset = tantalus_api.get('sequence_dataset', id=dataset['id'])
			if(len(updated_dataset['file_resources']) == 0):
				tantalus_api.delete('sequence_dataset', id=updated_dataset['id'])

	for id in library_id_set:
		print(id)

