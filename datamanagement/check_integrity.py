import os
import sys
import json
from dbclients.tantalus import TantalusApi
from datamanagement.utils.runtime_args import parse_runtime_args
from sets import Set
import click

@click.command()
@click.option("--delete", is_flag=True, help='Automatically delete all incorrect file instances')
@click.option("--all", is_flag=True, help='Run across all storage')
@click.option('--filepath', type=str)
@click.option('--libraryid', type=str)
@click.option('--id', type=str)
def init_check(delete, all, filepath, libraryid, id):

	multiple_libraries = False

	if all:
		print "Entered all mode"
		storage_type = click.prompt('Please enter the name of the storage')
		all_file_instances = tantalus_api.list("file_instance", storage__name=storage_type)
		for file_instance in all_file_instances:
			print "checking file instance of id:" + str(file_instance["id"]) + "on " + storage_type
			storage_client = tantalus_api.get_storage_client(file_instance['storage']['name'])
			if (storage_client.exists(tantalus_api.get("file_resource", id=file_instance['file_resource'])['filename'])):
				print "File Instance exist"
				if (storage_client.get_size(tantalus_api.get("file_resource", id=file_instance['file_resource'])['filename']) !=
						tantalus_api.get("file_resource",id=file_instance['file_resource'])['size']):
					if delete:
						print "Size Check Failed, Deleted"
						tantalus_api.delete('file_instance', file_instance['id'])
					else:
						print "Size Check Failed"
				else:
					print "Size Check Passed"
			else:
				if delete:
					print "Exist Check Failed, Deleted"
					tantalus_api.delete('file_instance', file_instance['id'])
				else:
					print "Exist Check Failed"
			file_resource = tantalus_api.get("file_resource", id=file_instance['file_resource'])
			# Checks whether the file instance was the last one associated with it's file resource
			if (len(file_resource['file_instances']) == 0):
				if delete:
					print "Deleted file resources"
					tantalus_api.delete("file_resource", file_resource['id'])
				else:
					pass


	else:
		if filepath:
			multiple_libraries = True
			with open(filepath, 'r') as f:
				for line in f:
					dataset_set_list.append(tantalus_api.list('sequence_dataset', library__library_id=line.strip()))
		elif(libraryid and id):
			dataset_set = tantalus_api.list('sequence_dataset', library__library_id=libraryid, id=int(id))
		elif(libraryid and not id):
			dataset_set = tantalus_api.list('sequence_dataset', library__library_id=libraryid)
		elif(not libraryid and id):
			dataset_set = tantalus_api.list('sequence_dataset', id=int(id))
		else:
			print("ERROR: Invalid search parameters provided")
			quit()


		if(multiple_libraries):
			for dataset_set in dataset_set_list:
				for dataset in dataset_set:
					check_files_for_given_dataset(dataset, library_id_set, delete)
					updated_dataset = tantalus_api.get('sequence_dataset', id=dataset['id'])
					if(len(updated_dataset['file_resources']) == 0):
						if delete:
							print("Deleting Sequence_Dataset {}".format(updated_dataset['id']))
							tantalus_api.delete('sequence_dataset', id=updated_dataset['id'])
						else:
							pass

		else:
			for dataset in dataset_set:
				check_files_for_given_dataset(dataset, library_id_set, delete)
				updated_dataset = tantalus_api.get('sequence_dataset', id=dataset['id'])
				if(len(updated_dataset['file_resources']) == 0):
					tantalus_api.delete('sequence_dataset', id=updated_dataset['id'])

		for id in library_id_set:
			print(id)



def check_files_for_given_dataset(dataset, library_id_set, delete):
	temp = tantalus_api.get_sequence_dataset_file_instances(dataset, 'shahlab')

	for file_instance in temp:
		print "checking file instance of id:" + str(file_instance["id"])
		storage_client = tantalus_api.get_storage_client(file_instance['storage']['name'])

		if(storage_client.exists(file_instance['file_resource']['filename'])):
			print "Exist Check Passed"
			if(storage_client.get_size(file_instance['file_resource']['filename']) != file_instance['file_resource']['size']):
				library_id_set.add(dataset['library']['library_id'])
				if delete:
					print(
					"File Instance ID {}, part of File Resource ID {}, associated with Sequence Dataset ID {}, does not match file size. Deleting...".format(
						file_instance['id'], file_instance['file_resource']['id'], dataset['id']))
					tantalus_api.delete('file_instance', file_instance['id'])
				else:
					print "Size Check Failed"
			else:
				print "Size Check Passed"
		else:
			library_id_set.add(dataset['library']['library_id'])
			if delete:
				print(
				"File Instance ID {}, part of File Resource ID {}, associated with Sequence Dataset ID {}, does not exist in Shahlab. Deleting...".format(
					file_instance['id'], file_instance['file_resource']['id'], dataset['id']))
				tantalus_api.delete('file_instance', file_instance['id'])
			else:
				print "Exist Check Failed"
		file_resource = tantalus_api.get("file_resource", id=file_instance['file_resource']['id'])
		#Checks whether the file instance was the last one associated with it's file resource
		if(len(file_resource['file_instances']) == 0):
			if delete:
				print "File Resource Deleted"
				tantalus_api.delete("file_resource", file_resource['id'])
			else:
				pass

#Accepts args 'library_id' and 'id' for library id and sequence dataset id
if __name__ == '__main__':

	print("Usage Example : \n"
		  "   python check_integrity.py --OPTIONS \n\n"
		  "OPTIONS:\n"
		  "--delete  Automatically delete all incorrect file instances\n"
		  "--all  Run across all storage\n"
		  "--libraryid  library id\n"
		  "--filepath  txt filepath of library ids\n"
		  "--id  sequence dataset id")

	library_id_set = Set()
	tantalus_api = TantalusApi()
	dataset_set_list = []

	init_check()
