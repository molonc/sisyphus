# /Users/miyuen/anaconda2/bin/python
# Download and store filepath of each sample in tsv, if equal number of seq lanes, pick largest file size

import requests
import sys
import pprint
import json
import pandas as pd
from dbclients.tantalus import TantalusApi

TANTALUS_API_URL = 'http://tantalus.bcgsc.ca/api/'
#Use ithProject.csv as test dataset
def find_ith_datasets(sample_ids_filename):
	max_sample_id = []
	max_complete_file_path = []

	sample_ids_df = pd.read_csv(sample_ids_filename)
	tantalus_api = TantalusApi()

	# filter api for sample_ids, WGS, HG19, is_compete
	for row in sample_ids_df.itertuples():	
	   	datasets = []
	   	file_resources = []
	   	#print(row)
	   	print("working on "+row.sample_id)
		datasets = list(tantalus_api.list("sequence_dataset", sample__sample_id=row.sample_id, library__library_type__name="WGS", reference_genome__name="HG19"))
		
		#find shahlab filepath for associated file resource number. Assumes there are always only 2 file resources
		#is_complete has to be manually filtered. API does not do auto filter
		if len(datasets)==1:
			best_filepath_thusfar = ""
			if datasets[0]["is_complete"]==True:
				good_data = datasets[0]
				fr_search = good_data["file_resources"][0]
				fr_result = list(tantalus_api.list("file_resource", id=fr_search)) 
				file_instance = fr_result[0]["file_instances"]
				storage = file_instance[0]["storage"]
				if storage["name"]=="shahlab":
					best_filepath_thusfar = file_instance[0]["filepath"]
				elif storage["name"]=="singlecellblob":
					best_filepath_thusfar = file_instance[0]["filepath"]
				elif storage["name"]=="ithdata":
					best_filepath_thusfar = file_instance[0]["filepath"]
				else:
					best_filepath_thusfar = "booo"

		else:
			max_size_thusfar = 0
			best_filepath_thusfar = ""
			for i in range(len(datasets)):			
				if datasets[i]["is_complete"]==True:	
					good_data = datasets[i]
					fr_search = good_data["file_resources"][0]
					fr_result = list(tantalus_api.list("file_resource", id=fr_search))
					max_size_thusfar = fr_result[0]["size"]
					file_instance = fr_result[0]["file_instances"]
					potential_file_path = file_instance[0]["filepath"]
					#filter by max file size
					if fr_result[0]["size"] >= max_size_thusfar:
						max_size_thusfar = fr_result[0]["size"]
						best_filepath_thusfar = potential_file_path
					else: 
						best_filepath_thusfar = "ahhh"

		max_sample_id.append(row.sample_id)
		max_complete_file_path.append(best_filepath_thusfar)
	
	# print out
	df = pd.DataFrame({"sample_id": max_sample_id, "filepath": max_complete_file_path, })
   	df.to_csv('ITH_shah_filepath.csv', index=False)
	
	#TEST
	print("done")

if __name__=='__main__':
	find_ith_datasets(sys.argv[1])
