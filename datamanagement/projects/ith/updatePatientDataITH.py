# /Users/miyuen/anaconda2/bin/python
# Update patient info on Tantalus using ITH metadata; update is_reference & patient fields
import re
import requests
import sys
import pprint
import json
import pandas as pd
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import BasicAPIClient 

TANTALUS_API_URL = 'http://127.0.0.1:8000/api/'
#TANTALUS_API_URL = 'http://tantalus.bcgsc.ca/api/'
#Use ithMetaData.csv as test input
def update_patients(metadata_filename, last_known_patient_id):

	ithMetaData=pd.read_csv(metadata_filename)
	patients=ithMetaData["patient_id"]
	tantalus_api=TantalusApi()

	# subset data into individual patients. 
	for i in set(patients):
		df=ithMetaData.loc[ithMetaData["patient_id"]==i,]
		sample_ids=df["dg_id"]
		# force patient_id to follow the prefix of normal
		for sample_id in sample_ids:
			if re.search("N$", sample_id):
				patient_id=sample_id[:-1]
			else:
				continue

		for sample_id in sample_ids:
			dataset=list(tantalus_api.list("sample", sample_id=sample_id))
			sample_id_id=dataset[0]["id"]
			#Update patient field
			update_patient_id(patient_id, sample_id_id, last_known_patient_id)
			#update is_reference field
			update_is_reference(sample_id, sample_id_id)
		
	#TEST
	print("done")

def update_patient_id(patient_id, sample_id_id, last_known_patient_id):
	tantalus_api=TantalusApi()
	patient=list(tantalus_api.list("patients", patient_id=patient_id))

	if len(patient)==0:
		#make new patient
		last_known_patient_id=last_known_patient_id+1
		tantalus_api.create("patients", id=last_known_patient_id,patient_id=patient_id, reference_id=None, external_patient_id=None, case_id=None)
		tantalus_api.update("sample", id=sample_id_id, patient=last_known_patient_id)
	else: 
		#add patient
		#print(patient)
		add_id=patient[0]["id"]
		tantalus_api.update("sample", id=sample_id_id, patient=add_id)

		
def update_is_reference(sample_id, sample_id_id):
	tantalus_api=TantalusApi()
	dataset=list(tantalus_api.list("sample", sample_id=sample_id))

	if re.search("N$", sample_id):
		tantalus_api.update("sample", id=sample_id_id, is_reference=True)
	else:
		tantalus_api.update("sample", id=sample_id_id, is_reference=False)

if __name__=='__main__':
	# Check this value. Update if necessary
	last_known_patient_id=899
	update_patients(sys.argv[1], last_known_patient_id)
