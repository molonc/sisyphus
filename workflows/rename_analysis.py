from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import NotFoundError
from datamanagement.utils import utils
import logging
import hashlib
import os

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

def rename_all_dlp_analyses():
	reference_genome_map = {
	    'grch37': 	'HG19',
	    'mm10': 	'MM10',
	}
	aligner_map = {
		'A': 	'BWA_ALN_0_5_7',
		'M':	'BWA_MEM_0_7_6A',
	}
	analysis_types = ('align', 'hmmcopy')
	analyses = tantalus_api.list('analysis')

	for analysis in analyses:
		analysis_type = analysis['analysis_type']
		print('Analysis id {}'.format(analysis['id']))

		if analysis_type not in analysis_types:
			print('Analysis {} not align/hmmmcopy ; skipping rename \n \n'.format(analysis['id']))
			continue

		jira_ticket = analysis['jira_ticket']

		# Test ticket/library
		if jira_ticket == 'SC-9999':
			continue

		try:
			colossus_analysis = colossus_api.get('analysis_information', analysis_jira_ticket=jira_ticket)

		except NotFoundError:
			print(NotFoundError)
			continue

		library_id = colossus_analysis['library']['pool_id']
		print('Renaming {} analysis {} for library {} and jira ticket {}'.format(
			analysis_type, 
			analysis['id'], 
			library_id, 
			jira_ticket)
		)
		reference_genome = colossus_analysis['reference_genome']['reference_genome']
		reference_genome = reference_genome_map[reference_genome]
		
		aligner = colossus_analysis['aligner']
		aligner = aligner_map[aligner]

		lanes = set()
		if len(analysis['input_datasets']) > 0:
			print('Getting Lanes...')
			input_datasets = analysis['input_datasets'] 
			for input_dataset in input_datasets:
				dataset = tantalus_api.get('sequence_dataset', id=input_dataset)
				for sequence_lane in dataset['sequence_lanes']:
					lane = "{}_{}".format(sequence_lane['flowcell_id'], sequence_lane['lane_number'])
					lanes.add(lane)
		else:
		# 	raise Exception('No input datasets for analysis id {}'.format(analysis['id']))
			print('No input datasets for analysis id {} \n'.format(analysis['id']))
			continue

		# lanes_hashed = hash("".join(list(lanes)))
		lanes = ", ".join(sorted(lanes))
		lanes = hashlib.md5(lanes)
		lanes_hashed = "{}".format(lanes.hexdigest()[:8])

		new_analysis_name = "sc_{}_{}_{}_{}_{}".format(
		    analysis_type, 
		    aligner, 
		    reference_genome, 
		    library_id,
		    lanes_hashed,
		)
		print("New {} analysis name for analysis {}: {}".format(analysis_type, analysis['id'], new_analysis_name))

		# Updating analysis objects on Tantalus with new name 
		try:
			tantalus_api.update('analysis', 
				analysis['id'],
				name=new_analysis_name,
			)
			print("Updating name on analysis {} \n\n".format(analysis['id']))

		except Exception as e:
			print(str(e))
			print("Cannot update {} with name {}".format(analysis['id'], new_analysis_name)) 

			conflict_analysis = tantalus_api.get('analysis', name=new_analysis_name)
			conflict_jira = conflict_analysis['jira_ticket']

			conflict_colossus_analysis = colossus_api.get('analysis_information', analysis_jira_ticket=conflict_jira)

			old_analysis_name = "_".join([new_analysis_name, 'old'])

			if conflict_colossus_analysis['analysis_submission_date'] < colossus_analysis['analysis_submission_date']:
				old_analysis_name = "_".join([new_analysis_name, 'old'])

				tantalus_api.update('analysis', 
					conflict_analysis['id'],
					name=old_analysis_name
				)

				tantalus_api.update('analysis', 
					analysis['id'],
					name=new_analysis_name,
				)

				print('Updating analysis {} with old name and analysis {} with new name {} \n\n'.format(
					conflict_analysis['id'],
					analysis['id'],
					new_analysis_name
					)
				)

			else:
				tantalus_api.update('analysis', 
					analysis['id'],
					name=old_analysis_name
				)

				print('Updating analysis {} with old name \n\n'.format(analysis['id']))


if __name__ == "__main__":
	rename_all_dlp_analyses()




