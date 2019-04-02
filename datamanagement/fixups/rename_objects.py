import os
import logging
import hashlib
import click

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import NotFoundError
from datamanagement.utils import utils

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

def rename_all_dlp_analyses():
	"""
	Rename of all dlp analysis in Tantalus to have form: 
		sc_<analysis_type>_<aligner>_<ref_genome>_<library_id>_<hashed_lanes>

	If duplicate analyses exists, rename older analysis with suffix '_old'
	"""

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

	analyses_to_delete = []

	for analysis in analyses:
		analysis_type = analysis['analysis_type']
		log.info('Analysis id {}'.format(analysis['id']))

		if analysis_type not in analysis_types:
			log.info('Analysis {} not align/hmmmcopy ; skipping rename \n \n'.format(analysis['id']))
			continue

		jira_ticket = analysis['jira_ticket']

		# Skip test ticket since test library id not in colossus
		if jira_ticket == 'SC-9999':
			continue

		try:
			colossus_analysis = colossus_api.get('analysis_information', analysis_jira_ticket=jira_ticket)

		except NotFoundError:
			log.info(NotFoundError)
			continue

		library_id = colossus_analysis['library']['pool_id']
		log.info('Renaming {} analysis {} for library {} and jira ticket {}'.format(
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
			log.info('Getting Lanes...')
			input_datasets = analysis['input_datasets'] 
			for input_dataset in input_datasets:
				dataset = tantalus_api.get('sequence_dataset', id=input_dataset)
				for sequence_lane in dataset['sequence_lanes']:
					lane = "{}_{}".format(sequence_lane['flowcell_id'], sequence_lane['lane_number'])
					lanes.add(lane)
		else:
			log.info('No input datasets for analysis id {}; jira ticket {}\n'.format(
				analysis['id'], 
				analysis["jira_ticket"])
			)
			continue

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

		if analysis["name"] == new_analysis_name:
			log.info("Analysis {} does not need renaming \n\n".format(analysis['id']))
			continue

		log.info("New {} analysis name for analysis {}: {}".format(
			analysis_type, 
			analysis['id'], 
			new_analysis_name)
		)

		# Updating analysis objects on Tantalus with new name 
		try:
			tantalus_api.update('analysis', 
				analysis['id'],
				name=new_analysis_name,
			)
			log.info("Updating name on analysis {} \n\n".format(analysis['id']))

		except Exception:
			log.info("Cannot update {} with name {}".format(analysis['id'], new_analysis_name)) 

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

				log.info('Updating analysis {} with old name and analysis {} with new name {} \n\n'.format(
					conflict_analysis['id'],
					analysis['id'],
					new_analysis_name
					)
				)

			else:
				try:
					log.info('Updating analysis {} with old name \n\n'.format(analysis['id']))
					tantalus_api.update('analysis', 
						analysis['id'],
						name=old_analysis_name
					)

				except:
					log.info("cannot update {} with old name {}; old analysis already exists".format(
						analysis['id'], 
						old_analysis_name)
					)


def rename_bam_datasets():
	"""
	Rename bam datasets to have form:
		BAM-<sample_id>_SC-WGS-<library_id>-lanes_<lanes_hashed>-<aligner>-<ref_genome>

	If duplicate bam dataset exists, rename older dataset with suffix '_old'
	"""

	bam_datasets = tantalus_api.list("sequencedataset", dataset_type="BAM", library__library_type__name="SC_WGS")

	for bam_dataset in bam_datasets:
		bam_dataset_name = bam_dataset["name"]

		library_id = bam_dataset["library"]["library_id"]
		sample_id = bam_dataset["sample"]["sample_id"]
		ref_genome = bam_dataset["reference_genome"]
		aligner = bam_dataset["aligner"]

		lanes = set()

		for sequence_lane in bam_dataset['sequence_lanes']:
			lane = "{}_{}".format(sequence_lane['flowcell_id'], sequence_lane['lane_number'])
			lanes.add(lane)

		lanes = ", ".join(sorted(lanes))
		lanes = hashlib.md5(lanes.encode('utf-8'))
		lanes_hashed = "{}".format(lanes.hexdigest()[:8])

		new_name = 	"BAM-{}-SC_WGS-{}-lanes_{}-{}-{}".format(
			sample_id,
			library_id,
			lanes_hashed,
			aligner,
			ref_genome,
		)
		if bam_dataset_name == new_name:
			log.info("Dataset {} does not need renaming \n\n".format(bam_dataset['id']))
			continue
		
		try:
			log.info("sequence dataset has name {}; renaming to {}\n".format(bam_dataset_name, new_name))

			tantalus_api.update("sequencedataset", id=bam_dataset["id"], name=new_name)

		except Exception as e:
			log.info("cannot rename sequence dataset {}: {}".format(bam_dataset["id"], e))

			conflict_dataset = tantalus_api.get("sequencedataset", name=new_name)

			if conflict_dataset["last_updated"] > bam_dataset["last_updated"]:
				log.info("Renaming dataset {} as {} \n".format(bam_dataset["id"], "{}_old".format(new_name)))
				tantalus_api.update("sequencedataset", bam_dataset["id"], name="{}_old".format(new_name))

			else:
				log.info("Renaming dataset {} as {}".format(conflict_dataset["id"], "{}_old".format(new_name)))
				tantalus_api.update("sequencedataset", conflict_dataset["id"], name="{}_old".format(new_name))

				log.info("Renaming dataset {} as {} \n".format(bam_dataset["id"], "{}_old".format(new_name)))
				tantalus_api.update("sequencedataset", bam_dataset["id"], name="{}".format(new_name))


@click.command()
@click.argument('rename_type', type=click.Choice(["analysis", "bam"]))
def main(rename_type):
	if rename_type == "analysis":
		log.info("Renaming dlp analyses on Tantalus")
		rename_all_dlp_analyses()
	elif rename_type == "bam":
		log.info("Renaming bam datasets on Tantalus")
		rename_bam_datasets()


if __name__ == "__main__":
	main()


