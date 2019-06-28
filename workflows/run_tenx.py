#!/usr/bin/env python
import os
import re
import sys
import time
import click
import shutil
import tarfile
import logging
import traceback
import subprocess
from itertools import chain
from jira import JIRA, JIRAError

import generate_inputs
import launch_pipeline

from dbclients.basicclient import NotFoundError
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from datamanagement.transfer_files import transfer_dataset

from models import TenXAnalysis, TenXAnalysisInfo

from workflows.utils import file_utils
from workflows.utils import log_utils
from workflows.utils import saltant_utils
from workflows.utils.colossus_utils import get_ref_genome
from workflows.utils.update_jira import update_jira_tenx


log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False

colossus_api = ColossusApi()
tantalus_api = TantalusApi()


def transfer_inputs(dataset_ids, results_ids, from_storage, to_storage):
	tantalus_api = TantalusApi()

	for dataset_id in dataset_ids:
		transfer_dataset(tantalus_api, dataset_id, 'sequencedataset', from_storage_name, to_storage_name)

	for results_id in results_ids:
		transfer_dataset(tantalus_api, results_id, 'resultsdataset', from_storage_name, to_storage_name)


def add_report(jira_ticket):
	"""
	Downloads reports tar file, untars, and adds summary.html report to ticket
	"""
	storage_client = tantalus_api.get_storage_client("scrna_reports")
	results_dataset = tantalus_api.get("resultsdataset", analysis__jira_ticket=jira_ticket)
	reports = list(tantalus_api.get_dataset_file_resources(
		results_dataset["id"], 
		"resultsdataset", 
		{"fileinstance__storage__name":"scrna_reports"}
	))

	filename = reports[0]["filename"]
	filepath = os.path.join("reports", jira_ticket)
	local_path = os.path.join(filepath, filename)
	if not os.path.exists(filepath):
		os.makedirs(filepath)

	blob = storage_client.blob_service.get_blob_to_path(
		container_name="reports",
		blob_name=filename,
		file_path=local_path
	)
	subprocess.call(['tar', '-xvf', local_path, '-C', filepath])

	report_files = os.listdir(local_path)

	summary_filename = "summary.html"
	if summary_filename in report_files:
		# FIXME: this only works if jira ticket has parent issue; add check

		# Get library ticket
		analysis = colossus_api.get("analysis", jira_ticket=jira_ticket)
		library_id= analysis["id"]
		library = colossus_api.get("tenxlibrary", id=library_id)
		library_ticket = library["jira_ticket"]

		log.info("adding report to parent ticket of {}".format(library_ticket))
		summary_filepath = os.path.join(local_path, summary_filename)
		add_attachment(library_ticket, summary_filepath, summary_filename)

	log.info("Removing {}".format(local_path))
	shutil.rmtree(local_path)


def create_analysis_jira_ticket(library_id, sample, library_ticket):
    '''
    Create analysis jira ticket as subtask of library jira ticket

    Args:
        info (dict): Keys: library_id

    Returns:
        analysis_jira_ticket: jira ticket id (ex. SC-1234)
    '''

    JIRA_USER = os.environ['JIRA_USER']
    JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
    jira_api = JIRA('https://www.bcgsc.ca/jira/',
        basic_auth=(JIRA_USER, JIRA_PASSWORD)
    )

    issue = jira_api.issue(library_ticket)

    # In order to search for library on Jira,
    # Jira ticket must include spaces
    sub_task = {
        'project': {'key': 'SC'},
        'summary': '{} - {} TenX Analysis'.format(sample, library_id),
        'issuetype' : { 'name' : 'Sub-task' },
        'parent': {'id': issue.key}
    }

    sub_task_issue = jira_api.create_issue(fields=sub_task)
    analysis_jira_ticket = sub_task_issue.key

    # Add watchers
    jira_api.add_watcher(analysis_jira_ticket, JIRA_USER)

    # Assign task to myself
    analysis_issue = jira_api.issue(analysis_jira_ticket)
    analysis_issue.update(assignee={'name': JIRA_USER})

    log.info('Created analysis ticket {} for library {}'.format(
        analysis_jira_ticket,
        library_id)
    )

    return analysis_jira_ticket


def start_automation(
	jira,
	version,
	args,
	run_options,
	analysis_info, 
	data_dir, 
	runs_dir,
	reference_dir,
	results_dir, 
	storages
):

	start = time.time()
	tantalus_analysis = TenXAnalysis(
		jira, 
		version,
		args, 
		run_options,
		storages=storages, 
		update=run_options["update"]
	)

	try:
		tantalus_analysis.set_run_status()

		if run_options["skip_pipeline"]:
			log.info("skipping pipeline")

		else:
			log_utils.sentinel(
				'Running SCRNA pipeline',
				tantalus_analysis.run_pipeline,
				version,
				data_dir,
				runs_dir, 
				reference_dir,
				results_dir, 
				args["library_id"],
				args["ref_genome"],
			)

	except Exception:
		tantalus_analysis.set_error_status()
		raise        


	tantalus_analysis.set_complete_status()

	output_dataset_ids = log_utils.sentinel(
		'Creating output datasets',
		tantalus_analysis.create_output_datasets,
		update=run_options['update'],
	)

	output_results_ids = log_utils.sentinel(
		'Creating output results',
		tantalus_analysis.create_output_results,
		update=run_options['update'],
		skip_missing=run_options["skip_missing"],
	)
	
	analysis_info.set_finish_status()

	# Update Jira ticket
	if not run_options["is_test_run"]:
	   update_jira_tenx(jira, args)

	add_report(jira)

	log.info("Done!")
	log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))


default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'normal_config_tenx.json')

@click.command()
@click.argument('version')
@click.argument('library_id')
@click.option('--testing', is_flag=True)
@click.option('--config_filename')
@click.option('--skip_pipeline', is_flag=True)
@click.option('--skip_missing', is_flag=True)
@click.option('--is_test_run', is_flag=True)
@click.option('--clean', is_flag=True)
@click.option('--tag', type=str, default='')
@click.option('--update', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
def main(
	version, 
	library_id,
    config_filename=None,
	**run_options
):

	if config_filename is None:
		config_filename = default_config

	log.info(config_filename)
	config = file_utils.load_json(config_filename)

	storages = config["storages"]

	library = colossus_api.get("tenxlibrary", name=library_id)
	sample = library["sample"]["sample_id"]
	library_ticket = library["jira_ticket"]

	# TODO: Move this to tenx automated scripts
	if len(library["analysis_set"])	== 0:
		jira = create_analysis_jira_ticket(library_id, sample, library_ticket)

	else:
		analysis_id = library["analysis_set"][0]
		analysis_object = colossus_api.get("analysis", id=analysis_id)
		jira = analysis_object["jira_ticket"]

	log.info("Running {}".format(jira))
	job_subdir = jira + run_options['tag']

	pipeline_dir = os.path.join(
		tantalus_api.get("storage", name=config["storages"]["local_results"])["storage_directory"], 
		job_subdir)

	log_utils.init_pl_dir(pipeline_dir, run_options['clean'])

	log_file = log_utils.init_log_files(pipeline_dir)
	log_utils.setup_sentinel(
		run_options['sisyphus_interactive'],
		os.path.join(pipeline_dir, "tenx"))

	# SCNRA pipeline working directories
	data_dir = os.path.join("/datadrive", "data", library_id)
	runs_dir = os.path.join("/datadrive", "runs", library_id)
	reference_dir = os.path.join("/datadrive", "reference")
	results_dir = os.path.join("/datadrive", "results", library_id)
	
	analysis_info = TenXAnalysisInfo(
		jira,
		version,
		library_id, 
	)

	if run_options["testing"]:
		ref_genome = "test"

	else:
		ref_genome = get_ref_genome(library, is_tenx=True)

	args = {}
	args['library_id'] = library_id
	args['ref_genome'] =  ref_genome
	args['version'] = version


	start_automation(
		jira,
		version,
		args,
		run_options,
		analysis_info,
		data_dir, 
		runs_dir,
		reference_dir,
		results_dir,
		storages
	)


if __name__ == "__main__":
	main()
	


