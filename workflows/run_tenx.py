#!/usr/bin/env python
import os
import re
import sys
import time
import click
import logging
import traceback
import subprocess
from itertools import chain

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

	log.info("Done!")
	log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))

	# Update Jira ticket
	if not run_options["is_test_run"]:
	   update_jira_tenx(jira, args)


default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'normal_config_tenx.json')

@click.command()
@click.argument('jira')
@click.argument('version')
@click.argument('library_id') 
@click.argument('ref_genome', type=click.Choice(["HG38", "MM10", "test"]))
@click.option('--config_filename')
@click.option('--skip_pipeline', is_flag=True)
@click.option('--skip_missing', is_flag=True)
@click.option('--is_test_run', is_flag=True)
@click.option('--clean', is_flag=True)
@click.option('--tag', type=str, default='')
@click.option('--update', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
def main(
	jira, 
	version, 
	library_id,
	ref_genome,
    config_filename=None,
	**run_options
):

	if config_filename is None:
		config_filename = default_config

	log.info(config_filename)
	config = file_utils.load_json(config_filename)

	job_subdir = jira + run_options['tag']

	pipeline_dir = os.path.join(
		tantalus_api.get("storage", name=config["storages"]["local_results"])["storage_directory"], 
		job_subdir)

	log_utils.init_pl_dir(pipeline_dir, run_options['clean'])

	log_file = log_utils.init_log_files(pipeline_dir)
	log_utils.setup_sentinel(
		run_options['sisyphus_interactive'],
		os.path.join(pipeline_dir, "tenx"))


	storages = config["storages"]

	# SCNRA pipeline working directories
	data_dir = os.path.join("scrnadata", job_subdir, "data")
	runs_dir = os.path.join("scrnadata", job_subdir, "runs")
	reference_dir = os.path.join("scrnadata", job_subdir, "reference")
	results_dir = os.path.join("scrnadata", job_subdir, "results")

	analysis_info = TenXAnalysisInfo(
		jira,
		version,
		library_id, 
	)

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
	



