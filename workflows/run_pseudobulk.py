#!/usr/bin/env python
import os
import re
import sys
import time
import logging
import subprocess
import traceback
import click
from itertools import chain

import arguments
import datamanagement.templates as templates
import launch_pipeline
import generate_inputs
from dbclients.tantalus import TantalusApi
from workflows.utils import saltant_utils
from workflows.utils import file_utils
from workflows.utils import log_utils
from datamanagement.transfer_files import transfer_dataset
from dbclients.basicclient import NotFoundError

from utils.log_utils import sentinel
from models import PseudoBulkAnalysis, Results


log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False

tantalus_api = TantalusApi()


def transfer_inputs(dataset_ids, results_ids, from_storage, to_storage):
    tantalus_api = TantalusApi()

    for dataset_id in dataset_ids:
        transfer_dataset(tantalus_api, dataset_id, 'sequencedataset', from_storage_name, to_storage_name)

    for results_id in results_ids:
        transfer_dataset(tantalus_api, results_id, 'resultsdataset', from_storage_name, to_storage_name)


def start_automation(
        args,
        config,
        pipeline_dir,
        results_dir,
        scpipeline_dir,
        tmp_dir,
        storages,
        job_subdir,
):
    start = time.time()

    args['job_subdir'] = job_subdir

    analysis_type = 'multi_sample_pseudo_bulk'

    tantalus_analysis = PseudoBulkAnalysis(args, storages=storages, update=args['update'])

    input_dataset_ids = sentinel(
        'Searching for input datasets',
        tantalus_analysis.search_input_datasets,
        args,
    )

    input_results_ids = sentinel(
        'Searching for input results',
        tantalus_analysis.search_input_results,
        args,
    )

    if storages["working_inputs"] != storages["remote_inputs"]:  
        sentinel(
            'Transferring input datasets from {} to {}'.format(
                storages["remote_inputs"], storages["working_inputs"]),
            transfer_inputs,
            input_dataset_ids,
            input_results_ids,
            storages["remote_inputs"],
            storages["working_inputs"],
        )

    local_results_storage = tantalus_api.get(
        'storage', 
        name=storages['local_results'])['storage_directory']

    inputs_yaml = os.path.join(local_results_storage, job_subdir, 'inputs.yaml')
    sentinel(
        'Generating inputs yaml',
        tantalus_analysis.generate_inputs_yaml,
        args,
        inputs_yaml,
    )

    tantalus_analysis.add_inputs_yaml(inputs_yaml, update=args['update'])

    try:
        tantalus_analysis.set_run_status()

        if args["skip_pipeline"]:
            log.info("skipping pipeline")

        else:
            sentinel(
                'Running single_cell {}'.format(analysis_type),
                tantalus_analysis.run_pipeline,
                args,
                results_dir,
                pipeline_dir,
                scpipeline_dir,
                tmp_dir,
                inputs_yaml,
                config,
            )

    except Exception:
        tantalus_analysis.set_error_status()
        raise

    tantalus_analysis.set_complete_status()

    output_dataset_ids = sentinel(
        'Creating output datasets',
        tantalus_analysis.create_output_datasets,
        update=args['update'],
    )

    output_results_ids = sentinel(
        'Creating output results',
        tantalus_analysis.create_output_results,
        update=args['update'],
    )

    if storages["working_inputs"] != storages["remote_inputs"] and output_datasets_ids != []:
        sentinel(
            'Transferring input datasets from {} to {}'.format(
                storages["working_inputs"], storages["remote_inputs"]),
            transfer_inputs,
            output_dataset_ids,
            output_results_ids,
            storages["remote_inputs"],
            storages["working_inputs"],
        )

    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))


default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'normal_config.json')


@click.command()
@click.argument('jira_ticket')
@click.argument('version')
@click.argument('inputs_tag_name')
@click.option('--config_filename')
@click.option('--clean', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
@click.option('--skip_pipeline', is_flag=True)
@click.option('--update', is_flag=True)
@click.option('--local_run', is_flag=True)
@click.option('--interactive', is_flag=True)
@click.option('--sc_config')
def run_pseudobulk(
        jira_ticket,
        version,
        inputs_tag_name,
        config_filename=None,
        **args
):
    if config_filename is None:
        config_filename = default_config

    config = file_utils.load_json(config_filename)

    args['jira'] = jira_ticket
    args['version'] = version

    job_subdir = jira_ticket

    pipeline_dir = os.path.join(
        tantalus_api.get("storage", name=config["storages"]["local_results"])["storage_directory"], 
        job_subdir)

    results_dir = os.path.join('singlecelldata', 'results', job_subdir, 'results')

    scpipeline_dir = os.path.join('singlecelldata', 'pipeline', job_subdir)

    tmp_dir = os.path.join('singlecelldata', 'temp', job_subdir)

    # Shahlab
    # - local: shahlab
    # - working: shahlab
    # - remote: singlecellblob
    # Blob
    # - local: headnode
    # - working: singlecellblob
    # - remote: singlecellblob

    log_utils.init_pl_dir(pipeline_dir, args['clean'])

    log_file = log_utils.init_log_files(pipeline_dir)
    log_utils.setup_sentinel(args['sisyphus_interactive'], pipeline_dir)
    
    start_automation(args, config, pipeline_dir, results_dir, scpipeline_dir, tmp_dir, config['storages'], job_subdir)


if __name__ == '__main__':
    run_pseudobulk()
