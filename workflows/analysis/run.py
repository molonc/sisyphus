#!/usr/bin/env python
import os
import re
import sys
import time
import click
import logging
import subprocess
from itertools import chain

from jira import JIRA

import workflows.launch_pipeline
import workflows.generate_inputs
import workflows.analysis.base
import workflows.analysis.dlp.breakpoint_calling
import workflows.analysis.dlp.merge_cell_bams
import workflows.analysis.dlp.split_wgs_bam
import workflows.analysis.dlp.variant_calling

import datamanagement.templates as templates
from datamanagement.transfer_files import transfer_dataset

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError

from workflows.utils import file_utils, log_utils
from workflows.utils.jira_utils import comment_jira


log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False

tantalus_api = TantalusApi()
colossus_api = ColossusApi()


def transfer_inputs(dataset_ids, results_ids, from_storage, to_storage):
    tantalus_api = TantalusApi()

    for dataset_id in dataset_ids:
        transfer_dataset(tantalus_api, dataset_id, 'sequencedataset', from_storage, to_storage)

    for results_id in results_ids:
        transfer_dataset(tantalus_api, results_id, 'resultsdataset', from_storage, to_storage)


default_config = os.path.realpath(os.path.join(os.path.dirname(os.path.realpath(__file__)), os.pardir, 'config', 'normal_config.json'))


@click.command()
@click.argument('analysis_id', type=int)
@click.option('--config_filename')
@click.option('--config_override')
@click.option('--skip_pipeline', is_flag=True)
@click.option('--skip_missing', is_flag=True)
@click.option('--local_run', is_flag=True)
@click.option('--update', is_flag=True)
@click.option('--is_test_run', is_flag=True)
@click.option('--sc_config')
@click.option('--inputs_yaml')
@click.option('--clean', is_flag=True)
@click.option('--interactive', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
@click.option('--jobs', type=int, default=1000)
@click.option('--saltant', is_flag=True)
def main(
        analysis_id,
        config_filename=None,
        **run_options
    ):

    if config_filename is None:
        config_filename = default_config

    analysis = workflows.analysis.base.Analysis.get_by_id(tantalus_api, analysis_id)

    jira_id = analysis.jira
    analysis_name = analysis.name

    if not templates.JIRA_ID_RE.match(jira_id):
        raise Exception(f'Invalid SC ID: {jira_id}')

    config = file_utils.load_json(config_filename)

    pipeline_dir = os.path.join(config['analysis_directory'], jira_id, analysis_name)

    scpipeline_dir = os.path.join('singlecelllogs', 'pipeline', jira_id)
    tmp_dir = os.path.join('singlecelltemp', 'temp', jira_id)

    log_utils.init_pl_dir(pipeline_dir, run_options['clean'])

    log_file = log_utils.init_log_files(pipeline_dir)
    log_utils.setup_sentinel(run_options['sisyphus_interactive'], os.path.join(pipeline_dir, analysis_name))

    storages = config['storages']

    start = time.time()

    if storages["working_inputs"] != storages["remote_inputs"]:
        log_utils.sentinel(
            'Transferring input datasets from {} to {}'.format(storages["remote_inputs"], storages["working_inputs"]),
            transfer_inputs,
            analysis.get_input_datasets(),
            analysis.get_input_results(),
            storages["remote_inputs"],
            storages["working_inputs"],
        )

    if run_options['inputs_yaml'] is None:
        inputs_yaml_filename = os.path.join(pipeline_dir, 'inputs.yaml')
        log_utils.sentinel(
            'Generating inputs yaml',
            analysis.generate_inputs_yaml,
            storages,
            inputs_yaml_filename,
        )
    else:
        inputs_yaml = run_options['inputs_yaml']

    try:
        analysis.set_run_status()

        dirs = [
            pipeline_dir,
            config['docker_path'],
            config['docker_sock_path'],
        ]
        # Pass all server storages to docker
        for storage_name in storages.values():
            storage = tantalus_api.get('storage', name=storage_name)
            if storage['storage_type'] == 'server':
                dirs.append(storage['storage_directory'])

        if run_options['saltant']:
            context_config_file = config['context_config_file']['saltant']
        else:
            context_config_file = config['context_config_file']['sisyphus']

        log_utils.sentinel(
            f'Running single_cell {analysis_name}',
            analysis.run_pipeline,
            scpipeline_dir=scpipeline_dir,
            tmp_dir=tmp_dir,
            inputs_yaml=inputs_yaml,
            context_config_file=context_config_file,
            docker_env_file=config['docker_env_file'],
            docker_server=config['docker_server'],
            dirs=dirs,
            storages=storages,
            run_options=run_options,
        )

    except Exception:
        analysis.set_error_status()
        raise Exception("pipeline failed")

    output_dataset_ids = log_utils.sentinel(
        'Creating {} output datasets'.format(analysis_name),
        analysis.create_output_datasets,
        storages,
        update=run_options['update'],
    )

    output_results_ids = log_utils.sentinel(
        'Creating {} output results'.format(analysis_name),
        analysis.create_output_results,
        storages,
        update=run_options['update'],
        skip_missing=run_options['skip_missing'],
    )

    if storages["working_inputs"] != storages["remote_inputs"] and output_dataset_ids != []:
        log_utils.sentinel(
            'Transferring input datasets from {} to {}'.format(storages["working_inputs"], storages["remote_inputs"]),
            transfer_inputs,
            output_dataset_ids,
            output_results_ids,
            storages["remote_inputs"],
            storages["working_inputs"],
        )

    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))

    analysis.set_complete_status()

    comment_jira(jira_id, f'finished {analysis_name} analysis')


if __name__ == '__main__':
    main()
