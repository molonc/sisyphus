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
import workflows.models

import datamanagement.templates as templates
from datamanagement.transfer_files import transfer_dataset

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError

from workflows.utils import file_utils, log_utils, colossus_utils
from workflows.utils.jira_utils import update_jira_dlp, add_attachment, comment_jira


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


def start_automation(
        jira,
        version,
        args,
        run_options,
        config,
        pipeline_dir,
        scpipeline_dir,
        tmp_dir,
        storages,
        job_subdir,
        analysis_type='',
):
    start = time.time()

    tantalus_analysis = workflows.models.SplitWGSBamAnalysis(
        jira,
        version,
        args,
        storages,
        run_options,
        update=run_options['update'],
    )

    if storages["working_inputs"] != storages["remote_inputs"]:
        log_utils.sentinel(
            'Transferring input datasets from {} to {}'.format(storages["remote_inputs"], storages["working_inputs"]),
            transfer_inputs,
            tantalus_analysis.get_input_datasets(),
            tantalus_analysis.get_input_results(),
            storages["remote_inputs"],
            storages["working_inputs"],
        )

    if run_options['inputs_yaml'] is None:
        local_results_storage = tantalus_api.get('storage', name=storages['local_results'])['storage_directory']

        inputs_yaml = os.path.join(local_results_storage, job_subdir, analysis_type, 'inputs.yaml')
        log_utils.sentinel(
            'Generating inputs yaml',
            tantalus_analysis.generate_inputs_yaml,
            inputs_yaml,
        )
    else:
        inputs_yaml = run_options['inputs_yaml']

    tantalus_analysis.add_inputs_yaml(inputs_yaml, update=run_options['update'])

    try:
        tantalus_analysis.set_run_status()

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
            f'Running single_cell {analysis_type}',
            tantalus_analysis.run_pipeline,
            scpipeline_dir=scpipeline_dir,
            tmp_dir=tmp_dir,
            inputs_yaml=inputs_yaml,
            context_config_file=context_config_file,
            docker_env_file=config['docker_env_file'],
            docker_server=config['docker_server'],
            dirs=dirs,
        )

    except Exception:
        tantalus_analysis.set_error_status()
        raise Exception("pipeline failed")

    output_dataset_ids = log_utils.sentinel(
        'Creating output datasets',
        tantalus_analysis.create_output_datasets,
        update=run_options['update'],
    )

    output_results_ids = log_utils.sentinel(
        'Creating {} output results'.format(analysis_type),
        tantalus_analysis.create_output_results,
        update=run_options['update'],
        skip_missing=run_options['skip_missing'],
        analysis_type=analysis_type,
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

    # TODO: confirm with andrew whether to move this down here
    tantalus_analysis.set_complete_status()


default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'normal_config.json')


@click.command()
@click.argument('jira')
@click.argument('version')
@click.argument('sample_id')
@click.argument('library_id')
@click.argument('aligner', type=click.Choice(['A', "M"]))
@click.argument('ref_genome')
@click.option('--config_filename')
@click.option('--skip_pipeline', is_flag=True)
@click.option('--skip_missing', is_flag=True)
@click.option('--update', is_flag=True)
@click.option('--is_test_run', is_flag=True)
@click.option('--sc_config')
@click.option('--inputs_yaml')
@click.option('--clean', is_flag=True)
@click.option('--tag', type=str, default='')
@click.option('--interactive', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
@click.option('--jobs', type=int, default=1000)
@click.option('--saltant', is_flag=True)
@click.option('--local_run', is_flag=True)
def main(
        jira,
        version,
        sample_id,
        library_id,
        aligner,
        ref_genome,
        config_filename=None,
        **run_options
    ):

    analysis_type = 'split_wgs_bam'

    if config_filename is None:
        config_filename = default_config

    if not templates.JIRA_ID_RE.match(jira):
        raise Exception(f'Invalid SC ID: {jira}')

    aligner_map = {'A': 'BWA_ALN_0_5_7', 'M': 'BWA_MEM_0_7_6A'}
    aligner = aligner_map[aligner]

    config = file_utils.load_json(config_filename)

    job_subdir = jira + run_options['tag']

    run_options['job_subdir'] = job_subdir

    pipeline_dir = os.path.join(
        tantalus_api.get("storage", name=config["storages"]["local_results"])["storage_directory"], job_subdir)

    scpipeline_dir = os.path.join('singlecelllogs', 'pipeline', job_subdir)
    tmp_dir = os.path.join('singlecelltemp', 'temp', job_subdir)

    log_utils.init_pl_dir(pipeline_dir, run_options['clean'])

    log_file = log_utils.init_log_files(pipeline_dir)
    log_utils.setup_sentinel(run_options['sisyphus_interactive'], os.path.join(pipeline_dir, analysis_type))

    log.info('Library ID: {}'.format(library_id))

    args = {}
    args['sample_id'] = sample_id
    args['library_id'] = library_id
    args['aligner'] = aligner
    args['ref_genome'] = ref_genome

    start_automation(
        jira,
        version,
        args,
        run_options,
        config,
        pipeline_dir,
        scpipeline_dir,
        tmp_dir,
        config['storages'],
        job_subdir,
        'split_wgs_bam',
    )


if __name__ == '__main__':
    main()
