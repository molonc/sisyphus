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

import datamanagement.templates as templates
import launch_pipeline
import generate_inputs
from dbclients.tantalus import TantalusApi
# from workflows.utils import saltant_utils
from workflows.utils import file_utils, log_utils
from workflows.utils.update_jira import update_jira
from datamanagement.transfer_files import transfer_dataset
from dbclients.basicclient import NotFoundError

from models import AnalysisInfo, AlignAnalysis, HmmcopyAnalysis, PseudoBulkAnalysis, Results


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
        jira,
        version,
        args,
        run_options,
        config,
        pipeline_dir,
        results_dir,
        scpipeline_dir,
        tmp_dir,
        analysis_info,
        analysis_type,
        storages,
        job_subdir,
):
    start = time.time()

    if analysis_type == 'align':
        tantalus_analysis = AlignAnalysis(jira, version, args, run_options, storages=storages, update=run_options['update'])
    elif analysis_type == 'hmmcopy':
        tantalus_analysis = HmmcopyAnalysis(jira, version, args, run_options, storages=storages, update=run_options['update'])
    else:
        raise ValueError()

    if storages["working_inputs"] != storages["remote_inputs"]:  
        log_utils.sentinel(
            'Transferring input datasets from {} to {}'.format(
                storages["remote_inputs"], storages["working_inputs"]),
            transfer_inputs,
            tantalus_analysis.get_input_datasets(),
            tantalus_analysis.get_input_results(),
            storages["remote_inputs"],
            storages["working_inputs"],
        )

    if run_options['inputs_yaml'] is None:
        local_results_storage = tantalus_api.get(
            'storage', 
            name=storages['local_results'])['storage_directory']

        inputs_yaml = os.path.join(local_results_storage, job_subdir, 'inputs.yaml')
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

        run_pipeline = tantalus_analysis.run_pipeline()

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

        log_utils.sentinel(
            'Running single_cell {}'.format(analysis_type),
            run_pipeline,
            results_dir=results_dir,
            scpipeline_dir=scpipeline_dir,
            tmp_dir=tmp_dir,
            tantalus_analysis=tantalus_analysis,
            analysis_info=analysis_info,
            inputs_yaml=inputs_yaml,
            context_config_file=config['context_config_file'],
            docker_env_file=config['docker_env_file'],
            dirs=dirs,
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

    output_result_ids = log_utils.sentinel(
        'Creating output results',
        tantalus_analysis.create_output_results,
        update=run_options['update'],
    )

    if storages["working_inputs"] != storages["remote_inputs"] and output_datasets_ids != []:
        log_utils.sentinel(
            'Transferring input datasets from {} to {}'.format(
                storages["working_inputs"], storages["remote_inputs"]),
            transfer_inputs,
            output_dataset_ids,
            output_results_ids,
            storages["remote_inputs"],
            storages["working_inputs"],
        )

    analysis_info.set_finish_status()
    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))

    # Update Jira ticket
    if not run_options["is_test_run"]:
        update_jira(jira, args['aligner'], analysis_type)


default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'normal_config.json')


@click.command()
@click.argument('jira')
@click.argument('version')
@click.argument('analysis_type')
@click.option('--gsc_lanes')
@click.option('--brc_flowcell_ids')
@click.option('--config_filename')
@click.option('--skip_pipeline', is_flag=True)
@click.option('--local_run', is_flag=True)
@click.option('--update', is_flag=True)
@click.option('--is_test_run', is_flag=True)
@click.option('--sc_config')
@click.option('--inputs_yaml')
@click.option('--index_sequences', multiple=True)
@click.option('--clean', is_flag=True)
@click.option('--tag', type=str, default='')
@click.option('--interactive', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
@click.option('--alignment_metrics')
@click.option('--jobs', type=int, default=1000)
def main(
        jira,
        version,
        analysis_type,
        gsc_lanes=None,
        brc_flowcell_ids=None,
        config_filename=None,
        **run_options
):
    if config_filename is None:
        config_filename = default_config

    if not templates.JIRA_ID_RE.match(jira):
        raise Exception('Invalid SC ID:'.format(jira))

    if gsc_lanes is not None:
        gsc_lanes = gsc_lanes.split(',')

    if brc_flowcell_ids is not None:
        brc_flowcell_ids = brc_flowcell_ids.split(',')

    config = file_utils.load_json(config_filename)

    job_subdir = jira + run_options['tag']

    run_options['job_subdir'] = job_subdir

    pipeline_dir = os.path.join(
        tantalus_api.get("storage", name=config["storages"]["local_results"])["storage_directory"], 
        job_subdir)

    results_dir = os.path.join('singlecelldata', 'results', job_subdir, 'results')

    scpipeline_dir = os.path.join('singlecelldata', 'pipeline', job_subdir)

    tmp_dir = os.path.join('singlecelldata', 'temp', job_subdir)

    log_utils.init_pl_dir(pipeline_dir, run_options['clean'])

    log_file = log_utils.init_log_files(pipeline_dir)
    log_utils.setup_sentinel(
        run_options['sisyphus_interactive'],
        os.path.join(pipeline_dir, analysis_type))

    analysis_info = AnalysisInfo(
        jira,
        log_file,
        version,
        update=run_options['update'],
    )

    log.info('Library ID: {}'.format(analysis_info.chip_id))
    
    library_id = analysis_info.chip_id
    if run_options["is_test_run"]:
        library_id += "TEST"

    args = {}
    args['ref_genome'] = analysis_info.reference_genome
    args['aligner'] = analysis_info.aligner
    args['library_id'] = library_id
    args['gsc_lanes'] = gsc_lanes
    args['brc_flowcell_ids'] = brc_flowcell_ids

    start_automation(
        jira,
        version,
        args,
        run_options,
        config,
        pipeline_dir,
        results_dir,
        scpipeline_dir,
        tmp_dir,
        analysis_info,
        analysis_type,
        config['storages'],
        job_subdir,
    )


if __name__ == '__main__':
    main()
