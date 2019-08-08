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
from distutils.version import StrictVersion

import datamanagement.templates as templates
import launch_pipeline
import generate_inputs
from dbclients.tantalus import TantalusApi
from workflows.utils import saltant_utils
from workflows.utils import file_utils
from workflows.utils import log_utils
from datamanagement.transfer_files import transfer_dataset
from dbclients.basicclient import NotFoundError

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
        transfer_dataset(tantalus_api, dataset_id, 'sequencedataset', from_storage, to_storage)

    for results_id in results_ids:
        transfer_dataset(tantalus_api, results_id, 'resultsdataset', from_storage, to_storage)


def get_alignment_metrics(storage, dataset_ids, normal_library, pipeline_dir):
    """
    Download alignment metrics for each library from input dataset

    Args:
        storage (str):          name of storage containing alignment metrics
        dataset_ids (list):     ids (int) of tantalus sequence datasets
        normal_library (str):   name of normal library
    """

    library_metrics_paths = dict()
    storage_client = tantalus_api.get_storage_client(storage)
    metrics_dir = os.path.join(pipeline_dir, "metrics")
    if not os.path.exists(metrics_dir):
        os.makedirs(metrics_dir)

    for dataset_id in dataset_ids:
        dataset = tantalus_api.get("sequencedataset", id=dataset_id)
        library = dataset["library"]["library_id"]
        
        if library == normal_library:
            continue

        analyses = list(tantalus_api.list("analysis", analysis_type__name="qc", input_datasets__library__library_id=library))
        jira_ticket = None
        for analysis in analyses:
            version = analysis["version"]
            if StrictVersion(version.strip('v')) >= StrictVersion('0.3.1'):
                jira_ticket = analysis["jira_ticket"]
                break

        if jira_ticket is None:
            raise Exception("No metrics file found for {} with is_contaminated column".format(library))

        metrics_filename = "{}_alignment_metrics.csv.gz".format(library)
        file_resources = list(tantalus_api.list(
            "file_resource",
            filename__startswith=jira_ticket,
            filename__endswith=metrics_filename
        ))    

        if len(file_resources) != 1:
            raise Exception("More than one file names {}".format(metrics_filename))

        filename = file_resources[0]["filename"]
        filepath = os.path.join(metrics_dir, metrics_filename)
        
        if not os.path.exists(filepath):
            log.info("Downloading {} to {}".format(filename, filepath))
            blob = storage_client.blob_service.get_blob_to_path(
                container_name="results",
                blob_name=filename,
                file_path=filepath
            )
        else:
            log.info("{} has already been downloaded. File at {}".format(filename, filepath))

        library_metrics_paths[library] = filepath

    return library_metrics_paths
    

def start_automation(
        jira_ticket,
        version,
        args,
        run_options,
        config,
        pipeline_dir,
        results_dir,
        scpipeline_dir,
        tmp_dir,
        storages,
        job_subdir,
        destruct_output,
        lumpy_output,
        haps_output,
        variants_output,
):
    start = time.time()

    analysis_type = 'multi_sample_pseudo_bulk'

    tantalus_analysis = PseudoBulkAnalysis(
        jira_ticket,
        version,
        args,
        run_options,
        storages=storages,
        update=run_options.get('update', False),
    )

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

    library_metrics_paths = get_alignment_metrics(
        storages["working_results"], 
        tantalus_analysis.get_input_datasets(), 
        args["matched_normal_library"],
        pipeline_dir
    )

    local_results_storage = tantalus_api.get(
        'storage', 
        name=storages['local_results'])['storage_directory']

    inputs_yaml = os.path.join(local_results_storage, job_subdir, 'inputs.yaml')
    log_utils.sentinel(
        'Generating inputs yaml',
        tantalus_analysis.generate_inputs_yaml,
        inputs_yaml,
        library_metrics_paths,
    )
    
    tantalus_analysis.add_inputs_yaml(inputs_yaml, update=run_options['update'])

    try:
        tantalus_analysis.set_run_status()

        if run_options["skip_pipeline"]:
            log.info("skipping pipeline")

        else:
            log_utils.sentinel(
                'Running single_cell {}'.format(analysis_type),
                tantalus_analysis.run_pipeline,
                results_dir,
                pipeline_dir,
                scpipeline_dir,
                tmp_dir,
                inputs_yaml,
                config,
                destruct_output,
                lumpy_output,
                haps_output,
                variants_output,
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
        skip_missing=run_options['skip_missing'],
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

    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))


default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'normal_config.json')


@click.command()
@click.argument('jira_ticket')
@click.argument('version')
@click.argument('inputs_tag_name')
@click.argument('matched_normal_sample')
@click.argument('matched_normal_library')
@click.option('--config_filename')
@click.option('--clean', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
@click.option('--skip_pipeline', is_flag=True)
@click.option('--update', is_flag=True)
@click.option('--skip_missing', is_flag=True)
@click.option('--local_run', is_flag=True)
@click.option('--interactive', is_flag=True)
@click.option('--is_test_run', is_flag=True)
@click.option('--sc_config')
def run_pseudobulk(
        jira_ticket,
        version,
        inputs_tag_name,
        matched_normal_sample,
        matched_normal_library,
        config_filename=None,
        **run_options
):
    if config_filename is None:
        config_filename = default_config

    config = file_utils.load_json(config_filename)

    args = dict(
        inputs_tag_name=inputs_tag_name,
        matched_normal_sample=matched_normal_sample,
        matched_normal_library=matched_normal_library,
    )

    job_subdir = jira_ticket

    run_options['job_subdir'] = job_subdir

    pipeline_dir = os.path.join(
        tantalus_api.get("storage", name=config["storages"]["local_results"])["storage_directory"],
        job_subdir)

    results_dir = os.path.join('singlecellresults', 'results', job_subdir)

    scpipeline_dir = os.path.join('singlecelllogs', 'pipeline', job_subdir)

    tmp_dir = os.path.join('singlecelltemp', 'temp', job_subdir)

    log_utils.init_pl_dir(pipeline_dir, run_options['clean'])

    storage_result_prefix = tantalus_api.get_storage_client("singlecellresults").prefix
    destruct_output = os.path.join(storage_result_prefix, jira_ticket, "results", "destruct")
    lumpy_output = os.path.join(storage_result_prefix, jira_ticket, "results", "lumpy")
    haps_output = os.path.join(storage_result_prefix, jira_ticket, "results", "haps")
    variants_output = os.path.join(
        storage_result_prefix, jira_ticket, "results", "variants")

    log_file = log_utils.init_log_files(pipeline_dir)
    log_utils.setup_sentinel(run_options['sisyphus_interactive'], pipeline_dir)
    
    start_automation(
        jira_ticket,
        version,
        args,
        run_options,
        config,
        pipeline_dir,
        results_dir,
        scpipeline_dir,
        tmp_dir,
        config['storages'],
        job_subdir,
        destruct_output,
        lumpy_output,
        haps_output,
        variants_output,
    )


if __name__ == '__main__':
    run_pseudobulk()
