#!/usr/bin/env python
import os
import re
import sys
import time
import click
import shutil
import tarfile
import logging
from datetime import datetime, timedelta
from dateutil import parser
import traceback
import subprocess
from itertools import chain
from jira import JIRA, JIRAError

import workflows.generate_inputs
import workflows.launch_pipeline

from dbclients.basicclient import NotFoundError
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from datamanagement.transfer_files import transfer_dataset

from workflows.tenx.models import TenXAnalysis, TenXAnalysisInfo

from workflows.utils import file_utils
from workflows.utils import log_utils
from workflows.utils import saltant_utils
from workflows.utils.colossus_utils import get_ref_genome
from workflows.utils.jira_utils import update_jira_tenx, add_attachment

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
log.addHandler(stream_handler)
log.propagate = False

colossus_api = ColossusApi()
tantalus_api = TantalusApi()


def download_data(storage_account, data_dir, library):
    # check if destination path exists
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # init storage client
    storage_client = tantalus_api.get_storage_client(storage_account)

    # list all blobs for library
    blobs = storage_client.list(library)

    for blob in blobs:
        if "_I1_" in blob:
            continue

        # get flowcell from path
        flowcell = os.path.basename(os.path.dirname(blob))
        # get fastq filename
        filename = os.path.basename(blob)

        # join destination path with flowcell name and create path
        flowcell_path = os.path.join(data_dir, flowcell)
        if not os.path.exists(flowcell_path):
            os.makedirs(flowcell_path)

        # format filepath
        filepath = os.path.join(flowcell_path, filename)
        # check if file already exists with same size from blob storage
        if os.path.exists(filepath) and os.path.getsize(filepath) == storage_client.get_size(blob):
            continue

        # download blob to path
        print(f"downloading {blob} to {filepath}")
        blob = storage_client.blob_service.get_blob_to_path(container_name="rnaseq", blob_name=blob, file_path=filepath)


def add_report(jira_ticket):
    """
    Downloads reports tar file, untars, and adds summary.html report to ticket
    """
    storage_client = tantalus_api.get_storage_client("scrna_reports")
    results_dataset = tantalus_api.get("resultsdataset", analysis__jira_ticket=jira_ticket)
    reports = list(
        tantalus_api.get_dataset_file_resources(
            results_dataset["id"],
            "resultsdataset",
            {"fileinstance__storage__name": "scrna_reports"},
        ), )

    filename = reports[0]["filename"]
    filepath = os.path.join("reports", jira_ticket)
    local_path = os.path.join(filepath, filename)
    if not os.path.exists(filepath):
        os.makedirs(filepath)

    blob = storage_client.blob_service.get_blob_to_path(
        container_name="reports",
        blob_name=filename,
        file_path=local_path,
    )
    subprocess.call(['tar', '-xvf', local_path, '-C', filepath])

    report_files = os.listdir(local_path)

    summary_filename = "summary.html"
    if summary_filename in report_files:
        # Get library ticket
        analysis = colossus_api.get("tenxanalysis", jira_ticket=jira_ticket)
        library_id = analysis["tenx_library"]
        library = colossus_api.get("tenxlibrary", id=library_id)
        library_ticket = library["jira_ticket"]

        log.info("adding report to parent ticket of {}".format(jira_ticket))
        summary_filepath = os.path.join(local_path, summary_filename)
        summary_filename = "{}_summary.html".format(jira_ticket)
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

    JIRA_USER = os.environ['JIRA_USERNAME']
    JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
    jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

    issue = jira_api.issue(library_ticket)

    # In order to search for library on Jira,
    # Jira ticket must include spaces
    sub_task = {
        'project': {
            'key': 'SC'
        },
        'summary': '{} - {} TenX Analysis'.format(sample, library_id),
        'issuetype': {
            'name': 'Sub-task'
        },
        'parent': {
            'id': issue.key
        }
    }

    sub_task_issue = jira_api.create_issue(fields=sub_task)
    analysis_jira_ticket = sub_task_issue.key

    # Add watchers
    jira_api.add_watcher(analysis_jira_ticket, JIRA_USER)

    # Assign task to myself
    analysis_issue = jira_api.issue(analysis_jira_ticket)
    analysis_issue.update(assignee={'name': JIRA_USER})

    log.info('Created analysis ticket {} for library {}'.format(analysis_jira_ticket, library_id))

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
    storages,
    library_pk,
    analysis_id,
):

    start = time.time()
    tantalus_analysis = TenXAnalysis(
        jira,
        version,
        args,
        run_options,
        library_pk,
        storages=storages,
        update=run_options["update"],
    )

    try:
        tantalus_analysis.set_run_status()
        analysis_info.set_run_status()

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
        analysis_info.set_error_status()

        print(f"pipeline failed; retry again with analysis id {analysis_id}")
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
        update_jira_tenx(jira, library_pk)

    add_report(jira)

    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))


default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'normal_config_tenx.json')


@click.group()
def cli():
    pass


@cli.command("run_all")
@click.option('--version')
@click.option('--jira', nargs=1)
@click.option('--no_download', is_flag=True)
@click.option('--data_dir')
@click.option('--runs_dir')
@click.option('--results_dir')
@click.option('--new_ticket', is_flag=True)
@click.option('--testing', is_flag=True)
@click.option('--config_filename')
@click.option('--skip_pipeline', is_flag=True)
@click.option('--skip_missing', is_flag=True)
@click.option('--is_test_run', is_flag=True)
@click.option('--clean', is_flag=True)
@click.option('--tag', type=str, default='')
@click.option('--update', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
@click.option('--ref_genome', type=click.Choice(['HG38', 'MM10']))
def run_all(
    version,
    jira=None,
    no_download=False,
    config_filename=None,
    new_ticket=False,
    data_dir=None,
    runs_dir=None,
    results_dir=None,
    **run_options,
):
    config = file_utils.load_json(default_config)

    # get latest analyses with status ready
    analyses_ready = tantalus_api.list(
        "analysis",
        analysis_type__name="tenx",
        status="ready",
        last_updated__gte=str(datetime.now() - timedelta(days=7)),
    )
    # get latest analyses with status error
    analyses_error = tantalus_api.list(
        "analysis",
        analysis_type__name="tenx",
        status="error",
        last_updated__gte=str(datetime.now() - timedelta(days=7)),
    )

    for analysis in chain(analyses_ready, analyses_error):
        jira_ticket = analysis["jira_ticket"]
        library_id = analysis["args"]["library_id"]

        run(analysis["id"], config["version"], **run_options)


@cli.command("run_single")
@click.argument('analysis_id', nargs=1, type=int)
@click.option('--version')
@click.option('--jira', nargs=1)
@click.option('--no_download', is_flag=True)
@click.option('--data_dir')
@click.option('--runs_dir')
@click.option('--results_dir')
@click.option('--new_ticket', is_flag=True)
@click.option('--testing', is_flag=True)
@click.option('--config_filename')
@click.option('--skip_pipeline', is_flag=True)
@click.option('--skip_missing', is_flag=True)
@click.option('--is_test_run', is_flag=True)
@click.option('--clean', is_flag=True)
@click.option('--tag', type=str, default='')
@click.option('--update', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
@click.option('--ref_genome', type=click.Choice(['HG38', 'MM10']))
def run_single(
    analysis_id,
    version,
    jira=None,
    no_download=False,
    config_filename=None,
    new_ticket=False,
    data_dir=None,
    runs_dir=None,
    results_dir=None,
    **run_options,
):
    config = file_utils.load_json(default_config)
    
    run(analysis_id, config["version"], **run_options)


def run(
    analysis_id,
    version,
    jira=None,
    no_download=False,
    config_filename=None,
    new_ticket=False,
    data_dir=None,
    runs_dir=None,
    results_dir=None,
    **run_options,
):
    run_options = run_options

    if config_filename is None:
        config_filename = default_config

    config = file_utils.load_json(config_filename)
    storages = config["storages"]

    analysis = tantalus_api.get("analysis", id=analysis_id)
    jira_ticket = analysis["jira_ticket"]
    library_id = analysis["args"]["library_id"]

    # get colossus library
    library = colossus_api.get(
        "tenxlibrary",
        name=library_id,
    )

    log.info("Running {}".format(jira_ticket))
    job_subdir = jira_ticket + run_options['tag']

    # init pipeline dir
    pipeline_dir = os.path.join(
        tantalus_api.get("storage", name=config["storages"]["local_results"])["storage_directory"],
        job_subdir,
    )

    log_utils.init_pl_dir(pipeline_dir, run_options['clean'])

    log_file = log_utils.init_log_files(pipeline_dir)
    log_utils.setup_sentinel(run_options['sisyphus_interactive'], os.path.join(pipeline_dir, "tenx"))

    # SCNRA pipeline working directories
    if data_dir is None:
        data_dir = os.path.join("/datadrive", "data")
    if runs_dir is None:
        runs_dir = os.path.join("/datadrive", "runs", library_id)
    if results_dir is None:
        results_dir = os.path.join("/datadrive", "results", library_id)

    reference_dir = os.path.join("/datadrive", "reference")

    if run_options["testing"]:
        ref_genome = "test"

    elif run_options["ref_genome"]:
        ref_genome = run_options["ref_genome"]
        log.info("Default reference genome being overwritten; using {}".format(run_options["ref_genome"]))

    else:
        ref_genome = get_ref_genome(library, is_tenx=True)

    args = {}
    args['library_id'] = library_id
    args['ref_genome'] = ref_genome
    args['version'] = version

    analysis_info = TenXAnalysisInfo(
        jira_ticket,
        config['version'],
        run_options,
        library["id"],
    )

    if not no_download:
        download_data(storages["working_inputs"], data_dir, library_id)

    start_automation(
        jira_ticket,
        config['version'],
        args,
        run_options,
        analysis_info,
        data_dir,
        runs_dir,
        reference_dir,
        results_dir,
        storages,
        library["id"],
        analysis_id,
    )


if __name__ == "__main__":
    cli()
