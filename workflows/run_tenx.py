#!/usr/bin/env python
import os
import re
import sys
import time
import click
import shutil
import tarfile
import logging
import datetime
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
from workflows.tenx.reports import generate_qc

from workflows.utils import file_utils
from workflows.utils import log_utils
from workflows.utils import saltant_utils
from workflows.utils.colossus_utils import get_ref_genome
from workflows.utils.jira_utils import update_jira_tenx, add_attachment, delete_ticket
from workflows.utils.tantalus_utils import create_tenx_analysis_from_library

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

colossus_api = ColossusApi()
tantalus_api = TantalusApi()


def download_data(storage_account, data_dir, library):
    # check if destination path exists
    sub_data_dir = os.path.join(data_dir, library)
    if not os.path.exists(sub_data_dir):
        os.makedirs(sub_data_dir)

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
        flowcell_path = os.path.join(sub_data_dir, flowcell)
        if not os.path.exists(flowcell_path):
            os.makedirs(flowcell_path)

        # format filepath
        filepath = os.path.join(flowcell_path, filename)
        # check if file already exists with same size from blob storage
        if os.path.exists(filepath) and os.path.getsize(filepath) == storage_client.get_size(blob):
            continue

        # download blob to path
        print(f"downloading {blob} to {filepath}")
        # blob_service.get_blob_to_path() might be deprecated 
        #blob = storage_client.blob_service.get_blob_to_path(container_name="rnaseq", blob_name=blob, file_path=filepath)
        storage_client.download(blob_name=blob, destination_file_path=filepath)


def add_report(library_pk, jira_ticket, runs_dir, results_dir, ref_genome, update=False):
    """
    Attaches cellranger summary and qc reprot to ticket
    """

    # get colossus library
    library = colossus_api.get("tenxlibrary", id=library_pk)
    # get library name and ticket
    library_id = library["name"]
    library_ticket = library["jira_ticket"]

    # file path to summary report generated from cellranger
    filepath = os.path.join(
        results_dir,
        f"{library_id}_report",
        "summary.html",
    )

    jira_filename = f"{jira_ticket}_{ref_genome}_summary.html"
    # add summary report from cellranger
    add_attachment(library_ticket, filepath, jira_filename, update=update)

    log.info("Creating QC reports")
    input_dir = os.path.join(runs_dir, ".cache", library_id)
    output_dir = os.path.join("/datadrive", "QC")
    generate_qc.rscript(library_id, input_dir, output_dir)
    generate_qc.generate_html(library_id, output_dir)

    # add qc report
    filepath = os.path.join(
        output_dir,
        "libraries",
        library_id,
        f"QC_report_{library_id}.html",
    )
    jira_filename = f"{jira_ticket}_{ref_genome}_QC_report.html"
    add_attachment(library_ticket, filepath, jira_filename, update=update)


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

    library = args['library_id']
    local_results = {
        "cellranger_filepath": os.path.join(runs_dir, library, f"{library}.tar.gz"),
        "rdata_filepath": os.path.join(runs_dir, ".cache", library, f"{library}_qcd.rdata"),
        "rdataraw_filepath": os.path.join(runs_dir, ".cache", library, f"{library}.rdata"),
        "report_filepath": os.path.join(results_dir, f"{library}.tar.gz"),
        "bam_filepath": os.path.join(runs_dir, library, "bams.tar.gz",),
    }

    log_utils.sentinel(
        'Uploading results to Azure',
        tantalus_analysis.upload_tenx_result,
        **local_results,
        update=run_options['update']
    )

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
        print(f"JIRA IS {jira}")
        update_jira_tenx(jira, library_pk)

    add_report(
        library_pk=library_pk,
        jira_ticket=jira,
        runs_dir=runs_dir,
        results_dir=results_dir,
        ref_genome=args['ref_genome'],
        update=run_options['update'],
    )

    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))


default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'normal_config_tenx.json')


@click.group()
def cli():
    pass


@cli.command("run_new")
@click.argument("library_id")
@click.option('--version')
@click.option('--no_download', is_flag=True)
@click.option('--data_dir')
@click.option('--runs_dir')
@click.option('--results_dir')
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
def run_new(
    library_id,
    version,
    jira=None,
    no_download=False,
    config_filename=None,
    data_dir=None,
    runs_dir=None,
    results_dir=None,
    **run_options,
):
    # get tenx library info
    library = colossus_api.get(
        "tenxlibrary",
        name=library_id,
    )
    sample = library["sample"]["sample_id"]
    library_ticket = library["jira_ticket"]

    # create jira ticket
    jira_ticket = create_analysis_jira_ticket(library_id, sample, library_ticket)

    # create colossus analysis
    colossus_analysis, _ = colossus_api.create(
        "tenxanalysis",
        fields={
            "version": "vm",
            "jira_ticket": jira_ticket,
            "run_status": "idle",
            "tenx_library": library["id"],
            "submission_date": str(datetime.date.today()),
            "tenxsequencing_set": [],
        },
        keys=["jira_ticket"],
    )

    # create tantalus analysis
    analysis = create_tenx_analysis_from_library(jira_ticket, library["name"])

    # check if analysis with same inputs has already been ran under different ticket
    if analysis["jira_ticket"] != jira_ticket:
        log.info(f"Analysis with same input datasets has already been ran under {analysis['jira_ticket']}")
        # remove jira ticket
        delete_ticket(jira_ticket)
        # remove colossus analysis
        colossus_api.delete("tenxanalysis", colossus_analysis["id"])
    else:
        # load config
        config = file_utils.load_json(default_config)

        run(
            analysis["id"],
            config["version"],
            jira=jira_ticket,
            no_download=no_download,
            config_filename=config_filename,
            data_dir=data_dir,
            runs_dir=runs_dir,
            results_dir=results_dir,
            **run_options,
        )


@cli.command("run_all")
@click.option('--version')
@click.option('--no_download', is_flag=True)
@click.option('--data_dir')
@click.option('--runs_dir')
@click.option('--results_dir')
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
        last_updated__gte=str(datetime.datetime.now() - datetime.timedelta(days=7)),
    )
    # get latest analyses with status error
    analyses_error = tantalus_api.list(
        "analysis",
        analysis_type__name="tenx",
        status="error",
        last_updated__gte=str(datetime.datetime.now() - datetime.timedelta(days=7)),
    )

    for analysis in chain(analyses_ready, analyses_error):
        jira_ticket = analysis["jira_ticket"]

        run(
            analysis["id"],
            config["version"],
            jira=jira_ticket,
            no_download=no_download,
            config_filename=config_filename,
            data_dir=data_dir,
            runs_dir=runs_dir,
            results_dir=results_dir,
            **run_options,
        )


@cli.command("run_single")
@click.option('--analysis_ids', '-a', type=int, multiple=True, default=[])
@click.option('--version')
@click.option('--jira', nargs=1)
@click.option('--no_download', is_flag=True)
@click.option('--data_dir')
@click.option('--runs_dir')
@click.option('--results_dir')
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
#@click.option('--flowcell_id', type=str, default='')
#@click.option('--lane_number', type=str, default='')
def run_single(
    analysis_ids,
    version,
    jira=None,
    no_download=False,
    config_filename=None,
    data_dir=None,
    runs_dir=None,
    results_dir=None,
    **run_options,
):
    if(len(analysis_ids) == 0):
        raise ValueError("Specify at least one --analysis_id!")

    config = file_utils.load_json(default_config)

    for analysis_id in analysis_ids:
        run(
            analysis_id,
            config["version"],
            jira=jira,
            no_download=no_download,
            config_filename=config_filename,
            data_dir=data_dir,
            runs_dir=runs_dir,
            results_dir=results_dir,
            **run_options,
        )

    os.system("az vm deallocate --resource-group bccrc-pr-cc-scrna-rg --name scrna-pipeline2 --subscription 436b89a7-3b73-4644-a97b-949c4d0f19f5")



def run(
    analysis_id,
    version,
    jira=None,
    no_download=False,
    config_filename=None,
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

    if analysis["status"] in ("running", "complete"):
        raise Exception(f'analysis {analysis_id} already {analysis["status"]}')

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

    if run_options["testing"]:
        ref_genome = "test"

    elif run_options["ref_genome"]:
        ref_genome = run_options["ref_genome"]
        log.info("Default reference genome being overwritten; using {}".format(run_options["ref_genome"]))

    else:
        ref_genome = get_ref_genome(library, is_tenx=True)

    # SCNRA pipeline working directories
    if data_dir is None:
        data_dir = os.path.join("/datadrive", "data")
    if runs_dir is None:
        runs_dir = os.path.join("/datadrive", "runs", "_".join([ref_genome, library_id]))
    if results_dir is None:
        results_dir = os.path.join("/datadrive", "results", "_".join([ref_genome, library_id]))

    reference_dir = os.path.join("/datadrive", "reference")
    
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
