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

import datamanagement.templates as templates
from datamanagement.transfer_files import transfer_dataset

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError

from workflows.utils import file_utils, log_utils, colossus_utils
from workflows.utils.jira_utils import update_jira_dlp, add_attachment, comment_jira

from workflows.models import AnalysisInfo, AlignAnalysis, HmmcopyAnalysis, AnnotationAnalysis, Results

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


def attach_qc_report(jira, library_id, storages):

    storage_client = tantalus_api.get_storage_client(storages["remote_results"])
    results_dataset = tantalus_api.get("resultsdataset", name="{}_annotation".format(jira))

    qc_filename = "{}_QC_report.html".format(library_id)
    jira_qc_filename = "{}_{}_QC_report.html".format(library_id, jira)

    qc_report = list(
        tantalus_api.get_dataset_file_resources(
            results_dataset["id"],
            "resultsdataset",
            {"filename__endswith": qc_filename},
        ))

    blobname = qc_report[0]["filename"]
    local_dir = os.path.join("qc_reports", jira)
    local_path = os.path.join(local_dir, jira_qc_filename)
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # Download blob
    blob = storage_client.blob_service.get_blob_to_path(
        container_name="results",
        blob_name=blobname,
        file_path=local_path,
    )

    # Get library ticket
    analysis = colossus_api.get("analysis_information", analysis_jira_ticket=jira)
    library_ticket = analysis["library"]["jira_ticket"]

    log.info("Adding report to parent ticket of {}".format(jira))
    add_attachment(library_ticket, local_path, jira_qc_filename)


def get_contamination_comment(jira_ticket):
    jira_user = os.environ['JIRA_USERNAME']
    jira_password = os.environ['JIRA_PASSWORD']
    jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(jira_user, jira_password))

    issue = jira_api.issue(jira_ticket)
    library_ticket_id = issue.fields.parent.key

    comment = f"""
    Hi [~jedwards], [~jbwang], [~jbiele],
    
    This is an automated message. \n 
    The pipeline detected that over 20% of this library's cell are contaminated. Would you still like to proceed with analysis?

    Best,

    [~{jira_user}]
    """

    comment_jira(library_ticket_id, comment)


def load_ticket(jira):
    log.info(f"Loading {jira} into Montage")
    try:
        # TODO: add directory in config
        subprocess.call([
            'ssh',
            '-t',
            'loader',
            f"bash /home/uu/montageloader2_flora/load_ticket.sh {jira}",
        ])
    except Exception as e:
        raise Exception(f"failed to load ticket: {e}")

    log.info(f"Successfully loaded {jira} into Montage")


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
        storages,
        job_subdir,
        analysis_info,
        analysis_type,
        output_dir,
):
    start = time.time()

    if analysis_type == "align":
        tantalus_analysis = AlignAnalysis(
            jira,
            version,
            args,
            run_options,
            storages=storages,
            update=run_options['update'],
        )
    elif analysis_type == "hmmcopy":
        tantalus_analysis = HmmcopyAnalysis(
            jira,
            version,
            args,
            run_options,
            storages=storages,
            update=run_options['update'],
        )
    elif analysis_type == "annotation":
        tantalus_analysis = AnnotationAnalysis(
            jira,
            version,
            args,
            run_options,
            storages=storages,
            update=run_options['update'],
        )
    else:
        raise Exception(f"{analysis_type} is not a valid analysis type")

    if storages["working_datasets"] != storages["remote_datasets"]:
        log_utils.sentinel(
            'Transferring input datasets from {} to {}'.format(
                storages["remote_datasets"],
                storages["working_datasets"],
            ),
            transfer_inputs,
            tantalus_analysis.get_input_datasets(),
            tantalus_analysis.get_input_results(),
            storages["remote_datasets"],
            storages["working_datasets"],
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
        analysis_info.set_run_status()
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

        if run_options['saltant']:
            context_config_file = config['context_config_file']['saltant']
        else:
            context_config_file = config['context_config_file']['sisyphus']

        log_utils.sentinel(
            'Running single_cell qc',
            run_pipeline,
            results_dir=results_dir,
            analysis_type=analysis_type,
            scpipeline_dir=scpipeline_dir,
            tmp_dir=tmp_dir,
            tantalus_analysis=tantalus_analysis,
            args=args,
            run_options=run_options,
            inputs_yaml=inputs_yaml,
            context_config_file=context_config_file,
            docker_env_file=config['docker_env_file'],
            docker_server=config['docker_server'],
            output_dir=output_dir,
            dirs=dirs,
        )

    except Exception:
        tantalus_analysis.set_error_status()
        analysis_info.set_error_status()
        if analysis_type == "align":
            analysis_type = "alignment"
        pipeline_log = os.path.join(scpipeline_dir, analysis_type, "log", "latest", "pipeline.log")

        if not run_options["skip_pipeline"] or not run_options["override_contamination"]:
            with open(pipeline_log) as f:
                lines = f.read()
                if "LibraryContaminationError" in lines:
                    log.error("LibraryContaminationError: over 20% of cells are contaminated")

                    get_contamination_comment(jira)

        raise Exception("pipeline failed")

    tantalus_analysis.set_complete_status()

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

    if storages["working_datasets"] != storages["remote_datasets"] and output_dataset_ids != []:
        log_utils.sentinel(
            'Transferring input datasets from {} to {}'.format(
                storages["working_datasets"],
                storages["remote_datasets"],
            ),
            transfer_inputs,
            output_dataset_ids,
            output_results_ids,
            storages["remote_datasets"],
            storages["working_datasets"],
        )

    # Update Jira ticket
    analysis_info.set_finish_status(analysis_type)
    if analysis_type == "annotation" and not run_options["is_test_run"]:
        update_jira_dlp(jira, args['aligner'])
        attach_qc_report(jira, args["library_id"], storages)
        analysis_info.set_finish_status()

    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))

    if analysis_type == "annotation":
        load_ticket(jira)


default_config = os.path.join(os.path.dirname(os.path.realpath(__file__)), 'config', 'normal_config.json')


@click.command()
@click.argument('jira')
@click.argument('version')
@click.argument('library_id')
@click.argument('aligner', type=click.Choice(['A', "M"]))
@click.argument('analysis_type', type=click.Choice(['align', 'hmmcopy', 'annotation']))
@click.option('--load_only', is_flag=True)
@click.option('--gsc_lanes')
@click.option('--brc_flowcell_ids')
@click.option('--config_filename')
@click.option('--skip_pipeline', is_flag=True)
@click.option('--skip_missing', is_flag=True)
@click.option('--local_run', is_flag=True)
@click.option('--update', is_flag=True)
@click.option('--override_contamination', is_flag=True)
@click.option('--is_test_run', is_flag=True)
@click.option('--sc_config')
@click.option('--inputs_yaml')
@click.option('--index_sequences', multiple=True)
@click.option('--clean', is_flag=True)
@click.option('--tag', type=str, default='')
@click.option('--smoothing', default='modal', type=click.Choice(['modal', 'loess']))
@click.option('--interactive', is_flag=True)
@click.option('--sisyphus_interactive', is_flag=True)
@click.option('--alignment_metrics')
@click.option('--jobs', type=int, default=1000)
@click.option('--saltant', is_flag=True)
def main(jira,
         version,
         library_id,
         aligner,
         analysis_type,
         load_only=False,
         gsc_lanes=None,
         brc_flowcell_ids=None,
         config_filename=None,
         **run_options):

    if load_only:
        load_ticket(jira)
        return "complete"

    if config_filename is None:
        config_filename = default_config

    if not templates.JIRA_ID_RE.match(jira):
        raise Exception(f'Invalid SC ID: {jira}')

    aligner_map = {
        'A': 'BWA_ALN_0_5_7',
        'M': 'BWA_MEM_0_7_6A',
    }

    aligner = aligner_map[aligner]

    # Get reference genome
    library_info = colossus_api.get("library", pool_id=library_id)
    reference_genome = colossus_utils.get_ref_genome(library_info)

    if gsc_lanes is not None:
        gsc_lanes = gsc_lanes.split(',')

    if brc_flowcell_ids is not None:
        brc_flowcell_ids = brc_flowcell_ids.split(',')

    config = file_utils.load_json(config_filename)

    job_subdir = jira + run_options['tag']

    run_options['job_subdir'] = job_subdir

    pipeline_dir = os.path.join(
        tantalus_api.get("storage", name=config["storages"]["local_results"])["storage_directory"], job_subdir)

    # get storage account for pipeline output results
    working_results_storage = config["storages"]["working_results"]
    storage_result_prefix = tantalus_api.get_storage_client(working_results_storage).prefix

    # get storage account for pipeline output datasets
    working_datasets_storage = config["storages"]["working_datasets"]
    results_storage_prefix = tantalus_api.get_storage_client(working_datasets_storage).prefix

    results_dir = os.path.join(results_storage_prefix, job_subdir)
    scpipeline_dir = os.path.join('singlecelllogs', 'pipeline', job_subdir)
    tmp_dir = os.path.join('singlecelltemp', 'temp', job_subdir)

    output_dir = os.path.join(results_storage_prefix, jira, "results", analysis_type)
    log_utils.init_pl_dir(pipeline_dir, run_options['clean'])

    log_file = log_utils.init_log_files(pipeline_dir)
    log_utils.setup_sentinel(run_options['sisyphus_interactive'], os.path.join(pipeline_dir, analysis_type))

    # Create analysis information object on Colossus
    analysis_info = AnalysisInfo(jira)

    log.info('Library ID: {}'.format(library_id))

    library_id = library_id
    if run_options["is_test_run"]:
        library_id += "TEST"

    args = {}
    args['aligner'] = aligner
    args['ref_genome'] = reference_genome
    args['library_id'] = library_id
    args['gsc_lanes'] = gsc_lanes
    args['brc_flowcell_ids'] = brc_flowcell_ids
    args['smoothing'] = run_options['smoothing']

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
        config['storages'],
        job_subdir,
        analysis_info,
        analysis_type,
        output_dir,
    )


if __name__ == '__main__':
    main()
