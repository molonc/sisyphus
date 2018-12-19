#!/usr/bin/env python
import os
import re
import sys
import time
import logging
import subprocess
import traceback
from itertools import chain

import arguments
import datamanagement.templates as templates
import launch_pipeline
import generate_inputs
from dbclients.tantalus import TantalusApi
from workflows.utils import saltant_utils
from workflows.utils import file_utils
from workflows.utils import log_utils
from datamanagement.transfer_files import transfer_files
from dbclients.basicclient import NotFoundError

from utils.log_utils import sentinel
from models import AnalysisInfo, AlignAnalysis, HmmcopyAnalysis, Results


log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False

tantalus_api = TantalusApi()

def start_automation(
        args,
        config,
        pipeline_dir,
        analysis_info,
        storages,
        job_subdir,
):
    start = time.time()

    library_id = analysis_info.chip_id
    if args["integrationtest"]:
        library_id += "TEST"

    args['ref_genome'] = analysis_info.reference_genome
    args['aligner'] = analysis_info.aligner
    args['job_subdir'] = job_subdir
    args["library_id"] = library_id

    dataset_ids = set()
    result_ids = set()

    for analysis_type in ('align', 'hmmcopy'):
        if analysis_type == 'align':
            align_analysis = AlignAnalysis(args, storages=storages, update=args['update'])
            tantalus_analysis = align_analysis
        elif analysis_type == 'hmmcopy':
            tantalus_analysis = HmmcopyAnalysis(align_analysis, args, storages=storages, update=args['update'])
        else:
            raise ValueError()

        try:
            # FIXME: if inputs exist in working_inputs, then we iterate over the file instances twice
            input_file_instances = tantalus_analysis.get_input_file_instances(storages["working_inputs"])
        except NotFoundError:
            # Start a file transfer to get the inputs
            tag_name = '_'.join([args['jira'], storages['remote_inputs']])
            tantalus_api.tag(
                tag_name,
                sequencedataset_set=tantalus_analysis.search_input_datasets(args))

            sentinel(
                'Transferring FASTQ files from {} to {}'.format(storages["remote_inputs"], storages["working_inputs"]),
                transfer_files,
                tag_name, 
                storages["remote_inputs"],
                storages["working_inputs"]
            )
            
        if args['inputs_yaml'] is None:
            local_results_storage = tantalus_api.get('storage', name=storages['local_results'])['storage_directory']

            inputs_yaml = os.path.join(local_results_storage, job_subdir, 'inputs.yaml')
            sentinel(
                'Generating inputs yaml',
                align_analysis.generate_inputs_yaml,
                args,
                inputs_yaml,
            )
        else:
            inputs_yaml = args['inputs_yaml']

        align_analysis.check_inputs_yaml(inputs_yaml)
        tantalus_analysis.add_inputs_yaml(inputs_yaml, update=args['update'])
        dataset_ids.update(tantalus_analysis.analysis['input_datasets'])

        if analysis_type == 'align' and args['no_align']:
            continue

        if analysis_type == 'hmmcopy' and args['no_hmmcopy']:
            continue

        try:
            tantalus_analysis.set_run_status()

            if args["testing"]:
                run_pipeline = launch_pipeline.run_pipeline2
            else:
                run_pipeline = launch_pipeline.run_pipeline

            dirs = [
                pipeline_dir, 
                config['docker_path'],
                config['docker_sock_path'],
            ]
            # Pass all server storages to docker
            for storage_name in storages.itervalues():
                storage = tantalus_api.get('storage', name=storage_name)
                if storage['storage_type'] == 'server':
                    dirs.append(storage['storage_directory'])

            sentinel(
                'Running single_cell {}'.format(analysis_type),
                run_pipeline,
                tantalus_analysis,
                analysis_info,
                inputs_yaml,
                docker_env_file=config['docker_env_file'],
                dirs=dirs,
            )
        except Exception:
            tantalus_analysis.set_error_status()
            raise

        if analysis_type == 'align':
            sentinel(
                'Creating output bam datasets',
                align_analysis.create_output_datasets,
                tag_name=args['bams_tag'],
                update=args['update'],
            )

        tantalus_results = tantalus_analysis.create_output_results(
            pipeline_dir,
            update=args['update'],
        )

        result_ids.add(tantalus_results.get_id())
        tantalus_analysis.set_complete_status()

    if storages['working_results'] != storages['remote_results']:
        # Tag the result datasets
        # Transfer them from working to remote

        tag_name = '_'.join([args['jira'], storages['working_results']])
        tantalus_api.tag(
            tag_name,
            resultsdataset_set=results_ids
        )
        sentinel(
            'Transferring results from shahlab to singlecellblob',
            transfer_files,
            tag_name,
            storages['working_results'],
            storages['remote_results']
        )

    analysis_info.set_finish_status()
    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))


def main(args):
    if not templates.JIRA_ID_RE.match(args['jira']):
        raise Exception('Invalid SC ID:'.format(args['jira']))

    config = file_utils.load_json(args['config'])

    job_subdir = args['jira'] + args['tag']

    pipeline_dir = os.path.join(
        tantalus_api.get("storage", name=config["storages"]["local_results"])["storage_directory"], 
        job_subdir)
    
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
    analysis_info = AnalysisInfo(
        args['jira'],
        log_file,
        args,
        update=args['update'],
    )

    log.info('Library ID: {}'.format(analysis_info.chip_id))

    try:
        start_automation(args, config, pipeline_dir, analysis_info, config['storages'], job_subdir)
    except Exception:
        if args['shahlab_run']:
            log_utils.send_logging_email(config['email'], '{} error'.format(args['jira']))
        raise


if __name__ == '__main__':
    args = arguments.get_args()
    main(args)

