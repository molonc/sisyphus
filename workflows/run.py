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

from utils import (saltant_utils,
                   file_utils, log_utils, file_transfers)
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
    args['jobs_dir'] = config['jobs_dir']
    args["library_id"] = library_id

    dataset_ids = set()
    result_ids = set()

    for analysis_type in ('align', 'hmmcopy'):
        if analysis_type == 'align':
            align_analysis = AlignAnalysis(args, update=args['update'])
            tantalus_analysis = align_analysis
        elif analysis_type == 'hmmcopy':
            tantalus_analysis = HmmcopyAnalysis(align_analysis, args, update=args['update'])
        else:
            raise ValueError()

        if not args['no_transfer'] and not args['shahlab_run']:
            sentinel(
                'Transferring FASTQ files from shahlab to singlecellblob',
                file_transfers.transfer_files,
                args['jira'],
                config,
                storages['local_inputs'],
                storages['remote_inputs'],
                tantalus_analysis.search_input_datasets(),
            )

        if args['inputs_yaml'] is None:
            local_results_storage = tantalus_api.get('storage', name=config['storages']['local_results'])['storage_directory']
            inputs_yaml = os.path.join(local_results_storage, job_subdir, 'inputs.yaml')
            sentinel(
                'Generating inputs yaml',
                align_analysis.generate_inputs_yaml,
                inputs_yaml,
                storages['working_inputs'],
            )
        else:
            inputs_yaml = args['inputs_yaml']

        align_analysis.check_inputs_yaml(inputs_yaml)
        tantalus_analysis.add_inputs_yaml(inputs_yaml, storages['local_results'], update=args['update'])
        dataset_ids.update(tantalus_analysis.analysis['input_datasets'])

        if analysis_type == 'align' and args['no_align']:
            continue

        if analysis_type == 'hmmcopy' and args['no_hmmcopy']:
            continue

        try:
            tantalus_analysis.set_run_status()

            local_inputs_storage = tantalus_api.get('storage', name=config['storages']['local_inputs'])['storage_directory']
            local_results_storage = tantalus_api.get('storage', name=config['storages']['local_results'])['storage_directory']

            if args["testing"]:
                run_pipeline = launch_pipeline.run_pipeline2
            else:
                run_pipeline = launch_pipeline.run_pipeline

            sentinel(
                'Running single_cell {}'.format(analysis_type),
                run_pipeline,
                tantalus_analysis,
                analysis_info,
                inputs_yaml,
                docker_env_file=config['docker_env_file'],
                dirs=[
                    pipeline_dir,
                    local_inputs_storage,
                    local_results_storage,
                ],
            )
        except Exception:
            tantalus_analysis.set_error_status()
            raise

        if analysis_type == 'align':
            sentinel(
                'Creating output bam datasets',
                align_analysis.create_output_datasets,
                storages['working_inputs'],
                tag_name=args['bams_tag'],
                update=args['update'],
            )

        tantalus_results = tantalus_analysis.create_output_results(
            storages['working_results'],
            pipeline_dir,
            update=args['update'],
        )

        result_ids.add(tantalus_results.get_id())
        tantalus_analysis.set_complete_status()

    if args['shahlab_run']:
        sentinel(
            'Transferring input datasets from shahlab to singlecellblob',
            file_transfers.transfer_files,
            args['jira'],
            config,
            storages['working_inputs'],
            storages['remote_inputs'],
            list(dataset_ids),
        )

        sentinel(
            'Transferring results from shahlab to singlecellblob',
            file_transfers.transfer_files,
            args['jira'],
            config,
            storages['working_results'],
            storages['remote_results'],
            list(result_ids),
            results=True,
        )
    
    analysis_info.set_finish_status()
    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))
    


def main():
    args = arguments.get_args()

    if not templates.JIRA_ID_RE.match(args['jira']):
        raise Exception('Invalid SC ID:', jira)

    config = file_utils.load_json(args['config'])

    job_subdir = args['jira'] + args['tag']

    pipeline_dir = os.path.join(
        config['jobs_dir'], job_subdir)

    # TODO: At this stage i want to specify:
    # results_subdir: the subdirectory in tantalus storage for the results for this ticket, based on jira ticket
    # results_storage: storage on the vm or local system for storing the results
    # inputs_storage: storage on the local system for storing the input datasets
    # pipeline_dir: location of temporary files for pypeliner
    
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
        traceback.print_exc()
        if args['shahlab_run']:
            log_utils.send_logging_email(config['email'], '{} error'.format(args['jira']))
        raise Exception('pipeline failed')


if __name__ == '__main__':
    main()
