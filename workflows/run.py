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

from utils import (colossus_utils, saltant_utils,
                   file_utils, log_utils, file_transfers, bcl2fastq)
from utils.log_utils import sentinel
from models import AnalysisInfo, AlignAnalysis, HmmcopyAnalysis, Results


log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False


# TODO: remove
'''
def run_bcl2fastq_and_send_to_tantalus(flowcell_id, path_to_archive, run_id, library_id, shahlab_run=False):
    """
    Runs BCL2FASTQ on BCL files from the BRC and pushes them to Tantalus.
    Args:
        flowcell_id (str)
        path_to_archive (str)
        run_id (str)
        library_id (str)
        shahlab_run (bool)
    """
    bcl_directory = templates.BCL_DIR.format(run_id=run_id)
    if not os.path.exists(bcl_directory):
        os.makedirs(bcl_directory)

    fastq_directory = templates.FASTQ_DIR.format(run_id=run_id)
    if not os.path.exists(fastq_directory):
        os.makedirs(fastq_directory)

    sentinel(
        'Retrieving BCL files',
        bcl2fastq.retrive_bcl_files,
        path_to_archive,
        bcl_directory,
    )

    sentinel(
        'Getting samplesheet',
        bcl2fastq.get_samplesheet,
        bcl_directory,
        library_id,
        flowcell_id,
     )

    sentinel(
        'Running bcl2fastq',
        bcl2fastq.run_bcl2fastq,
        bcl_directory,
        fastq_directory,
        shahlab_run,
    )

    sentinel(
        'Sending bcl2fastq output to Tantalus',
        tantalus.push_bcl2fastq_paths,
        {flowcell_id: fastq_directory},
        'shahlab',
    )
'''


def get_jobs(args):
    jobs = []
    if not args['no_align']:
        jobs.append('align')
    if not args['no_hmmcopy']:
        jobs.append('hmmcopy')
    return jobs


def start_automation(args, config, pipeline_dir, analysis_info):
    start = time.time()

    if args['shahlab_run']:
        location = 'shahlab'
        inputs_yaml_storage = config['jobs_storage']
        results_storage = config['jobs_storage']
        storage_type = 'server'
    else:
        location = 'singlecellblob'
        inputs_yaml_storage = None
        results_storage = 'singlecellblob_results'
        storage_type = 'blob'

    args['ref_genome'] = analysis_info.reference_genome
    args['aligner'] = analysis_info.aligner
    args['library_id'] = analysis_info.chip_id
    args['jobs_dir'] = config['jobs_dir']

    config_override = launch_pipeline.get_config_override(analysis_info, args['shahlab_run'])

    dataset_ids = set()
    result_ids = set()

    for analysis_type in get_jobs(args):
        if analysis_type == 'align':
            align_analysis = AlignAnalysis(args)
            tantalus_analysis = align_analysis
        elif analysis_type == 'hmmcopy':
            tantalus_analysis = HmmcopyAnalysis(align_analysis, args)
        else:
            raise ValueError()

        if args['inputs_yaml'] is None:
            inputs_yaml = os.path.join(pipeline_dir, 'inputs.yaml')
            sentinel(
                'Generating inputs yaml',
                align_analysis.generate_inputs_yaml,
                inputs_yaml,
                location,
            )
        else:
            inputs_yaml = args['inputs_yaml']

        align_analysis.check_inputs_yaml(inputs_yaml)
        tantalus_analysis.add_inputs_yaml(inputs_yaml, inputs_yaml_storage)
        dataset_ids.update(tantalus_analysis.input_datasets)

        try:
            tantalus_analysis.set_run_status()
            sentinel(
                'Running single_cell {}'.format(analysis_type),
                launch_pipeline.run_pipeline,
                tantalus_analysis,
                analysis_type,
                inputs_yaml,
                container_version=args['container_version'],
                docker_env_file=config['docker_env_file'],
                config_override=config_override,
                max_jobs=args['jobs'],
            )
        except Exception:
            tantalus_analysis.set_error_status()
            raise

        if analysis_type == 'align':
            sentinel(
                'Creating output bam datasets',
                align_analysis.create_output_datasets,
                location,
                tag_name=args['bams_tag'],
            )

        tantalus_results = tantalus_analysis.create_output_results(
            results_storage, 
            pipeline_dir, 
            config_override['version'],
        )

        result_ids.add(tantalus_results.get_id())
        tantalus_analysis.set_complete_status()

    if args['shahlab_run']:
        sentinel(
            'Transferring input datasets and results to blob',
            file_transfers.transfer_files,
            args['jira'],
            config,
            'shahlab',
            'singlecellblob',
            dataset_ids.union(result_ids),
        )

    analysis_info.set_finish_status()
    log.info("Done!")
    log.info("------ %s hours ------" % ((time.time() - start) / 60 / 60))


def main():
    args = arguments.get_args()

    # Check arguments
    if not args["shahlab_run"] and args["container_version"] is None:
        raise Exception("Set --container_version when running on Azure")

    if not templates.JIRA_ID_RE.match(args['jira']):
        raise Exception('Invalid SC ID:', jira)

    config = file_utils.load_json(args['config'])

    template_args = {'jira': args['jira'], 'tag': args['tag']}

    if args['shahlab_run']:
        template = templates.SHAHLAB_PIPELINE_DIR
        template_args['jobs_dir'] = config['jobs_dir']
    else:
        template = templates.AZURE_PIPELINE_DIR

    pipeline_dir = template.format(**template_args)

    log_utils.init_pl_dir(pipeline_dir, args['clean'])

    log_file = log_utils.init_log_files(pipeline_dir)
    log_utils.setup_sentinel(args['sisyphus_interactive'], pipeline_dir)
    analysis_info = AnalysisInfo(
        args['jira'],
        log_file,
        args,
    )

    # TODO: kind of redundant
    blob_path = templates.BLOB_RESULTS_DIR.format(**template_args)
    analysis_info.update_results_path('blob_path', blob_path)

    log.info('Library ID: {}'.format(analysis_info.chip_id))

    try:
        start_automation(args, config, pipeline_dir, analysis_info)
    except Exception:
        traceback.print_exc()
        if args['shahlab_run']:
            log_utils.send_logging_email(config['email'], '{} Error')


if __name__ == '__main__':
    main()
