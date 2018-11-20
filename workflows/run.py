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

    dataset_ids = set()
    result_ids = set()

    for analysis_type in ('align', 'hmmcopy'):
        if analysis_type == 'align':
            align_analysis = AlignAnalysis(args)
            tantalus_analysis = align_analysis
        elif analysis_type == 'hmmcopy':
            tantalus_analysis = HmmcopyAnalysis(align_analysis, args)
        else:
            raise ValueError()

        if not args['no_transfer'] and not args['shahlab_run']:
            sentinel(
                'Transferring FASTQ files from shahlab to singlecellblob',
                file_transfers.transfer_files,
                args['jira'],
                config,
                'shahlab',
                'singlecellblob',
                tantalus_analysis.search_input_datasets(),
            )

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
        dataset_ids.update(tantalus_analysis.analysis['input_datasets'])

        if analysis_type == 'align' and args['no_align']:
            continue

        if analysis_type == 'hmmcopy' and args['no_hmmcopy']:
            continue

        try:
            tantalus_analysis.set_run_status()
            sentinel(
                'Running single_cell {}'.format(analysis_type),
                launch_pipeline.run_pipeline,
                tantalus_analysis,
                analysis_info,
                inputs_yaml,
                docker_env_file=config['docker_env_file'],
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
                update=True,
            )

        
        tantalus_results = tantalus_analysis.create_output_results(
            results_storage, 
            pipeline_dir, 
        )

        result_ids.add(tantalus_results.get_id())
        tantalus_analysis.set_complete_status()        

    if args['shahlab_run']:
        sentinel(
            'Transferring input datasets from shahlab to singlecellblob',
            file_transfers.transfer_files,
            args['jira'],
            config,
            'shahlab',
            'singlecellblob',
            list(dataset_ids),
        )

        sentinel(
            'Transferring results from shahlab to singlecellblob',
            file_transfers.transfer_files,
            args['jira'],
            config,
            config['jobs_storage'],
            'singlecellblob_results',
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
        update=True,
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
            log_utils.send_logging_email(config['email'], '{} error'.format(args['jira']))


if __name__ == '__main__':
    main()
