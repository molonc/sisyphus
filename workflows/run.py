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
from models import AnalysisInfo, AlignAnalysis, HmmcopyAnalysis, PseudoBulkAnalysis, Results


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
        results_dir,
        scpipeline_dir,
        tmp_dir,
        analysis_info,
        analysis_type,
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

    results_ids = set()

    if analysis_type == 'align':
        tantalus_analysis = AlignAnalysis(args, storages=storages, update=args['update'])
    elif analysis_type == 'hmmcopy':
        tantalus_analysis = HmmcopyAnalysis(args, storages=storages, update=args['update'])
    elif analysis_type == 'pseudobulk':
        tantalus_analysis = PseudoBulkAnalysis(args, update=args['update'])
    else:
        raise ValueError()

    try:
        # FIXME: if inputs exist in working_inputs, then we iterate over the file instances twice
        input_file_instances = tantalus_analysis.get_input_file_instances(storages["working_inputs"])
    except NotFoundError:
        # Start a file transfer to get the inputs
        tag_name = '_'.join([args['jira'], storages['remote_inputs'], "import"])
        tantalus_api.tag(
            tag_name,
            sequencedataset_set=tantalus_analysis.search_input_datasets(args))

        sentinel(
            'Transferring {} input files from {} to {}'.format(
                analysis_type, storages["remote_inputs"], storages["working_inputs"]),
            transfer_files,
            tag_name, 
            storages["remote_inputs"],
            storages["working_inputs"])
        
    if args['inputs_yaml'] is None:
        local_results_storage = tantalus_api.get(
            'storage', 
            name=storages['local_results'])['storage_directory']

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

    if args["_".join(["no", analysis_type])]:
        continue

    try:
        tantalus_analysis.set_run_status()

        run_pipeline = tantalus_analysis.run_pipeline(args)

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
            tantalus_analysis=tantalus_analysis,
            analysis_info=analysis_info,
            inputs_yaml=inputs_yaml,
            docker_env_file=config['docker_env_file'],
            dirs=dirs,
        )
    except Exception:
        tantalus_analysis.set_error_status()
        raise

    if analysis_type == 'align':
        tag_name = "_".join([args["jira"], storages["working_inputs"], "bams"])

        sentinel(
            'Creating output bam datasets',
            align_analysis.create_output_datasets,
            update=args['update'],
            tag_name=tag_name,
        )

        sentinel(
            "Transferring BAM files from {} to {}".format(
                storages["working_inputs"], storages["remote_inputs"]),
            transfer_files,
            tag_name,
            storages["working_inputs"],
            storages["remote_inputs"])

    tantalus_results = tantalus_analysis.create_output_results(
        pipeline_dir,
        update=args['update'],
    )

    results_ids.add(tantalus_results.get_id())
    tantalus_analysis.set_complete_status()

    tag_name = '_'.join([args['jira'], storages['working_results'], "results"])
    tantalus_api.tag(
        tag_name,
        resultsdataset_set=list(results_ids),
    )

    sentinel(
        "Transferring results from {} to {}".format(
            storages["working_results"], storages['remote_results']),
        transfer_files,
        tag_name,
        storages['working_results'],
        storages['remote_results'])

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
    
    results_dir = os.path.join('singlecelldata', 'results', job_subdir, 'results')

    scpipeline_dir = os.path.join('singlecelldata', 'pipeline', job_subdir)

    tmp_dir = os.path.join('singlecelldata', 'temp', job_subdir)

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

    analysis_type = args['analysis_type']

    log.info('Library ID: {}'.format(analysis_info.chip_id))

    start_automation(args, config, pipeline_dir, results_dir, scpipeline_dir, tmp_dir, analysis_info, config['storages'], job_subdir)


if __name__ == '__main__':
    args = arguments.get_args()
    main(args)