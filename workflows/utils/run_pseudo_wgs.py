#!/usr/bin/env python
import argparse
import json
import os
import re
import subprocess
import time
from datetime import datetime
import pytz
import logging
import requests
import pandas as pd
import yaml

from analysis_information import AnalysisInfo
from utils import *
import azure_run
import generate_pipeliner_inputs as gpi
import run_bcl2fastq as rbcl
import tantalus
from file_transfers import archive_ftp, archive_pipeline, archive_results
from tantalus import make_tantalus_query
from generate_pipeliner_inputs import get_locations_from_tantalus_bam_object
from generate_pipeliner_inputs import get_file_instance_path
from generate_pipeliner_inputs import generate_sample_info

tz = pytz.timezone('Canada/Pacific')
t = datetime.now(tz)

starttime = '{}-{}-{:02d}_{}-{}-{}'.format(t.hour, t.minute, t.second, t.month, t.day, t.year)

logging.getLogger('urllib3').setLevel(logging.INFO)

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
ch = logging.StreamHandler()
ch.setFormatter(formatter)
log.addHandler(ch)
log.propagate = False


def initialize_log_files(pl_dir):
    log_dir = os.path.join(pl_dir, 'logs')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    log_file = os.path.join(log_dir, '{}.log'.format(starttime))
    fh = logging.FileHandler(os.path.join(log_dir, '{}.log'.format(starttime)))
    fh.setFormatter(formatter)
    log.addHandler(fh)

    latest_file = os.path.join(log_dir, 'latest')
    if os.path.exists(latest_file):
        os.remove(latest_file)
    os.symlink(log_file, latest_file)

    return str(log_file)


def get_args():
    """Aquire arguments"""
    parser = argparse.ArgumentParser()
    parser.add_argument("analysis_type", help="Type of analysis to run",
        choices=('merge_bams', 'variant_calling', 'pseudo_wgs', 'split_normal', 'breakpoint_calling', 'germline_calling', 'copy_number_calling'))
    parser.add_argument("jira_id", help="The SC id ex. SC-401")
    parser.add_argument("--config", default='normal_config.json', help="The Location of the config file")
    parser.add_argument("--clean", help="Remove the old working directory, if present",action="store_true")
    parser.add_argument("--tag", default="", help="Appends the tag to the end of the working directory")
    parser.add_argument("--rerun", default=False, action="store_true")
    parser.add_argument("--matched_normal", help='matched normal filename')
    parser.add_argument("--normal_region_template", help='matched normal region template override')
    parser.add_argument("--normal_jira_id", help="The SC id ex. SC-401")
    parser.add_argument("--inputs_only", help='only generate inputs', action='store_true')

    args = parser.parse_args()

    return args


def get_config(path):
    with open(path,"r") as f:
        json_data = f.read()

    config = json.loads(json_data)
    return config


def init_pl_dir(jira_ticket, clean, jobs_dir, optional_appended_name=""):
    """Create a pipeline directory in the jobs dir
    Args:
        SC-code: the jira ticket associated with this run
        clean: boolean, if true, wipes directory if it already exists
    Returns:
        the locations of the newly created or preexisting jobs directory
    """
    log.debug("Cleaning working directory: " + str(clean))
    pl_dir = os.path.join(jobs_dir, jira_ticket + optional_appended_name)

    if clean:
        subprocess.check_call(["rm","-rf",pl_dir])
    subprocess.check_call(["mkdir","-p",pl_dir])

    return pl_dir


def get_inputs_yaml(yaml_filename, jira_id, colossus_api, location='singlecellblob'):
    params = {'tag_name': jira_id}
    bams = make_tantalus_query("dataset/bam_file", params)

    bam_paths = []

    for bam in bams:
        locations = get_locations_from_tantalus_bam_object(bam)

        if location not in locations:
            raise Exception('bam file {} expected in storage {}'.format(bam['id'], location))

        bam_filepath = get_file_instance_path(bam["bam_file"], location)

        index_sequence = list(set([rg['index_sequence'] for rg in bam['read_groups']]))
        library_ids = list(set([rg['dna_library']['library_id'] for rg in bam['read_groups']]))

        assert len(index_sequence) == 1
        assert len(library_ids) == 1

        bam_paths.append({
            "bam": str(bam_filepath).strip('/'),
            "index_sequence": index_sequence[0],
            "library_id": library_ids[0],
        })

    bam_paths = pd.DataFrame(bam_paths)

    sample_info = []
    for library_id in bam_paths['library_id'].unique():
        sample_info.append(generate_sample_info(library_id, None, colossus_api))
    sample_info = pd.concat(sample_info, ignore_index=True)

    bam_paths = bam_paths.merge(sample_info, on=['library_id', 'index_sequence'], how='left')

    if bam_paths['cell_id'].isnull().any():
        raise Exception('some missing cells')

    path_data = {}
    for idx, row in bam_paths.iterrows():
        path_data[str(row['cell_id'])] = {'bam': row['bam']}

    with open(yaml_filename, 'w') as f:
        yaml.dump(path_data, f, default_flow_style=False)


def run_pipeline(pl_dir, sample_info, fastq_info, chip_id, jobs, conda_env, pl_config_file, output_dir):
    """Build a run.sh script to run the pipeline and use it to run the pipeline
    Args:
        pl_dir: location of the working directory in which to run the pipeline
        sample_info: location of the sample_info file
        fastq_info: location of the fastq)info file
        chip_id: the inputs's associated chip_id
        jobs: max number of jobs to submit to the queue
        conda_env: name of the conda enviroment used to run the pipeline
    Returns:
        the locations of the run.sh script, the output of the pipeline, and the pipelines
        working directory in a tuple
    """
    working_dir = os.path.join(pl_dir, "working/")
    run_script = os.path.join(pl_dir,  "run.sh")

    with open(run_script,"w") as f:
        f.write("ulimit -u 16384\n")
        f.write("source activate {}\n".format(conda_env))
        string = "single_cell {} {} {} {} {} --tmpdir {} --loglevel DEBUG --submit asyncqsub --nocleanup \
            --maxjobs {} --nativespec ' -hard -q shahlab.q -P shahlab_high -V -l h_vmem={{mem}}G' --sentinal_only\n"
        run_string = string.format(
            sample_info,
            fastq_info,
            chip_id,
            output_dir,
            pl_config_file,
            working_dir,
            str(jobs))

        f.write(run_string)
    # No idea why this needs this all of a sudden
    exit_code = subprocess.check_call(["ssh","shahlab15","bash",run_script])
    if exit_code != 0:
        log.info("run_script: " + run_script)
        raise Exception("Pipeline failed with exit code {}".format(exit_code))


def main():
    start_time = time.time()

    args = get_args()
    conf = get_config(args.config)

    pl_dir = init_pl_dir(args.jira_id, args.clean, conf["jobs_dir"], args.tag)

    input_yaml_filename = os.path.join('/datadrive/testing/inputs', args.jira_id, 'inputs.yaml')
    subprocess.check_call(["mkdir", "-p", os.path.join('/datadrive/testing/inputs', args.jira_id)])

    if not os.path.exists(input_yaml_filename):
        get_inputs_yaml(input_yaml_filename, args.jira_id, conf["colossus_api"])
    else:
        print 'using existing inputs yaml: ', input_yaml_filename

    temp_container = 'temp'
    results_container = 'results'
    analysis_dir = '/datadrive'
    temp_dir = os.path.join(analysis_dir, temp_container)
    results_dir = os.path.join(analysis_dir, results_container)
    pipeline_dir = os.path.join('pipeline', args.analysis_type, args.jira_id)

    pipeline_version = '0_1_3'
    home_dir = '/datadrive/testing'

    config_filename = os.path.join(home_dir, 'pseudowgs/workflow_automation/config/grch37/azure/', 'single_cell.yaml')

    tumour_region_template = os.path.join('temp', args.jira_id, 'bams/tumour_merged_{region}.bam')
    normal_region_template = os.path.join('temp', args.jira_id, 'bams/normal_merged_{region}.bam')

    if args.normal_region_template:
        normal_region_template = args.normal_region_template

    subprocess.check_call(["mkdir", "-p", pipeline_dir])

    if args.analysis_type == 'variant_calling':
        command = ['single_cell',
            'variant_calling',
            '--tmpdir', os.path.join(temp_container, args.jira_id, 'tmp'),
            '--pipelinedir', pipeline_dir,
            '--maxjobs', '1000', '--nocleanup', '--loglevel', 'DEBUG', '--sentinal_only',
            '--storage', 'pypeliner.contrib.azure.blobstorage.AzureBlobStorage',
            '--submit', 'pypeliner.contrib.azure.batchqueue.AzureJobQueue',
            '--out_dir', os.path.join(results_container, args.jira_id, 'results'),
            '--input_yaml', input_yaml_filename,
            '--tumour_template', tumour_region_template,
            '--normal_template', normal_region_template,
        ]

        if args.matched_normal is not None:
            command.extend(['--matched_normal', args.matched_normal])

    elif args.analysis_type == 'breakpoint_calling':
        command = ['single_cell',
            'breakpoint_calling',
            '--tmpdir', os.path.join(temp_container, args.jira_id, 'tmp'),
            '--pipelinedir', pipeline_dir,
            '--maxjobs', '1000', '--nocleanup', '--loglevel', 'DEBUG', '--sentinal_only',
            '--storage', 'pypeliner.contrib.azure.blobstorage.AzureBlobStorage',
            '--submit', 'pypeliner.contrib.azure.batchqueue.AzureJobQueue',
            '--out_dir', os.path.join(results_container, args.jira_id, 'results'),
            '--input_yaml', input_yaml_filename,
            '--matched_normal', args.matched_normal,
        ]

    elif args.analysis_type == 'merge_bams':
        command = ['single_cell',
            'merge_bams',
            '--tmpdir', os.path.join(temp_container, args.jira_id, 'tmp'),
            '--pipelinedir', pipeline_dir,
            '--maxjobs', '1000', '--nocleanup', '--loglevel', 'DEBUG', '--sentinal_only',
            '--storage', 'pypeliner.contrib.azure.blobstorage.AzureBlobStorage',
            '--submit', 'pypeliner.contrib.azure.batchqueue.AzureJobQueue',
            '--out_dir', os.path.join(results_container, args.jira_id, 'results'),
            '--input_yaml', input_yaml_filename,
            '--merged_bam_template', tumour_region_template,
        ]

    elif args.analysis_type == 'copy_number_calling':
        if args.normal_jira_id is not None:
            normal_yaml_filename = os.path.join('/datadrive/testing/inputs', args.normal_jira_id, 'inputs.yaml')
        else:
            normal_yaml_filename = os.path.join('/datadrive/testing/inputs', args.jira_id, 'normal_inputs.yaml')
            with open(normal_yaml_filename, 'w') as f:
                f.write(yaml.dump({'normal': {'bam': args.matched_normal}}))
        command = ['single_cell',
            'copy_number_calling',
            '--tmpdir', os.path.join(temp_container, args.jira_id, 'tmp'),
            '--pipelinedir', pipeline_dir,
            '--maxjobs', '1000', '--nocleanup', '--loglevel', 'DEBUG',# '--sentinal_only',
            '--storage', 'pypeliner.contrib.azure.blobstorage.AzureBlobStorage',
            '--submit', 'pypeliner.contrib.azure.batchqueue.AzureJobQueue',
            '--out_dir', os.path.join(results_container, args.jira_id, 'results'),
            '--tumour_yaml', input_yaml_filename,
            '--normal_yaml', normal_yaml_filename,
            '--clone_id', args.jira_id,
        ]

    print ' '.join(command)
    if not args.inputs_only:
        subprocess.check_call(command)


if __name__ == '__main__':
    main()

