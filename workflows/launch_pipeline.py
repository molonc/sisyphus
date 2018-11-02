import os
import re
import sys
import json
import subprocess
import logging
from distutils.version import StrictVersion

from utils import log_utils
import datamanagement.templates as templates

log = logging.getLogger('sisyphus')

def update_config(config, key, value):
    """
    Update a field in the configuration.

    Args:
        config (dict)
        key (str)
        value (str)
    """
    if (value is not None) and (config[key] != value):
        config[key] = value
        return True
    return False


def get_config_override(analysis_info, shahlab_run):
    """
    Get a dictionary of default configuration options that
    override existing single cell pipeline configuration options.

    Args:
        analysis_info (AnalysisInformation)
        shahlab_run (bool)
    """
    config = {
        'cluster':              'azure',
        'aligner':              'bwa-mem',
        'reference':            'grch37',
        'smoothing_function':   'modal',
        'version':              '0.2.2',
    }

    cluster = 'shahlab' if shahlab_run else 'azure'
    update_config(config, 'cluster', cluster)
    update_config(config, 'aligner', analysis_info.aligner)
    update_config(config, 'reference', analysis_info.reference_genome)
    update_config(config, 'smoothing_function', analysis_info.smoothing)
    # FIXME: pipeline version in analysis information is not useful
    # update_config(config, 'version', analysis_info.pipeline_version)
    return config


def get_config_string(config):
    config_string = json.dumps(config)
    config_string = ''.join(config_string.split())  # Remove all whitespace
    return r"'{}'".format(config_string)


def run_pipeline2(*args, **kwargs):
    print args, kwargs

def run_pipeline(
        tantalus_analysis,
        analysis_type,
        inputs_yaml,
        container_version,
        docker_env_file,
        config_override=None,
        max_jobs=400):

    args = tantalus_analysis.args
    config_override_string = get_config_string(config_override)
    results_dir = tantalus_analysis.get_results_dir()
    tmp_dir = tantalus_analysis.get_tmp_dir()
    scpipeline_dir = tantalus_analysis.get_scpipeine_dir()

    run_cmd = [
        'single_cell',          analysis_type,
        '--input_yaml',         inputs_yaml,
        '--out_dir',            results_dir,
        '--library_id',         args['library_id'],
        '--config_override',    config_override_string,
        '--tmpdir',             tmp_dir,
        '--maxjobs',            str(max_jobs),
        '--nocleanup',
        '--sentinal_only',
        '--loglevel',           'DEBUG',
        '--pipelinedir',        scpipeline_dir,
    ]

    if args['shahlab_run']:
        run_cmd += [
            '--submit',         'asyncqsub',
            '--nativespec',     "' -hard -q shahlab.q -V -l h_vmem=20G -pe ncpus {ncpus}'",
        ]
    else:
        run_cmd += [
            '--submit',         'azurebatch',
            '--storage',        'azureblob',
        ]

        # Append docker command to the beginning
        docker_cmd = [
            'docker', 'run', '-w', '$PWD',
            '-v',   '/home:/home',
            '-v',   '/datadrive:/datadrive',
            '-v',   '/results:/results', '--rm',
            '-v',   '/var/run/docker.sock:/var/run/docker.sock',
            '-v',   '/usr/bin/docker:/usr/bin/docker',
            '--env-file', docker_env_file,
            'shahlab.azurecr.io/scp/single_cell_pipeline:{}'.format(container_version),
        ]

        run_cmd = docker_cmd + run_cmd


    has_classifier = StrictVersion(config_override['version']) >= StrictVersion('0.1.5')
    if (analysis_type == 'hmmcopy') and (has_classifier) and (classifier):
        alignment_metrics = templates.ALIGNMENT_METRICS.format(
            results_dir=results_dir,
            library_id=args['library_id'],
        )

        log.info('Using alignment metrics file {}'.format(alignment_metrics))
        run_cmd += ['--alignment_metrics', alignment_metrics]

    if args['sc_config'] is not None:
        run_cmd += ['--config_file', args['sc_config']]
    if args['interactive']:
        run_cmd += ['--interactive']
    if args['rerun']:
        run_cmd += ['--rerun']

    run_cmd_string = r' '.join(run_cmd)
    log.debug(run_cmd_string)
    subprocess.check_call(run_cmd_string, shell=True)
