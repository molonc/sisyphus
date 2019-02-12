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
    aligner_map = {
        'BWA_ALN_0_5_7':    'bwa-aln',
        'BWA_MEM_0_7_6A':   'bwa-mem',
    }

    reference_genome_map = {
        'HG19': 'grch37',
        'MM10': 'mm10',
    }

    if key == 'aligner' and value in aligner_map:
        value = aligner_map[value]

    elif key == 'reference' and value in reference_genome_map:
        value = reference_genome_map[value]

    if (value is not None) and (config[key] != value):
        config[key] = value
        return True
    return False

def get_config_override(analysis_info):
    """
    Get a dictionary of default configuration options that
    override existing single cell pipeline configuration options.

    Args:
        analysis_info (AnalysisInformation)
    """
    config = {
        'cluster':              'azure',
        'aligner':              'bwa-mem',
        'reference':            'grch37',
        'smoothing_function':   'modal',
        'containers':           {"mounts": ["/refdata", "/datadrive", "/mnt", "/home"]}
    }

    cluster = 'azure'
    update_config(config, 'cluster', cluster)
    update_config(config, 'aligner', analysis_info.aligner)
    update_config(config, 'reference', analysis_info.reference_genome)
    update_config(config, 'smoothing_function', analysis_info.smoothing)
    return config


def get_config_string(analysis_info):
    config_string = json.dumps(get_config_override(analysis_info))
    config_string = ''.join(config_string.split())  # Remove all whitespace
    return r"'{}'".format(config_string)


def run_pipeline2(*args, **kwargs):
    print args, kwargs

def run_pipeline(
        results_dir,
        scpipeline_dir,
        tmp_dir,
        tantalus_analysis,
        #HACK
        #analysis_info,
        inputs_yaml,
        context_config_file,
        docker_env_file,
        max_jobs='400',
        dirs=()):

    args = tantalus_analysis.args
    #HACK
    config_override_string = ''#get_config_string(analysis_info)
    
    run_cmd = [
        'single_cell',          'multi_sample_pseudo_bulk', #tantalus_analysis.analysis_type,
        '--input_yaml',         inputs_yaml,
        '--out_dir',            results_dir,
        #HACK
        #'--library_id',         args['library_id'],
        #'--config_override',    config_override_string,
        '--tmpdir',             tmp_dir,
        '--maxjobs',            str(max_jobs),
        '--nocleanup',
        '--sentinal_only',
        '--loglevel',           'DEBUG',
        '--pipelinedir',        scpipeline_dir,
        '--context_config',     context_config_file,
    ]

    if args['local_run']:
        run_cmd += ["--submit", "local"]

    else:
        run_cmd += [
            '--submit',         'azurebatch',
            '--storage',        'azureblob',
        ]

    # Append docker command to the beginning
    docker_cmd = [
        'docker', 'run', '-w', '$PWD',
        '-v', '$PWD:$PWD',
        '-v', '/var/run/docker.sock:/var/run/docker.sock',
        '-v', '/usr/bin/docker:/usr/bin/docker',
        '--rm',
        '--env-file', docker_env_file,
    ]

    for d in dirs:
        docker_cmd.extend([
            '-v', '{d}:{d}'.format(d=d),
        ])

    docker_cmd.append(
        'shahlab.azurecr.io/scp/single_cell_pipeline:{}'.format(args['version'])
    )

    run_cmd = docker_cmd + run_cmd


    has_classifier = StrictVersion(args['version'].strip('v')) >= StrictVersion('0.1.5')
    if (tantalus_analysis.analysis_type == 'hmmcopy') and (has_classifier):
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

    run_cmd_string = r' '.join(run_cmd)
    log.debug(run_cmd_string)
    subprocess.check_call(run_cmd_string, shell=True)
