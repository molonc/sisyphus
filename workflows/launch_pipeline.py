import os
import re
import sys
import json
import subprocess
import logging
from distutils.version import StrictVersion

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
        'BWA_ALN_0_5_7': 'bwa-aln',
        'BWA_MEM_0_7_6A': 'bwa-mem',
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


def get_config_override(args, run_options):
    """
    Get a dictionary of default configuration options that
    override existing single cell pipeline configuration options.

    Args:
        args (dict)
    """

    config = {
        'cluster': 'azure',
        'aligner': 'bwa-mem',
        'reference': 'grch37',
        'smoothing_function': 'modal',
    }

    if run_options["override_contamination"]:
        config["alignment"] = {'fastq_screen_params': {'strict_validation': False}}

    cluster = 'azure'
    update_config(config, 'cluster', cluster)
    update_config(config, 'aligner', args["aligner"])
    update_config(config, 'reference', args["ref_genome"])
    update_config(config, 'smoothing_function', args["smoothing"])

    return config


def get_config_string(args, run_options):
    config_string = json.dumps(get_config_override(args, run_options))
    config_string = ''.join(config_string.split()) # Remove all whitespace
    return r"'{}'".format(config_string)


def run_pipeline2(*args, **kwargs):
    print(args, kwargs)


def run_pipeline(
        analysis_type,
        args,
        version,
        run_options,
        scpipeline_dir,
        tmp_dir,
        inputs_yaml,
        context_config_file,
        docker_env_file,
        docker_server,
        output_dirs,
        dirs=(),
        docker_options={},
):
    # TODO: go through docker options and add to run_cmd
    config_override_string = get_config_string(args, run_options)

    run_cmd = [
        f'single_cell {analysis_type}',
        '--input_yaml',
        inputs_yaml,
        '--tmpdir',
        tmp_dir,
        '--pipelinedir',
        scpipeline_dir,
        '--library_id',
        args['library_id'],
        '--config_override',
        config_override_string,
        '--maxjobs',
        str(docker_options['maxjobs']),
        '--nocleanup',
        '--sentinel_only',
        '--context_config',
        context_config_file,
    ]

    for docker_option, value in docker_options.items():
        if isinstance(value, bool):
            run_cmd += [
                f'--{docker_option}',
            ]
        # check value non empty
        elif value:
            run_cmd += [
                f'--{docker_option}',
                value,
            ]

    for option_name, output_dir in output_dirs.items():
        run_cmd += [
            f'--{option_name}',
            output_dir,
        ]

    if run_options['local_run']:
        run_cmd += ["--submit", "local"]

    else:
        run_cmd += [
            '--submit',
            'azurebatch',
            '--storage',
            'azureblob',
        ]

    # Append docker command to the beginning
    docker_cmd = [
        'docker',
        'run',
        '-w',
        '$PWD',
        '-v',
        '$PWD:$PWD',
        '-v',
        '/var/run/docker.sock',
        '-v',
        '/usr/bin/docker',
        '--rm',
        '--env-file',
        docker_env_file,
    ]

    for d in dirs:
        docker_cmd.extend([
            '-v',
            '{d}:{d}'.format(d=d),
        ])

    docker_cmd.append(f'{docker_server}:{version}')
    run_cmd = docker_cmd + run_cmd

    if run_options['sc_config'] is not None:
        run_cmd += ['--config_file', run_options['sc_config']]
    if run_options['interactive']:
        run_cmd += ['--interactive']

    run_cmd_string = r' '.join(run_cmd)
    log.debug(run_cmd_string)
    subprocess.check_call(run_cmd_string, shell=True)
