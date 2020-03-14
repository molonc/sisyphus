import os
import re
import sys
import json
import subprocess
import logging
from distutils.version import StrictVersion

import datamanagement.templates as templates

log = logging.getLogger('sisyphus')


def run_pipeline(
        analysis_type,
        version,
        run_options,
        scpipeline_dir,
        tmp_dir,
        inputs_yaml,
        context_config_file,
        docker_env_file,
        docker_server,
        output_dirs,
        cli_args=(),
        max_jobs='400',
        dirs=(),
):
    config_override = run_options.get('config_override')

    run_cmd = [
        f'single_cell {analysis_type}',
        '--input_yaml',
        inputs_yaml,
        '--tmpdir',
        tmp_dir,
        '--pipelinedir',
        scpipeline_dir,
        '--maxjobs',
        str(max_jobs),
        '--nocleanup',
        '--sentinel_only',
        '--context_config',
        context_config_file,
    ]

    run_cmd.extend(cli_args)

    if config_override is not None:
        run_cmd += [
            '--config_override',
            f'\'{config_override}\'',
        ]

    for option_name, output_dir in output_dirs.items():
        run_cmd += [
            f'--{option_name}',
            output_dir,
        ]

    if run_options['saltant']:
        run_cmd += ['--loglevel', 'ERROR']
    else:
        run_cmd += ['--loglevel', 'DEBUG']

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

    if run_options.get("skip_pipeline"):
        log.info('skipping pipeline on request')
    else:
        subprocess.check_call(run_cmd_string, shell=True)

