import os
import logging
import io
import itertools
import yaml
import click
import pandas as pd

import dbclients.tantalus
from datamanagement.utils.constants import LOGGING_FORMAT


@click.command()
@click.argument('analysis_id', nargs=-1, type=int)
@click.option('--version')
@click.option('--status')
def update_analyses(analysis_id, version=None, status=None):
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    tantalus_api = dbclients.tantalus.TantalusApi()

    for id_ in analysis_id:
        analysis = tantalus_api.get('analysis', id=id_)
        name = analysis['name']

        update_args = {}
        if status is not None:
            update_args['status'] = status
        if version is not None:
            update_args['version'] = version

        logging.info(f'updating {id_}, {name} with {update_args}')
        tantalus_api.update('analysis', id=id_, **update_args)


if __name__ == '__main__':
    update_analyses()
