#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys
import click
import json
import collections
import re

from dbclients.basicclient import NotFoundError
from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from add_generic_results import add_generic_results


CELLENONE_RESULTS_TEMPLATE = 'CELLENONE_{library_id}'
CELLENONE_RESULTS_TYPE = 'CELLENONE'
CELLENONE_RESULTS_VERSION = 'v1'

CELLENONE_IMPORT_TEMPLATE = 'CELLENONE_RAW_IMPORT_{library_id}_{version}'
CELLENONE_IMPORT_TYPE = 'CELLENONE_RAW_IMPORT'
CELLENONE_IMPORT_VERSION = 'v1'


def add_cellenone_results(filepaths, library_id, storage_name, tag_name=None, update=False, remote_storage_name=None):
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    results_name = CELLENONE_RESULTS_TEMPLATE.format(library_id=library_id)
    results_type = CELLENONE_RESULTS_TYPE
    results_version = CELLENONE_RESULTS_VERSION

    results_dataset = add_generic_results(
        filepaths=filepaths,
        storage_name=storage_name,
        results_name=results_name,
        results_type=results_type,
        results_version=results_version,
        library_ids=[library_id],
        recursive=True,
        tag_name=tag_name,
        update=update,
        remote_storage_name=remote_storage_name,
    )


@click.group()
def cli():
    pass


@cli.command()
@click.argument('filepaths', nargs=-1)
@click.option('--storage_name')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
@click.option('--remote_storage_name')
def glob_cellenone_data(filepaths, storage_name, tag_name=None, update=False, remote_storage_name=None):

    tantalus_api = TantalusApi()

    library_paths = collections.defaultdict(set)

    for filepath in filepaths:
        match = re.match(r".*/single_cell_indexing/Cellenone/Cellenone_images/(\d+)_(A\d+[A-Z]*)/", filepath)
        if match is None:
            logging.warning('skipping malformed {}'.format(filepath))
            continue

        fields = match.groups()
        date = fields[0]
        library_id = fields[1]

        try:
            tantalus_api.get('dna_library', library_id=library_id)
        except NotFoundError:
            logging.warning('skipping file with unknown library {}'.format(filepath))
            continue

        library_paths[library_id].add(filepath)

    for library_id in library_paths:
        analysis_name = CELLENONE_IMPORT_TEMPLATE.format(
            library_id=library_id, version=CELLENONE_RESULTS_VERSION)
        analysis_type = CELLENONE_IMPORT_TYPE
        analysis_version = CELLENONE_IMPORT_VERSION

        fields = {
            'name': analysis_name,
            'analysis_type': analysis_type,
            'version': analysis_type,
            'jira_ticket': None,
            'args': {'library_id': library_id},
            # 'input_datasets': input_datasets,
            # 'input_results': input_results,
        }

        keys = [
            'name',
            'jira_ticket',
        ]

        analysis, updated = tantalus_api.create(
            'analysis',
            fields,
            keys,
            do_update=update)

        analysis, updated = tantalus_api.create('analysis', fields, keys, do_update=update)

        status = analysis['status']
        if updated:
            status = 'error'

        if status == 'complete':
            logging.info(f'skipping import of {library_id} already complete')
            continue

        add_cellenone_results(
            library_paths[library_id], library_id,
            storage_name, tag_name=tag_name, update=update,
            remote_storage_name=remote_storage_name,
        )

        logging.info(f'import of {library_id} completed')
        analysis = tantalus_api.update('analysis', id=analysis['id'], status='complete')


@cli.command()
@click.argument('filepaths', nargs=-1, required=True)
@click.option('--library_id', required=True)
@click.option('--storage_name')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
@click.option('--remote_storage_name')
def add_cellenone_data(
        filepaths, library_id, storage_name,
        tag_name=None, update=False,
        remote_storage_name=None):

    tantalus_api = TantalusApi()

    add_cellenone_results(
        filepaths, library_id,
            storage_name, tag_name=tag_name, update=update,
            remote_storage_name=remote_storage_name,
        )


if __name__=='__main__':
    cli()

