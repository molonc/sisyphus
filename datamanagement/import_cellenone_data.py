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


def add_cellenone_results(filepaths, library_id, storage_name, tag_name=None, update=False):
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    sample_id = colossus_api.get('library', pool_id=library_id)['sample']['sample_id']

    try:
        tantalus_api.get('sample', sample_id=sample_id)
    except NotFoundError:
        logging.warning('skipping files {} with unknown sample {}'.format(filepaths, sample_id))
        return

    results_name = 'CELLENONE_{}'.format(library_id)
    results_type = 'CELLENONE'
    results_version = None

    add_generic_results(
        filepaths=filepaths,
        sample_ids=[sample_id],
        storage_name=storage_name,
        results_name=results_name,
        results_type=results_type,
        results_version=results_version,
        tag_name=tag_name,
        update=update,
    )


@click.group()
def cli():
    pass


@cli.command()
@click.argument('filepaths', nargs=-1)
@click.option('--storage_name')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
def glob_cellenone_data(filepaths, storage_name, tag_name=None, update=False):

    tantalus_api = TantalusApi()

    library_paths = collections.defaultdict(set)

    for filepath in filepaths:
        match = re.match(r".*/single_cell_indexing/Cellenone/Cellenone_images/(\d+)_(A\d+[A-Z]*)/(A\d+[A-Z]*).txt", filepath)
        if match is None:
            logging.warning('skipping malformed {}'.format(filepath))
            continue

        fields = match.groups()
        date = fields[0]
        library_id = fields[1]
        library_id2 = fields[2]

        if library_id != library_id2:
            logging.warning('skipping different library ids {}'.format(filepath))
            continue

        try:
            tantalus_api.get('dna_library', library_id=library_id)
        except NotFoundError:
            logging.warning('skipping file with unknown library {}'.format(filepath))
            continue

        library_paths[library_id].add(filepath)

    for library_id in library_paths:
        add_cellenone_results(
            library_paths[library_id], library_id,
            storage_name, tag_name=tag_name, update=update)


@cli.command()
@click.argument('filepaths', nargs=-1)
@click.option('--library_id')
@click.option('--storage_name')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
def add_cellenone_data(
        filepaths, library_id, storage_name,
        tag_name=None, update=False):

    tantalus_api = TantalusApi()

    add_cellenone_results(
        library_paths[library_id], library_id,
        storage_name, tag_name=tag_name, update=update)


if __name__=='__main__':
    cli()

