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


def add_microscope_results(filepaths, chip_id, library_ids, storage_name, tag_name=None, update=False, remote_storage_name=None):
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    results_name = 'MICROSCOPE_{}'.format(chip_id)
    results_type = 'MICROSCOPE'
    results_version = None

    try:
        existing_results = tantalus_api.get('results', name=results_name)
    except NotFoundError:
        existing_results = None

    if existing_results is not None and not update:
        logging.info(f'results for {chip_id} exist, not processing')
        return

    results_dataset = add_generic_results(
        filepaths=filepaths,
        storage_name=storage_name,
        results_name=results_name,
        results_type=results_type,
        results_version=results_version,
        library_ids=library_ids,
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
def glob_microscope_data(filepaths, storage_name, tag_name=None, update=False, remote_storage_name=None):

    tantalus_api = TantalusApi()

    chip_paths = collections.defaultdict(set)
    chip_libraries = collections.defaultdict(set)

    for filepath in filepaths:
        match = re.match(r".*/single_cell_indexing/Microscope/(\d+)_(A\d+[A-Z]*)", filepath)
        if match is None:
            logging.warning('skipping malformed {}'.format(filepath))
            continue

        fields = match.groups()
        date = fields[0]
        chip_id = fields[1]

        libraries = list(tantalus_api.list('dna_library', library_id__startswith=chip_id))

        if len(libraries) == 0:
            logging.error('skipping file with unknown library {}'.format(filepath))
            continue

        library_ids = set([library['library_id'] for library in libraries])

        chip_paths[chip_id].add(filepath)
        chip_libraries[chip_id].update(library_ids)

    for chip_id in chip_paths:
        add_microscope_results(
            chip_paths[chip_id], chip_id, chip_libraries[chip_id],
            storage_name, tag_name=tag_name, update=update,
            remote_storage_name=remote_storage_name,
        )


@cli.command()
@click.argument('filepaths', nargs=-1, required=True)
@click.option('--chip_id', required=True)
@click.option('--library_id', multiple=True)
@click.option('--storage_name')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
@click.option('--remote_storage_name')
def add_microscope_data(
        filepaths, chip_id, library_id, storage_name,
        tag_name=None, update=False,
        remote_storage_name=None):

    tantalus_api = TantalusApi()

    add_microscope_results(
        filepaths, chip_id, library_id,
        storage_name, tag_name=tag_name, update=update,
        remote_storage_name=remote_storage_name,
    )


if __name__=='__main__':
    cli()

