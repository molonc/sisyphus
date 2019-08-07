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


def add_microscope_results(filepaths, library_id, storage_name, tag_name=None, update=False, remote_storage_name=None):
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    results_name = 'MICROSCOPE_{}'.format(library_id)
    results_type = 'MICROSCOPE'
    results_version = None

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
def glob_microscope_data(filepaths, storage_name, tag_name=None, update=False, remote_storage_name=None):

    tantalus_api = TantalusApi()

    library_paths = collections.defaultdict(set)

    for filepath in filepaths:
        match = re.match(r".*/single_cell_indexing/Microscope/(\d+)_(A\d+[A-Z]*)/", filepath)
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
        add_microscope_results(
            library_paths[library_id], library_id,
            storage_name, tag_name=tag_name, update=update,
            remote_storage_name=remote_storage_name,
        )


@cli.command()
@click.argument('filepaths', nargs=-1, required=True)
@click.option('--library_id', required=True)
@click.option('--storage_name')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
@click.option('--remote_storage_name')
def add_microscope_data(
        filepaths, library_id, storage_name,
        tag_name=None, update=False,
        remote_storage_name=None):

    tantalus_api = TantalusApi()

    add_microscope_results(
        filepaths, library_id,
            storage_name, tag_name=tag_name, update=update,
            remote_storage_name=remote_storage_name,
        )


if __name__=='__main__':
    cli()

