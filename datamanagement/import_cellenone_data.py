#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys
import click
import json

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from add_generic_results import add_generic_results


@click.command()
@click.argument('filepaths', nargs=-1)
@click.option('--library_id', multiple=True)
@click.option('--storage_name')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
def add_cellenone_data(
        filepaths, library_id, storage_name,
        tag_name=None, update=False):

    tantalus_api = TantalusApi()
    colossus_api = ColossusApi()

    sample_id = colossus_api.get('library', pool_id=library_id)['sample']['sample_id']

    results_name = 'CELLENONE_{}'.format(library_id)
    results_type = 'CELLENONE'
    results_version = None

    add_generic_dataset(
        filepaths=filepaths,
        sample_ids=[sample_id],
        storage_name=storage_name,
        results_name=results_name,
        results_type=results_type,
        results_version=results_version,
        tag_name=tag_name,
        update=update,
    )

