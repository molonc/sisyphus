from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import logging
import click
import pprint

import pandas as pd

from datamanagement.utils.gsc import GSCAPI
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError


@click.command()
@click.argument('sample_ids_filename', required=True)
@click.argument('tag_name', required=True)
def find_pseudobulk_datasets(sample_ids_filename, tag_name):
    sample_ids = [l.strip() for l in open(sample_ids_filename).readlines()]

    tantalus_api = TantalusApi()

    pseudobulk_datasets = []

    for sample_id in sample_ids:
        datasets = list(tantalus_api.list(
            'sequence_dataset',
            sample__sample_id=sample_id,
            dataset_type='BAM',
            library__library_type__name='SC_WGS',
            aligner__name='BWA_ALN_0_5_7',
            reference_genome__name='HG19',
        ))

        datasets = filter(lambda d: d['is_complete'], datasets)

        if len(datasets) == 0:
            raise Exception('no datasets for {}'.format(sample_id))

        library_ids = set([a['library']['library_id'] for a in datasets])

        for library_id in library_ids:
            library_datasets = filter(lambda d: d['library']['library_id'] == library_id, datasets)

            if len(library_datasets) != 1:
                print sample_id, library_id
                pprint.pprint(library_datasets)

            pseudobulk_datasets.append(library_datasets[0]['id'])

    tantalus_api.tag(tag_name, sequencedataset_set=pseudobulk_datasets)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    find_pseudobulk_datasets()
