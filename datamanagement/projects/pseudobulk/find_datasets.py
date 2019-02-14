from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import logging
import click
import json
import pprint

import pandas as pd

from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi


def get_dataset(tantalus_api, sample_id, library_id):
    datasets = list(tantalus_api.list(
        'sequence_dataset',
        sample__sample_id=sample_id,
        library__library_id=library_id,
        library__library_type__name='SC_WGS',
        dataset_type='BAM',
        aligner__name='BWA_ALN_0_5_7',
        reference_genome__name='HG19',
    ))

    datasets = filter(lambda d: d['is_complete'], datasets)

    if len(datasets) != 1:
        for dataset in datasets:
            dataset['file_resources'] = '...'
            print(json.dumps(dataset, indent=4))
        raise Exception('{} datasets for {}'.format(len(datasets), library_id))

    return datasets[0]


@click.command()
@click.argument('search_ids_filename', required=True)
@click.option('--tag_name', required=False)
def find_pseudobulk_datasets(search_ids_filename, tag_name=None):
    tantalus_api = TantalusApi()

    search_ids = pd.read_csv(
        search_ids_filename, sep='\t', names=['sample_id', 'library_id'])

    pseudobulk_datasets = []

    for idx, row in search_ids.iterrows():
        dataset = get_dataset(tantalus_api, row['sample_id'], row['library_id'])
        print(dataset['sample']['sample_id'], dataset['library']['library_id'])
        pseudobulk_datasets.append(dataset['id'])

    if tag_name is not None:
        tantalus_api.tag(tag_name, sequencedataset_set=pseudobulk_datasets)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    find_pseudobulk_datasets()
