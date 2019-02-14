from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import logging
import itertools
import click
import pprint
import yaml
import collections

import pandas as pd

from datamanagement.utils.constants import LOGGING_FORMAT
import dbclients.colossus
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError


def get_cell_index_map(library_id):
    cell_index_map = {}
    for sublib in dbclients.colossus.get_colossus_sublibraries_from_library_id(library_id):
        cell_id = '{}-{}-R{:02d}-C{:02d}'.format(
            sublib['sample_id']['sample_id'],
            sublib['library']['pool_id'],
            sublib['row'],
            sublib['column'],
        )
        index_sequence = sublib['primer_i7'] + '-' + sublib['primer_i5']
        cell_index_map[index_sequence] = cell_id
    return cell_index_map


@click.command()
@click.argument('normal_cell_ids', required=True)
@click.argument('tumour_cell_ids', required=True)
@click.argument('storage_name', required=True)
@click.argument('inputs_yaml_filename', required=True)
def create_pseudobulk_analysis(normal_cell_ids, tumour_cell_ids, storage_name, inputs_yaml_filename):
    tantalus_api = TantalusApi()

    normal_cell_ids = [l.strip() for l in open(normal_cell_ids).readlines()]
    tumour_cell_ids = [l.strip() for l in open(tumour_cell_ids).readlines()]

    sample_library_cells = collections.defaultdict(set)
    for cell_id in itertools.chain(normal_cell_ids, tumour_cell_ids):
        sample_id, library_id, row, column = cell_id.split('-')
        sample_library_cells[(sample_id, library_id)].add(cell_id)

    cell_file_instances = {}
    for sample_id, library_id in sample_library_cells:
        cell_indices = get_cell_index_map(library_id)

        datasets = list(tantalus_api.list(
            'sequence_dataset',
            dataset_type='BAM',
            sample__sample_id=sample_id,
            library__library_id=library_id,
            library__library_type__name='SC_WGS',
            aligner__name='BWA_ALN_0_5_7',
            reference_genome__name='HG19',
        ))

        datasets = filter(lambda d: d['is_complete'], datasets)

        if len(datasets) == 0:
            raise Exception('no datasets for {}, {}'.format(sample_id, library_id))

        dataset = datasets[0]

        file_resources = tantalus_api.list('file_resource', sequencedataset__id=dataset['id'])
        for file_resource in file_resources:
            if not file_resource['filename'].endswith('.bam'):
                continue
            index_sequence = file_resource['sequencefileinfo']['index_sequence']
            cell_id = cell_indices[index_sequence]
            assert cell_id not in cell_file_instances
            file_instance = tantalus_api.get_file_instance(file_resource, storage_name)
            filepath = file_instance['filepath']
            cell_file_instances[cell_id] = {
                'filepath': filepath,
                'sample_id': sample_id,
                'library_id': library_id,
            }

    pseudobulk_inputs = dict()
    for cell_id in normal_cell_ids:
        sample_id = cell_file_instances[cell_id]['sample_id']
        filepath = str(cell_file_instances[cell_id]['filepath'])
        assert filepath == (pseudobulk_inputs
            .setdefault('normal', {})
            .setdefault(sample_id, {})
            .setdefault(cell_id, {})
            .setdefault('bam', filepath))

    for cell_id in tumour_cell_ids:
        sample_id = cell_file_instances[cell_id]['sample_id']
        library_id = cell_file_instances[cell_id]['library_id']
        filepath = str(cell_file_instances[cell_id]['filepath'])
        assert filepath == (pseudobulk_inputs
            .setdefault('tumour', {})
            .setdefault(sample_id, {})
            .setdefault(cell_id, {})
            .setdefault('bam', filepath))

    with open(inputs_yaml_filename, 'w') as inputs_yaml:
        yaml.dump(pseudobulk_inputs, inputs_yaml, default_flow_style=False)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    create_pseudobulk_analysis()
