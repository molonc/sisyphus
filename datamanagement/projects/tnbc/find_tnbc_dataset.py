from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import logging
import click

import pandas as pd

from datamanagement.utils.gsc import GSCAPI
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError


@click.command()
@click.argument('library_ids_filename', required=True)
def find_tnbc_datasets(library_ids_filename):
    library_ids = [l.strip() for l in open(library_ids_filename).readlines()]

    tantalus_api = TantalusApi()

    tnbc_datasets = []

    for library_id in library_ids:
        if ',' in library_id:
            library_id_1, library_id_2 = library_id.split(',')
            library_id_pairs = [
                library_id_1 + '_' + library_id_2,
                library_id_2 + '_' + library_id_1,
            ]
            datasets = []
            for library_id_pair in library_id_pairs:
                datasets.extend(list(tantalus_api.list(
                    'sequence_dataset',
                    library__library_id=library_id_pair,
                    dataset_type='BAM',
                    library__library_type__name='WGS',
                    aligner__name='BWA_MEM_0_7_6A',
                    reference_genome__name='HG19',
                )))

        else:
            datasets = list(tantalus_api.list(
                'sequence_dataset',
                library__library_id=library_id,
                dataset_type='BAM',
                library__library_type__name='WGS',
                aligner__name='BWA_MEM_0_7_6A',
                reference_genome__name='HG19',
            ))

        # Special case A30655 which did not have all the lanes in the GSC merge
        if library_id == 'A30655':
            lanes = [
                ('HGFCJCCXY', '6'),
                ('D2GEWACXX', '1'),
                ('D2EYCACXX', '6'),
                ('D291GACXX', '2'),
                ('C2PV5ACXX', '2'),
                ('C2PV5ACXX', '1'),
            ]
            def is_correct_lanes(d):
                if len(d['sequence_lanes']) != len(lanes):
                    return False
                for l in d['sequence_lanes']:
                    if (l['flowcell_id'], l['lane_number']) not in lanes:
                        return False
                return True
            datasets = filter(is_correct_lanes, datasets)

        else:
            datasets = filter(lambda d: d['is_complete'], datasets)

        if len(datasets) != 1:
            logging.error('{} datasets for library {}'.format(len(datasets), library_id))
            continue

        dataset = datasets[0]

        is_on_blob = True

        filenames = {}
        for file_resource in tantalus_api.list('file_resource', sequencedataset__id=dataset['id']):
            filenames[file_resource['file_type']] = file_resource['filename']
            storages = [instance['storage']['name'] for instance in file_resource['file_instances']]
            if 'singlecellblob' not in storages:
                is_on_blob = False

        tnbc_datasets.append(dict(
            dataset_pk=dataset['id'],
            library_id=library_id,
            sample_id=dataset['sample']['sample_id'],
            bam_filename=filenames['BAM'],
            bam_index_filename=filenames.get('BAI'),
            is_on_blob=is_on_blob,
        ))

    tnbc_datasets = pd.DataFrame(tnbc_datasets)

    tnbc_pks = list(set(tnbc_datasets['dataset_pk'].values))

    tantalus_api.tag('TNBC_Project', sequencedataset_set=tnbc_pks)

    tnbc_datasets.to_csv(sys.stdout, index=False)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    find_tnbc_datasets()
