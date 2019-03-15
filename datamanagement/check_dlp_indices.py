from __future__ import print_function

import click
import collections
from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi


@click.command()
@click.option('--library_id')
def check_indices(library_id=None):
    tantalus_api = TantalusApi()
    colossus_api = ColossusApi()

    if library_id is None:
        library_ids = set([a['pool_id'] for a in colossus_api.list('library')])

    else:
        library_ids = [library_id]

    for library_id in library_ids:

        # Get colossus sublibrary indices
        sublibraries = colossus_api.list('sublibraries', library__pool_id=library_id)
        colossus_indices = set([a['primer_i7'] + '-' + a['primer_i5'] for a in sublibraries])

        datasets = tantalus_api.list(
            'sequence_dataset',
            library__library_id=library_id,
            library__library_type__name='SC_WGS',
            dataset_type='FQ',
        )

        lane_datasets = collections.defaultdict(list)

        for dataset in datasets:

            assert len(dataset['sequence_lanes']) == 1

            flowcell_lane = '_'.join([
                dataset['sequence_lanes'][0]['flowcell_id'],
                dataset['sequence_lanes'][0]['lane_number'],
            ])

            lane_datasets[flowcell_lane].append(dataset)

        for flowcell_lane in lane_datasets:

            import json
            # Get tantalus sublibraries and indices
            tantalus_indices = set()
            tantalus_dataset_ids = []
            tantalus_sequencing_centre = set()
            for dataset in lane_datasets[flowcell_lane]:
                # if dataset['id'] != 6809:
                #     continue
                print(dataset['sample']['sample_id'], dataset['library']['library_id'], dataset['id'])
                file_resources = list(tantalus_api.list('file_resource', sequencedataset__id=dataset['id']))
                print(len(file_resources))
                print(len(set([a['sequencefileinfo']['index_sequence'] for a in file_resources])))
                # print(json.dumps(sorted(file_resources, key=lambda a: a['id']), sort_keys=True, indent=4))
                # print(json.dumps(sorted([a['sequencefileinfo']['index_sequence'] for a in file_resources]), sort_keys=True, indent=4))
                tantalus_indices.update(set([a['sequencefileinfo']['index_sequence'] for a in file_resources]))
                print(len(set(tantalus_indices)))
                # print(json.dumps(sorted(tantalus_indices), sort_keys=True, indent=4))
                tantalus_dataset_ids.append(dataset['id'])
                tantalus_sequencing_centre.update([a['sequencing_centre'] for a in dataset['sequence_lanes']])

            assert len(tantalus_sequencing_centre) == 1
            tantalus_sequencing_centre = list(tantalus_sequencing_centre)[0]

            print(len(tantalus_indices))
            print(len(colossus_indices))

            if len(colossus_indices - tantalus_indices) > 0:
                print('library {}, datasets {}, lane {}, sequencing_centre {}: {} in colossus but not tantalus'.format(
                    library_id, tantalus_dataset_ids, flowcell_lane, tantalus_sequencing_centre,
                    len(colossus_indices - tantalus_indices)))

            if len(tantalus_indices - colossus_indices) > 0:
                print('library {}, datasets {}, lane {}, sequencing_centre {}: {} in tantalus but not colossus'.format(
                    library_id, tantalus_dataset_ids, flowcell_lane, tantalus_sequencing_centre,
                    len(tantalus_indices - colossus_indices)))

            if tantalus_indices == colossus_indices:
                print('library {}, datasets {}, lane {}, sequencing_centre {}: OK'.format(
                    library_id, tantalus_dataset_ids, flowcell_lane, tantalus_sequencing_centre))


if __name__ == '__main__':
    check_indices()

