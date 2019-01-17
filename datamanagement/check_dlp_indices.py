import collections
from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi


if __name__ == '__main__':
    tantalus_api = TantalusApi()
    colossus_api = ColossusApi()

    library_ids = set([a['pool_id'] for a in colossus_api.list('library')])

    for library_id in library_ids:

        # Get colossus sublibrary indices
        sublibraries = colossus_api.list('sublibraries', library__pool_id=library_id)
        colossus_indices = set([a['primer_i7'] + '-' + a['primer_i5'] for a in sublibraries])

        datasets = tantalus_api.list(
            'sequence_dataset',
            library__library_id=library_id,
            library__library_type='SC_WGS',
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

            # Get tantalus sublibraries and indices
            tantalus_indices = set()
            tantalus_dataset_ids = []
            for dataset in lane_datasets[flowcell_lane]:
                file_resources = list(tantalus_api.list('file_resource', sequencedataset__id=dataset['id']))
                tantalus_indices.update(set([a['sequencefileinfo']['index_sequence'] for a in file_resources]))
                tantalus_dataset_ids.append(dataset['id'])

            if len(colossus_indices - tantalus_indices) > 0:
                print 'library {}, datasets {}: {} in colossus but not tantalus'.format(
                    library_id, tantalus_dataset_ids, len(colossus_indices - tantalus_indices))

            if len(tantalus_indices - colossus_indices) > 0:
                print 'library {}, datasets {}: {} in tantalus but not colossus'.format(
                    library_id, tantalus_dataset_ids, len(tantalus_indices - colossus_indices))

            if tantalus_indices == colossus_indices:
                print 'library {}, datasets {}: OK'.format(library_id, tantalus_dataset_ids)
