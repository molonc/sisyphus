import collections
import dateutil.parser


def get_most_recent_dataset(tantalus_api, **kwargs):
    datasets = collections.OrderedDict()
    for dataset in tantalus_api.list('sequencedataset', **kwargs):
        if not dataset['is_complete']:
            continue

        datasets[dateutil.parser.parse(dataset['last_updated'])] = dataset

    if len(datasets) == 0:
        raise ValueError(f'no datasets found with search parameters {kwargs}')

    return datasets.popitem()[1]


def get_cell_bams(tantalus_api, dataset, storages, passed_cell_ids=None):
    colossus_api = dbclients.colossus.ColossusApi()

    index_sequence_sublibraries = colossus_api.get_sublibraries_by_index_sequence(dataset['library']['library_id'])

    file_instances = tantalus_api.get_dataset_file_instances(
        dataset['id'],
        'sequencedataset',
        storages['working_inputs'],
        filters={'filename__endswith': '.bam'},
    )

    cell_bams = {}

    for file_instance in file_instances:
        file_resource = file_instance['file_resource']

        index_sequence = file_resource['sequencefileinfo']['index_sequence']
        cell_id = index_sequence_sublibraries[index_sequence]['cell_id']

        if passed_cell_ids is not None and cell_id not in passed_cell_ids:
            continue

        cell_bams[cell_id] = {}
        cell_bams[cell_id]['bam'] = str(file_instance['filepath'])

    return cell_bams

