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


