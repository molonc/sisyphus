import collections
import dateutil.parser


def get_most_recent(tantalus_api, table_name, **kwargs):
    datasets = collections.OrderedDict()
    for dataset in tantalus_api.list(table_name, **kwargs):
        datasets[dateutil.parser.parse(dataset['last_updated'])] = dataset
    return datasets.popitem()[1]


