import argparse
import logging
import sys

from dbclients.tantalus import TantalusApi

from utils.constants import LOGGING_FORMAT

logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger('sisyphus')
logging.getLogger('azure.storage').setLevel(logging.ERROR)


def get_tantalus_api():
    tantalus_api = TantalusApi()
    return tantalus_api


def get_single_cell_bam_datasets(tantalus_api):
    """
    grab relevant info regarding bam datasets from tantalus
    :param tantalus_api: tantalus api client
    :type tantalus_api: dbclients.tantalus.TantalusApi
    :return: list of datasets
    :rtype: list of dicts
    """
    datasets = tantalus_api.list("sequencedataset")

    data = {}

    for dataset in datasets:

        name = dataset['name']

        libraryid = dataset["library"]["library_id"]
        lanes = [lane["id"] for lane in dataset["sequence_lanes"]]

        datasetid = dataset["id"]

        complete = dataset["is_complete"]

        numlanes = len(dataset["sequence_lanes"])

        sampleid = dataset["sample"]["sample_id"]

        if not dataset["library"]["library_type"] == "SC_WGS":
            continue

        if not dataset["dataset_type"] == "BAM":
            continue

        if (sampleid, libraryid) not in data:
            data[(sampleid, libraryid)] = []

        data[(sampleid, libraryid)].append(
            {
                'id': datasetid, 'is_complete': complete,
                'num_lanes': numlanes, 'lanes': lanes,
                'name': name
            }
        )

    return data


def group_datasets_by_num_lanes(datasets):
    """
    group the datasets on number of lanes, this will
    allow us to track down the datasets that should be
    kept and ones that need to be deleted
    :param datasets: list of datasets parsed from api
    :type datasets: list of dict
    :return: datasets grouped on num_lanes
    :rtype: dict
    """
    sorted_ds = {}
    for ds in datasets:
        if ds['num_lanes'] not in sorted_ds:
            sorted_ds[ds['num_lanes']] = []
        sorted_ds[ds['num_lanes']].append(ds)
    return sorted_ds


def double_check_max_lanes_data(grouped_datasets):
    """
    most of this is overkill, but I prefer a script that breaks over
    one that might delete some files it shouldn't have
    :param grouped_datasets: datasets grouped on number of lanes
    :type grouped_datasets: dict
    """

    max_num_lanes = sorted(grouped_datasets)[-1]

    max_lanes_data = grouped_datasets[max_num_lanes]

    # all datasets should be "complete"
    assert all([dataset["is_complete"] for dataset in max_lanes_data])

    names = [dataset['name'] for dataset in max_lanes_data]

    supported_aligners = {'BWA_ALN_0_5_7', 'BWA_MEM_0_7_6A'}
    aligner = set([name.split('-')[5] for name in names])
    assert aligner.issubset(supported_aligners), 'pipeline only supports bwa-mem and bwa-aln'

    possible_invalid_results = [val for val in names if 'HG19_old' in val]
    if possible_invalid_results:
        logger.warn("skipping: {}".format(' '.join(possible_invalid_results)))
    # at the moment, we can only have 2 at most. bwa-mem and bwa-aln
    assert len(names) - len(possible_invalid_results) <= 2, 'there are more than 2 copies of results'

    lanes = [name.split('-')[4] for name in names]
    assert len(set(lanes)) == 1, 'dataset with max number of laes should all have the same lanes'


def get_datasets_to_delete(grouped_datasets):
    """
    datasets that dont have max no of lanes
    :param grouped_datasets: datasets grouped on num lanes
    :type grouped_datasets: dict
    :return: dataset
    :rtype: generator
    """
    # all datasets that dont have max number of lanes
    for numlanes in sorted(grouped_datasets)[:-1]:
        datasets = grouped_datasets[numlanes]
        for dataset in datasets:
            assert not dataset['is_complete']
            yield dataset


def delete_from_tantalus(tantalus_api, dataset_id):
    """
    deleting a dataset in tantalus is equivalent to
    flagging all of its instances as deleted
    :param tantalus_api: tantalus api client
    :type tantalus_api: dbclients.tantalus.TantalusApi
    :param dataset_id: dataset id in tantalus
    :type dataset_id: int
    """

    logger.warn("deleting {} from tantalus".format(dataset_id))
    tantalus_api.delete('sequencedataset', id=dataset_id)


def delete_incomplete_datasets(tantalus_api, datasets, dry_run=False):
    """
    track down files that need to be deleted
    :param tantalus_api: tantalus api client
    :type tantalus_api: dbclients.tantalus.TantalusApi
    :param datasets: datasets parsed from tantalus
    :type datasets: list
    :return: file metadata and paths
    :rtype: dict
    """
    for i, (sample_id, library_id) in enumerate(datasets):

        datasets_per_sample_lib = datasets[(sample_id, library_id)]

        if len(datasets_per_sample_lib) == 1:
            continue

        grouped_ds = group_datasets_by_num_lanes(datasets_per_sample_lib)

        double_check_max_lanes_data(grouped_ds)

        for dataset in get_datasets_to_delete(grouped_ds):
            logging.warn(
                "dataset with name: {} and id: {} is incomplete".format(
                    dataset['name'], dataset['id']
                )
            )
            if not dry_run:
                delete_from_tantalus(tantalus_api, dataset['id'])


def parse_args():
    """
    specify and parse args
    """
    parser = argparse.ArgumentParser(
        description='''build and push docker containers'''
    )

    parser.add_argument('--dry_run',
                        action='store_true',
                        default=False,
                        help='do not delete anything')

    args = parser.parse_args()

    args = vars(args)

    return args


def main():
    args = parse_args()

    tantalus_api = get_tantalus_api()

    datasets = get_single_cell_bam_datasets(tantalus_api)

    delete_incomplete_datasets(tantalus_api, datasets, dry_run=args['dry_run'])


if __name__ == "__main__":
    main()
