import logging
import sys

from dbclients.tantalus import TantalusApi

from utils.constants import LOGGING_FORMAT

logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
logger = logging.getLogger('sisyphus')
logging.getLogger('azure.storage').setLevel(logging.ERROR)

import argparse


class ArgparseException(Exception):
    pass


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

        fileresources = dataset["file_resources"]

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
                'file_resources': fileresources, 'name': name
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


def get_file_instances(tantalus_api, dataset, storage_type):
    """
    get all file instances for a dataset
    :param tantalus_api: tantalus api client
    :type tantalus_api: dbclients.tantalus.TantalusApi
    :param dataset: data parsed about dataset from tantalus api
    :type dataset: dict
    :return: file_instance to delete
    :rtype:
    """
    for fileresourceid in dataset['file_resources']:
        fileresourcedetails = tantalus_api.get("file_resource", id=fileresourceid)
        file_instances = fileresourcedetails['file_instances']
        for file_instance in file_instances:
            if file_instance['storage']['storage_type'] == storage_type:
                yield file_instance


def get_files_to_delete(tantalus_api, datasets, storage_type='blob'):
    """
    track down files that need to be deleted
    :param storage_type: returns file instances that match specified type.
    :type storage_type:  str
    :param tantalus_api: tantalus api client
    :type tantalus_api: dbclients.tantalus.TantalusApi
    :param datasets: datasets parsed from tantalus
    :type datasets: list
    :return: file metadata and paths
    :rtype: dict
    """

    num_datasets = len(datasets)

    for i, (sample_id, library_id) in enumerate(datasets):

        logger.info('sample:{} library:{} progress:{}/{}'.format(sample_id, library_id, i, num_datasets))

        datasets_per_sample_lib = datasets[(sample_id, library_id)]

        if len(datasets_per_sample_lib) == 1:
            continue

        grouped_ds = group_datasets_by_num_lanes(datasets_per_sample_lib)

        double_check_max_lanes_data(grouped_ds)

        for dataset in get_datasets_to_delete(grouped_ds):
            for file_instance in get_file_instances(tantalus_api, dataset, storage_type):
                yield file_instance


def delete_from_tantalus(tantalus_api, filepath, resource_id):
    """
    :param tantalus_api: tantalus api client
    :type tantalus_api: dbclients.tantalus.TantalusApi
    :param filepath: filepath
    :type filepath: str
    :param resource_id: file instance id in tantalus
    :type resource_id
: int
    """
    logger.warn("deleting {} from tantalus".format(filepath))
    tantalus_api.delete('file_resource', id=resource_id)


def delete_from_azure(storage_client, filepath):
    """
    :param storage_client: tantalus api client
    :type storage_client: dbclients.tantalus.BlobStorageClient
    :param filepath: path of file to delete
    :type filepath: str
    """
    logger.warn("deleting {} from blob storage".format(filepath))
    storage_client.delete(filepath)


def delete_file_instances(tantalus_api, files_to_delete):
    """
    :param tantalus_api: tantalus api client
    :type tantalus_api: dbclients.tantalus.TantalusApi
    :param files_to_delete: path to file generated by list mode
    :type files_to_delete: str
    """
    storage_client = tantalus_api.get_storage_client("singlecellblob")

    with open(files_to_delete) as file_instances:
        assert file_instances.readline() == 'resource_id,,filepath\n'

        for line in file_instances:
            line = line.strip().split(',')
            resource_id = int(line[0])
            filepath = line[1]
            delete_from_tantalus(tantalus_api, filepath, resource_id)
            delete_from_azure(storage_client, filepath)


def list_file_instances(file_instances, output):
    """
    :param file_instances: file metadata and paths
    :type file_instances: list of dicts
    :param output: path to file to write list of files to
    :type output: str
    """
    with open(output, 'w') as outfile:
        outfile.write('resource_id,,filepath\n')
        for file_instance in file_instances:
            filepath = file_instance['filepath']
            file_resource = file_instance['file_resource']
            outfile.write('{},{}\n'.format(file_resource, filepath))


def parse_args():
    """
    specify and parse args
    """
    parser = argparse.ArgumentParser(
        description='''build and push docker containers'''
    )

    subparsers = parser.add_subparsers()

    subparser = subparsers.add_parser("list_files")
    subparser.set_defaults(which='list')
    subparser.add_argument('output',
                           help='output file')

    subparser = subparsers.add_parser("remove_files")
    subparser.set_defaults(which='remove')
    subparser.add_argument('input',
                           help='input file')

    args = parser.parse_args()

    args = vars(args)

    return args


def main():
    args = parse_args()

    tantalus_api = get_tantalus_api()

    if args['which'] == 'list':

        datasets = get_single_cell_bam_datasets(tantalus_api)

        file_instances = get_files_to_delete(tantalus_api, datasets, storage_type='blob')

        list_file_instances(file_instances, args['output'])

    elif args['which'] == 'remove':
        delete_file_instances(tantalus_api, args['input'])
    else:
        raise ArgparseException("Unexpected arguments")


if __name__ == "__main__":
    main()
