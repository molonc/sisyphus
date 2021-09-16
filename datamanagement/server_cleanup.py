import sys
import os
import logging
import json
import click
from dbclients.tantalus import TantalusApi, DataError
from dbclients.basicclient import NotFoundError
from utils.constants import LOGGING_FORMAT
import pandas as pd

from workflows.utils.tantalus_utils import (
    get_analyses_from_jira,
    get_resultsdataset_from_analysis,
    get_sequencedataset_from_analysis,
    get_sequencedataset_from_library_id,
    get_sequencing_lane_from_library_id,
)

tantalus_api = TantalusApi()
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
logging.getLogger('azure.storage').setLevel(logging.ERROR)

@click.group()
def main():
    click.echo("Running...")

# input sequence -> sequencing lane, file resources

# Grab all analyses by JIRA; https://tantalus.canadacentral.cloudapp.azure.com/genericapi/tantalus/Analysis?jira_ticket=SC-4526
# Grab all results by analysis ID; https://tantalus.canadacentral.cloudapp.azure.com/genericapi/tantalus/ResultsDataset?analysis=7945 

# import -> align (dataset, result)
# align (output dataset)-> hmmcopy (result)
# align (result), hmmcopy (result) -> annotation (result)
# align (output dataset) * 2, annotation (result) -> breakpoint (result)
# align (output dataset), annotation (result), variant calling (result) -> snv genotyping (result)
# nothing? -> variant calling (result)
# align (output dataset), annotation (result) -> merge cell bams


# Analysis -> input sequence, input result dataset, output sequence, output result dataset,
# Order should be
# file instance -> file resource -> output result
# file instance -> file resource -> output sequence

@main.command()
@click.argument('storage_name')
@click.option('--dataset-id', '-id', type=int, required=True, multiple=True)
def delete_sequencedataset_recursive(
    storage_name,
    dataset_id,
):
    """
    Given sequencedataset ID delete the dataset and associated file resources
    """
    storage_client = tantalus_api.get_storage_client(storage_name)

    # output result dataset
    for _id in dataset_id:
        sequence_dataset = tantalus_api.get("sequencedataset", id=_id)
        dataset_type = 'sequencedataset'

        file_resource_ids = sequence_dataset['file_resources']

        for file_resource_id in file_resource_ids:
            delete_file_instances(file_resource_id)
            delete_blob(storage_client, file_resource_id)
            delete_file_resource(file_resource_id)

        delete_dataset(dataset_type, _id)


@main.command()
@click.argument('storage_name')
@click.option('--library-id', type=str, required=True)
@click.option('--dry-run', is_flag=True)
def delete_lanes_by_library(
    storage_name,
    library_id,
    dry_run=False,
):
    """
    Given flowcell ID and lane number (flowcellID_laneNumber), delete associated sequencing lane and input dataset

    Args:
        library_id (str): DLP library ID (e.g. A118319A)
    """
    storage_client = tantalus_api.get_storage_client(storage_name)

    sequence_datasets = get_sequencedataset_from_library_id(library_id)
    sequencing_lanes = get_sequencing_lane_from_library_id(library_id)

    for sequence_dataset in sequence_datasets:
        dataset_id = sequence_dataset['id']
        dataset_type = 'sequencedataset'
        file_resource_ids = sequence_dataset['file_resources']
        sequencing_lane = sequence_dataset['sequence_lanes']

        for file_resource_id in file_resource_ids:
            delete_file_instances(file_resource_id)
            delete_blob(storage_client, file_resource_id)
            delete_file_resource(file_resource_id)

        delete_dataset(dataset_type, dataset_id)

    for sequencing_lane in sequencing_lanes:
        sequencing_lane_id = sequencing_lane['id']

        delete_sequencing_lane(sequencing_lane_id)

@main.command()
@click.option('--result-storage-name', '-r', type=str, required=True)
@click.option('--data-storage-name', '-d', type=str, required=True)
@click.option('--analysis-id', '-id', type=int, required=True, multiple=True)
def delete_analyses(
    result_storage_name,
    data_storage_name,
    analysis_id,
):
    """
    Given list of analysis IDs, hard delete corresponding outputs (SequenceDataset and resultsdataset)

    Args:
        analysis_id (int): analysis ID
    """
    for _id in analysis_id:
        delete_analysis_and_outputs(
            analysis_id=_id,
            result_storage_name=result_storage_name,
            data_storage_name=data_storage_name,
        )

def delete_analysis_and_outputs(
    analysis_id,
    result_storage_name,
    data_storage_name,
    dry_run=False,
):
    """
    Given analysis ID, hard delete corresponding output (SequenceDataset and resultsdataset)

    Args:
        analysis_id (int): analysis ID
    """
    data_storage_client = tantalus_api.get_storage_client(data_storage_name)
    result_storage_client = tantalus_api.get_storage_client(result_storage_name)
    # output sequence dataset
    sequence_datasets = get_sequencedataset_from_analysis(analysis_id)

    # output result dataset
    results_datasets = get_resultsdataset_from_analysis(analysis_id)

    for sequence_dataset in sequence_datasets:
        dataset_id = sequence_dataset['id']
        dataset_type = 'sequencedataset'
        file_resource_ids = sequence_dataset['file_resources']
        sequencing_lane = sequence_dataset['sequence_lanes']

        for file_resource_id in file_resource_ids:
            delete_file_instances(file_resource_id)
            delete_blob(data_storage_client, file_resource_id)
            delete_file_resource(file_resource_id)

        delete_dataset(dataset_type, dataset_id)

    for results_dataset in results_datasets:
        dataset_id = results_dataset['id']
        dataset_type = 'resultsdataset'
        file_resource_ids = results_dataset['file_resources']

        for file_resource_id in file_resource_ids:
            delete_file_instances(file_resource_id)
            delete_blob(result_storage_client, file_resource_id)
            delete_file_resource(file_resource_id)

        delete_dataset(dataset_type, dataset_id)

    delete_analysis(analysis_id)

def delete_file_instances(file_resource_id):
    try:
        file_instances = tantalus_api.list("file_instance", file_resource=file_resource_id)
    except Exception as e:
        logging.warning(f"file instance with file_resource_id, {file_resource_id}, does not exist. Skipping...")
        return

    for file_instance in file_instances:
        try:
            tantalus_api.delete("file_instance", file_instance['id'])
            logging.info(f"deleted file instance {file_instance['id']}")
        except Exception as e:
            logging.warning(f"cannot delete file instance {file_instance['id']}. Skipping...")

def delete_file_resource(file_resource_id):
    try:
        tantalus_api.delete("file_resource", file_resource_id)
        logging.info(f"removing file resource {file_resource_id}")
    except Exception as e:
        logging.warning(f"cannot delete file resource {file_resource_id}. Skipping...")

def delete_dataset(dataset_type, dataset_id):
    try:
        tantalus_api.delete(dataset_type, dataset_id)
        logging.info(f"removing {dataset_type} {dataset_id}")
    except Exception as e:
        logging.warning(f"cannot delete dataset {dataset_id}. Skipping...")

def delete_blob(storage_client, file_resource_id):
    file_resource = tantalus_api.get("file_resource", id=file_resource_id)
    blob_name = file_resource['filename']

    if (storage_client.exists(blob_name)):
        storage_client.delete(blob_name)
        logging.info(f"deleted blob {blob_name} from {storage_client.storage_account}/{storage_client.storage_container}")
    else:
        logging.warning(f"blob {blob_name} does not exist in {storage_client.storage_account}/{storage_client.storage_container}. Skipping...")

def delete_analysis(analysis_id):
    try:
        tantalus_api.delete("analysis", analysis_id)
        logging.info(f"removing analysis {analysis_id}")
    except Exception as e:
        logging.warning(f"cannot delete analysis {analysis_id}. Skipping...")

def delete_sequencing_lane(sequencing_lane_id):
    try:
        tantalus_api.delete("sequencing_lane", sequencing_lane_id)
        logging.info(f"removing sequencing_lane {sequencing_lane_id}")
    except Exception as e:
        logging.warning(f"cannot delete sequencing_lane {sequencing_lane_id}. Skipping...")

def delete_files(file_resource_id):
        """
        Delete a file and remove from all datasets.

        Args:
            file_resource (dict)
        """

        file_instances = tantalus_api.list("file_instance", file_resource=file_resource_id)
        for file_instance in file_instances:
            file_instance = tantalus_api.update(
                "file_instance",
                id=file_instance["id"],
                is_deleted=True,
            )
            logging.info(f"deleted file instance {file_instance['id']}")

        for dataset_type in ("sequencedataset", "resultsdataset"):
            datasets = tantalus_api.list(dataset_type, file_resources__id=file_resource_id)
            for dataset in datasets:
                file_resources = list(set(dataset["file_resources"]))
                file_resources.remove(file_resource_id)
                logging.info(f"removing file resource {file_resource_id} from {dataset['id']}")
                tantalus_api.update(dataset_type, id=dataset["id"], file_resources=file_resources)

#@main.command()
#@click.argument('storage_name')
#@click.argument('dataset_type')
#@click.option('--dataset_id', type=int)
#@click.option('--tag_name')
#@click.option('--check_remote')
#@click.option('--dry_run', is_flag=True)
def old_delete(
    storage_name,
    dataset_type,
    dataset_id=None,
    tag_name=None,
    check_remote=None,
    dry_run=False,
):
    logging.info('cleanup up storage {}'.format(storage_name))

    if check_remote:
        logging.info('checking remote {}'.format(check_remote))
    else:
        logging.warning('not checking remote')

    storage_client = tantalus_api.get_storage_client(storage_name)

    remote_client = None
    if check_remote is not None:
        remote_client = tantalus_api.get_storage_client(check_remote)

    if dataset_id is None and tag_name is None:
        raise ValueError('require either dataset id or tag name')

    if dataset_id is not None and tag_name is not None:
        raise ValueError('require exactly one of dataset id or tag name')

    if dataset_id is not None:
        logging.info('cleanup up dataset {}, {}'.format(dataset_id, dataset_type))
        datasets = tantalus_api.list(dataset_type, id=dataset_id)

    if tag_name is not None:
        logging.info('cleanup up tag {}'.format(tag_name))
        datasets = tantalus_api.list(dataset_type, tags__name=tag_name)

    total_data_size = 0
    file_num_count = 0

    for dataset in datasets:
        logging.info('checking dataset with id {}, name {}'.format(
            dataset['id'], dataset['name']))

        # Optionally skip datasets not present and intact on the remote storage
        if check_remote is not None:
            if not tantalus_api.is_dataset_on_storage(dataset['id'], dataset_type, check_remote):
                logging.warning('not deleting dataset with id {}, not on remote storage '.format(
                    dataset['id'], check_remote))
                continue

            # For each file instance on the remote, check if it exists and has the correct size in tantalus
            remote_file_size_check = True
            for file_instance in tantalus_api.get_dataset_file_instances(dataset['id'], dataset_type, check_remote):
                try:
                    tantalus_api.check_file(file_instance)
                except DataError:
                    logging.exception('check file failed')
                    remote_file_size_check = False

            # Skip this dataset if any files failed
            if not remote_file_size_check:
                logging.warning("skipping dataset {} that failed check on {}".format(
                    dataset['id'], check_remote))
                continue

        # Check consistency with the removal storage
        file_size_check = True
        for file_instance in tantalus_api.get_dataset_file_instances(dataset['id'], dataset_type, storage_name):
            try:
                tantalus_api.check_file(file_instance)
            except DataError:
                logging.exception('check file failed')
                file_size_check = False

        # Skip this dataset if any files failed
        if not file_size_check:
            logging.warning("skipping dataset {} that failed check on {}".format(
                dataset['id'], storage_name))
            continue

        # Delete all files for this dataset
        for file_instance in tantalus_api.get_dataset_file_instances(dataset['id'], dataset_type, storage_name):
            if dry_run:
                logging.info("would delete file instance with id {}, filepath {}".format(
                    file_instance['id'], file_instance['filepath']))
            else:
                logging.info("deleting file instance with id {}, filepath {}".format(
                    file_instance['id'], file_instance['filepath']))
                tantalus_api.update("file_instance", id=file_instance['id'], is_deleted=True)
            total_data_size += file_instance['file_resource']['size']
            file_num_count += 1

    logging.info("deleted a total of {} files with size {} bytes".format(
        file_num_count, total_data_size))


if __name__ == "__main__":
    main()


