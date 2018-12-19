import argparse
import getpass
import workflows
import subprocess
import yaml
import os
import json
import logging
import mock
import io

import workflows.run
import workflows.arguments
import datamanagement.templates as templates
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError
from workflows.utils import saltant_utils, file_utils
from datamanagement.transfer_files import transfer_files
from models import AlignAnalysis, HmmcopyAnalysis

log = logging.getLogger('sisyphus')

if 'TANTALUS_API_URL' not in os.environ:
    os.environ['TANTALUS_API_URL'] = 'http://127.0.0.1:8000/api/'

tantalus_api = TantalusApi()

JIRA_TICKET = "SC-9999"
LIBRARY_ID = "A96213ATEST"

test_dataset = tantalus_api.get("sequence_dataset", dataset_type="FQ", library__library_id=LIBRARY_ID)

# TODO: check more fields in datasets

def download_data(storage_name, storage_dir, tag_name):
    """
    Get the storage storage_name from Tantalus (create it if it doesn't 
    already exist) and download input fastqs tagged with tag_name from 
    singlecellblob to storage storage_name using Saltant.
    Args:
        storage_name: name of Tantalus storage to download files to
        storage_dir: directory corresponding to storage_name
        tag_name: tag in Tantalus that identifies the input fastqs
    """
    tantalus_api.get_or_create(
        "storage_server", 
        storage_type="server", 
        name=storage_name,
        storage_directory=storage_dir,
    )

    transfer_files(
        tag_name,
        "singlecellblob",
        storage_name,
    )


def create_fake_results(
        tantalus_analysis,
        analysis_info,
        inputs_yaml,
        docker_env_file,
        max_jobs='400',
        dirs=()):
    
    stream = io.BytesIO()
    stream.write("")

    results_storage_client = tantalus_api.get_storage_client(storages['working_results'])
    inputs_storage_client = tantalus_api.get_storage_client(storages['working_inputs'])

    result_filenames = tantalus_analysis.get_results_filenames()

    for filename in result_filenames:
        results_storage_client.write_data(filename, stream) 

    inputs_dict = file_utils.load_yaml(inputs_yaml)
    bam_paths = [cell_info["bam"] for cell_id, cell_info in input_dict.iteritems()]

    if tantalus_analysis.analysis_type == "align":
        for bam_path in bam_paths:
            bam_filename = os.path.relpath(bam_path, inputs_storage_client.prefix)
            inputs_storage_client.write_data(bam_filename, stream)


@mock.patch("workflows.launch_pipeline.run_pipeline")
@mock.patch("workflows.arguments.get_args")
def test_run_pipeline(mock_parse_args, mock_run_pipeline, side_effect=create_fake_results):
    # TODO: finish this
    workflow.run.main()  # Pass the correct arguments here

    args = mock_run_pipeline.call_args

    mock_run_pipeline.assert_called_once()


def run_pipeline(pipeline_version, config_json):
    """
    Create a test user configuration file and use it to
    run the pipeline using Sisyphus.
    Args:
        pipeline_version: single cell pipeline version, for example v0.2.3
        config_json: configuration file for pipeline
        local_run: if True, run the single cell pipeline locally
    """

    args = workflows.arguments.get_args([
        JIRA_TICKET, pipeline_version,
        '--config', config_json,
        '--no_transfer',
        '--update',
        '--integrationtest',
    ])

    workflows.launch_pipeline.run_pipeline = mock_run_pipeline
    workflows.run.main(args)


def check_output_files(filepaths, storage_name, storage_client):
    """
    Check to see whether a given list of file paths exist at storage_name and 
    whether the correct Tantalus models (file resource and file instance) 
    have been created.
    """
    file_resources = set()
    for filepath in filepaths:
        filename = tantalus_api.get_file_resource_filename(storage_name, filepath)

        if not storage_client.exists(filename):
            raise Exception("filename {} could not be found in storage {}".format(filename, storage_name))

        try:
            file_resource = tantalus_api.get("file_resource", filename=filename)
        except NotFoundError:
            raise Exception("no file resource with filename {} exists".format(filename))

        file_resources.add(file_resource["id"])

        file_instance_exists = False
        for file_instance in file_resource["file_instances"]:
            if file_instance["storage"]["name"] == storage_name:
                file_instance_exists = True

        if not file_instance_exists:
            raise Exception("no file instance in {} exists for file resource {}".format(
                storage_name, file_resource["id"]))

    return file_resources


def check_bams(storage_name):
    align_analysis = tantalus_api.get("analysis", name=JIRA_TICKET + "_align")
    hmmcopy_analysis = tantalus_api.get("analysis", name=JIRA_TICKET + "_hmmcopy")

    bam_datasets = tantalus_api.list("sequence_dataset", analysis__jira_ticket=JIRA_TICKET, dataset_type="BAM")
    bam_dataset_pks = [bam_dataset["id"] for bam_dataset in bam_datasets]

    if set(bam_dataset_pks) !=  set(hmmcopy_analysis["input_datasets"]):
        raise Exception("bam datasets associated with analysis jira ticket do not match",
            " those in hmmcopy input datasets")

    # Get bam paths from the inputs yaml
    headnode_client = tantalus_api.get_storage_client("headnode")
    assert len(align_analysis["logs"]) == 1
    inputs_yaml_pk = align_analysis["logs"][0]
    f = headnode_client.open_file(tantalus_api.get("file_resource", id=inputs_yaml_pk)["filename"])
    bam_paths = []
    for info in yaml.load(f).itervalues():
        bam_paths.append(info["bam"])
        bam_paths.append(info["bam"] + ".bai")
    f.close()

    storage_client = tantalus_api.get_storage_client(storage_name)

    bam_file_resources = check_output_files(bam_paths, storage_name, storage_client)
    dataset_file_resources = set()
    print(hmmcopy_analysis["input_datasets"])
    for dataset_id in hmmcopy_analysis["input_datasets"]:
        dataset = tantalus_api.get("sequence_dataset", id=dataset_id)
        dataset_file_resources.update(dataset["file_resources"])

    if bam_file_resources != dataset_file_resources:
        raise Exception("file resources for output bams do not match hmmcopy input datasets")


def check_results(storage_name):
    storage_client = tantalus_api.get_storage_client(storage_name)

    for analysis_type in ("align", "hmmcopy"):
        if analysis_type == "align":
            dirname = "alignment"
            yaml_field = "alignment"
        else:
            dirname = "hmmcopy_autoploidy"
            yaml_field = "hmmcopy"

        # TODO: move to datamanagement.templates
        info_yaml_path = os.path.join(JIRA_TICKET, "results", "results", dirname, "info.yaml")
        f = storage_client.open_file(info_yaml_path)
        result_infos = yaml.load(f).values()['results']
        f.close()

        result_paths = []
        for result_name, result_info in result_infos.iteritems():
            filename = result_info["filename"]

            result_paths.append(filename)

            if result_name == "{}_metrics".format(yaml_field):
                check_metrics_file(analysis_type, result_info["filename"], storage_client)

        result_file_resources = check_output_files(result_paths, storage_name, storage_client)

        result = tantalus_api.get("results", name=JIRA_TICKET + "_" + analysis_type)
        if result_file_resources != set(result["file_resources"]):
            raise Exception("file resources for result files do not match those in {}".format(result["name"]))


def cleanup_bams(storage_name):
    storage_client = tantalus_api.get_storage_client(storage_name)

    try:
        bam_dataset = tantalus_api.get("sequence_dataset", analysis__jira_ticket=JIRA_TICKET, dataset_type="BAM")
    except NotFoundError:
        return

    storage_client = tantalus_api.get_storage_client(storage_name)

    for file_resource_pk in bam_dataset["file_resources"]:
        log.info("deleting file_resource {}".format(file_resource_pk))
        filename = tantalus_api.get("file_resource", id=file_resource_pk)["filename"]
        if not storage_client.exists(filename):
            continue
        storage_client.delete(filename)

    log.info("deleting sequence dataset {}".format(bam_dataset["name"]))
    tantalus_api.delete("sequence_dataset", bam_dataset["id"])


def cleanup_results(storage_name):
    storage_client = tantalus_api.get_storage_client(storage_name)

    for analysis_type in ("align", "hmmcopy"):
        try:
            results = tantalus_api.get("results", name=JIRA_TICKET + "_" + analysis_type)
        except NotFoundError:
            continue

        for file_resource_pk in results["file_resources"]:
            log.info("deleting file_resource {}".format(file_resource_pk))
            filename = tantalus_api.get("file_resource", id=file_resource_pk)["filename"]
            if not storage_client.exists(filename):
                continue
            storage_client.delete(filename)

        log.info("deleting result {}".format(results["name"]))
        tantalus_api.delete("results", results["id"])

        log.info("deleting analysis {}".format(analysis["name"]))
        analysis = tantalus_api.get("analysis", name=JIRA_TICKET + "_" + analysis_type)
        tantalus_api.delete("analysis", analysis["id"])


def check_metrics_file(analysis_type, filepath, storage_client):
    # TODO
    pass


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("--config_json", required=True)
    parser.add_argument("--pipeline_version", required=True)
    return dict(vars(parser.parse_args()))


if __name__ == '__main__':
    config = file_utils.load_json(args['config'])
    storages = config['storages']
    args = parse_args()
    tag_name = "IntegrationTestFastqs"
#    storage_name = "andrewmac"
#    storage_dir = "/Users/amcphers/Scratch/tantalus_storage/"
    # download_data(storage_name, storage_dir, tag_name)
    run_pipeline(args["pipeline_version"], args["config_json"])

    # test_run_pipeline()

    bams_storage = "singlecellblob"
    results_storage = "singlecellblob_results"

    check_bams(bams_storage)
    check_results(results_storage)
    cleanup_bams(bams_storage)
    cleanup_results(results_storage)

