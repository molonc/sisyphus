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
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError
from workflows.utils import saltant_utils, file_utils
from datamanagement.transfer_files import transfer_files
from workflows.models import AlignAnalysis, HmmcopyAnalysis

log = logging.getLogger('sisyphus')

if 'TANTALUS_API_URL' not in os.environ:
    os.environ['TANTALUS_API_URL'] = 'http://127.0.0.1:8000/api/'

tantalus_api = TantalusApi()


def create_fake_results(
        tantalus_analysis,
        analysis_info,
        inputs_yaml,
        docker_env_file,
        max_jobs='400',
        dirs=()):
    
    stream = io.BytesIO()
    stream.write("")

    storages = tantalus_analysis.storages
    results_storage_client = tantalus_api.get_storage_client(
        storages['working_results'])
    inputs_storage_client = tantalus_api.get_storage_client(
        storages['working_inputs'])

    result_filenames = tantalus_analysis.get_results_filenames(
        tantalus_analysis.args)

    for filename in result_filenames:
        results_storage_client.write_data(filename, stream) 

    inputs_dict = file_utils.load_yaml(inputs_yaml)
    bam_paths = [cell_info["bam"] for cell_id, cell_info in inputs_dict.iteritems()]

    if tantalus_analysis.analysis_type == "align":
        for bam_path in bam_paths:
            bam_filename = os.path.relpath(bam_path, inputs_storage_client.prefix)
            inputs_storage_client.write_data(bam_filename, stream)
            inputs_storage_client.write_data(bam_filename + ".bai", stream)


@mock.patch("workflows.launch_pipeline.run_pipeline")
def test_run_pipeline(mock_run_pipeline, jira=None, version=None):
    # Create fake results instead of running pipeline
    mock_run_pipeline.side_effect = create_fake_results 

    arglist = [jira, version, "--update", "--integrationtest"]
    args = workflows.arguments.get_args(arglist=arglist)
    workflows.run.main(args)

    check_fake_outputs(mock_run_pipeline.call_args_list)
    cleanup_fake_outputs(mock_run_pipeline.call_args_list)


def check_fake_outputs(call_args_list):
    check_fake_bams(call_args_list)
    check_fake_results(call_args_list)


def cleanup_fake_outputs(call_args_list):
    cleanup_fake_bams(call_args_list)
    cleanup_fake_results(call_args_list)


def check_fake_bams(call_args_list):
    args, kwargs = call_args_list[1]

    hmmcopy_analysis = kwargs["tantalus_analysis"]
    assert hmmcopy_analysis.analysis_type == "hmmcopy"

    jira = hmmcopy_analysis.args["jira"]
    storages = hmmcopy_analysis.storages

    bam_datasets = tantalus_api.list(
        "sequence_dataset", 
        analysis__jira_ticket=jira, 
        dataset_type="BAM")

    bam_dataset_pks = set()
    for bam_dataset in bam_datasets:
        # Each bam dataset should be associated with the align analysis
        assert bam_dataset["analysis"] == hmmcopy_analysis.args["align_analysis"]
        bam_dataset_pks.add(bam_dataset["id"])

    if set(bam_dataset_pks) != set(hmmcopy_analysis.analysis["input_datasets"]):
        raise Exception("bam datasets associated with analysis jira ticket do not match",
            " those in hmmcopy input datasets")

    inputs_dict = file_utils.load_yaml(kwargs["inputs_yaml"])

    bam_filenames = []
    for cell_id, cell_info in inputs_dict.iteritems():
        bam = tantalus_api.get_file_resource_filename(storages["working_inputs"], cell_info["bam"])
        bam_filenames.append(bam)
        bam_filenames.append(bam + ".bai")

    bam_file_resources = check_output_files(bam_filenames, storages["remote_inputs"])
    dataset_file_resources = set()
    for dataset_id in input_dataset_pks:
        dataset = tantalus_api.get("sequence_dataset", id=dataset_id)
        dataset_file_resources.update(dataset["file_resources"])

    if bam_file_resources != dataset_file_resources:
        raise Exception("file resources for output bams do not match",
            "hmmcopy input datasets")


def check_output_files(filenames, storage_name):
    """
    Check to see whether a given list of file paths exist at storage_name and 
    whether the correct Tantalus models (file resource and file instance) 
    have been created.
    """
    storage_client = tantalus_api.get_storage_client(storage_name)
    file_resources = set()
    for filename in filenames:
        if not storage_client.exists(filename):
            raise Exception("filename {} could not be found in storage {}".format(
                filename, storage_name))

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


def check_fake_results(call_args_list):
    for call in call_args_list:
        args, kwargs = call

        tantalus_analysis = kwargs["tantalus_analysis"]
        storages = tantalus_analysis.storages
        storage = tantalus_api.get("storage", name=storages["remote_results"])

        result_paths = []
        for result_filename in tantalus_analysis.get_results_filenames(tantalus_analysis.args):
            result_filepath = os.path.join(storage["prefix"], result_filename)
            result_paths.append(result_filename)

        result_file_resources = check_output_files(result_paths, storages["remote_results"])

        results = tantalus_api.get("results", name=tantalus_analysis.analysis["name"])
        if result_file_resources != set(results["file_resources"]):
            raise Exception("file resources for result files do not match", 
                "those in {}".format(results["name"]))


def cleanup_bams(call_args_list):
    args, kwargs = call_args_list[1]
    hmmcopy_analysis = kwargs["tantalus_analysis"]
    assert hmmcopy_analysis.analysis_type == "hmmcopy"

    storages = hmmcopy_analysis.storages

    try:
        bam_dataset = tantalus_api.get(
            "sequence_dataset", 
            analysis__jira_ticket=hmmcopy_analysis.args["jira"], 
            dataset_type="BAM")
    except NotFoundError:
        return

    working_storage_client = tantalus_api.get_storage_client(storages["working_inputs"])
    remote_storage_client = tantalus_api.get_storage_client(storages["remote_inputs"])

    for file_resource_pk in bam_dataset["file_resources"]:
        filename = tantalus_api.get("file_resource", id=file_resource_pk)["filename"]
        log.info("deleting {}".format(filename))
        
        if working_storage_client.exists(filename):
            working_storage_client.delete(filename)

        if remote_storage_client.exists(filename):
            remote_storage_client.delete(filename)

    log.info("deleting sequence dataset {}".format(bam_dataset["name"]))
    tantalus_api.delete("sequence_dataset", bam_dataset["id"])


def cleanup_results(call_args_list):
    for call in call_args_list:
        args, kwargs = call

        tantalus_analysis = kwargs["tantalus_analysis"]
        storages = tantalus_analysis.storages

        name = tantalus_analysis.analysis["name"]

        try:
            results = tantalus_api.get("results", name=name)
            analysis = tantalus_api.get("analysis", name=name)
        except NotFoundError:
            continue

        working_storage_client = tantalus_api.get_storage_client(storages["working_results"])
        remote_storage_client = tantalus_api.get_storage_client(storages["remote_results"])

        result_filenames = tantalus_analysis.get_results_filenames(tantalus_analysis.args)

        for result_filename in result_filenames:
            log.info("deleting {}".format(result_filename))

            if working_storage_client.exists(filename):
                working_storage_client.delete(filename)

            if remote_storage_client.exists(filename):
                remote_storage_client.delete(filename)

        log.info("deleting results {}".format(name))
        tantalus_api.delete("results", name=name)
        log.info("deleting analysis {}".format(name))
        tantalus_api.delete("analysis", name=name)


def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("jira")
    parser.add_argument("version")
    return dict(vars(parser.parse_args()))


if __name__ == '__main__':
    args = parse_args()
    tag_name = "IntegrationTestFastqs"

    test_run_pipeline(jira=args["jira"], version=args["version"])
