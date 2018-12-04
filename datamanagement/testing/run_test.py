from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError
from workflows.utils import saltant_utils, file_utils
import argparse
import getpass
import workflows
import subprocess
import yaml
import os
import json

tantalus_api = TantalusApi()

JIRA_TICKET = "SC-1678"
LIBRARY_ID = "A96213ATEST"

test_dataset = tantalus_api.get("sequence_dataset", dataset_type="FQ", library__library_id=LIBRARY_ID)


def download_data(storage_name, storage_dir, queue_name, tag_name):
	"""
	Get the storage storage_name from Tantalus (create it if it doesn't 
	already exist) and download input fastqs tagged with tag_name from 
	singlecellblob to storage storage_name using Saltant.
	Args:
		storage_name: name of storage to download files to
		storage_dir: directory corresponding to storage_name
		queue_name: celery queue to use
		tag_name: tag in Tantalus that identifies the input fastqs
	"""
	tantalus_api.get_or_create(
		"storage_server", 
		storage_type="server", 
		name=storage_name,
		storage_directory=storage_dir,
		queue_prefix=storage_name)

	file_transfer_args = dict(
		tag_name=tag_name,
		from_storage="singlecellblob",
		to_storage=storage_name)

	saltant_utils.get_or_create_task_instance(
		"IntegrationTestTransfer_" + storage_name, 
		getpass.getuser(), 
		file_transfer_args,
		saltant_utils.get_task_type_id("File transfer"), 
		queue_name)


def run_pipeline(pipeline_version, storage_name, local_run=False):
	"""
	Create a test user configuration file and use it to 
	run the pipeline using Sisyphus.
	"""
	config_dir = os.path.join(os.path.dirname(workflows.__file__), "config")
	test_config_path = os.path.join(config_dir, "test_config.json")
	test_config = file_utils.load_json(os.path.join(config_dir, "normal_config.json"))
	test_config["test_storage"] = storage_name

	with open(test_config_path, "w") as outfile:
		json.dump(test_config, outfile)

	script = os.path.join(os.path.dirname(workflows.__file__), "run.py")
	run_cmd = [
		"python", 
		script, 
		JIRA_TICKET,
		pipeline_version,
		"--clean",  # removes the pipeline directory
		"--config", test_config_path,
		"--no_transfer",
		"--integrationtest",
		"--update"]

	if local_run:
		run_cmd.append("--local_run")

	print(' '.join(run_cmd))

	returncode = subprocess.check_call(run_cmd)
	if returncode != 0:
		raise Exception("single cell pipeline did not complete")


def check_output_files(filepaths, storage_name, storage_client):
	"""
	Check to see whether the filepaths exist at storage_name and 
	have been added to Tantalus.
	"""
	file_resources = set()
	for filepath in filepaths:
		filename = tantalus_api.get_file_resource_filename(storage_name, filepath)

		if not client.exists(filename):
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
	if set(bam_datasets) !=  set(hmmcopy_analysis["input_datasets"]):
		raise Exception("bam datasets associated with analysis jira ticket do not match",
			" those in hmmcopy input datasets")

	# Get bam paths from the inputs yaml
	headnode_client = tantalus_api.get_storage_client("headnode")
	assert len(align_analysis["logs"]) == 1
	inputs_yaml_pk = analysis["logs"][0]
	f = headnode_client.open_file(tantalus_api.get("file_resource", id=inputs_yaml_pk)["filename"])
	bam_paths = []
	for info in yaml.load(f).itervalues():
		bam_paths.append(info["bam"])
	f.close()

	storage_client = tantalus_api.get_storage_client(storage_name)

	bam_file_resources = check_output_files(bam_paths, storage_name, storage_client)
	dataset_file_resources = set()
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

		info_yaml_path = os.path.join(JIRA_TICKET, "results", "results", dirname, "info.yaml")
		f = results_client.open_file(info_yaml_path)
		results_infos = yaml.load(f)[yaml_field]['results']
		f.close()

		result_paths = []
		for result_name, result_info in result_infos.iteritems():
			filename = result_info["filename"]

			result_paths.append(filename)

			if result_name == "{}_metrics".format(yaml_field):
				check_metrics_file(analysis_type, result_info["filename"], storage_client)

		result_paths = [result_info["filename"] for result_info in result_infos]
		results_filenames.extend(results_paths)
		result_file_resources = check_output_files(result_paths, storage_name, storage_client)

		result = tantalus_api.get("results", name=JIRA_TICKET + "_" + analysis_type)
		if result_file_resources != set(result["file_resources"]):
			raise Exception("file resources for result files do not match those in {}".format(result["name"]))


def cleanup_bams(storage_name):
	storage_client = tantalus_api.get_storage_client(storage_name)

	bam_dataset = tantalus_api.get("sequence_dataset", analysis__jira_ticket=JIRA_TICKET, dataset_type="BAM")

	storage_client = tantalus_api.get_storage_client(storage_name)

	for file_resource_pk in bam_dataset["file_resources"]:
		filename = tantalus_api.get("file_resource", id=file_resource_pk)["filename"]
		storage_client.delete(filename)

	tantalus_api.delete("sequence_dataset", bam_dataset["id"])


def cleanup_results(storage_name):
	storage_client = tantalus_api.get_storage_client(storage_name)

	for analysis_type in ("align", "hmmcopy"):
		results = tantalus_api.get("results", name=JIRA_TICKET + "_" + analysis_type)
		for file_resource_pk in results["file_resources"]:
			filename = tantalus_api.get("file_resource", id=file_resource_pk)["filename"]
			storage_client.delete(filename)

		tantalus_api.delete("results", results["id"])

		analysis_pk = tantalus_api.get("analysis", name=JIRA_TICKET + "_" + analysis_type)
		tantalus_api.delete("analysis", analysis_pk)


def check_metrics_file(analysis_type, filepath, storage_client):
	# TODO
	pass


def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument("--storage_name", required=True)
	parser.add_argument("--storage_dir", required=True)
	parser.add_argument("--queue_name", required=True)
	parser.add_argument("--pipeline_version", required=True)
	parser.add_argument("--local", default=False, action="store_true")
	return dict(vars(parser.parse_args()))


if __name__ == '__main__':
	args = parse_args()
	tag_name = "IntegrationTestFastqs"
	download_data(args["storage_name"], args["storage_dir"], args["queue_name"], tag_name)
	run_pipeline(args["pipeline_version"], args["storage_name"], local_run=args["local"])

	bams_storage = "singlecellblob"
	results_storage = "singlecellblob_results"

	check_bams(bams_storage)
	check_results(results_storage)
	cleanup_bams(bams_storage)
	cleanup_results(results_storage)

