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
import logging

log = logging.getLogger('sisyphus')
tantalus_api = TantalusApi()

JIRA_TICKET = "SC-1678"
LIBRARY_ID = "A96213ATEST"

test_dataset = tantalus_api.get("sequence_dataset", dataset_type="FQ", library__library_id=LIBRARY_ID)

# TODO: check more fields in datasets

def download_data(storage_name, storage_dir, queue_name, tag_name):
	"""
	Get the storage storage_name from Tantalus (create it if it doesn't 
	already exist) and download input fastqs tagged with tag_name from 
	singlecellblob to storage storage_name using Saltant.
	Args:
		storage_name: name of Tantalus storage to download files to
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


def run_pipeline(pipeline_version, storage_name, extra_args=None):
	"""
	Create a test user configuration file and use it to 
	run the pipeline using Sisyphus.
	Args:
		pipeline_version: single cell pipeline version, for example v0.2.3
		storage_name: name of Tantalus storage containing input files
		local_run: if True, run the single cell pipeline locally
	"""
	config_dir = os.path.join(os.path.dirname(workflows.__file__), "config")
	test_config_path = os.path.join(config_dir, "test_config.json")
	test_config = file_utils.load_json(os.path.join(config_dir, "normal_config.json"))
	test_config["jobs_storage"] = storage_name

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

	if extra_args is not None:
		run_cmd.extend(["--" + arg for arg in extra_args])

	print(' '.join(run_cmd))

	returncode = subprocess.check_call(run_cmd)
	if returncode != 0:
		raise Exception("single cell pipeline did not complete")


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
		result_infos = yaml.load(f)[yaml_field]['results']
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
	parser.add_argument("--storage_name")
	parser.add_argument("--storage_dir")
	parser.add_argument("--queue_name")
	parser.add_argument("--pipeline_version")
	parser.add_argument("--extra_args", nargs='*')  # arguments to pass to Sisyphus, but with the "--" stripped from the start
	return dict(vars(parser.parse_args()))


if __name__ == '__main__':
	args = parse_args()
	tag_name = "IntegrationTestFastqs"
	download_data(args["storage_name"], args["storage_dir"], args["queue_name"], tag_name)
	run_pipeline(args["pipeline_version"], args["storage_name"], extra_args=args['extra_args'])

	bams_storage = "singlecellblob"
	results_storage = "singlecellblob_results"

	check_bams(bams_storage)
	check_results(results_storage)
	cleanup_bams(bams_storage)
	cleanup_results(results_storage)

