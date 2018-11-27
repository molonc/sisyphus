from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError
from workflows.utils import saltant_utils, file_utils
import argparse
import getpass
import workflows
import subprocess
import yaml

tantalus_api = TantalusApi()

JIRA_TICKET = "SC-1678"
LIBRARY_ID = "A96213ATEST"

test_dataset = tantalus_api.get("sequence_dataset", library__library_id=LIBRARY_ID)

# TODO: set up a celery worker locally on VM 

def download_data(storage_name, storage_dir, queue_name, tag_name):

	# TODO: check this get or create
	tantalus_api.get_or_create(
		"storage", 
		storage_type="server", 
		name=storage_name,
		storage_directory=storage_dir)

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


def run_pipeline(pipeline_version, storage_name):
	config_dir = os.path.join(os.path.dirname(workflows.__file__), "config")
	test_config_path = os.path.join(config_dir, "test_config.json")
	test_config = file_utils.load_json(os.path.join(config_dir, "normal_config.json"))
	test_config["test_storage"] = storage_name

	with open(test_config_path) as outfile:
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

	subprocess.check_call(run_cmd)


def check_bams(align_analysis, hmmcopy_analysis):
	# TODO: determine storage client based on whether we're running on Azure or on Shahlab
	headnode_client = tantalus_api.get_storage_client("headnode")
	data_client = tantalus_api.get_storage_client("singlecellblob")

	# Get the bam paths from the inputs yaml on the headnode
	assert len(align_analysis["logs"]) == 1
	inputs_yaml_pk = analysis["logs"][0]
	f = headnode_client.open_file(tantalus_api.get("file_resource", id=inputs_yaml_pk)["filename"])
	bam_paths = []
	for info in yaml.load(f).itervalues():
		bam_paths.append(info["bam"])
	f.close()

	bam_file_resources = set()

	for bam_path in bam_paths:
		filename = os.path.relpath(bam_path, data_client.prefix)
		if not data_client.exists(filename):
			raise Exception("output bam {} could not be found in singlecellblob".format(filename))

		try:
			file_resource = tantalus_api.get("file_resource", filename=filename)
		except NotFoundError:
			raise Exception("no file resource with filename {} exists".format(filename))

		if not file_resource["analysis"] == analysis["id"]:
			raise Exception("analysis field in file resource {} does not match {}".format(
				file_resource["id"], align_analysis["name"]))

		bam_file_resources.add(file_resource["id"])

		file_instance_exists = False
		for file_instance in file_resource["file_instances"]:
			if file_instance["storage"]["name"] == "singlecellblob":
				file_instance_exists = True

		if not file_instance_exists:
			raise Exception("no file instance in singlecellblob exists for file resource {}".format(
				file_resource["id"]))

	dataset_file_resources = set()
	for dataset_id in hmmcopy_analysis["input_datasets"]:
		dataset = tantalus_api.get("sequence_dataset", id=dataset_id)
		dataset_file_resources.update(dataset["file_resources"])

	if file_resources != dataset_file_resources:
		raise Exception("file resources for output bams do not match hmmcopy input datasets")


# def __is_gzip(self, filename):
#    """
#    Uses the file contents to check if the file is gzip or not.
#    The magic number for gzip is 1f 8b
#    """
#    with open(filename) as f:
#        file_start = f.read(4)

#        if file_start.startswith("\x1f\x8b\x08"):
#            return True
#        return False

# def get_file_linecount(self):
#     def blocks(files, size=65536):
#         while True:
#             b = files.read(size)
#             if not b: break
#             yield b

#     if self.__is_gzip(self.args.input):
#         input_stream = gzip.open(self.args.input, 'rb')
#     else:
#         input_stream = open(self.args.input)

#     return sum(bl.count("\n") for bl in blocks(input_stream))

def check_outputs():
	# TODO: check the results have been pushed tantalus and uploaded to azure with the api
	# check number of cells in each table, 

	results_client = tantalus_api.get_storage_client("singlecellblob_results")

	align_analysis = tantalus_api.get("analysis", name=JIRA_TICKET + "_align")
	hmmcopy_analysis = tantalus_api.get("analysis", name=JIRA_TICKET + "_hmmcopy")

	check_bams(align_analysis, hmmcopy_analysis)

	align_results = tantalus_api.get("results", JIRA_TICKET + )

	# TODO: load the info yaml and make sure all the files exist on Azure and are in Tantalus

	pass


def cleanup():
	# TODO: delete bam & results datasets in Tantalus and in Azure
	pass


def parse_args():
	parser = argparse.ArgumentParser()
	parser.add_argument("--storage_name", required=True)
	parser.add_argument("--storage_dir", required=True)
	parser.add_argument("--queue_name", required=True)
	parser.add_argument("--pipeline_version", required=True)
	return dict(vars(parser.parse_args()))


if __name__ == '__main__':
	args = parse_args()
	tag_name = "IntegrationTestFastqs"
	download_data(args["storage_name"], args["storage_dir"], args["queue_name"], tag_name)
	run_pipeline(args["pipeline_version"], args["storage_name"])
