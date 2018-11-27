from dbclients.tantalus import TantalusApi
from workflows.utils import saltant_utils, file_utils
import argparse
import getpass
import workflows
import subprocess

tantalus_api = TantalusApi()

JIRA_TICKET = "SC‌-1678"
LIBRARY_ID = "A96213ATEST"

test_dataset = tantalus_api.get("sequence_dataset", library__library_id=library_id)

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
		"--config", test_config_path,
		"--no_transfer",
		"--integrationtest",
		"--update"]

	subprocess.check_call(run_cmd)


def check_results():
	# TODO: check the results have been pushed tantalus and uploaded to azure with the api
	# check number of cells in each table, 
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
