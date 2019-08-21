import os
import re
import click
import logging
from tabulate import tabulate
from distutils.version import StrictVersion

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import NotFoundError

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

JIRA_TICKET_REGEX = r".*SC-\d{4}$"


def get_analysis_ticket_info(ticket_or_library, library_id):
	"""
	Get analysis ticket information from colossus

	Args:
		ticket_or_library: 		(str) user input passed when script was called
		library_id: 		(str) library id

	Returns:
		analysis_ticket:	(str) jira ticket id
		analysis_info: 		(dict) analysis information from colossus
	"""

	analyses = list(colossus_api.list("analysis_information", library__pool_id=library_id))

	if len(analyses) == 0:
		raise Exception("No analyses tickets exists for library {}".format(library_id))

	elif len(analyses) == 1:
		analysis_ticket = analyses[0]["analysis_jira_ticket"]

	else:
		print("There are multiple analysis tickets for {}".format(library_id))

		table_header = ["Ticket", "Aligner", "Version"]
		tickets = [[a["analysis_jira_ticket"], a["aligner"], a["version"]] for a in analyses]
		analysis_info_table = tabulate(tickets, headers=table_header, showindex="always")

		print("Valid analysis tickets are:\n\n{}\n\n".format(analysis_info_table))

		index_options_str = "0-{}".format(len(tickets)-1)
		index_selected = input(
			"""Please enter the index of the desired analysis ticket. Options are values {}: """
			.format(index_options_str)
		)

		try:
			index_selected = int(index_selected)
		except Exception:
			index_selected = None

		# Check if user selected index is valid
		while index_selected not in range(0, len(analyses)) or not index_selected:
			index_options = list(range(0, len(analyses)))
			index_selected = input("Invalid input. Please choose from {}: ".format(index_options))

			try:
				index_selected = int(index_selected)
			except Exception:
				index_selected = None

			if  index_selected in range(0, len(analyses)):
				break

	analysis_object = analyses[index_selected]
	analysis_ticket = tickets[index_selected][0]
	print("\nAnalysis ticket chosen: {}\n\n".format(analysis_ticket))

	return analysis_ticket, analysis_object


def check_user_input(intial_question, options):

	answer = input(intial_question)

	while answer not in options:
		answer = input("Invalid input. Options are {}: ".format(options))

		if answer in options:
			break

	return answer


def check_user_file_input(user_input):
	file_choices = ["metrics", "reads", "segments", "plots"]
	if user_input.lower() == "y":
		return "all"

	elif user_input.lower() == "n":
		file_table = tabulate([[f] for f in file_choices], showindex="always")
		file_question = "\n{}\n\nPlease enter the index of the desired file: ".format(file_table)

		file_index = check_user_input(file_question, list(map(str, range(len(file_choices)))))

	print("\nPreparing to download {} files...\n".format(file_choices[int(file_index)]))
	return file_choices[int(file_index)]


def get_reads(jira_ticket, version, library_id):

	filenames = []
	if StrictVersion(version.strip('v')) < StrictVersion('0.2.23'):
		file_resources = list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_multiplier0_reads.csv.gz".format(library_id)
		))
	else:
		file_resources = list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_reads.csv.gz".format(library_id)
		))

	filenames += [f["filename"] for f in file_resources]

	return filenames


def get_metrics(jira_ticket, version, library_id):

	filenames = []
	if StrictVersion(version.strip('v')) < StrictVersion('0.2.23'):

		file_resources = list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_alignment_metrics.csv.gz".format(library_id)
		))
		file_resources += list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_gc_metrics.csv.gz".format(library_id)
		))
		file_resources += list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_multiplier0_metrics.csv.gz".format(library_id)
		))
	else:
		file_resources = list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_alignment_metrics.csv.gz".format(library_id)
		))
		file_resources += list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_gc_metrics.csv.gz".format(library_id)
		))
		file_resources += list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_metrics.csv.gz".format(library_id)
		))

	filenames += [f["filename"] for f in file_resources]

	return filenames


def get_segments(jira_ticket, version, library_id):
	filenames = []
	if StrictVersion(version.strip('v')) < StrictVersion('0.2.23'):
		file_resources = list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_multiplier0_segments.csv.gz".format(library_id)
		))
	else:
		file_resources = list(tantalus_api.list(
			"file_resource",
			filename__startswith=jira_ticket,
			filename__endswith="{}_segments.csv.gz".format(library_id)
		))

	filenames += [f["filename"] for f in file_resources]
	return filenames

def get_plots(jira_ticket, version, library_id):

	filenames = []

	file_resources = list(tantalus_api.list(
		"file_resource",
		filename__startswith=jira_ticket,
		filename__endswith="{}_plot_metrics.pdf".format(library_id)
	))

	file_resources += list(tantalus_api.list(
		"file_resource",
		filename__startswith=jira_ticket,
		filename__endswith="{}_heatmap_by_ec_filtered.pdf".format(library_id)
	))

	file_resources += list(tantalus_api.list(
		"file_resource",
		filename__startswith=jira_ticket,
		filename__endswith="{}_heatmap_by_ec.pdf".format(library_id)
	))
	filenames += [f["filename"] for f in file_resources]

	return filenames


def get_all_files(jira_ticket, version, library_id):
	filenames = []

	filenames += get_reads(jira_ticket, version, library_id)
	filenames += get_metrics(jira_ticket, version, library_id)
	filenames += get_segments(jira_ticket, version, library_id)
	filenames += get_plots(jira_ticket, version, library_id)

	return filenames

@click.command()
@click.argument('ticket_or_library', nargs=1)
@click.option('--file',  type=click.Choice(['metrics', 'reads', 'plots', 'segments', 'all']))
def main(ticket_or_library, file=None):
	# Define source and destination storage clients
	from_storage_name = "singlecellresults"
	to_storage_name = "downloads"

	from_storage = tantalus_api.get_storage(from_storage_name)
	from_storage_client = tantalus_api.get_storage_client(from_storage_name)

	to_storage = tantalus_api.get_storage(to_storage_name)
	to_storage_client = tantalus_api.get_storage_client(to_storage_name)

	# Check if user input is a jira ticket
	# If so, check if its a analysis ticket.
	# Else check if its a library ticket and return analysis tickets
	ticket_or_library = ticket_or_library.upper()
	if re.match(JIRA_TICKET_REGEX, ticket_or_library):
		jira_ticket = ticket_or_library
		possible_library_ticket = None

		try:
			analysis_object = colossus_api.get("analysis_information", analysis_jira_ticket=jira_ticket)
			analysis_ticket = jira_ticket
			library_id = analysis_object["library"]["pool_id"]
			print("{} is a valid analysis ticket\n".format(analysis_ticket))

		except NotFoundError:
			possible_library_ticket = ticket_or_library

		if possible_library_ticket is not None:
			try:
				library = colossus_api.get("library", jira_ticket=possible_library_ticket)
				library_id = library["pool_id"]
				print("Input is a valid library ticket\n")
				analysis_ticket, analysis_object = get_analysis_ticket_info(ticket_or_library, library_id)
			except NotFoundError:
				raise Exception("{} is not a valid library ticket or analysis ticket")

	else:

		try:
			library = colossus_api.get("library", pool_id=ticket_or_library)
			library_id = library["pool_id"]
			analysis_ticket, analysis_object = get_analysis_ticket_info(ticket_or_library, library_id)
		except NotFoundError:
			raise Exception("{} is not an analysis ticket nor is a valid library".format(ticket_or_library))

	files_to_download = file
	if file is None:
		files_question = "File specific parameter was not passed. "
		files_question += "Do you want to proceed with downloading metrics, reads, segments, and plots? (y/n) "
		download_all = check_user_input(files_question, ["y", "Y", "n", "N"])
		files_to_download = check_user_file_input(download_all)

	version = analysis_object["version"]
	if files_to_download == "reads":
		filenames = get_reads(analysis_ticket, version, library_id)

	elif files_to_download == "metrics":
		filenames = get_metrics(analysis_ticket, version, library_id)

	elif files_to_download == "segments":
		filenames = get_segments(analysis_ticket, version, library_id)

	elif files_to_download == "plots":
		filenames = get_plots(analysis_ticket, version, library_id)

	elif files_to_download == "all":
		filenames = get_all_files(analysis_ticket, version, library_id)

	downloaded_files = []
	for filename in filenames:
		filepath_parsed = filename.split("/")
		analysis_type = filepath_parsed[-2]
		file = filepath_parsed[-1]

		# REFACTOR
		# This takes care of results from old version of scpipeline
		if analysis_type == "plots":
			if "heatmap" in filename:
				analysis_type = "hmmcopy_autoploidy"
			else:
				analysis_type = "alignment"

		subdir = os.path.join(to_storage_name, analysis_ticket, analysis_type)
		filepath = os.path.join(to_storage_name, analysis_ticket, analysis_type, file)

		if not os.path.exists(subdir):
			os.makedirs(subdir)

		print("Downloading {} to {}".format(file, subdir))
		blob = from_storage_client.blob_service.get_blob_to_path(
			container_name="results",
			blob_name=filename,
			file_path=filepath
		)
		downloaded_files.append(filepath)

	print("\n********** Download complete **********\n\n")
	for file in downloaded_files:
		print(file)


if __name__ == "__main__":
	main()
