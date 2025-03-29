from server_cleanup import delete_analysis_and_outputs
from workflows.utils.jira_utils import delete_ticket
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi
import argparse

parser = argparse.ArgumentParser(description="""Description: Clean bam and result of a library but keep the lates analysis run""")
parser.add_argument("-l", help="Library id")
args=parser.parse_args()
tantalus_api = TantalusApi()
colossus_api = ColossusApi()

jiras=set()
# input libarary you want to clean

biggest_id=0
obj = list(colossus_api.list('analysis_information',library__pool_id=args.l))
for info in obj:
	if int(info["id"]) > biggest_id and info["analysis_run"]["run_status"]=="complete":
		biggest_id = info["id"]

for info in obj:
	print(info["id"])
	if info["id"] != biggest_id:
		jiras.add(info["analysis_jira_ticket"])

for jira in jiras:
	aid=[]
	query = tantalus_api.list('analysis', jira_ticket=jira)
	for info in query:
		aid.append(info["id"])

	for i in aid:
		delete_analysis_and_outputs(i,"singlecellresults","singlecellblob",clean_azure=True)
	obj = list(colossus_api.list('analysis_information',analysis_jira_ticket=jira))
	caid = []
	for info in obj:
		caid.append(info["id"])
	for i in caid:
		colossus_api.delete('analysis_information',id=i)

	try:
		delete_ticket(jira)
	except:
		continue


