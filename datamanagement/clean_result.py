from server_cleanup import delete_analysis_and_outputs
from workflows.utils.jira_utils import delete_ticket
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi
tantalus_api = TantalusApi()
colossus_api = ColossusApi()
import argparse

parser = argparse.ArgumentParser(description="""Description: Clean bam and result of all analysis under the same Jira analyisi ticket""")
parser.add_argument("-j", help="analysis jira ticket", nargs='+', required=True)
args=parser.parse_args()

jiras=set()
# input libarary you want to clean

for jira in args.j:
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


