from server_cleanup import delete_analysis_and_outputs
from workflows.utils.jira_utils import delete_ticket
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi
tantalus_api = TantalusApi()
colossus_api = ColossusApi()
import argparse

parser = argparse.ArgumentParser(description="""Description: Clean result of a singlar analysis using a analysis id""")
parser.add_argument("-a", help="analysis jira ticket", nargs='+', required=True)
args=parser.parse_args()

for i in args.a:
        delete_analysis_and_outputs(i,"singlecellresults","singlecellblob",clean_azure=True)
