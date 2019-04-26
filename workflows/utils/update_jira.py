import os
import logging
from jira import JIRA, JIRAError

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

colossus_api = ColossusApi()
tantalus_api = TantalusApi()

log = logging.getLogger('sisyphus')
JIRA_USER = os.environ['JIRA_USER']
JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

def update_jira_dlp(jira_id, aligner, analysis_type):
    logging.info("Updating description on {}".format(jira_id))

    if analysis_type == "align":
        description = [
            '(/) Alignment with ' + aligner,
            '(x) Hmmcopy',
            '(/) Classifier',
            '(x) MT bam extraction',
            '(/) Path to results on blob:',
            '{noformat}Container: singlecelldata\nresults/' + jira_id + '{noformat}',
            '(x) Upload to Montage',
        ]

        update_description(jira_id, description, "jbiele")

    elif analysis_type == "hmmcopy":
        description = [
            '(/) Alignment with ' + aligner,
            '(/) Hmmcopy',
            '(/) Classifier',
            '(x) MT bam extraction',
            '(/) Path to results on blob:',
            '{noformat}Container: singlecelldata\nresults/' + jira_id + '{noformat}',
            '(x) Upload to Montage',
        ]

        update_description(jira_id, description, "jbiele", remove_watcher=True)



def update_jira_tenx(jira_id, args):
    """
    Update analysis jira ticket desription with link to Tantalus result dataset and 
        Colossus library

    Args:
        jira (str): Jira ticket ID
        args (dict):
    """

    results_dataset = tantalus_api.get("resultsdataset", analysis__jira_ticket=jira)
    results_dataset_id = results_dataset["id"]

    library = colossus_api.get("tenxlibrary", name=args["library_id"])
    library_id = library["id"]

    description = [
        "{noformat}Container: scrnadata\n",
        "Tantalus Results: https://tantalus.canadacentral.cloudapp.azure.com/results/{}".format(
            results_dataset_id
        ),
        "Colossus Library: https://colossus.canadacentral.cloudapp.azure.com/tenx/library/{}".format(
            library_id
        ),
    ]

    update_description(jira_id, description, "coflanagan", remove_watcher=True)


def update_description(jira_id, description, assignee, remove_watcher=False):

    description = '\n\n'.join(description)
    issue = jira_api.issue(jira_id)

    issue.update(notify=False, assignee={"name": assignee}, description=description)

    # Remove self as watcher
    if remove_watcher:
        jira_api.remove_watcher(issue, JIRA_USER)

