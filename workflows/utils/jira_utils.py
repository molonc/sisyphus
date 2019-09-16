import os
import logging
from jira import JIRA, JIRAError

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

colossus_api = ColossusApi()
tantalus_api = TantalusApi()

log = logging.getLogger('sisyphus')

def get_jira_api():
    jira_user = os.environ['JIRA_USERNAME']
    jira_password = os.environ['JIRA_PASSWORD']
    jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(jira_user, jira_password))

    return jira_api


def get_parent_issue(jira_id):
    """
    Get parent ticket id

    Args:
        jira_id (str): Jira ticket id

    Returns:
        parent_ticket (str): Ticket id of parent ticket

    """
    jira_api = get_jira_api()

    issue = jira_api.issue(jira_id)

    try:
        parent_ticket = issue.fields.parent.key
    
    except:
        log.info(f"{jira_id} is not a sub-task")
        return None

    return parent_ticket


def comment_jira(jira_id, comment):
    """
    Comment on jira ticket
    """

    jira_api = get_jira_api()
    log.info("Commenting \n{} on ticket {}".format(
		comment,
		jira_id)
	)
    
    jira_api.add_comment(jira_id, comment)


def update_jira_dlp(jira_id, aligner):

    logging.info("Updating description on {}".format(jira_id))

    description = [
        '(/) Alignment with ' + aligner,
        '(/) Hmmcopy',
        '(/) Path to results on blob:',
        '{noformat}Container: singlecellresults\nresults/' + jira_id + '{noformat}',
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

    results_dataset = tantalus_api.get("resultsdataset", analysis__jira_ticket=jira_id)
    results_dataset_id = results_dataset["id"]

    library = colossus_api.get("tenxlibrary", name=args["library_id"])
    library_id = library["id"]

    description = [
        "{noformat}Storage Account: scrnadata\n {noformat}",
        "Tantalus Results: https://tantalus.canadacentral.cloudapp.azure.com/results/{}".format(
            results_dataset_id
        ),
        "Colossus Library: https://colossus.canadacentral.cloudapp.azure.com/tenx/library/{}".format(
            library_id
        ),
    ]

    update_description(jira_id, description, "coflanagan", remove_watcher=True)


def update_description(jira_id, description, assignee, remove_watcher=False):
    jira_api = get_jira_api()

    description = '\n\n'.join(description)
    issue = jira_api.issue(jira_id)

    issue.update(notify=False, assignee={"name": assignee}, description=description)

    # Remove self as watcher
    if remove_watcher:
        jira_api.remove_watcher(issue, JIRA_USER)


def add_attachment(jira_id, attachment_file_path, attachment_filename):
    """
    Checks if file is already added to jira ticket; attaches if not. 
    """

    jira_api = get_jira_api()

    issue = jira_api.issue(jira_id)
    current_attachments = [a.filename for a in issue.fields.attachment]

    if attachment_filename in current_attachments:
        log.info("{} already added to {}".format(attachment_filename, jira_id))

    else:
        log.info("Adding {} to {}".format(attachment_filename, jira_id))
        jira_api.add_attachment(issue=jira_id, attachment=attachment_file_path)

