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
    log.info("Commenting \n{} on ticket {}".format(comment, jira_id))

    jira_api.add_comment(jira_id, comment)


def update_jira_dlp(jira_id, aligner):

    logging.info("Updating description on {}".format(jira_id))

    description = [
        '(/) Alignment with ' + aligner,
        '(/) Hmmcopy',
        '(/) Path to results on blob:',
        '{noformat}Container: singlecellresults\nresults/' + jira_id + '{noformat}',
    ]

    parent_ticket = get_parent_issue(jira_id)

    library = colossus_api.get("library", jira_ticket=parent_ticket)
    sample = library["sample"]["sample_id"]

    if sample.startswith("TFRI"):
        update_description(jira_id, description, "shwu", remove_watcher=True)

    else:
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
        "Tantalus Results: https://tantalus.canadacentral.cloudapp.azure.com/results/{}".format(results_dataset_id),
        "Colossus Library: https://colossus.canadacentral.cloudapp.azure.com/tenx/library/{}".format(library_id),
    ]

    update_description(jira_id, description, "coflanagan", remove_watcher=True)


def update_description(jira_id, description, assignee, remove_watcher=False):
    jira_api = get_jira_api()

    description = '\n\n'.join(description)
    issue = jira_api.issue(jira_id)

    issue.update(notify=False, assignee={"name": assignee}, description=description)


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


def create_jira_ticket_from_library(library_id):
    """
    Create analysis jira ticket as subtask of library jira ticket

    Args:
        library_id (str): Keys: library name

    Returns:
        analysis_jira_ticket: jira ticket id (ex. SC-1234)
    """
    jira_user = os.environ['JIRA_USERNAME']
    jira_api = get_jira_api()

    library = colossus_api.get('library', pool_id=library_id)
    sample_id = library['sample']['sample_id']

    library_jira_ticket = library['jira_ticket']
    issue = jira_api.issue(library_jira_ticket)

    log.info('Creating analysis JIRA ticket as sub task for {}'.format(library_jira_ticket))

    # In order to search for library on Jira,
    # Jira ticket must include spaces
    sub_task = {
        'project': {
            'key': 'SC'
        },
        'summary': 'Analysis of {} - {}'.format(sample_id, library_id),
        'issuetype': {
            'name': 'Sub-task'
        },
        'parent': {
            'id': issue.key
        }
    }

    sub_task_issue = jira_api.create_issue(fields=sub_task)
    analysis_jira_ticket = sub_task_issue.key

    # Add watchers
    jira_api.add_watcher(analysis_jira_ticket, jira_user)
    jira_api.add_watcher(analysis_jira_ticket, 'jedwards')
    jira_api.add_watcher(analysis_jira_ticket, 'jbiele')
    jira_api.add_watcher(analysis_jira_ticket, 'jbwang')
    jira_api.add_watcher(analysis_jira_ticket, 'elaks')

    # Assign task to myself
    analysis_issue = jira_api.issue(analysis_jira_ticket)
    analysis_issue.update(assignee={'name': jira_user})

    log.info('Created analysis ticket {} for library {}'.format(analysis_jira_ticket, library_id))

    return analysis_jira_ticket
