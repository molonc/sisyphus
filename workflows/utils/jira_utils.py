import os
import settings
import logging
from jira import JIRA, JIRAError

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from dbclients.utils.dbclients_utils import (
    get_tantalus_base_url,
    get_colossus_base_url,
)

from constants.url_constants import (
    ALHENA_BASE_URL,
)
COLOSSUS_BASE_URL = get_colossus_base_url()
TANTALUS_BASE_URL = get_tantalus_base_url()

colossus_api = ColossusApi()
tantalus_api = TantalusApi()

log = logging.getLogger('sisyphus')

jira_user = os.environ['JIRA_USERNAME']
jira_password = os.environ['JIRA_PASSWORD']
jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(jira_user, jira_password))


def get_parent_issue(jira_id):
    """
    Get parent ticket id

    Args:
        jira_id (str): Jira ticket id

    Returns:
        parent_ticket (str): Ticket id of parent ticket

    """

    issue = jira_api.issue(jira_id)

    try:
        parent_ticket = issue.fields.parent.key

    except:
        log.info(f"{jira_id} is not a sub-task")
        return None

    return parent_ticket

def is_child(jira_id):
    """
    Find if the JIRA issue is a subtask (i.e. has parent)

    Args:
        jira_id: Jira ticket id (e.g. SC-1234)

    Return:
        True if is a subtask, False otherwise
    """
    issue = jira_api.issue(jira_id)

    # raise exception if issue doesn't have fields property
    if not hasattr(issue, 'fields'):
        raise ValueError("JIRA issue missing 'fields' property. Cannot check parent-child relationship of an issue!")

    return hasattr(issue.fields, 'parent')

def comment_jira(jira_id, comment):
    """
    Comment on jira ticket
    """

    log.info("Commenting \n{} on ticket {}".format(comment, jira_id))

    jira_api.add_comment(jira_id, comment)


def delete_ticket(jira_id):
    """
    Comment on jira ticket
    """

    log.info(f"deleting {jira_id}")

    issue = jira_api.issue(jira_id)

    issue.delete()


def close_ticket(jira_id):
    """
    Close jira ticket

    Arguments:
        jira_id {str} -- jira ticket id
    """
    # get jira issue
    issue = jira_api.issue(jira_id)
    # check if ticket isn't closed
    if issue.fields.status.name != "Closed":
        # close ticket
        jira_api.transition_issue(issue, '2')


def update_jira_dlp(jira_id, aligner):

    logging.info("Updating description on {}".format(jira_id))

    description = [
        '(/) Alignment with ' + aligner,
        '(/) Hmmcopy',
        '(/) Path to results on blob:',
        '{noformat}Container: singlecellresults\nresults/' + jira_id + '{noformat}',
    ]

    issue = jira_api.issue(jira_id)

    if issue.fields.status.name != "Closed":
        # assign parent ticket to justina
        parent_jira_id = get_parent_issue(jira_id)
        parent_issue = jira_api.issue(parent_jira_id)
        parent_issue.update(assignee={"name": "jbwang"})

        update_description(jira_id, description, jira_user, remove_watcher=True)
        close_ticket(jira_id)


def update_jira_tenx(jira_id, library_pk):
    """
    Update analysis jira ticket desription with link to Tantalus result dataset and 
        Colossus library

    Args:
        jira (str): Jira ticket ID
        args (dict):
    """

    results_dataset = tantalus_api.get("resultsdataset", analysis__jira_ticket=jira_id)
    results_dataset_id = results_dataset["id"]

    library = colossus_api.get("tenxlibrary", id=int(library_pk))

    description = [
        "{noformat}Storage Account: scrnadata\n {noformat}",
        f"Tantalus Results: {TANTALUS_BASE_URL}/results/{results_dataset_id}",
        f"Colossus Library: {COLOSSUS_BASE_URL}/tenx/library/{library_pk}",
    ]

    # assign parent ticket
    parent_jira_id = get_parent_issue(jira_id)
    issue = jira_api.issue(parent_jira_id)
    if library['sample']['sample_id'].startswith("TFRI"):
        issue.update(assignee={"name": "shwu"})
    else:
        issue.update(assignee={"name": "jbwang"})

    update_description(jira_id, description, jira_user, remove_watcher=True)
    close_ticket(jira_id)


def update_description(jira_id, description, assignee, remove_watcher=False):

    description = '\n\n'.join(description)
    issue = jira_api.issue(jira_id)

    issue.update(assignee={"name": assignee}, description=description)


def add_attachment(jira_id, attachment_file_path, attachment_filename='', update=False):
    """
    Checks if file is already added to jira ticket; attaches if not. 
    """

    issue = jira_api.issue(jira_id)
    current_attachments = [a.filename for a in issue.fields.attachment]

    if (attachment_filename in current_attachments and not update):
        log.info("{} already added to {}".format(attachment_filename, jira_id))

    else:
        log.info("Adding {} to {}".format(attachment_filename, jira_id))
        if(attachment_filename):
            jira_api.add_attachment(issue=jira_id, attachment=attachment_file_path, filename=attachment_filename)
        else:
            jira_api.add_attachment(issue=jira_id, attachment=attachment_file_path)

def create_ticket(key, summary):
    """
    Create jira ticket
    
    Arguments:
        key {str} -- Project
        summary {str} -- Ticket description
    
    Returns:
        str -- created ticket id
    """
    mode = settings.mode.lower()
    jira_user = os.environ['JIRA_USERNAME']
    jira_key = 'SC' if mode == 'production' else 'MIS'

    task = {
        'project': {
            'key': jira_key
        },
        'summary': summary,
        'issuetype': {
            'name': 'Task'
        },
    }

    issue = jira_api.create_issue(fields=task)

    return issue.key


def create_jira_ticket_from_library(library_id):
    """
    Create analysis jira ticket as subtask of library jira ticket

    Args:
        library_id (str): Keys: library name

    Returns:
        analysis_jira_ticket: jira ticket id (ex. SC-1234)
    """
    mode = settings.mode.lower()
    jira_user = os.environ['JIRA_USERNAME']
    jira_key = 'SC' if mode == 'production' else 'MIS'

    library = colossus_api.get('library', pool_id=library_id)
    sample_id = library['sample']['sample_id']

    library_jira_ticket = library['jira_ticket']
    issue = jira_api.issue(library_jira_ticket)

    log.info('Creating analysis JIRA ticket as sub task for {}'.format(library_jira_ticket))

    # In order to search for library on Jira,
    # Jira ticket must include spaces
    sub_task = {
        'project': {
            'key': jira_key
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

    # Assign task to myself
    analysis_issue = jira_api.issue(analysis_jira_ticket)
    analysis_issue.update(assignee={'name': jira_user})

    log.info('Created analysis ticket {} for library {}'.format(analysis_jira_ticket, library_id))

    return analysis_jira_ticket

def get_description_from_jira_id(jira_id):
    """
    Find and return description associated with the JIRA ticket from Colossus. 
    If jira_id does not exist on Colossus, it will raise NotFoundError.

    Args:
        jira_id: jira ticket id (ex. SC-1234)

    Returns:
        description: description associated with the JIRA ticket in Colossus
    """
    
    analysis_info = colossus_api.get("analysis_information", analysis_jira_ticket=jira_id)
    description = analysis_info["library"]["description"]
    
    return description

def get_title_from_jira_id(jira_id):
    """
    Find and return title associated with the JIRA ticket from Colossus.
    If jira_id does not exist on Colossus, it will raise NotFoundError.

    Args:
        jira_id: jira ticket id (ex. SC-1234)

    Returns:
        title: title associated with the JIRA ticket in Colossus
    """
    analysis_info = colossus_api.get("analysis_information", analysis_jira_ticket=jira_id)
    sample_id = analysis_info["library"]["sample"]["sample_id"]
    library_id = analysis_info["library"]["pool_id"]

    title = f"Analysis of {sample_id} - {library_id}"
    
    return title

def create_alhena_description(jira_id):
    """
    Create link to Alhena and return it along with other sequencing information.

    Args:
        jira_id: jira ticket id (ex. SC-1234)

    Returns:
        alhena_description: link to Alhena along with other sequencing information
    """

    alhena_link = f'Link to Alhena: {ALHENA_BASE_URL}/alhena/dashboards/{jira_id}'

    analysis_info = colossus_api.get("analysis_information", analysis_jira_ticket=jira_id)

    date = analysis_info["analysis_run"]["last_updated"].split("T")[0]
    aligner = "MEM" if analysis_info["aligner"] == "M" else "ALN"
    lane_count = len(analysis_info["lanes"])
    sequencing = 0
    version = analysis_info["version"]

    if ("library" in analysis_info and "dlpsequencing_set" in analysis_info["library"]):
        for seq in analysis_info["library"]["dlpsequencing_set"]:
            sequencing =+ seq["number_of_lanes_requested"]

    alhena_description = f"{date} ({jira_id}) || ALIGNER:{aligner} || {lane_count}/{sequencing} Lanes || version:{version} || {alhena_link}"

    return alhena_description

def update_jira_alhena(jira_id):
    """
    Update JIRA ticket description to add Alhena related information.

    Args:
        jira_id: jira ticket id (ex. SC-1234)
    """

    # Find parent issue given JIRA ID if child issue
    if (is_child(jira_id)):     
        parent_id = get_parent_issue(jira_id)
    else:
        parent_id = jira_id

    parent = jira_api.issue(parent_id)

    # Retrieve description if it already exists, and remove duplicate viz server link
    if (hasattr(parent.fields, 'description') and parent.fields.description):
        description_list = parent.fields.description.split("\n")

        for i, line in enumerate(description_list):
            if (f'{ALHENA_BASE_URL}/alhena/dashboards/{jira_id}' in line):
                description_list.pop(i)
    else:
        description_list = []
    description_list.append(create_alhena_description(jira_id))

    parent.update(description="\n".join(description_list))

