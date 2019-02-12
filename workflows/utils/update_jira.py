"""
Author: sochan

python update_jira.py SC-1234 
updates the description of the jira ticket and assigns it to Emma
"""
from jira import JIRA, JIRAError
import os
import logging

log = logging.getLogger('sisyphus')
JIRA_USER = os.environ['JIRA_USER']
JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

def update_jira(jira_id, aligner, analysis_type):
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

        description = '\n\n'.join(description)
        issue = jira_api.issue(jira_id)

        issue.update(notify=False, description=description)

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

        description = '\n\n'.join(description)
        issue = jira_api.issue(jira_id)

        issue.update(notify=False, assignee={"name": "elaks"}, description=description)
