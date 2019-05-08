import os
import sys
import logging
from jira import JIRA, JIRAError
from constants import LOGGING_FORMAT

# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

JIRA_USER = os.environ['JIRA_USER']
JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

def comment_jira(jira_id, comment):
	logging.info("Commenting \n{} on ticket {}".format(
		comment, 
		jira_id)
	)

	jira_api.add_comment(jira_id, comment)



