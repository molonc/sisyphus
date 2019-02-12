from jira import JIRA, JIRAError
import os

JIRA_USER = os.environ['JIRA_USER']
JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

def comment_jira(jira_id, comment_info):
	issue = jira_api.issue(jira_id)
	comment = jira_api.add_comment(jira_id, comment_info)



