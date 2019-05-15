import os
from utils.update_jira import update_jira_tenx
from dbclients.colossus import ColossusApi
from jira import JIRA, JIRAError


colossus_api = ColossusApi()
JIRA_USER = os.environ['JIRA_USER']
JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))
			
def get_project_id_from_name():
    '''
    Gets a project ID from the provided project Name
    Example: 'Single Cell' returns 11220
    '''
    try:
        projects = sorted(jira_api.projects(), key=lambda project: project.name.strip())
        for project in projects:
            if(project.name == 'Single Cell'):
                return project.id
        return None
    except JIRAError as e:
        raise JIRAError()

project_id = get_project_id_from_name()

def create_jira_ticket(library):
	issue_dict = {
        'project': {'id': int(project_id)},
        'summary': '{} - {}'.format(library['sample']['sample_id'], library['description']),
        'description': 'Awaiting first sequencing...',
        'issuetype': {'name': 'Task'},
        'reporter': {'name': 'coflanagan'},
        'assignee': {'name': None},
	}
	new_issue = jira_api.create_issue(fields=issue_dict)
	return str(new_issue)

def main():
	tenx_library_list = colossus_api.list('tenxlibrary')
	for library in tenx_library_list:
		if library['jira_ticket'] is None:
			jira_ticket = create_jira_ticket(library)
			print(jira_ticket)
			library_id = library['id']
			print(library_id)
			colossus_api.update('tenxlibrary', library_id, fields={'jira_ticket': jira_ticket})
			print(colossus_api.get('tenxlibrary', fields={'id': library_id}))
			break

if __name__ == '__main__':
	main()
