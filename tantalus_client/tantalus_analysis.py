from generic_tasks import get_or_create, make_tantalus_query, tantalus_update


def create_analysis(analysis_dict):
	'''
	Checks if an analysis object with the given name exists in Tantalus and
	creates it if it doesn't exist.
	Args:
		name: name of the analysis object
		jira_ticket: Jira ticket associated with the analysis
		last_updated: date and time the analysis object was last updated (only used when creating an analysis object)
		args: string of command line arguments for the analysis
		status: run status of the analysis
	Retuns:
		Analysis with the given parameters
	'''

	return get_or_create('analysis', **analysis_dict)


def analysis_update(id, **fields):
	'''
	Update the analysis object with the given ID with the given field
	Args:
		id: the ID of the analysis object to update
		fields: analysis object fields to update
	Returns:
		Analysis with the updated parameters
	'''

	return tantalus_update('analysis', id, **fields)
