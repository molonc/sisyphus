from generic_tasks import make_tantalus_query, get_or_create


def query_for_sample(sample_id):
	'''
	Get the sample object from Tantalus for the given sample ID
	Args:
		sample_id: sample ID to query (e.g. SA501)
	Returns:
		Sample with the given sample ID
	'''

	sample = make_tantalus_query('sample', {'sample_id': sample_id})

	if len(sample) == 0:
		raise Exception('No sample {}'.format(sample_id))

	return sample.pop()


def create_results(results_dict):
	'''
	Checks if an results object with the given name exists in Tantalus and
	creates it if it doesn't exist.
	Args:
		name: name of the results object
		results_type: type of analysis that generated the results
		results_version: pipeline version that generated the results
		analysis: ID of the associated analysis object
		sample_ids: IDs of the associated samples (primary IDs in Tantalus, not SA501)
		file_resources: IDs of the associated file resources
	Retuns:
		Reults with the given parameters
	'''

	return get_or_create('results', **results_dict) 
