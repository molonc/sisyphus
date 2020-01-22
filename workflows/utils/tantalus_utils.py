import os
import datamanagement.templates as templates
import dbclients.tantalus
from workflows.analysis.dlp import alignment, hmmcopy, annotation

tantalus_api = dbclients.tantalus.TantalusApi()


def sequence_dataset_match_lanes(dataset, lane_ids):
    if lane_ids is None:
        return True

    dataset_lanes = get_lanes_from_dataset(dataset)
    return set(lane_ids) == set(dataset_lanes)


def get_flowcell_lane(lane):
    if lane['lane_number'] == '':
        return lane['flowcell_id']
    else:
        return '{}_{}'.format(lane['flowcell_id'], lane['lane_number'])


def get_storage_type(storage_name):
    """
    Return the storage type of a storage with a given name
    Args:
        storage_name (string)
    Returns:
        storage_type (string)
    """

    storage = tantalus_api.get_storage(storage_name)

    return storage['storage_type']


def get_upstream_datasets(results_ids):
    """
    Get all datasets upstream of a set of results.
    Args:
        results_ids (list): list of results primary keys
    Returns:
        dataset_ids (list): list of dataset ids
    """
    results_ids = set(results_ids)

    upstream_datasets = set()
    visited_results_ids = set()

    while len(results_ids) > 0:
        results_id = results_ids.pop()

        if results_id in visited_results_ids:
            raise Exception('cycle in search for upstream datasets')

        results = tantalus_api.get('resultsdataset', id=results_id)

        if results['analysis']:
            analysis = tantalus_api.get('analysis', id=results['analysis'])
            upstream_datasets.update(analysis['input_datasets'])
            results_ids.update(analysis['input_results'])

    return list(upstream_datasets)


def create_qc_analyses_from_library(library_id):
    """ 
    Create align, hmmcopy, and annotation analysis objects

    Args:
        library_id (str): library name
    """
    config = file_utils.load_json(config_filename)

    # taxonomy id map
    taxonomy_id_map = {
        '9606': 'HG19',
        '10090': 'MM10',
    }

    # create ticket
    jira_ticket = get_analyses.create_analysis_jira_ticket(library_id)

    # get library info from colossus
    library = colossus_api.get('library', pool_id=library_id)
    taxonomy_id = library['sample']['taxonomy_id']

    args = {}
    args['library_id'] = library_id
    args['aligner'] = "BWA_MEM_0_7_6A"
    args['ref_genome'] = taxonomy_id_map[taxonomy_id]
    args['gsc_lanes'] = None
    args['brc_flowcell_ids'] = None

    # creates align analysis object on tantalus
    alignment.create_analysis(jira_ticket, version, args)

    # delete arguments not needed for hmmcopy and annotation
    del args['gsc_lanes']
    del args['brc_flowcell_ids']

    # creates hmmcopy analysis object on tantalus
    hmmcopy.create_analysis(jira_ticket, version, args)
    # creates annotation analysis object on tantalus
    annotation.create_analysis(jira_ticket, version, args)
