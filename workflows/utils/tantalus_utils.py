import os
import datamanagement.templates as templates
import dbclients.tantalus
import dbclients.colossus
from workflows.analysis.dlp.alignment import AlignmentAnalysis
from workflows.analysis.dlp.annotation import AnnotationAnalysis
from workflows.analysis.dlp.hmmcopy import HMMCopyAnalysis
from workflows.utils.colossus_utils import get_ref_genome

tantalus_api = dbclients.tantalus.TantalusApi()
colossus_api = dbclients.colossus.ColossusApi()


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


def create_qc_analyses_from_library(library_id, jira_ticket, version, analysis_type, aligner="M"):
    """ 
    Create align, hmmcopy, and annotation analysis objects

    Args:
        library_id (str): library name
        jira_ticket (str): jira ticket key ex. SC-####
        version (str): version of singlecellpipeline ex. v#.#.#
    """

    # get library info from colossus
    library = colossus_api.get('library', pool_id=library_id)
    reference_genome = get_ref_genome(library)

    aligner_map = {"A": "BWA_ALN", "M": "BWA_MEM"}

    # add arguments
    args = {}
    args['library_id'] = library_id
    # default aligner is BWA_MEM
    args['aligner'] = aligner_map[aligner]
    args['ref_genome'] = reference_genome
    args['gsc_lanes'] = None
    args['brc_flowcell_ids'] = None

    # creates align analysis object on tantalus
    if analysis_type == "align":
        AlignmentAnalysis.create_from_args(tantalus_api, jira_ticket, version, args)

    else:
        # delete arguments not needed for hmmcopy and annotation
        del args['gsc_lanes']
        del args['brc_flowcell_ids']

        if analysis_type == "hmmcopy":
            HMMCopyAnalysis.create_from_args(tantalus_api, jira_ticket, version, args)

        elif analysis_type == "annotation":
            AnnotationAnalysis.create_from_args(tantalus_api, jira_ticket, version, args)
        else:
            raise Exception(f"{analysis_type} is an invalid analysis type")