import os
import datamanagement.templates as templates
import dbclients.tantalus
import dbclients.colossus
from workflows.analysis.dlp import alignment
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


def create_analysis(analysis_type, jira_ticket, version, args):
    """
    Creates analysis object on tantalus with minimal fields

    Args:
        analysis_type (str): type of analysis
        jira_ticket (str): jira ticket key ex. SC-####
        version (str): version of singlecellpipeline ex. v#.#.#
        args (dict): analysis info 
    """
    # get align analysis in order to get template for analysis name
    align_analysis = tantalus_api.get(
        "analysis",
        jira_ticket=jira_ticket,
        analysis_type__name="align",
    )

    # replace align in name with analysis type
    name = align_analysis["name"].replace("align", analysis_type)

    # set fields and keys
    fields = dict(
        name=name,
        analysis_type=analysis_type,
        version=version,
        jira_ticket=jira_ticket,
        args=args,
        status="ready",
    )

    keys = ['name']

    # create analysis
    analysis, _ = tantalus_api.create('analysis', fields, keys, get_existing=True)


def create_qc_analyses_from_library(library_id, jira_ticket, version):
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

    # add arguments
    args = {}
    args['library_id'] = library_id
    # default aligner is BWA_MEM_0_7_6A
    args['aligner'] = "BWA_MEM_0_7_6A"
    args['ref_genome'] = reference_genome
    args['gsc_lanes'] = None
    args['brc_flowcell_ids'] = None

    # creates align analysis object on tantalus
    alignment.create_analysis(jira_ticket, version, args)

    # delete arguments not needed for hmmcopy and annotation
    del args['gsc_lanes']
    del args['brc_flowcell_ids']

    # cannot create hmmcopy analysis using create_analysis method from HMMCopyAnalysis
    # because this would require the bam datasets and files to already be created
    # instead, create "empty" analysis with minimal fields
    create_analysis("hmmcopy", jira_ticket, version, args)

    # cannot create annotation analysis using create_analysis method from AnnotationAnalysis
    # because this would require the align and hmmcopy results and files to already be created
    # instead, create "empty" analysis with minimal fields
    create_analysis("annotation", jira_ticket, version, args)
