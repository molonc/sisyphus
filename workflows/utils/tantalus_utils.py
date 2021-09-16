import os
import datamanagement.templates as templates
import dbclients.tantalus
import dbclients.colossus

# Analysis imports
from workflows.analysis.dlp.alignment import AlignmentAnalysis
from workflows.analysis.dlp.annotation import AnnotationAnalysis
from workflows.analysis.dlp.hmmcopy import HMMCopyAnalysis
from workflows.tenx.models import TenXAnalysis

from workflows.utils.colossus_utils import get_ref_genome
from workflows.utils import file_utils

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


def create_tenx_analysis_from_library(jira, library, taxonomy_id=None):
    """Creates tenx analysis on Tantalus

    Args:
        jira (str): JIRA ID (e.g. SC-1234)
        library (str): Library Name

    Returns:
        Object: Tantalus Analysis 
    """
    # get config
    default_config = os.path.join(
        os.path.dirname(os.path.dirname(os.path.realpath(__file__))),
        'config',
        'normal_config_tenx.json',
    )
    config = file_utils.load_json(default_config)

    # init args
    args = {}
    args['library_id'] = library
    library_info = colossus_api.get('tenxlibrary', name=library)
    args['ref_genome'] = get_ref_genome(library_info, is_tenx=True, taxonomy_id=taxonomy_id)
    args['version'] = config["version"]

    # get list of storages
    storages = config["storages"]

    # create analysis
    analysis = TenXAnalysis(
        jira,
        config["version"],
        args,
        storages=storages,
        update=True,
    )

    return analysis.analysis

def get_analyses_from_jira(jira):
    """
    Get all the analyses associated with JIRA ID.

    Args:
        jira (str): JIRA ID (e.g. SC-1234)

    Returns:
        analyses (generator): generator of analysis linked with specified JIRA ID
    """
    analyses = tantalus_api.list("analysis", jira_ticket=jira)

    return analyses

def get_resultsdataset_from_analysis(analysis):
    """
    Get all the output resultsdataset associated with analysis ID.

    Args:
        analysis (int): analysis ID (e.g. 1234)

    Returns:
        resultsdataset (generator): generator of resultsdataset linked with specific analysis 
    """
    resultsdataset = tantalus_api.list("resultsdataset", analysis=analysis)

    return resultsdataset

def get_sequencedataset_from_analysis(analysis):
    """
    Get all the output sequencedataset associated with analysis ID.

    Args:
        analysis (int): analysis ID (e.g. 1234)

    Returns:
        sequencedataset (generator): generator of sequencedataset linked with specific analysis 
    """
    sequencedataset = tantalus_api.list("sequencedataset", analysis=analysis)

    return sequencedataset

def get_sequencedataset_from_library_id(library_id):
    """
    Get all the output sequencedataset associated with library ID.

    Args:
        library_id (int): library ID (e.g. A12345A)

    Returns:
        sequencedataset (generator): generator of sequencedataset linked with specific analysis 
    """
    sequencedataset = tantalus_api.list("sequencedataset", library__library_id=library_id)

    return sequencedataset

def get_sequencing_lane_from_library_id(library_id):
    """
    Get all the output sequencing_lane associated with library ID.

    Args:
        library_id (int): library ID (e.g. A12345A)

    Returns:
        sequencing_lane (generator): generator of sequencing_lane linked with specific library ID 
    """
    sequencing_lane = tantalus_api.list("sequencing_lane", dna_library__library_id=library_id)

    return sequencing_lane
