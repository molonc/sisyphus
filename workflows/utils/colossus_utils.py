import os
import logging

from dbclients.colossus import ColossusApi
from dbclients.basicclient import NotFoundError

colossus_api = ColossusApi()

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False


def get_sequencing_ids(library_info):
    """
    Given library id (str), return list of sequencings
    """
    sequencings = [sequencing['id'] for sequencing in library_info['dlpsequencing_set']]
    return sequencings


def get_lanes_from_sequencings(sequencing_id_list):
    """
    Given list of sequencing ids, return list of all unique lanes
    """
    lanes = set()
    for sequencing_id in sequencing_id_list:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)
        for lane in sequencing['dlplane_set']:
            lanes.add(lane['id'])

    return list(lanes)


def get_ref_genome(library_info, is_tenx=False):
    """
    Get reference genome from taxonomy id

    Args:
        library_info (dict): Library from colossus
        is_tenx (bool): boolean whether library is a tenx library

    Return:
        reference_genome (str)
    """

    if is_tenx:
        taxonomy_id_map = {
            '9606': 'HG38',
            '10090': 'MM10',
        }

    else:
        taxonomy_id_map = {
            '9606': 'HG19',
            '10090': 'MM10',
        }

    taxonomy_id = library_info['sample']['taxonomy_id']
    reference_genome = taxonomy_id_map[taxonomy_id]

    return reference_genome


def create_colossus_analysis(library_id, jira_ticket, version, aligner):
    """
    Create analysis objects on Colossus

    Args:
        library_id (str): Library/Pool id
        jira_ticket (str): Jira ticket id (ex. SC-1234)
        version (str): Version of pipeline
        aligner (str): Shortened name of aligner (A or M)

    Returns:
        analysis_id (int): pk of analysis information object on Colossus
    """

    try:
        analysis = colossus_api.get("analysis_information", analysis_jira_ticket=jira_ticket)

    except NotFoundError:
        taxonomy_id_map = {
            # grch37
            '9606': 1,
            # mm10
            '10090': 2,
        }

        library_info = colossus_api.get('library', pool_id=library_id)
        taxonomy_id = library_info['sample']['taxonomy_id']
        ref_genome_pk = taxonomy_id_map[taxonomy_id]

        sequencing_ids = get_sequencing_ids(library_info)
        lanes = get_lanes_from_sequencings(sequencing_ids)

        log.info(
            f"Creating analysis information object for {library_info['sample']['sample_id']}_{library_id} on Colossus")

        analysis, _ = colossus_api.create(
            'analysis_information',
            fields=dict(
                library=library_info['id'],
                version=version,
                sequencings=sequencing_ids,
                reference_genome=ref_genome_pk,
                aligner=aligner,
                analysis_jira_ticket=jira_ticket,
                lanes=lanes,
            ),
            keys=["analysis_jira_ticket"],
        )

        analysis_run, _ = colossus_api.create(
            'analysis_run',
            fields=dict(
                run_status='idle',
                dlpanalysisinformation=analysis["id"],
                blob_path=jira_ticket,
            ),
            keys=["dlpanalysisinformation"],
        )

        colossus_api.update('analysis_information', analysis['id'], analysis_run=analysis_run['id'])

        log.info('Created analysis {} on colossus'.format(analysis['id']))

    analysis_id = analysis['id']

    return analysis_id
