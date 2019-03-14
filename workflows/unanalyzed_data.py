import os
import logging
from jira import JIRA, JIRAError
from datetime import datetime
from collections import defaultdict

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import NotFoundError

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

log = logging.getLogger('sisyphus')

def get_lanes_from_bams_datasets():
    '''
    Get lanes of all bam datasets

    Return:
        bam_lanes: list of lanes
    '''
    bam_lanes = []
    bam_datasets = tantalus_api.list('sequence_dataset', library__library_type__name="SC_WGS", dataset_type="BAM")
    for bam_dataset in bam_datasets:
        for lane in bam_dataset['sequence_lanes']:
            bam_lanes.append((lane['flowcell_id'], lane['lane_number']))

    return bam_lanes


def search_for_unaligned_data():
    '''
    Get lanes from fastq datasets and compare with lanes in bam datasets.
    If fastq lanes are not in bam lanes, add the library associated to the lane to libraries to analyze

    Return:
        libraries_to_analyze: list of library ids
    '''

    log.info('Searching for unaligned data')
    bam_lanes = get_lanes_from_bams_datasets()

    fastq_lanes = []
    unaligned_lanes = []

    fastq_datasets = tantalus_api.list('sequence_dataset', library__library_type__name="SC_WGS", dataset_type="FQ")
    for fastq_dataset in fastq_datasets:
        library_id = fastq_dataset['library']['library_id']
        for lane in fastq_dataset['sequence_lanes']:
            flowcell_id = lane['flowcell_id']
            lane_number = lane['lane_number']
            if (flowcell_id, lane_number) not in bam_lanes:
                log.info("Unaligned data for library_id {}, flowcell_id {}, lane_number {}".format(library_id, flowcell_id, lane_number))
                unaligned_lanes.append('{}_{}'.format(lane['flowcell_id'], lane['lane_number']))

    sequencing_ids = set()
    for lane in unaligned_lanes:
        try:
            lane_infos = list(colossus_api.list('lane', flow_cell_id=lane))
        except NotFoundError as e:
            log.info(e)
            lane_infos = None 
            continue
        # Get sequencing associated with lanes
        if lane_infos is not None:
            for lane_info in lane_infos:
                sequencing_ids.add(lane_info['sequencing'])

    # Get libraries associated with library
    libraries_to_analyze = set()
    for sequencing_id in sequencing_ids:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)
        libraries_to_analyze.add(sequencing['library'])

    return list(libraries_to_analyze)


def search_for_no_hmmcopy_data():
    '''
    Get lanes from input datasets of hmmcopy analyses and compare with lanes in bam datasets.
    If bame lanes are not in hmmcopy lanes, add the library associated to the lane to libraries to analyze

    Return:
        libraries_to_analyze: list of library ids
    '''
    log.info('Searching for no hmmcopy data')
    bam_lanes = get_lanes_from_bams_datasets()


    # TODO: Filter for complete hmmcopy analysis only
    # Filtering for all hmmcopy will not catch all lanes that need hmmcopy
    hmmcopy_lane_inputs = []
    hmmcopy_analyses = tantalus_api.list('analysis', analysis_type__name="hmmcopy")
    for hmmcopy_analysis in hmmcopy_analyses:
        for dataset_id in hmmcopy_analysis['input_datasets']:
            dataset = tantalus_api.get('sequence_dataset', id=dataset_id)
            for lane in dataset['sequence_lanes']:
                hmmcopy_lane_inputs.append((lane['flowcell_id'], lane['lane_number']))

    no_hmmcopy_lanes = []
    for lane in bam_lanes:
        if lane not in hmmcopy_lane_inputs:
            log.info("Data for flowcell_id {} and lane_number {} has not been run with hmmcopy".format(
                lane[0], lane[1]))
            no_hmmcopy_lanes.append("{}_{}".format(lane[0], lane[1]))

    sequencing_ids = set()
    for lane in no_hmmcopy_lanes:
        try:
            lane_infos = list(colossus_api.list('lane', flow_cell_id=lane))
        except NotFoundError as e:
            log.info(e)
            lane_infos = None 
            continue

        # Get sequencing associated with lanes
        if lane_infos is not None:
            for lane_info in lane_infos:
                sequencing_ids.add(lane_info['sequencing'])

    # Get libraries associated with library
    libraries_to_analyze = set()
    for sequencing_id in sequencing_ids:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)
        libraries_to_analyze.add(sequencing['library'])

    return list(libraries_to_analyze)

