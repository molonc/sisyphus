import os
import logging
from jira import JIRA, JIRAError
from datetime import datetime
from collections import defaultdict
from distutils.version import StrictVersion

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import NotFoundError

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

log = logging.getLogger('sisyphus')


def get_lanes_from_bams_datasets(library_type):
    '''
    Get lanes of all bam datasets

    Args:
        library_type (str)

    Return:
        bam_lanes (list)
    '''
    bam_lanes = []
    bam_datasets = tantalus_api.list('sequence_dataset', library__library_type__name=library_type, dataset_type="BAM")
    for bam_dataset in bam_datasets:
        for lane in bam_dataset['sequence_lanes']:
            flowcell = f"{lane['flowcell_id']}_{lane['lane_number']}"
            bam_lanes.append(flowcell)

    return bam_lanes


def search_for_unaligned_data(library_type, bam_lanes):
    '''
    Get lanes from fastq datasets and compare with lanes in bam datasets. If
        fastq lanes are not in bam lanes, add the library associated to the
        lane to libraries to analyze.

    Return:
        libraries_to_analyze: list of library ids
    '''

    log.info('Searching for unaligned data')

    unaligned_lanes = []
    libraries_to_analyze = set()

    fastq_datasets = tantalus_api.list(
        'sequence_dataset',
        library__library_type__name=library_type,
        dataset_type="FQ",
    )
    for fastq_dataset in fastq_datasets:
        library_id = fastq_dataset['library']['library_id']
        for lane in fastq_dataset['sequence_lanes']:
            flowcell = f"{lane['flowcell_id']}_{lane['lane_number']}"
            if flowcell not in bam_lanes:
                log.info(f"Library {library_id}: Unaligned data for lane {flowcell}")
                unaligned_lanes.append(flowcell)

    sequencing_ids = set()
    for lane in unaligned_lanes:
        lane_infos = list(colossus_api.list('lane', flow_cell_id=lane))
        if not lane_infos:
            continue
        # Get sequencing associated with lanes
        for lane_info in lane_infos:
            sequencing_ids.add(lane_info['sequencing'])

    # Get libraries associated with library
    libraries_to_analyze = set()
    for sequencing_id in sequencing_ids:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)
        libraries_to_analyze.add(sequencing['library'])

    return list(libraries_to_analyze)


def search_for_no_hmmcopy_data(bam_lanes):
    """
    Get lanes from input datasets of hmmcopy analyses and qc analyses and 
    compare with lanes in bam datasets. If bam lanes are not in hmmcopy 
    lanes, add the library associated to the lane to libraries to analyze.

    Return:
        libraries_to_analyze (list): list of library ids
    """

    log.info('Searching for no hmmcopy data')

    # Search for lanes that already been ran under hmmcopy
    hmmcopy_lane_inputs = []
    hmmcopy_analyses = tantalus_api.list('analysis', analysis_type__name="hmmcopy", status="complete")
    for hmmcopy_analysis in hmmcopy_analyses:
        for dataset_id in hmmcopy_analysis['input_datasets']:
            dataset = tantalus_api.get('sequence_dataset', id=dataset_id)
            for lane in dataset['sequence_lanes']:
                flowcell = f"{lane['flowcell_id']}_{lane['lane_number']}"
                hmmcopy_lane_inputs.append(flowcell)

    # Search for lanes that already been ran under QC
    qc_analyses = tantalus_api.list('analysis', analysis_type__name="qc", status="complete")
    for qc_analysis in qc_analyses:
        qc_bam_datasets = tantalus_api.list(
            "sequencedataset",
            analysis__jira_ticket=qc_analysis["jira_ticket"],
            dataset_type="BAM",
        )
        for dataset in qc_bam_datasets:
            for lane in dataset['sequence_lanes']:
                flowcell = f"{lane['flowcell_id']}_{lane['lane_number']}"
                log.info(f"Lane {flowcell} already ran under QC")
                hmmcopy_lane_inputs.append(flowcell)

    # Get lanes that have not been ran with hmmcopy or QC
    no_hmmcopy_lanes = []
    for lane in bam_lanes:
        if lane not in hmmcopy_lane_inputs:
            log.info(f"Lane {lane} has not been run with hmmcopy")
            no_hmmcopy_lanes.append(lane)

    # Get sequencing associated with lanes
    sequencing_ids = set()
    for lane in no_hmmcopy_lanes:
        lane_infos = list(colossus_api.list('lane', flow_cell_id=lane))
        if not lane_infos:
            continue

        for lane_info in lane_infos:
            sequencing_ids.add(lane_info['sequencing'])

    # Get libraries associated with sequencing
    libraries_to_analyze = set()
    for sequencing_id in sequencing_ids:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)
        libraries_to_analyze.add(sequencing['library'])

    return list(libraries_to_analyze)


def search_for_no_annotation_data(aligner):
    """ 
    Search tantalus for all hmmcopy analyses with a specific version without annotations

    Returns:
        libraries_to_analyze (list): list of library ids
    """
    aligner_map = {'A': 'BWA_ALN_0_5_7', 'M': 'BWA_MEM_0_7_6A'}
    libraries_to_analyze = set()
    hmmcopy_analyses = list(tantalus_api.list(
        'analysis',
        analysis_type__name="hmmcopy",
        status="complete",
    ))
    annotation_analyses = list(tantalus_api.list(
        'analysis',
        analysis_type__name="annotation",
        status="complete",
    ))

    annotation_tickets = [analysis["jira_ticket"] for analysis in annotation_analyses]

    for analysis in hmmcopy_analyses:
        if analysis["args"]["aligner"] != aligner_map[aligner]:
            continue
        if StrictVersion(analysis["version"].strip('v')) >= StrictVersion('0.5.0'):
            if analysis["jira_ticket"] not in annotation_tickets:
                log.info(f"need to run annotations on library {analysis['args']['library_id']}")
                libraries_to_analyze.add(analysis["args"]["library_id"])

    return list(libraries_to_analyze)
