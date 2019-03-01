from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from collections import defaultdict
from datetime import datetime
from dbclients.basicclient import NotFoundError
from jira import JIRA, JIRAError
import logging
import hashlib
import os

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

def get_lanes_from_bams_datasets():
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
    '''
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
                print("Unaligned data for library_id {}, flowcell_id {}, lane_number {}".format(library_id, flowcell_id, lane_number))
                unaligned_lanes.append('{}_{}'.format(lane['flowcell_id'], lane['lane_number']))

    # Might just be double check
    sequencing_ids_from_lanes = set()
    for lane in unaligned_lanes:
        try:
            lane_infos = list(colossus_api.list('lane', flow_cell_id=lane))
        except NotFoundError as e:
            print(e)
            lane_infos = None 
            continue
        # Get sequencing associated with lanes
        if lane_infos is not None:
            for lane_info in lane_infos:
                sequencing_ids_from_lanes.add(lane_info['sequencing'])

    # Get libraries associated with library
    libraries_to_analyze = set()
    for sequencing_id in sequencing_ids_from_lanes:
        sequencing = tantalus_api.get('sequencing', id=sequencing_id)
        libraries_to_analyze.add(sequencing['library__pool_id'])

    return libraries_to_analyze


def search_for_no_hmmcopy_data():
    bam_lanes = get_lanes_from_bams_datasets()

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
            # TODO: print library id and jira ticket as well??
            print("Data for flowcell_id {} and lane_number {} has not been run with hmmcopy".format(
                lane[0], lane[1]))
            no_hmmcopy_lanes.append("{}_{}".format(lane[0], lane[1]))

    # Might just be for a double check
    sequencing_ids_from_lanes = set()
    for lane in no_hmmcopy_lanes:
        try:
            lane_infos = list(colossus_api.list('lane', flow_cell_id=lane))
        except NotFoundError as e:
            print(e)
            lane_infos = None 
            continue

        # Get sequencing associated with lanes
        if lane_infos is not None:
            for lane_info in lane_infos:
                sequencing_ids_from_lanes.add(lane_info['sequencing'])

    # Get libraries associated with library
    libraries_to_analyze = set()
    for sequencing_id in sequencing_ids_from_lanes:
        sequencing = tantalus_api.get('sequencing', id=sequencing_id)
        libraries_to_analyze.add(sequencing['library__pool_id'])

    return libraries_to_analyze


# def get_analyses_to_run(sequencing_ids, analysis_type):
#     ''' 
#     Get analyses to run by comparing latest sequencing date to latest complete analysis date

#     Args:
#     sequencing_ids: List of sequencing ids
#     analysis_type: String containing either 'align' or 'hmmcopy'

#     Returns:
#     analyses_to_run: List of dictionaries with keys library_id, latest_sequencing_date (maybe remove), and analysis_type
#     '''

#     analyses_to_run = []
#     complete_statuses = ('complete', '{}_complete'.format(analysis_type))

#     for sequencing_id in sequencing_ids:
#         sequencing = colossus_api.get('sequencing', id=sequencing_id)
#         dlp_library_id = sequencing['library'] 
#         print("\nComparing sequencing dates and analysis dates for library {} for {} analysis".format(dlp_library_id, analysis_type))

#         latest_sequencing_date = ""
#         # Check: Sequencing dates may not have been set
#         sequencing_dates = [lane['sequencing_date'] for lane in sequencing['dlplane_set']]
#         sequencing_dates.sort(reverse=True)

#         latest_sequencing_date = sequencing_dates[0]

#         if not latest_sequencing_date:
#             # Maybe: Raise exception vs logging warning
#             # raise Exception("Sequencing date is not set")
#             print("Lanes do not have sequencing dates for sequencing id {}".format(sequencing_id))
#             continue

#         # TODO: Compare latest sequencing date to analysis date
#         # Check if analysis is actually bwa-aln?
#         analyses = list(colossus_api.list('analysis_information', library__pool_id=dlp_library_id))

#         if len(analyses) == 0:
#             analyses_to_run.append(dict(library_id=dlp_library_id, latest_sequencing_date=latest_sequencing_date, analysis_type=analysis_type))
#             print("No analysis information for library {}; adding to analyses to run".format(dlp_library_id))
#             continue

#         latest_analysis_date = ""
#         analysis_dates = []
#         for analysis in analyses:
#             # May need to change if looking for bwa-mem aligner analyses
#             # Maybe: Pass desired aligner as argument
#             if analysis['aligner'] != "A":
#                 continue

#             analysis_run = analysis['analysis_run']
#             if analysis_run['run_status'] not in complete_statuses:
#                 continue

#             analysis_dates.append(analysis_run['last_updated'])  
        
#         if analysis_dates: 
#             analysis_dates.sort(reverse=True)
#             latest_analysis_date = analysis_dates[0]

#         if not latest_analysis_date:
#             print("No completed analysis for library {}; adding to analyses to run".format(dlp_library_id))
#             analyses_to_run.append(dict(library_id=dlp_library_id, latest_sequencing_date=latest_sequencing_date, analysis_type=analysis_type))
#             continue

#         if latest_sequencing_date > latest_analysis_date:
#             print("Latest sequencing date for library {} was on {} but latest analysis was {}".format(
#                 dlp_library_id, latest_sequencing_date, latest_analysis_date))

#             analyses_to_run.append(dict(library_id=dlp_library_id, latest_sequencing_date=latest_sequencing_date, analysis_type=analysis_type))

#         else:
#             print("Library {} does not need {} analysis".format(dlp_library_id, analysis_type))

#     return analyses_to_run


# def get_sequencings(library_id):
#     library_info = colossus_api.get('library', pool_id=library_id)
#     sequencings = [sequencing['id'] for sequencing in library_info['dlpsequencing_set']]
#     return sequencings


# def get_new_analyses_to_run(analysis_type):
#     '''
#     Filter dlp libraries by 
#     '''
#     print('\n \n FINDING {} ANALYSIS TO RUN...'.format(analysis_type))
#     taxonomy_id_map = {
#         '9606':      'HG19',
#         '10090':     'MM10',
#     }
#     aligner = 'BWA_ALN_0_5_7'
#     libraries = list(colossus_api.list('library', exclude_from_analysis=False))
#     analyses_to_run = []

#     for library in libraries:
#         lanes = set()        
#         library_id = library['pool_id']
#         print("Checking library {}".format(library_id))
#         taxonomy_id = library['sample']['taxonomy_id']
#         reference_genome = taxonomy_id_map[taxonomy_id]
#         sequencing_ids = get_sequencings(library_id)
#         data_imported = True

#         if not sequencing_ids:
#             print('Library {} has no sequencings; skipping'.format(library_id))
#             continue

#         latest_sequencing_date = ""
#         for sequencing_id in sequencing_ids:
#             sequencing = colossus_api.get('sequencing', id=sequencing_id)

#             # Check if all lanes have been imported
#             if sequencing['number_of_lanes_requested'] != 0 and len(sequencing['dlplane_set']) != sequencing['number_of_lanes_requested']:
#                 data_imported = False
#                 print("Not all data has been imported; skipping")
#                 break

#             # Get latest sequencing date
#             sequencing_dates = [lane['sequencing_date'] for lane in sequencing['dlplane_set']]
#             sequencing_dates.sort(reverse=True)

#             if sequencing_dates:
#                 latest_sequencing_date = sequencing_dates[0]
#             else:
#                 latest_sequencing_date = None

#             for lane in sequencing['dlplane_set']:
#                 lanes.add(lane['flow_cell_id']) 

#         if data_imported == True:
#             lanes = ", ".join(sorted(lanes))
#             lanes = hashlib.md5(lanes)
#             lanes_hashed = "{}".format(lanes.hexdigest()[:8])
#             analysis_name = "sc_{}_{}_{}_{}_{}".format(
#                 analysis_type, 
#                 aligner, 
#                 reference_genome, 
#                 library_id,
#                 lanes_hashed,
#             )

#             # Check if analysis already exists on Tantalus and colossus??
#             try:
#                 analysis = tantalus_api.get('analysis', name=analysis_name)
#                 print("Analysis already exists for {}; name: {}".format(library_id, analysis_name))
#             except NotFoundError:
#                 # Check if analyses already created but just not run
#                 print("Need to run analysis for {}, name: {}".format(library_id, analysis_name))
#                 analyses_to_run.append(dict(
#                     library_id=library_id, 
#                     latest_sequencing_date=latest_sequencing_date,
#                     analysis_type=analysis_type
#                     )
#                 )

#     return analyses_to_run





