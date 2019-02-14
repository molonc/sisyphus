from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from collections import defaultdict
from dbclients.basicclient import NotFoundError
import logging

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
                logging.info("Unaligned data for library_id {}, flowcell_id {}, lane_number {}".format(library_id, flowcell_id, lane_number))
                unaligned_lanes.append('{}_{}'.format(lane['flowcell_id'], lane['lane_number']))

    logging.info("unaligned_lanes {} ".format(unaligned_lanes))
    # Might just be double check
    sequencing_ids_from_lanes = set()
    for lane in unaligned_lanes:
        try:
            lane_infos = list(colossus_api.list('lane', flow_cell_id=lane))
        except NotFoundError as e:
            logging.info(e)
            lane_infos = None 
            continue
        # Get sequencing associated with lanes
        if lane_infos is not None:
            for lane_info in lane_infos:
                sequencing_ids_from_lanes.add(lane_info['sequencing'])

    logging.info("sequencings {} ".format(sequencing_ids_from_lanes))
    unaligned_data = get_analyses_to_run(sequencing_ids_from_lanes, 'align')

    return unaligned_data

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
            # TODO: logging.info library id and jira ticket as well??
            logging.info("Data for flowcell_id {} and lane_number {} has not been run with hmmcopy".format(
                lane[0], lane[1]))
            no_hmmcopy_lanes.append("{}_{}".format(lane[0], lane[1]))

    # Might just be for a double check
    sequencing_ids_from_lanes = set()
    for lane in no_hmmcopy_lanes:
        try:
            lane_infos = list(colossus_api.list('lane', flow_cell_id=lane))
        except NotFoundError as e:
            logging.info(e)
            lane_infos = None 
            continue

        # Get sequencing associated with lanes
        if lane_infos is not None:
            for lane_info in lane_infos:
                sequencing_ids_from_lanes.add(lane_info['sequencing'])

    # Double checks and get a list of libraries needing hmmcopy 
    no_hmmcopy_data = get_analyses_to_run(sequencing_ids_from_lanes, 'hmmcopy')

    return no_hmmcopy_data

def get_analyses_to_run(sequencing_ids, analysis_type):
    analyses_to_run = []
    complete_statuses = ('complete', '{}_complete'.format(analysis_type))

    for sequencing_id in sequencing_ids:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)
        dlp_library_id = sequencing['library'] 
        logging.info("Comparing sequencing dates and analysis dates for library {} for {} analysis".format(dlp_library_id, analysis_type))

        latest_sequencing_date = ""
        # Check: Sequencing dates may not have been set
        sequencing_dates = [lane['sequencing_date'] for lane in sequencing['dlplane_set']]
        sequencing_dates.sort(reverse=True)

        latest_sequencing_date = sequencing_dates[0]

        if not latest_sequencing_date:
            # Maybe: Raise exception vs logging warning
            # raise Exception("Sequencing date is not set")
            logging.warning("Lanes do not have sequencing_dates for sequencing id {}".format(sequencing_id))
            continue

        # TODO: Compare latest sequencing date to analysis date
        # Check if analysis is actually bwa-aln?
        analyses = list(colossus_api.list('analysis_information', library__pool_id=dlp_library_id))

        if len(analyses) == 0:
            analyses_to_run.append(dict(library_id=dlp_library_id))
            logging.info("No analysis information for library {}; adding to analyses to run".format(dlp_library_id))
            continue

        latest_analysis_date = ""
        analysis_dates = []
        for analysis in analyses:
            # May need to change if looking for bwa-mem aligner analyses
            # Maybe: Pass desired aligner as argument
            if analysis['aligner'] != "A":
                continue

            analysis_run = analysis['analysis_run']
            if analysis_run['run_status'] not in complete_statuses:
                continue

            analysis_dates.append(analysis_run['last_updated'])  
        
        if analysis_dates: 
            analysis_dates.sort(reverse=True)
            latest_analysis_date = analysis_dates[0]

        if not latest_analysis_date:
            logging.info("No completed analysis for library {}".format(dlp_library_id))
            analyses_to_run.append(dict(library_id=dlp_library_id))
            continue

        if latest_sequencing_date > latest_analysis_date:
            logging.info("Latest sequencing date for library {} was on {} but latest analysis was {} \n".format(
                dlp_library_id, latest_sequencing_date, latest_analysis_date))
            analyses_to_run.append(dict(library_id=dlp_library_id))

        else:
            logging.info("Library {} does not need {} analysis \n".format(dlp_library_id, analysis_type))

    return analyses_to_run               
                

if __name__ == '__main__':
    no_hmmcopy_data = search_for_no_hmmcopy_data()
    unaligned_data = search_for_unaligned_data()

    for data in no_hmmcopy_data:
        for lib in data.keys():
            logging.info("Need to create hmmcopy analysis for {}".format(data[lib]))

    for data in unaligned_data:
        for lib in data.keys():
            logging.info("Need to create align analysis for {}".format(data[lib]))

