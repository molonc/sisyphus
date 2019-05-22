#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import string
import sys
import time
import collections
import click
import pandas as pd
import datetime
from collections import defaultdict
from datamanagement.utils.constants import LOGGING_FORMAT
from datamanagement.utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
from datamanagement.utils.comment_jira import comment_jira
import datamanagement.templates as templates
from utils.filecopy import rsync_file, try_gzip
from utils.gsc import get_sequencing_instrument, GSCAPI
from utils.runtime_args import parse_runtime_args
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi


solexa_run_type_map = {"Paired": "P"}

def reverse_complement(sequence):
    return str(sequence[::-1]).translate(str.maketrans("ACTGactg", "TGACtgac"))

def decode_raw_index_sequence(raw_index_sequence, instrument, rev_comp_override):
    i7 = raw_index_sequence.split("-")[0]
    i5 = raw_index_sequence.split("-")[1]

    if rev_comp_override is not None:
        if rev_comp_override == "i7,i5":
            pass
        elif rev_comp_override == "i7,rev(i5)":
            i5 = reverse_complement(i5)
        elif rev_comp_override == "rev(i7),i5":
            i7 = reverse_complement(i7)
        elif rev_comp_override == "rev(i7),rev(i5)":
            i7 = reverse_complement(i7)
            i5 = reverse_complement(i5)
        else:
            raise Exception("unknown override {}".format(rev_comp_override))

        return i7 + "-" + i5

    if instrument == "HiSeqX":
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)
    elif instrument == "HiSeq2500":
        i7 = reverse_complement(i7)
    elif instrument == "NextSeq550":
        i7 = reverse_complement(i7)
        i5 = reverse_complement(i5)
    else:
        raise Exception("unsupported sequencing instrument {}".format(instrument))

    return i7 + "-" + i5


def query_colossus_dlp_cell_info(colossus_api, library_id):

    sublibraries = colossus_api.get_colossus_sublibraries_from_library_id(library_id)

    cell_samples = {}
    for sublib in sublibraries:
        index_sequence = sublib["primer_i7"] + "-" + sublib["primer_i5"]
        cell_samples[index_sequence] = sublib["sample_id"]["sample_id"]

    return cell_samples


def query_colossus_dlp_rev_comp_override(colossus_api, library_id):
    library_info = colossus_api.query_libraries_by_library_id(library_id)

    rev_comp_override = {}
    for sequencing in library_info["dlpsequencing_set"]:
        for lane in sequencing["dlplane_set"]:
            rev_comp_override[lane["flow_cell_id"]] = sequencing["rev_comp_override"]

    return rev_comp_override


# Mapping from filename pattern to read end, pass/fail
filename_pattern_map = {
    "_1.fastq.gz": (1, True),
    "_1_*.concat_chastity_passed.fastq.gz": (1, True),
    "_1_chastity_passed.fastq.gz": (1, True),
    "_1_chastity_failed.fastq.gz": (1, False),
    "_1_*bp.concat.fastq.gz": (1, True),
    "_1_*bp_*.concat_chastity_passed.fastq.gz":(1, True), 
    "_1_*bp.concat_chastity_passed.fastq.gz": (1, True),
    "_2.fastq.gz": (2, True),
    "_2_*.concat_chastity_passed.fastq.gz": (2, True),
    "_2_chastity_passed.fastq.gz": (2, True),
    "_2_chastity_failed.fastq.gz": (2, False),
    "_2_*bp.concat.fastq.gz": (2, True),
    "_2_*bp_*.concat_chastity_passed.fastq.gz":(2, True),
    "_2_*bp.concat_chastity_passed.fastq.gz": (2, True),

}


def get_existing_fastq_data(tantalus_api, dlp_library_id):
    ''' Get the current set of fastq data in tantalus.

    Args:
        dlp_library_id: library id for the dlp run

    Returns:
        existing_data: set of tuples of the form (flowcell_id, lane_number, index_sequence, read_end)
    '''

    existing_flowcell_ids = []

    lanes = tantalus_api.list('sequencing_lane', dna_library__library_id=dlp_library_id)

    for lane in lanes:
        existing_flowcell_ids.append((lane['flowcell_id'], lane['lane_number']))

    return set(existing_flowcell_ids)


def get_gsc_library_info(library_infos, external_identifier):

    if len(library_infos) == 0:
        logging.error('no libraries with external_identifier {} in gsc api'.format(external_identifier))
        return None
    elif len(library_infos) > 1:
        raise Exception(
            "multiple libraries with external_identifier {} in gsc api".format(
                external_identifier
            )
        )

    # Querying by external_identifier had exactly one result.
    library_info = library_infos[0]

    return library_info


def check_colossus_gsc_library_id(sequencing, colossus_gsc_id, gsc_id, update=False):
    """ Update colossus lane and GSC library information.
    """

    if colossus_gsc_id != gsc_id:
        if not update:
            logging.warning("""
                GSC library id on Colossus does not match GSC library id at the GSC.\n
                Colossus GSC ID: {}\n
                Actual GSC ID: {}\n
                Tip: Run again with --update flag""".format(sequencing['id'], colossus_gsc_id, gsc_id)
            )
            raise

        logging.info("Updating GSC ID on sequencing {} from {} to {}".format(
            sequencing['id'],
            colossus_gsc_id, 
            gsc_id)
        )

        colossus_api.update(
            'sequencing',
            sequencing['id'],
            gsc_library_id=gsc_id
        )

    return gsc_id
    

def import_gsc_dlp_paired_fastqs(colossus_api, tantalus_api, sequencing, storage, internal_id=None, tag_name=None, update=False, check_library=False, dry_run=False):
    ''' Import dlp fastq data from the GSC.
    
    Args:
        colossus_api:   (object) Basic client for colossus
        tantalus_api:   (object) Basic client for tantalus
        sequencing:     (dict) sequencing on colossus 
        storage:        (str) to storage details for transfer

    Kwargs:
        tag_name: a tag to add to imported data
        update: update an existing dataset
        check_library: only check the library, dont load
        dry_run: check for new lanes, dont import

    '''

    # Get Jira ticket and GSC sequencing id associated with the library in order to comment about import status
    dlp_library_id = sequencing["library"]
    gsc_library_id = sequencing["gsc_library_id"]
    library_info = colossus_api.query_libraries_by_library_id(dlp_library_id)
    jira_ticket = library_info["jira_ticket"]

    sequencing_colossus_path = "https://colossus.canadacentral.cloudapp.azure.com/dlp/sequencing/{}".format(
        sequencing['id']
    )

    if library_info["exclude_from_analysis"]:
        logging.info("{} is excluded from analysis; skipping check for lanes.".format(dlp_library_id))
        return False

    if internal_id is not None:
        # Check if the internal ID passed in matches the GSC ID entered in Colossus
        if gsc_library_id != internal_id:
            raise Exception("Internal library id {} given does not match Colossus gsc library id for {}: {}".format(
                internal_id, dlp_library_id, gsc_library_id)
            )

        gsc_library_id = internal_id

    logging.info("Checking library {} and sequencing {} for additional lanes".format(
        dlp_library_id, 
        sequencing["id"])
    )

    # Existing fastqs in tantalus as a set of tuples of
    # the form (flowcell_id, lane_number, index_sequence, read_end)
    # skip if we are checking an existing library
    if not check_library:
        existing_data = get_existing_fastq_data(tantalus_api, dlp_library_id)

    else:
        existing_data = []

    primary_sample_id = library_info['sample']['sample_id']
    cell_samples = query_colossus_dlp_cell_info(colossus_api, dlp_library_id)
    rev_comp_overrides = query_colossus_dlp_rev_comp_override(
        colossus_api, dlp_library_id
    )

    external_identifier = "{}_{}".format(primary_sample_id, dlp_library_id)

    gsc_api = GSCAPI()
    
    # Query GSC for fastqs and library information. Possibilities:
    # (1) fastqs by parent library (gsc id) has results 
    # (2) library by external identifier (<primary_sample_id>_<dlp_library_id>) has results 

    # The following scenarios are can happen:

    # (1) and (2)
    # Check if sequencing on colossus has gsc id that matches the gsc id on gsc database
    # If above is true, import. Else update colossus and import.


    # (2) and not (1)
    # Colossus GSC id likely entered incorrectly. 
    # Get correct id off GSC database, update colossus, and import.

    # (1) and not (2)
    # GSC ID is likely an internal id (begins with 'IX'). Import.

    # Not (1) and not (2)
    # No data is available on the GSC. Either database error or sequencing has not finished.

    gsc_fastq_infos = gsc_api.query("fastq?parent_library={}".format(gsc_library_id))
    gsc_library_infos = gsc_api.query(
        "library?external_identifier={}".format(external_identifier)
    )

    gsc_library_info = get_gsc_library_info(gsc_library_infos, external_identifier)

    if len(gsc_fastq_infos) == 0:
        logging.warning("no fastqs available in gsc database under parent library {}".format(
            gsc_library_id)
        )

        if gsc_library_info is None:
            return None

    if gsc_library_info is not None:
        gsc_library_id = check_colossus_gsc_library_id(
            sequencing, gsc_library_id, gsc_library_info["name"], update=update
        )

    if check_library:
        logging.info('Checking data for {}, {}'.format(dlp_library_id, gsc_library_id))

    else:
        logging.info('Importing data for {}, {}'.format(dlp_library_id, gsc_library_id))

    fastq_file_info = []

    lanes = []

    gsc_lane_fastq_file_infos = defaultdict(list)

    sequencing_instrument_map = {
        'HiSeqX'    : 'HX',
        'HiSeq2500' : 'H2500'
    }

    for fastq_info in gsc_fastq_infos:
        flowcell_id = str(fastq_info['libcore']['run']['flowcell']['lims_flowcell_code'])
        lane_number = str(fastq_info['libcore']['run']['lane_number'])
        sequencing_date = str(fastq_info["libcore"]["run"]["run_datetime"])
        sequencing_instrument = get_sequencing_instrument(
                fastq_info["libcore"]["run"]["machine"]
            )
        sequencing_instrument = sequencing_instrument_map[sequencing_instrument]
        gsc_lane_fastq_file_infos[(flowcell_id, lane_number, sequencing_date, sequencing_instrument)].append(fastq_info)

    for (flowcell_id, lane_number, sequencing_date, sequencing_instrument) in gsc_lane_fastq_file_infos.keys():

        # Check if lanes are in Tantalus
        if (flowcell_id, lane_number) in existing_data:
            logging.info('Skipping fastqs with flowcell id {}, lane number {}'.format(
                flowcell_id, lane_number))
            new = False

        else:
            logging.info("Importing lane {}_{}.".format(flowcell_id, lane_number))
            new = True

        lanes.append(
            {
                "flowcell_id" :             flowcell_id,
                "lane_number" :             lane_number,
                "sequencing_date" :         sequencing_date,
                "sequencing_instrument":    sequencing_instrument,
                "new":                      new,
            }
        )

        if not new:
            continue

        if dry_run:
            continue

        for fastq_info in gsc_lane_fastq_file_infos[(flowcell_id, lane_number, sequencing_date, sequencing_instrument)]:
            fastq_path = fastq_info["data_path"]

            if fastq_info["status"] != "production":
                logging.info(
                    "skipping file {} marked as {}".format(
                        fastq_info["data_path"], fastq_info["status"]
                    )
                )
                continue

            if fastq_info['removed_datetime'] is not None:
                logging.info('skipping file {} marked as removed {}'.format(
                    fastq_info['data_path'], fastq_info['removed_datetime']))
                continue

            try:
                try_gzip(fastq_path)
            except Exception as e:
                if check_library:
                    logging.warning('failed to gunzip')
                    continue
                else:
                    raise

            sequencing_instrument = get_sequencing_instrument(
                fastq_info["libcore"]["run"]["machine"]
            )
            solexa_run_type = fastq_info["libcore"]["run"]["solexarun_type"]
            read_type = solexa_run_type_map[solexa_run_type]

            primer_id = fastq_info["libcore"]["primer_id"]
            primer_info = gsc_api.query("primer/{}".format(primer_id))
            raw_index_sequence = primer_info["adapter_index_sequence"]

            flowcell_lane = flowcell_id
            if lane_number is not None:
                flowcell_lane = flowcell_lane + "_" + str(lane_number)

            rev_comp_override = rev_comp_overrides.get(flowcell_lane)

            index_sequence = decode_raw_index_sequence(
                raw_index_sequence, sequencing_instrument, rev_comp_override
            )

            filename_pattern = fastq_info["file_type"]["filename_pattern"]
            read_end, passed = filename_pattern_map.get(filename_pattern, (None, None))

            logging.info(
                "loading fastq %s, raw index %s, index %s, %s",
                fastq_info["id"],
                raw_index_sequence,
                index_sequence,
                fastq_path,
            )

            if read_end is None:
                raise Exception("Unrecognized file type: {}".format(filename_pattern))

            if not passed:
                continue

            try:
                cell_sample_id = cell_samples[index_sequence]
            except KeyError:
                raise Exception('unable to find index {} for flowcell lane {} for library {}'.format(
                    index_sequence, flowcell_lane, dlp_library_id))

            extension = ''
            if fastq_path.endswith('.gz'):
                extension = '.gz'
            elif not fastq_path.endswith('.fastq'):
                raise ValueError('unknown extension for filename {}'.format(fastq_path))

            tantalus_filename = templates.SC_WGS_FQ_TEMPLATE.format(
                primary_sample_id=primary_sample_id,
                dlp_library_id=dlp_library_id,
                flowcell_id=flowcell_id,
                lane_number=lane_number,
                cell_sample_id=cell_sample_id,
                index_sequence=index_sequence,
                read_end=read_end,
                extension=extension,
            )

            tantalus_path = os.path.join(storage["prefix"], tantalus_filename)

            fastq_file_info.append(
                dict(
                    dataset_type="FQ",
                    sample_id=cell_sample_id,
                    library_id=dlp_library_id,
                    library_type="SC_WGS",
                    index_format="D",
                    sequence_lanes=[
                        dict(
                            flowcell_id=flowcell_id,
                            lane_number=lane_number,
                            sequencing_centre="GSC",
                            sequencing_instrument=sequencing_instrument,
                            sequencing_library_id=gsc_library_id,
                            read_type=read_type,
                        )
                    ],
                    read_end=read_end,
                    index_sequence=index_sequence,
                    filepath=tantalus_path,
                )
            )

            if not check_library:
                if storage['storage_type'] == 'server': 
                    rsync_file(fastq_path, tantalus_path)

                elif storage['storage_type'] == 'blob':
                    storage_client = tantalus_api.get_storage_client(storage['name'])
                    storage_client.create(tantalus_filename, fastq_path)

    import_info = dict(
        dlp_library_id=dlp_library_id,
        gsc_library_id=gsc_library_id,
        lanes=lanes,
    )
    if len(fastq_file_info) == 0:
        logging.info("Available data for library {} already imported".format(dlp_library_id))
        return import_info

    fastq_paired_end_check(fastq_file_info)

    cell_index_sequences = set(cell_samples.keys())

    fastq_lane_index_sequences = collections.defaultdict(set)

    # Check that all fastq files refer to indices known in colossus
    for info in fastq_file_info:
        if info['index_sequence'] not in cell_index_sequences:
            raise Exception('fastq {} with index {}, flowcell {}, lane {} with index not in colossus'.format(
                info['filepath'], info['index_sequence'], info['sequence_lanes'][0]['flowcell_id'],
                info['sequence_lanes'][0]['lane_number']))

        flowcell_lane = (
            info['sequence_lanes'][0]['flowcell_id'],
            info['sequence_lanes'][0]['lane_number'])
        fastq_lane_index_sequences[flowcell_lane].add(info['index_sequence'])
    logging.info('all fastq files refer to indices known in colossus')

    # Check that all index sequences in colossus have fastq files
    for flowcell_lane in fastq_lane_index_sequences:
        for index_sequence in cell_index_sequences:
            if index_sequence not in fastq_lane_index_sequences[flowcell_lane]:
                raise Exception('no fastq found for index sequence {}, flowcell {}, lane {}'.format(
                    index_sequence, flowcell_lane[0], flowcell_lane[1]))
    logging.info('all indices in colossus have fastq files')

    if not check_library:
        create_sequence_dataset_models(
            fastq_file_info, storage["name"], tag_name, tantalus_api, update=update
        )

        comment_status(jira_ticket, lanes, gsc_library_id, sequencing_colossus_path)

    logging.info("Library {} imported successfully".format(dlp_library_id))

    return import_info


def comment_status(jira_ticket, lanes, gsc_library_id, sequencing_colossus_path):
    """
    Comment on library JIRA ticket about successfully imported lanes

    Args:
        jira_ticket:                (str) jira ticket id ex. SC-1234
        lanes:                      (dict) lane information
        gsc_library_id:             (str) gsc library id (PX id or IX id)
        sequencing_colossus_path:   (str) url to colossus sequencing       
    """
    comment = "Import successful: \n"
    for lane in lanes:
        if lane['new'] == True:
            comment += """\nLane: {}_{}
                Sequencing Date: {} """.format(
                lane["flowcell_id"],
                lane["lane_number"],
                sequencing_date,
            )

    comment += """\nGSC library ID: {}
        {}""".format(
        gsc_library_id,
        sequencing_colossus_path
    )
    comment_jira(jira_ticket, comment)


def update_colossus_lane(colossus_api, sequencing, lanes):
    """ Update the colossus lanes for a sequencing

    Raises an exception if fewer lanes imported than were expected.
    """
    for lane_to_update in lanes:
        if lane_to_update['new']:
            flowcell_id = "{}_{}".format(lane_to_update['flowcell_id'], lane_to_update['lane_number'])
            logging.info("Adding lane {} to Colossus.".format(flowcell_id))
            lane = colossus_api.get_or_create(
                "lane", sequencing=sequencing['id'], 
                flow_cell_id=flowcell_id,
            )

            if lane['sequencing_date'] != lane_to_update['sequencing_date']:
                colossus_api.update(
                    'lane',
                    lane['id'],
                    sequencing_date=lane_to_update['sequencing_date']
                )


def check_lanes(colossus_api, sequencing, num_lanes):
    """ Check if number_of_lanes_requested is equal to number of lanes.

    Updates number_of_lanes_requested if necessary
    """

    if sequencing['number_of_lanes_requested'] < num_lanes:
        logging.info('Sequencing goal is less than total number of lanes. Updating.')
        colossus_api.update(
            'sequencing',
            sequencing['id'],
            number_of_lanes_requested=num_lanes
        )

    elif sequencing['number_of_lanes_requested'] > num_lanes:
        raise Exception("Expected number of lanes is {} but total lanes imported is {}".format(
            sequencing['number_of_lanes_requested'], num_lanes))


def write_import_statuses(successful_libs, failed_libs):
    """
    Writes text file regarding successful and failed imports

    Args:
        successful_libs:    (dict)
        failed_libs:        (dict)  
    """
    import_status_path = os.path.join(os.environ['DATAMANAGEMENT_DIR'], 'import_statuses.txt')
    
    if os.path.exists(import_status_path):
        os.remove(import_status_path)

    file = open(import_status_path, 'w+')
    file.write("Date: {}\n\n".format(str(datetime.date.today())))

    file.write("Successful imports: \n")

    for successful_lib in successful_libs:
        file.write('\n{}, {} \n'.format(successful_lib['dlp_library_id'], successful_lib['gsc_library_id']))
        for lane in successful_lib['lanes']:
            flowcell = "{}_{}".format(lane['flowcell_id'], lane['lane_number'])  
            lane_message = "Flowcell: {}, Sequencing Date: {} \n".format(flowcell, lane['sequencing_date'])    
            file.write(lane_message)   
        file.write('Sequencing submitted on {}'.format(successful_lib['submission_date']))   
    
    failed_libs.sort(key=lambda x: x['error'], reverse=True)
    file.write("\n\nFailed imports: \n")
    for failed_lib in failed_libs:
        file.write("{}: {}; sequencing submitted on {}\n".format(
            failed_lib['dlp_library_id'], 
            failed_lib['error'], 
            failed_lib['submission_date']))
    file.close()


@click.command()
@click.argument('storage_name', nargs=1)
@click.option('--dlp_library_id', nargs=1)
@click.option('--internal_id')
@click.option('--tag_name')
@click.option('--all', is_flag=True)
@click.option('--update', is_flag=True)
@click.option('--check_library', is_flag=True)
@click.option('--dry_run', is_flag=True)
def main(storage_name, dlp_library_id=None, internal_id=None, tag_name=None, all=False, update=False, check_library=False, dry_run=False):

    # Set up the root logger
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    # Connect to the Tantalus API (this requires appropriate environment
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    successful_libs = []
    failed_libs = []

    storage = tantalus_api.get("storage", name=storage_name)
    sequencing_list = list()

    if dry_run:
        logging.info("This is a dry run. No lanes will be imported.")

    # Importing a single library
    if dlp_library_id is not None:
        sequencing_list = list(colossus_api.list('sequencing',  sequencing_center='BCCAGSC', library__pool_id=dlp_library_id))

    elif all:
        sequencing_list = list(colossus_api.list('sequencing',  sequencing_center='BCCAGSC'))

    else:
        sequencing_list = list(colossus_api.list('sequencing', sequencing_center='BCCAGSC'))
        sequencing_list = list(filter(lambda s: s['number_of_lanes_requested'] != len(s['dlplane_set']), sequencing_list))


    for sequencing in sequencing_list:
        try:
            import_info = import_gsc_dlp_paired_fastqs(
                colossus_api,
                tantalus_api,
                sequencing, 
                storage,
                internal_id,
                tag_name,
                update=update,
                check_library=check_library,
                dry_run=dry_run
            )

            if import_info is None:
                submission_date = sequencing["submission_date"]
                failed_libs.append(
                    dict(
                        dlp_library_id=sequencing["library"],
                        submission_date=submission_date, 
                        error="Doesn't exist on GSC",
                    )
                )
                continue

            elif import_info is False:
                continue

            update_colossus_lane(colossus_api, sequencing, import_info['lanes'])
            check_lanes(colossus_api, sequencing, len(sequencing["dlplane_set"]))

            import_info['submission_date'] = sequencing['submission_date']
            successful_libs.append(import_info)

        except Exception as e:
            submission_date = sequencing["submission_date"]
            failed_libs.append(dict(
                dlp_library_id=sequencing["library"],
                submission_date=submission_date,
                error=str(e),
                )
            )

            logging.exception(("Library {} failed to import: {}".format(sequencing["library"], e)))
            continue

    # Only write import statuses for bulk imports
    if all or dlp_library_id is None:
        # Sort lists by date in descending order
        successful_libs.sort(key=lambda x: datetime.datetime.strptime(x['submission_date'], '%Y-%m-%d'), reverse=True)
        failed_libs.sort(key=lambda x: datetime.datetime.strptime(x['submission_date'], '%Y-%m-%d'), reverse=True)
        write_import_statuses(successful_libs, failed_libs)


if __name__ == "__main__":
    main()
