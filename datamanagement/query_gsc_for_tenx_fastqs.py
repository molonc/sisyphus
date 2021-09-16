#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import re
import sys
import gzip
import click
import logging
import datetime
import subprocess
from collections import defaultdict

from jira import JIRA
from jira.exceptions import JIRAError

from datamanagement.templates import (TENX_FASTQ_BLOB_TEMPLATE, TENX_SCRNA_DATASET_TEMPLATE)

from datamanagement.utils.constants import LOGGING_FORMAT
from datamanagement.utils.utils import get_lanes_hash
from datamanagement.add_generic_dataset import add_generic_dataset
from datamanagement.utils.gsc import get_sequencing_instrument, GSCAPI

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import FieldMismatchError, NotFoundError

from datamanagement.utils.comment_jira import comment_jira

from workflows.utils.tantalus_utils import create_tenx_analysis_from_library

from dbclients.utils.dbclients_utils import (
    get_colossus_base_url,
)
import settings

from common_utils.utils import (
    validate_mode,
)

# make sure we are using correct mode (i.e. prod, dev, staging, etc.)
validate_mode(settings.mode)

COLOSSUS_BASE_URL = get_colossus_base_url()

gsc_api = GSCAPI()
tantalus_api = TantalusApi()
colossus_api = ColossusApi()
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

JIRA_USER = os.environ['JIRA_USERNAME']
JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

filename_pattern_map = {
    "*_1_*.raw.fastq.gz": (1, True),
    "*_2_*.raw.fastq.gz": (2, True),
    "*_1_*.fastq.gz": (1, True),
    "*_2_*.fastq.gz": (2, True),
}

sequencing_instrument_map = {
    'HiSeqX': 'HX',
    'HiSeq2500': 'H2500',
    'NextSeq550': 'NextSeq550',
}
storage_client = tantalus_api.get_storage_client("scrna_fastq")

TAXONOMY_MAP = {
    '9606': 'HG38',
    '10090': 'MM10',
}

def get_existing_fastq_data(tantalus_api, library):
    ''' Get the current set of fastq data in tantalus.

    Args:
        library (str): tenx library name

    Returns:
        existing_data: set of lanes (flowcell_id, lane_number)
    '''

    existing_flowcell_ids = []

    lanes = tantalus_api.list('sequencing_lane', dna_library__library_id=library)

    for lane in lanes:
        existing_flowcell_ids.append(f"{lane['flowcell_id']}_{lane['lane_number']}")

    return set(existing_flowcell_ids)


def create_analysis_jira_ticket(library_id, sample, library_ticket, reference_genome):
    '''
    Create analysis jira ticket as subtask of library jira ticket

    Args:
        info (dict): Keys: library_id

    Returns:
        analysis_jira_ticket: jira ticket id (ex. SC-1234)
    '''

    JIRA_USER = os.environ['JIRA_USERNAME']
    JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
    jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

    issue = jira_api.issue(library_ticket)

    # In order to search for library on Jira,
    # Jira ticket must include spaces

    # JIRA project key: use 'SC' for production, 'MIS' for all other modes
    project_key = 'SC' if (settings.mode.lower() == 'production') else 'MIS'

    sub_task = {
        'project': {
            'key': project_key
        },
        'summary': '{} - {} - {} TenX Analysis'.format(sample, library_id, reference_genome),
        'issuetype': {
            'name': 'Sub-task'
        },
        'parent': {
            'id': issue.key
        }
    }

    sub_task_issue = jira_api.create_issue(fields=sub_task)
    analysis_jira_ticket = sub_task_issue.key

    # Add watchers
    jira_api.add_watcher(analysis_jira_ticket, JIRA_USER)

    # Assign task to myself
    analysis_issue = jira_api.issue(analysis_jira_ticket)
    analysis_issue.update(assignee={'name': JIRA_USER})

    logging.info('Created analysis ticket {} for library {}'.format(analysis_jira_ticket, library_id))

    return analysis_jira_ticket


def import_tenx_fastqs(
    storage_name,
    sequencing,
    taxonomy_id=None,
    skip_upload=False,
    ignore_existing=False,
    skip_jira=False,
    no_comments=False,
    update=False,
    ):
    storage_client = tantalus_api.get_storage_client(storage_name)

    # get colossus sequencing id
    sequencing_id = sequencing["id"]
    # get pool id from sequencing
    pool_id = sequencing["tenx_pool"]
    # get colossus tenx pool object
    pool = colossus_api.get("tenxpool", id=pool_id)
    # get pool name
    pool_name = pool['pool_name']

    # get gsc id (this may not have been filled out)
    gsc_pool_id = sequencing["gsc_library_id"]
    # query gsc by gsc pool id
    gsc_pool_infos = gsc_api.query(f"library?name={gsc_pool_id}")

    # check if not results returned
    if not gsc_pool_id:
        # query gsc by our indentifier instead i.e. colossus pool name
        gsc_pool_infos = gsc_api.query(f"library?external_identifier={pool_name}")
        if gsc_pool_infos:
            # get name used internally at gsc
            gsc_pool_id = gsc_pool_infos[0]["name"]

    # try to fetch for gsc pool info again
    gsc_pool = gsc_api.query(f"library?name={gsc_pool_id}")

    # if no results found for a second time, exit
    if not gsc_pool:
        logging.info(f"cannot find data for {pool_name}, {gsc_pool_id}")
        return None

    # get id of gsc pool
    pool_id = gsc_pool[0]["id"]

    # get information about sequecing run
    run_info = gsc_api.query(f"run?library_id={pool_id}")

    logging.info(f"Importing {pool_name} ")

    # init dictionary to be used for collecting library index pairs
    index_lib = dict()

    pool_libraries = []

    # for each library in the pool, collect the sample and index of the library
    for library in pool["libraries"]:
        # get colossus tenx library
        tenxlib = colossus_api.get("tenxlibrary", id=library)
        library = tenxlib['name']

        pool_libraries.append(library)
        # get sample name
        sample = tenxlib["sample"]["sample_id"]
        # get index name
        index_used = tenxlib["tenxlibraryconstructioninformation"]["index_used"]
        # index always ends with comma, so remove comma from name
        index = index_used.split(",")[0]
        print(f"{tenxlib['name']} {tenxlib['sample']['sample_id']} {index}")
        # add info keyed by index
        index_lib[index] = dict(tenxlib=tenxlib, library=tenxlib['name'], sample=tenxlib['sample']['sample_id'])

    # iterate through all sequencing runs of this pool
    for run in run_info:
        run_id = run["id"]
        # get all libcores of run
        # in the case of tenx, a libcore represents a colossus tenxlibrary
        libcore = gsc_api.query(f"libcore?run_id={run_id}&relations=primer%2Crun%2Clibrary&primer_columns=name")

        gsc_sublibraries = []
        dataset_ids = []

        # skip run if no libcores found
        if not libcore:
            logging.info(f"no libcore")
            continue

        for lib in libcore:
            lanes = []
            lane_pks = []

            filenames = []

            index = lib["primer"]["name"]
            flowcell_id = lib["run"]["flowcell_id"]
            flowcell = gsc_api.query(f"flowcell?id={flowcell_id}")

            # check if the libcore is associated with a library in the pool
            try:
                tenxlib = index_lib[index]["tenxlib"]
                library = index_lib[index]["library"]
                sample = index_lib[index]["sample"]
            except Exception as e:
                logging.error(f"Index not found: {e}")
                raise Exception(f"Index not found: {e}")

            # collect sequencing info
            flowcell_id = str(flowcell[0]['lims_flowcell_code'])
            lane_number = str(lib['run']['lane_number'])
            sequencing_date = str(lib["run"]["run_datetime"])
            # format sequencing date to be compatible with Colossus?
            #sequencing_date = sequencing_date.split('T')[0]
            sequencing_instrument = get_sequencing_instrument(lib["run"]["machine"])
            sequencing_instrument = sequencing_instrument_map[sequencing_instrument]

            flowcell_lane = f"{flowcell_id}_{lane_number}"
            # get existing data
            if not (ignore_existing):
                existing_data = get_existing_fastq_data(tantalus_api, library)
                if flowcell_lane in existing_data:
                    logging.info(f"skipping {flowcell_lane} since already imported")
                    continue

            # get internal gsc library name
            gsc_library_id = lib["library"]["name"]
            print(f"internal GSC id is {gsc_library_id}")
            # update library's gsc name
            colossus_api.update("tenxlibrary", id=tenxlib["id"], gsc_library_id=gsc_library_id)

            gsc_sublibraries.append(gsc_library_id)

            # query for fastqs of the library
            fastqs = gsc_api.query(f"concat_fastq?libcore_id={lib['id']}")
            #print(fastqs)

            for fastq in fastqs:
                filename_pattern = fastq["file_type"]["filename_pattern"]

                read_end, passed = filename_pattern_map.get(filename_pattern, (None, None))

                if read_end is None:
                    logging.info("Unrecognized file type: {}".format(filename_pattern))
                    continue

                # construct fastq name
                new_filename = "_".join([library, sample, "S1", f"L00{lane_number}", f"R{read_end}", "001.fastq.gz"])
                fullpath = os.path.join(storage_client.prefix, library, flowcell_lane, new_filename)
                filenames.append(fullpath)

                # add fastq to cloud storage
                if not(skip_upload):
                    storage_client.create(
                        os.path.join(library, flowcell_lane, new_filename),
                        fastq["data_path"],
                        update=True,
                    )

            # if no files were found move onto next library
            if not filenames:
                print(f"no data for run_id: {run_id}; lane {flowcell_id}_{lane_number}")
                continue

            # collect and add lane info
            lane = dict(flowcell_id=flowcell_id, lane_number=str(lane_number))
            lanes.append(lane)

            # create tantalus library
            dna_library = tantalus_api.get_or_create(
                "dna_library",
                library_id=library,
                library_type="SC_RNASEQ",
                index_format="TENX",
            )

            try:
                lane_object = tantalus_api.get(
                    "sequencing_lane",
                    flowcell_id=flowcell_id,
                    lane_number=str(lane_number),
                    dna_library=dna_library["id"],
                )

                tantalus_api.update(
                    "sequencing_lane",
                    id=lane_object["id"],
                    sequencing_centre="GSC",
                    sequencing_instrument=sequencing_instrument,
                    read_type="TENX",
                )

            except:
                lane_object, _ = tantalus_api.create(
                    "sequencing_lane",
                    fields=dict(
                        flowcell_id=flowcell_id,
                        lane_number=str(lane_number),
                        sequencing_centre="GSC",
                        sequencing_instrument=sequencing_instrument,
                        read_type="TENX",
                        dna_library=dna_library["id"],
                    ),
                    keys=[
                        "flowcell_id",
                        "lane_number",
                        "sequencing_centre",
                        "dna_library",
                    ],
                    get_existing=True,
                )

            lane_pks.append(lane_object["id"])

            dataset_name = TENX_SCRNA_DATASET_TEMPLATE.format(
                dataset_type="FQ",
                sample_id=sample,
                library_type="SC_RNASEQ",
                library_id=library,
                taxonomy=TAXONOMY_MAP[taxonomy_id],
                lanes_hash=get_lanes_hash(lanes),
            )
            sequence_dataset = add_generic_dataset(
                filepaths=filenames,
                sample_id=sample,
                library_id=library,
                storage_name="scrna_fastq",
                dataset_name=dataset_name,
                dataset_type="FQ",
                sequence_lane_pks=lane_pks,
                reference_genome=TAXONOMY_MAP[taxonomy_id],
                update=True,
            )

            dataset_ids.append(sequence_dataset)

            url = f"{COLOSSUS_BASE_URL}/tenx/sequencing/{sequencing_id}"
            comment = f"Import successful:\n\nLane: {flowcell_lane}\nGSC Library ID: {gsc_library_id}\n{url}"

            comments = jira_api.comments(tenxlib["jira_ticket"])
            commented = False
            for c in comments:
                if c.body == comment:
                    commented = True
                    break

            if not commented:
                comment_jira(tenxlib["jira_ticket"], comment)

            # create jira ticket
            if not(skip_jira):
                jira_ticket = create_analysis_jira_ticket(
                    library_id=library,
                    sample=sample,
                    library_ticket=tenxlib['jira_ticket'],
                    reference_genome=TAXONOMY_MAP[taxonomy_id],
                )
                # create colossus analysis
                analysis, _ = colossus_api.create(
                    "tenxanalysis",
                    fields={
                        "version": "vm",
                        "jira_ticket": jira_ticket,
                        "run_status": "idle",
                        "tenx_library": tenxlib["id"],
                        "submission_date": str(datetime.date.today()),
                        "tenxsequencing_set": [],
                    },
                    keys=["jira_ticket"],
                )
                # create tantalus analysis
                create_tenx_analysis_from_library(jira_ticket, library, taxonomy_id=taxonomy_id)

        # check if data has been imported
        if filenames:
            # add lanes to colossus
            colossus_lane = colossus_api.get_or_create(
                "tenxlane",
                flow_cell_id=flowcell_lane,
                sequencing=sequencing_id,
            )
            # update lane with gsc id and date
            colossus_api.update(
                "tenxlane",
                id=colossus_lane["id"],
                tantalus_datasets=list(set(dataset_ids)),
                gsc_sublibrary_names=gsc_sublibraries,
                sequencing_date=sequencing_date,
            )

    # check if gsc id hasn't been added correctly
    if sequencing["gsc_library_id"] != gsc_pool_id:
        logging.info("Updating gsc library id of sequencing {} from {} to {}".format(
            sequencing["id"], sequencing["gsc_library_id"], gsc_pool_id))
        colossus_api.update("tenxsequencing", sequencing["id"], gsc_library_id=gsc_pool_id)

    logging.info("Succesfully imported {} {}".format(pool_name, gsc_pool_id))

    import_info = dict(
        pool_name=pool_name,
        libraries=pool_libraries,
        gsc_library_id=gsc_pool_id,
    )

    return import_info


def write_import_log(successful_pools, failed_pools):
    import_status_path = os.path.join(os.environ['DATAMANAGEMENT_DIR'], 'tenx_import_statuses.txt')

    logging.info("Writing import statuses to {}".format(import_status_path))

    if os.path.exists(import_status_path):
        os.remove(import_status_path)

    file = open(import_status_path, 'w+')
    file.write("Date: {}\n\n".format(str(datetime.date.today())))

    file.write("Successful imports: \n")

    for pool in successful_pools:
        file.write('\n{}, {}\n'.format(pool['pool_name'], pool['gsc_library_id']))

        file.write("Libraries:\n")
        file.write("{}\n".format("\n".join(pool["libraries"])))

        if pool['lane_requested_date'] is None:
            file.write("INFO: Latest lane requested date set to None\n\n")
            continue

        file.write('Latest lane requested on {}\n\n'.format(pool['lane_requested_date']))

    file.write("\n\nFailed imports: \n\n")
    for pool in failed_pools:
        file.write("{}: {}\n".format(pool["pool_name"], pool["error"]))

        if pool['lane_requested_date'] is None:
            file.write("INFO: Latest lane requested date set to None\n\n")
            continue

        file.write('Latest lane requested on {}\n\n'.format(pool['lane_requested_date']))


@click.command()
@click.argument('storage_name', nargs=1)
@click.option('--pool_id', type=int)
@click.option('--taxonomy_id', type=click.Choice(['9606', '10090']))
@click.option('--skip_upload', is_flag=True)
@click.option('--ignore_existing', is_flag=True)
@click.option('--skip_jira', is_flag=True)
@click.option('--all', is_flag=True)
@click.option('--no_comments', is_flag=True)
@click.option('--update', is_flag=True)
def main(
    storage_name,
    pool_id=None,
    taxonomy_id='9606',
    skip_upload=False,
    ignore_existing=False,
    skip_jira=False,
    all=False,
    no_comments=False,
    update=False,
    ):
    successful_pools = []
    failed_pools = []

    # make sure it is a valid taxonomy ID
    if taxonomy_id is not None:
        # As of 2021, only two reference genomes for TenX libraries
        # 9606: HG38, 10090: MM10
        if (taxonomy_id not in TAXONOMY_MAP):
            raise ValueError('Taxonomy ID must be one of 9606 or 10090 for TenX libraries!')

    # check if specific pool passed
    if pool_id is not None:
        # get sequencings associated to pool
        sequencing_list = list(colossus_api.list('tenxsequencing', tenx_pool=pool_id, sequencing_center='BCCAGSC'))

    # check if all flagged passed
    elif all:
        # get all tenx sequencings
        sequencing_list = list(colossus_api.list('tenxsequencing', sequencing_center='BCCAGSC'))

    else:
        # get sequencings that need lanes imported
        logging.info("Searching for pools with lanes not yet imported.")
        sequencing_list = list(colossus_api.list('tenxsequencing', sequencing_center='BCCAGSC'))
        sequencing_list = list(
            filter(lambda s: s['number_of_lanes_requested'] > len(s['tenxlane_set']), sequencing_list))

    for sequencing in sequencing_list:
        # skip sequencing is no pool attached
        if sequencing["tenx_pool"] is None:
            continue

        try:
            import_info = import_tenx_fastqs(
                storage_name,
                sequencing,
                taxonomy_id=taxonomy_id,
                skip_upload=skip_upload,
                ignore_existing=ignore_existing,
                skip_jira=skip_jira,
                no_comments=no_comments,
                update=update,
            )

            # check if information does not exists on gsc
            if import_info is None:
                lane_requested_date = sequencing["lane_requested_date"]
                failed_pools.append(
                    dict(
                        pool_name="TENXPOOL{}".format(str(sequencing["tenx_pool"]).zfill(4)),
                        lane_requested_date=sequencing["lane_requested_date"],
                        error="Doesn't exist on GSC",
                    ))
                continue

            # add pool to successful import
            import_info["lane_requested_date"] = sequencing["lane_requested_date"]
            successful_pools.append(import_info)

        except Exception as e:
            raise Exception(e)
            logging.error("Failed to import {}: {}".format(sequencing["tenx_pool"], str(e)))
            failed_pools.append(
                dict(
                    pool_name="TENXPOOL{}".format(str(sequencing["tenx_pool"]).zfill(4)),
                    lane_requested_date=sequencing["lane_requested_date"],
                    error=str(e),
                ))
            continue

    write_import_log(successful_pools, failed_pools)


if __name__ == "__main__":
    main()
