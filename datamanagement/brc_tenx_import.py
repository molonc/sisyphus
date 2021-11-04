import os
import re
import sys
import gzip
import click
import logging
import pprint
import datetime
from collections import defaultdict
from jira import JIRA
from jira.exceptions import JIRAError

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import NotFoundError

from datamanagement.add_generic_dataset import add_generic_dataset
from datamanagement.utils.utils import get_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT

from workflows.utils.tantalus_utils import create_tenx_analysis_from_library


from datamanagement.templates import (
    TENX_SCRNA_DATASET_TEMPLATE
)
import settings

logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

TAXONOMY_MAP = {
    '9606': 'HG38',
    '10090': 'MM10',
}

# Move this to update_jira.py
JIRA_USERNAME = os.environ['JIRA_USERNAME']
JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USERNAME, JIRA_PASSWORD))

JIRA_MESSAGE = """Successfully imported FASTQs to Azure storage, Tantalus and Colossus. 
Files can be found at:
{{noformat}}
{filepaths}
{{noformat}}
"""

def upload_to_azure(storage_client, blobname, filepath, update=False):
    if(storage_client.exists(blobname)):
        if(storage_client.get_size(blobname) == os.path.getsize(filepath)):
            logging.info(f"{blobname} already exists and is the same size. Skipping...")

            return
        else:
            if not(update):
                message = f"{blobname} has different size from {filepath}. Please specify --update option to overwrite."
                logging.error(message)
                raise ValueError(message)

    storage_client.create(
        blobname,
        filepath,
        update=update,
    )

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

def update_jira(blob_paths, jira_ticket):
    """
    Checks if the jira ticket exists on the gsc. If it does, add a comment 
    regarding the completion of the data import

    Args:
        blob_paths:     (list) blob paths that were imported
        jira_ticket:    (str) the ticket ID on GSC jira
    """
    out_files = ""
    for blob_path in blob_paths:
        out_files += "\n{}".format(blob_path)
    
    msg = JIRA_MESSAGE.format(
        filepaths=out_files
    )
    try:
        issue = jira_api.issue(jira_ticket)
    except JIRAError:
        logging.error("The jira ticket {} does not exist. Skipping ticket update".format(jira_ticket))
        return

    comment = jira_api.add_comment(jira_ticket, msg)


@click.command()
@click.argument("pool_name")
@click.argument("flowcell")
@click.argument("bcl_directory")
@click.option('--taxonomy_id', type=click.Choice(['9606', '10090']), default='9606')
@click.option("--update", is_flag=True)
@click.option('--skip_jira', is_flag=True)
@click.option("--no_comments", is_flag=True)
def main(
    pool_name,
    flowcell,
    bcl_directory,
    taxonomy_id='9606',
    skip_jira=False,
    update=False,
    no_comments=False
    ):
    # make sure it is a valid taxonomy ID
    if taxonomy_id is not None:
        # As of 2021, only two reference genomes for TenX libraries
        # 9606: HG38, 10090: MM10
        if (taxonomy_id not in TAXONOMY_MAP):
            raise ValueError('Taxonomy ID must be one of 9606 or 10090 for TenX libraries!')

    pool_id = int(pool_name.strip("TENXPOOL"))
    pool = colossus_api.get(
        "tenxpool",
        id=pool_id
    )

    sequencing_id = pool["tenxsequencing_set"][0]

    storage_account = "scrna_fastq"

    storage_client = tantalus_api.get_storage_client(storage_account)
    library_dir_names = os.listdir(bcl_directory)

    logging.info("Importing {}".format(pool_name))
    for library_name in library_dir_names:

        logging.info("Importing fastqs for {}".format(library_name))
        filenames = dict()

        try:
            library = colossus_api.get("tenxlibrary", name=library_name)
        except NotFoundError:
            logging.error(
                "Cannot find library {} in Colossus".format(library_name))
            continue

        if(library['id'] not in pool['libraries']):
            logging.error(
                f"Library {library_name} is not part of {pool_name}")
            continue

        jira_ticket = library["jira_ticket"]
        sample = library["sample"]["sample_id"]

        fastqs = os.listdir(os.path.join(bcl_directory, library_name))

        dna_library = tantalus_api.get_or_create(
            "dna_library",
            library_id=library_name,
            library_type="SC_RNASEQ",
            index_format="TENX"
        )

        for fastq in fastqs:
            fastq_parsed = fastq.split("_")
            new_filename = "_".join(
                [library_name, sample, "_".join(fastq_parsed[-4:])])
            lane_match = re.match(
                r".+_L00(\d+)",
                fastq,
            )
            lane_number = lane_match.groups()[0]
            flowcell_lane = "{}_{}".format(flowcell, lane_number)
            blobname = os.path.join(library_name, flowcell_lane, new_filename)
            local_fastq_path = os.path.join(bcl_directory, library_name, fastq)

            upload_to_azure(
                storage_client=storage_client,
                blobname=blobname,
                filepath=local_fastq_path,
                update=update,
            )

            if lane_number not in filenames:
                filenames[lane_number] = []

            filenames[lane_number].append(
                os.path.join(storage_client.prefix, blobname)
            )

        for lane_number in filenames:
            dataset_ids = []

            lanes = list()
            lane_pks = list()
            lane = dict()
            lane["flowcell_id"] = flowcell
            lane["lane_number"] = lane_number
            lanes.append(lane)

            lane_object = tantalus_api.get_or_create(
                "sequencing_lane",
                flowcell_id=flowcell,
                lane_number=lane_number,
                sequencing_centre="BRC",
                sequencing_instrument="NextSeq500",
                read_type="TENX",
                dna_library=dna_library["id"]
            )

            lane_pks.append(lane_object["id"])

            dataset_name = TENX_SCRNA_DATASET_TEMPLATE.format(
                dataset_type="FQ",
                sample_id=sample,
                library_type="SC_RNASEQ",
                library_id=library_name,
                lanes_hash=get_lanes_hash(list(lanes)),
            )

            dataset_id = add_generic_dataset(
                filepaths=filenames[lane_number],
                sample_id=sample,
                library_id=library_name,
                storage_name="scrna_fastq",
                dataset_name=dataset_name,
                dataset_type="FQ",
                sequence_lane_pks=lane_pks,
                reference_genome="HG38",
                update=True
            )

            dataset_ids.append(dataset_id)

            colossus_lane = colossus_api.get_or_create(
                "tenxlane", 
                flow_cell_id="{}_{}".format(flowcell, lane_number),
                sequencing=sequencing_id,
            )
            
            logging.info("Adding datasets {} to colossus lane {}".format(dataset_ids, flowcell))
            if colossus_lane["tantalus_datasets"] is not None:
                dataset_ids = list(set(dataset_ids))
                
            colossus_api.update(
                "tenxlane", 
                id=colossus_lane["id"], 
                tantalus_datasets=dataset_ids
            )

        # create jira ticket
        if not(skip_jira):
            analysis_ticket = create_analysis_jira_ticket(
                library_id=library_name,
                sample=sample,
                library_ticket=jira_ticket,
                reference_genome=TAXONOMY_MAP[taxonomy_id],
            )
            # create colossus analysis
            analysis, _ = colossus_api.create(
                "tenxanalysis",
                fields={
                    "version": "vm",
                    "jira_ticket": analysis_ticket,
                    "run_status": "idle",
                    "tenx_library": library["id"],
                    "submission_date": str(datetime.date.today()),
                    "tenxsequencing_set": [],
                },
                keys=["jira_ticket"],
            )
            # create tantalus analysis
            create_tenx_analysis_from_library(
                jira=analysis_ticket,
                library=library_name,
                taxonomy_id=taxonomy_id)

            logging.info("Succesfully imported {}".format(pool_name))

        if not no_comments:
            update_jira(filenames[lane_number], jira_ticket)


if __name__ == "__main__":
    main()


