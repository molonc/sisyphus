#!/usr/bin/env python
import os
import click
import logging
import subprocess
from datetime import datetime, timedelta
from dateutil import parser

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from workflows.analysis.dlp import (
    alignment,
    hmmcopy,
    annotation,
)

from workflows import run_pseudobulk

from workflows.utils import saltant_utils, file_utils, tantalus_utils, colossus_utils
from workflows.utils.jira_utils import update_jira_dlp, add_attachment, comment_jira

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

# load config file
config = file_utils.load_json(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'config',
        'normal_config.json',
    ))


def attach_qc_report(jira, library_id, storages):
    """ 
    Adds qc report to library jira ticket

    Arguments:
        jira {str} -- id of jira ticket e.g SC-1234
        library_id {str} -- library name
        storages {dict} -- dictionary of storages names for results and inputs
    """

    storage_client = tantalus_api.get_storage_client(storages["remote_results"])
    results_dataset = tantalus_api.get(
        "resultsdataset",
        name="{}_annotation_{}".format(
            jira,
            library_id,
        ),
    )

    qc_filename = "{}_QC_report.html".format(library_id)
    jira_qc_filename = "{}_{}_QC_report.html".format(library_id, jira)

    qc_report = list(
        tantalus_api.get_dataset_file_resources(
            results_dataset["id"],
            "resultsdataset",
            {"filename__endswith": qc_filename},
        ))

    blobname = qc_report[0]["filename"]
    local_dir = os.path.join("qc_reports", jira)
    local_path = os.path.join(local_dir, jira_qc_filename)
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # Download blob
    blob = storage_client.blob_service.get_blob_to_path(
        container_name="results",
        blob_name=blobname,
        file_path=local_path,
    )

    # Get library ticket
    analysis = colossus_api.get("analysis_information", analysis_jira_ticket=jira)
    library_ticket = analysis["library"]["jira_ticket"]

    log.info("Adding report to parent ticket of {}".format(jira))
    add_attachment(library_ticket, local_path, jira_qc_filename)


def load_data_to_montage(jira):
    """
    SSH into loader machine and triggers import into montage
    
    Arguments:
        jira {str} -- jira id
    
    Raises:
        Exception: Ticket failed to load
    """
    log.info(f"Loading {jira} into Montage")
    try:
        # TODO: add directory in config
        subprocess.call([
            'ssh',
            '-t',
            'loader',
            f"bash /home/uu/montageloader2_flora/load_ticket.sh {jira}",
        ])
    except Exception as e:
        raise Exception(f"failed to load ticket: {e}")

    log.info(f"Successfully loaded {jira} into Montage")


def run_viz():
    """
    Update jira ticket, add QC report, and load data on Montage
    """
    # get completed analyses that need montage loading
    analyses = colossus_api.list(
        "analysis_information",
        montage_status="Pending",
        analysis_run__run_status="complete",
    )

    for analysis in analyses:
        # get library id
        library_id = analysis["library"]["pool_id"]

        # skip analyses older than this year
        # parse off ending time range
        last_updated_date = parser.parse(analysis["analysis_run"]["last_updated"][:-6])
        if last_updated_date < datetime(2020, 1, 1):
            continue

        jira_ticket = analysis["analysis_jira_ticket"]

        # update ticket description
        update_jira_dlp(jira_ticket, analysis["aligner"])

        # upload qc report to jira ticket
        attach_qc_report(jira_ticket, library_id, config["storages"])

        # load analysis into montage
        load_data_to_montage(jira_ticket)


def run_align(jira, args):
    """
    Run align if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            library_id 
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """

    # get analysis
    try:
        analysis = tantalus_api.get(
            "analysis",
            jira_ticket=jira,
            analysis_type="align",
            input_datasets__library__library_id=args['library_id'],
        )
    except:
        analysis = None

    if not analysis:
        # create breakpoint calling analysis
        analysis = alignment.AlignmentAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )
        analysis = analysis.analysis
        log.info(f"created align analysis {analysis['id']} under ticket {jira}")

    # check status
    if analysis['status'] in ('complete', 'running'):
        log.info("align has status {} for library {}".format(
            analysis['status'],
            args['library_id'],
        ))
        return analysis['status'] == 'complete'

    log.info(f"running align analysis {analysis['id']}")
    saltant_utils.run_analysis(
        analysis['id'],
        'align',
        jira,
        config["scp_version"],
        args['library_id'],
        args['aligner'],
        config,
    )

    return False


def run_hmmcopy(jira, args):
    """
    Run hmmcopy if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            library_id 
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """

    # get analysis
    try:
        analysis = tantalus_api.get(
            "analysis",
            jira_ticket=jira,
            analysis_type="hmmcopy",
            input_datasets__library__library_id=args['library_id'],
        )
    except:
        analysis = None

    if not analysis:
        # create breakpoint calling analysis
        analysis = hmmcopy.HMMCopyAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )
        analysis = analysis.analysis
        log.info(f"created hmmcopy analysis {analysis['id']} under ticket {jira}")

    # check status
    if analysis['status'] in ('complete', 'running'):
        log.info("hmmcopy has status {} for library {}".format(
            analysis['status'],
            args['library_id'],
        ))
        return analysis['status'] == 'complete'

    log.info(f"running hmmcopy analysis {analysis['id']}")
    saltant_utils.run_analysis(
        analysis['id'],
        'hmmcopy',
        jira,
        config["scp_version"],
        args['library_id'],
        args['aligner'],
        config,
    )

    return False


def run_annotation(jira, args):
    """
    Run annotation if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            library_id 
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """

    # get analysis
    try:
        analysis = tantalus_api.get(
            "analysis",
            jira_ticket=jira,
            analysis_type="annotation",
            input_datasets__library__library_id=args['library_id'],
        )
    except:
        analysis = None

    if not analysis:
        # create breakpoint calling analysis
        analysis = annotation.AnnotationAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )
        analysis = analysis.analysis
        log.info(f"created annotation analysis {analysis['id']} under ticket {jira}")

    # check status
    if analysis['status'] in ('complete', 'running'):
        log.info("annotation has status {} for library {}".format(
            analysis['status'],
            args['library_id'],
        ))
        return analysis['status'] == 'complete'

    log.info(f"running annotation analysis {analysis['id']}")
    saltant_utils.run_analysis(
        analysis['id'],
        'annotation',
        jira,
        config["scp_version"],
        args['library_id'],
        args['aligner'],
        config,
    )

    return False


def run_qc(aligner):
    """
    Gets all qc (align, hmmcopy, annotation) analyses set to ready 
    and checks if requirements have been satisfied before triggering
    run on saltant.

    Arguments:
        aligner {str} -- name of aligner 
    """

    # get colossus analysis information objects with status not complete
    analyses = colossus_api.list(
        "analysis_information",
        analysis_run__run_status_ne="complete",
        aligner=aligner if aligner else config["default_aligner"],
    )

    for analysis in analyses:
        # get library id
        library_id = analysis["library"]["pool_id"]

        # skip analyses older than this year
        # parse off ending time range
        last_updated_date = parser.parse(analysis["analysis_run"]["last_updated"][:-6])
        if last_updated_date < datetime(2020, 1, 1):
            continue

        # get jira ticket
        jira = analysis["analysis_jira_ticket"]

        # init args for analysis
        args = {
            'library_id': library_id,
            'aligner': "BWA_MEM" if analysis['aligner'] == "M" else "BWA_ALN",
            'ref_genome': colossus_utils.get_ref_genome(analysis["library"]),
        }

        log.info(f"checking ticket {jira} library {library_id}")

        # track qc analyses
        statuses = {
            "align": False,
            "hmmcopy": False,
            "annotation": False,
        }
        statuses["align"] = run_align(jira, args)

        # check align is complete
        if statuses["align"]:
            # run hmmcopy
            statuses["hmmcopy"] = run_hmmcopy(jira, args)

        # check hmmcopy complete
        if statuses["hmmcopy"]:
            # run annotation
            statuses["annotation"] = run_annotation(jira, args)

        # check annotation complete
        if statuses["annotation"]:
            # run pseudobulk
            run_pseudobulk.run(jira, library_id)


@click.command()
@click.option("--aligner", type=click.Choice(['A', 'M']))
def main(aligner):
    # run qcs
    run_qc(aligner)

    # update ticket and load to montage
    run_viz()


if __name__ == "__main__":
    main()
