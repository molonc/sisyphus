#!/usr/bin/env python
import os
import click
import logging
import subprocess
from datetime import datetime, timedelta
from dateutil import parser

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from workflows.utils import saltant_utils, file_utils, tantalus_utils
from workflows.utils.jira_utils import update_jira_dlp, add_attachment, comment_jira

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False

tantalus_api = TantalusApi()
colossus_api = ColossusApi()


def attach_qc_report(jira, library_id, storages):

    storage_client = tantalus_api.get_storage_client(storages["remote_results"])
    results_dataset = tantalus_api.get("resultsdataset", name="{}_annotation".format(jira))

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


def load_ticket(jira):
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


@click.command()
@click.option("--aligner", type=click.Choice(['A', 'M']))
def main(aligner):
    """
    Gets all qc (align, hmmcopy, annotation) analyses set to ready 
    and checks if requirements have been satisfied before triggering
    run on saltant.

    Kwargs:
        aligner (str): name of aligner 
    """

    # load config file
    config = file_utils.load_json(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'config',
            'normal_config.json',
        ))

    # map of type of analyses required before particular analysis can run
    # note: keep this order to avoid checking requirements more than once
    required_analyses_map = {
        'annotation': [
            'hmmcopy',
            'align',
        ],
        'hmmcopy': ['align'],
        'align': [],
    }

    # get colossus analysis information objects with status not complete
    analyses = colossus_api.list(
        "analysis_information",
        analysis_run__run_status_ne="complete",
        aligner=aligner if aligner else config["default_aligner"],
    )

    for analysis in analyses:
        # get library id
        library_id = analysis["library"]["pool_id"]
        log.info(f"{library_id}")

        # skip analysis if marked as complete
        status = analysis["analysis_run"]["run_status"]

        # skip analyses older than this year
        # parse off ending time range
        last_updated_date = parser.parse(analysis["analysis_run"]["last_updated"][:-6])
        if last_updated_date < datetime(2020, 1, 1):
            continue

        jira_ticket = analysis["analysis_jira_ticket"]
        log.info(f"checking ticket {jira_ticket} library {library_id}")
        for analysis_type in required_analyses_map:
            log.info(f"checking requirements for {analysis_type}")
            # check if analysis exists on tantalus
            try:
                tantalus_analysis = tantalus_api.get(
                    'analysis',
                    jira_ticket=jira_ticket,
                    analysis_type__name=analysis_type,
                )
            except:
                tantalus_analysis = None

            if tantalus_analysis is not None:
                # check if running or complete
                status = tantalus_analysis["status"]
                if status in ('running', 'complete'):
                    log.info(f"skipping {analysis_type} for {jira_ticket} since status is {status}")

                    # update run status on colossus
                    if analysis_type == "annotation" and status == "complete":
                        analysis_run_id = analysis["analysis_run"]["id"]
                        analysis_run = colossus_api.get("analysis_run", id=analysis_run_id)
                        colossus_api.update("analysis_run", id=analysis_run_id, run_status="complete")

                    continue

                log.info(f"running {analysis_type} in library {library_id} with ticket {jira_ticket}")
                # otherwise run analysis
                saltant_utils.run_analysis(
                    tantalus_analysis['id'],
                    analysis_type,
                    jira_ticket,
                    config["scp_version"],
                    library_id,
                    aligner if aligner else config["default_aligner"],
                    config,
                )
            else:
                # set boolean determining trigger of run
                is_ready_to_create = True
                # check if required completed analyses exist
                for required_analysis_type in required_analyses_map[analysis_type]:
                    try:
                        required_analysis = tantalus_api.get(
                            'analysis',
                            jira_ticket=jira_ticket,
                            analysis_type__name=required_analysis_type,
                            status="complete",
                        )
                    except:
                        log.error(
                            f"a completed {required_analysis_type} analysis is required to run before {analysis_type} runs for {jira_ticket}"
                        )
                        # set boolean as false since analysis cannot be created yet
                        is_ready_to_create = False
                        break

                # create analysis and trigger on saltant if analysis creation has met requirements
                if is_ready_to_create:
                    log.info(f"creating {analysis_type} analysis for ticket {jira_ticket}")

                    try:
                        tantalus_utils.create_qc_analyses_from_library(
                            library_id,
                            jira_ticket,
                            config["scp_version"],
                            analysis_type,
                        )
                    except Exception as e:
                        log.error(f"failed to create {analysis_type} analysis for ticket {jira_ticket}")
                        continue
                    tantalus_analysis = tantalus_api.get(
                        'analysis',
                        jira_ticket=jira_ticket,
                        analysis_type__name=analysis_type,
                    )

                    log.info(f"running {analysis_type} in library {library_id} with ticket {jira_ticket}")
                    saltant_utils.run_analysis(
                        tantalus_analysis['id'],
                        analysis_type,
                        jira_ticket,
                        config["scp_version"],
                        library_id,
                        aligner if aligner else config["default_aligner"],
                        config,
                    )

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
        update_jira_dlp(jira_ticket, "M")
        # upload qc report to jira ticket
        attach_qc_report(jira_ticket, library_id, config["storages"])

        # load analysis into montage
        load_ticket(jira_ticket)


if __name__ == "__main__":
    main()
