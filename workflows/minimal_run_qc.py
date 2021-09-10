#!/usr/bin/env python
import os
import click
import logging
import traceback
import subprocess
from datetime import datetime, timedelta
from dateutil import parser

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi
from dbclients.slack import SlackClient

from workflows.analysis.dlp import (
    alignment,
    hmmcopy,
    annotation,
)

from workflows import run_pseudobulk

from workflows.utils import saltant_utils, file_utils, tantalus_utils, colossus_utils
from workflows.utils.jira_utils import update_jira_dlp, add_attachment, comment_jira, update_jira_alhena

from common_utils.utils import get_last_n_days

from constants.workflows_constants import ALHENA_VALID_PROJECTS

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def attach_qc_report(
    tantalus_api,
    colossus_api,
    jira,
    library_id,
    storage_name
    ):
    storage_client = tantalus_api.get_storage_client(storage_name)
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
    blob_client = storage_client.blob_service.get_blob_client('results', blobname)
    with open(local_path, "wb") as my_blob:
        download_stream = blob_client.download_blob()
        my_blob.write(download_stream.readall())
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
            'loader_montage',
            f"bash /home/uu/montageloader2_flora/load_ticket.sh {jira}",
        ])
    except Exception as e:
        raise Exception(f"failed to load ticket: {e}")

    log.info(f"Successfully loaded {jira} into Montage")

def generate_alhena_loader_projects_cli_args(projects):
    """
    Generate command line arguments to be passed to alhena loader script

    Args:
        projects: list of dict {'id': project_id, 'name': project_name}

    Returb:
        project_args: project arguments to be passed to alhena loader script
    """
    project_args_list = []

    for project in projects:
        project_name = project['name']

        if(project_name in ALHENA_VALID_PROJECTS):
            project_args_list.append(f'--project {ALHENA_VALID_PROJECTS[project_name]}')

    project_args = ' '.join(project_args_list)

    return project_args if project_args else ''

def generate_loader_command(jira, project_args, _reload, _filter, es_host="10.1.0.8"):
    """
    Generate loader script command and arguments

    Arguments:
        jira {str} -- jira id
        args {str} -- project args to be fed into the loader script
        es_host {str} -- elasticsearch VM host IP address
    """
    extra_args = ''
    base_command = f'bash /home/spectrum/alhena-loader/load_ticket.sh {jira} {es_host}'

    # handle reload
    if(_reload):
        extra_args = ' '.join([extra_args, "true"])
    else:
        extra_args = ' '.join([extra_args, "false"])

    # handle filter
    if(_filter):
        extra_args = ' '.join([extra_args, "true"])
    else:
        extra_args = ' '.join([extra_args, "false"])

    # handle invalid project args
    if(project_args):
        extra_args = ' '.join([extra_args, f'"{project_args}"'])

    command = base_command + extra_args

    return command

def load_data_to_alhena(jira, _reload=False, _filter=False, es_host="10.1.0.8"):
    """
    SSH into loader machine and triggers import into Alhena
    
    Arguments:
        jira {str} -- jira id
        es_host {str} -- elasticsearch VM host IP address
    
    Raises:
        Exception: Ticket failed to load
    """
    log.info(f"Loading {jira} into Alhena")

    projects = colossus_utils.get_projects_from_jira_id(jira)

    projects_cli_args = generate_alhena_loader_projects_cli_args(projects)
    loader_command = generate_loader_command(
        jira=jira,
        project_args=projects_cli_args,
        _reload=_reload,
        _filter=_filter,
        es_host=es_host,
    )

    try:
        # TODO: add directory in config
        subprocess.call([
            'ssh',
            '-t',
            'loader',
            loader_command,
        ])

        log.info(f"Successfully loaded {jira} into Alhena")
    except Exception as e:
        raise Exception(f"failed to load ticket: {e}") 

def run_viz(
    tantalus_api,
    colossus_api,
    storage_name,
    ):
    """
    Update jira ticket, add QC report, and load data on Montage
    """
    # get completed analyses that need montage loading
    analyses = colossus_api.list(
        "analysis_information",
        montage_status="Pending",
        analysis_run__run_status="complete",
    )

    failed = []
    for analysis in analyses:
        # get library id
        library_id = analysis["library"]["pool_id"]

        # skip analyses older than this year
        # parse off ending time range
        last_updated_date = parser.parse(analysis["analysis_run"]["last_updated"][:-6])
        if last_updated_date < get_last_n_days(90):
            continue

        jira_ticket = analysis["analysis_jira_ticket"]

        # upload qc report to jira ticket
        # upload qc report to jira ticket
        try:
            attach_qc_report(
                tantalus_api,
                colossus_api,
                jira_ticket,
                library_id,
                storage_name
            )
        except Exception as e:
            traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
            message = f"Attaching QC report failed for {library_id}, {jira_ticket}.\n {str(traceback_str)}"
            log.error(message)
            failed.append(f"{library_id}, {jira_ticket}")
            continue
        
        try:
            # load analysis into montage
            load_data_to_montage(jira_ticket)
        except Exception as e:
            traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
            message = f"Montage loading failed for {library_id}, {jira_ticket}.\n {str(traceback_str)}"
            log.error(message)
            failed.append(f"{library_id}, {jira_ticket}")
            continue

        # close jira ticket and update ticket description
        try:
            update_jira_dlp(jira_ticket, analysis["aligner"])
        except Exception as e:
            traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
            message = f"Updating JIRA failed for {library_id}, {jira_ticket}.\n {str(traceback_str)}"
            log.error(message)
            failed.append(f"{library_id}, {jira_ticket}")
            continue

    # propagate error after the loop
    if(failed):
        base = f"An error occurred while uploading to Montage in run_qc.py.\n"
        message = base + '\n'.join(failed)

        raise ValueError(message)

def run_viz_alhena(
    tantalus_api,
    colossus_api,
    storage_name,
    library_pool_id,
    _reload=False,
    _filter=False,
    ):
    """
    Update jira ticket, add QC report, and load data on Montage
    """
    # get completed analyses that need montage loading
    #analyses = colossus_api.list(
    #    "analysis_information",
    #    montage_status="Pending",
    #    analysis_run__run_status="complete",
    #)
    analyses = colossus_api.list("analysis_information", library__pool_id=library_pool_id)

    failed = []
    for analysis in analyses:
        # get library id
        library_id = analysis["library"]["pool_id"]

        # skip analyses older than this year
        # parse off ending time range
        last_updated_date = parser.parse(analysis["analysis_run"]["last_updated"][:-6])
        if last_updated_date < get_last_n_days(90):
            continue

        jira_ticket = analysis["analysis_jira_ticket"]

        # upload qc report to jira ticket
        try:
            attach_qc_report(
                tantalus_api,
                colossus_api,
                jira_ticket,
                library_id,
                storage_name
            )
        except Exception as e:
            traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
            message = f"Attaching QC report failed for {library_id}, {jira_ticket}.\n {str(traceback_str)}"
            log.error(message)
            failed.append(f"{library_id}, {jira_ticket}")
            continue

        try:
            # load analysis into alhena
            load_data_to_alhena(jira=jira_ticket, _reload=_reload, _filter=_filter)
        except Exception as e:
            traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
            message = f"Alhena loading failed for {library_id}, {jira_ticket}.\n {str(traceback_str)}"
            log.error(message)
            failed.append(f"{library_id}, {jira_ticket}")
            continue

        # close jira ticket and update ticket description
        try:
            update_jira_dlp(jira_ticket, analysis["aligner"])
            update_jira_alhena(jira_ticket)
        except Exception as e:
            traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
            message = f"Updating JIRA failed for {library_id}, {jira_ticket}.\n {str(traceback_str)}"
            log.error(message)
            failed.append(f"{library_id}, {jira_ticket}")
            continue

    # propagate error after the loop
    if(failed):
        base = f"An error occurred while uploading to Alhena in run_qc.py.\n"
        message = base + '\n'.join(failed)

        raise ValueError(message)

def run_align(
    tantalus_api,
    jira,
    args,
    config
    ):
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
        )
    except:
        analysis = None

    if not analysis:
        # create breakpoint calling analysis
        analysis = alignment.AlignmentAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            {
                **args,
                "gsc_lanes": None,
                "brc_flowcell_ids": None,
            },
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


def run_hmmcopy(
    tantalus_api,
    jira,
    args,
    config,
    ):
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


def run_annotation(
    tantalus_api,
    jira,
    args,
    config,
    ):
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


def run_qc(
    aligner,
    tantalus_api,
    colossus_api,
    slack_client,
    config,
    ):
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

    failed = []
    for analysis in analyses:
        # get library id
        library_id = analysis["library"]["pool_id"]

        # skip analyses older than this year
        # parse off ending time range
        last_updated_date = parser.parse(analysis["analysis_run"]["last_updated"][:-6])
        if last_updated_date < get_last_n_days(90):
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
        try:
            statuses["align"] = run_align(tantalus_api, jira, args, config)
        except Exception as e:
            traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
            message = f"Alignment failed for {library_id}, {jira}.\n {str(traceback_str)}"
            log.error(message)
            failed.append(f"{library_id}, {jira}")
            continue

        # check align is complete
        if statuses["align"]:
            # run hmmcopy
            try:
                statuses["hmmcopy"] = run_hmmcopy(tantalus_api, jira, args, config)
            except Exception as e:
                traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
                message = f"HMMcopy failed for {library_id}, {jira}.\n {str(traceback_str)}"
                log.error(message)
                failed.append(f"{library_id}, {jira}")
                continue

        # check hmmcopy complete
        if statuses["hmmcopy"]:
            # run annotation
            try:
                statuses["annotation"] = run_annotation(tantalus_api, jira, args, config)
            except Exception as e:
                traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
                message = f"Annotation failed for {library_id}, {jira}.\n {str(traceback_str)}"
                log.error(message)
                failed.append(f"{library_id}, {jira}")
                continue

        # check annotation complete
        if statuses["annotation"]:
            # update status on colossus
            try:
                analysis_run_id = analysis["analysis_run"]["id"]
                analysis_run = colossus_api.get(
                    "analysis_run",
                    id=analysis_run_id,
                )
                colossus_api.update(
                    "analysis_run",
                    id=analysis_run_id,
                    run_status="complete",
                )
            except Exception as e:
                traceback_str = "".join(traceback.format_exception(etype=None, value=e, tb=e.__traceback__))
                message = f"Updating colossus failed for {library_id}, {jira}.\n {str(traceback_str)}"
                log.error(message)
                failed.append(f"{library_id}, {jira}")
                continue

    # propagate error after the loop
    if(failed):
        base = f"An error occurred while running Analysis in run_qc.py.\n"
        message = base + '\n'.join(failed)

        raise ValueError(message)    

#    # get annotation analysis completed in last week
#    analyses = tantalus_api.list(
#        "analysis",
#        status="complete",
#        analysis_type__name="annotation",
#        last_updated__gte=str(datetime.now() - timedelta(days=21)),
#    )
#
#    for analysis in analyses:
#        try:
#            # run pseudobulk
#            run_pseudobulk.run(
#                analysis["jira_ticket"],
#                analysis["args"]["library_id"],
#            )
#        except Exception as e:
#            traceback.print_exc()
#            log.error(f"Failed to run pseudobulk for {analysis['args']['library_id']}: {e}")


@click.command()
@click.option("--library_id", type=str, required=True)
@click.option("--aligner", type=click.Choice(['A', 'M']))
def main(library_id, aligner):
    tantalus_api = TantalusApi()
    colossus_api = ColossusApi()
    slack_client = SlackClient()

    # load config file
    config = file_utils.load_json(
        os.path.join(
            os.path.dirname(os.path.realpath(__file__)),
            'config',
            'normal_config.json',
        ))
    storage_name = config['storages']['remote_results']
    # run qcs
    try:
#        run_qc(
#            aligner,
#            tantalus_api,
#            colossus_api,
#            slack_client,
#            config,
#        )

        # update ticket and load to montage
        run_viz_alhena(
            tantalus_api,
            colossus_api,
            storage_name,
            library_id,
            _filter=True,
        )

    except Exception as e:
        print(e)
        #slack_client.post(f"{e}")


if __name__ == "__main__":
    main()
