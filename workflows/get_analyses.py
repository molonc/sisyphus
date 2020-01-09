import os
import click
import logging
import hashlib
import screenutils

from jira import JIRA, JIRAError
from datetime import datetime
from collections import defaultdict

from workflows.unanalyzed_data import *

import datamanagement.templates as templates

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import NotFoundError

from workflows.utils import file_utils
from workflows.utils import saltant_utils
from workflows.utils.colossus_utils import get_ref_genome

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False


def get_sequencings(library_info):
    '''
    Given library id (str), return list of sequencings
    '''
    sequencings = [sequencing['id'] for sequencing in library_info['dlpsequencing_set']]
    return sequencings


def get_lanes_from_sequencings(sequencing_id_list):
    '''
    Given list of sequencing ids, return list of all unique lanes
    '''
    lanes = set()
    for sequencing_id in sequencing_id_list:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)
        for lane in sequencing['dlplane_set']:
            lanes.add(lane['id'])

    return list(lanes)


def get_analyses_to_run(version, aligner, check=False):
    '''
    Find libraries needed for analysis and create JIRA tickets and analysis
    objects on tantalus and colossus

    Args:
        version (str): Version of pipeline
        aligner (str): Either BWA_ALN_0_5_7 or BWA_MEM_0_7_6A (as 03/01/2019)
        check (bool): If true, find libraries to analyze only; no new creations

    Returns:
        analyses_tickets (list): List of JIRA tickets
    '''
    library_type = "SC_WGS"
    bam_lanes = get_lanes_from_bams_datasets(library_type)
    unanalyzed_data_libraries = search_for_unaligned_data(library_type, bam_lanes)
    no_hmmcopy_libraries = search_for_no_hmmcopy_data(bam_lanes)
    no_annotation_libraries = search_for_no_annotation_data(aligner)

    analyses_tickets = dict(
        align=dict(),
        hmmcopy=dict(),
        annotation=dict(),
    )

    if check:
        log.info("*** Dry run ***")
        log.info("Libraries with unanalyzed data: {}".format(unanalyzed_data_libraries))

    else:
        log.info("Checking libraries with unanalyzed data")
        for library_id in unanalyzed_data_libraries:
            analysis_info = check_library_for_analysis(library_id, aligner, version, "align")

            if analysis_info is None:
                continue

            jira_ticket = analysis_info['jira_ticket']
            if not analysis_info["analysis_created"]:
                log.info(f"need to create {analysis_info['name']}")
                tantalus_analysis = create_tantalus_analysis(
                    analysis_info['name'],
                    jira_ticket,
                    analysis_info['library_id'],
                    'align',
                    version,
                )

                colossus_analysis = create_colossus_analysis(
                    analysis_info['library_id'],
                    jira_ticket,
                    version,
                    aligner,
                )

            analyses_tickets['align'][jira_ticket] = library_id

        log.info("Checking libraries needing hmmcopy")
        for library_id in no_hmmcopy_libraries:
            analysis_info = check_library_for_analysis(library_id, aligner, version, "hmmcopy")
            if analysis_info is None:
                continue

            jira_ticket = analysis_info['jira_ticket']
            if not analysis_info["analysis_created"]:
                log.info(f"need to create {analysis_info['name']}")
                tantalus_analysis = create_tantalus_analysis(
                    analysis_info['name'],
                    jira_ticket,
                    analysis_info['library_id'],
                    'hmmcopy',
                    version,
                )

            analyses_tickets['hmmcopy'][jira_ticket] = library_id

        log.info("Checking libraries needing annotation")
        for library_id in no_annotation_libraries:
            analysis_info = check_library_for_analysis(library_id, aligner, version, "annotation")

            if analysis_info is None:
                continue

            jira_ticket = analysis_info['jira_ticket']
            if not analysis_info["analysis_created"]:
                log.info(f"need to create {analysis_info['name']}")
                tantalus_analysis = create_tantalus_analysis(
                    analysis_info['name'],
                    jira_ticket,
                    analysis_info['library_id'],
                    'annotation',
                    version,
                )

            analyses_tickets['annotation'][jira_ticket] = library_id

    return analyses_tickets


def search_input_datasets(library_id, aligner, reference_genome):
    '''
    Search for input datasets

    Args:
        library_id (str)
        aligner (str): Either BWA_ALN_0_5_7 or BWA_MEM_0_7_6A (as 03/01/2019)
        reference_genome (str): Either HG19 or MM10 (as 03/01/2019)

    Return:
        dataset_ids (list)

    '''
    datasets = tantalus_api.list(
        'sequencedataset',
        library__library_id=library_id,
        dataset_type='FQ',
    )

    dataset_ids = set([dataset["id"] for dataset in datasets])

    return list(dataset_ids)


def check_library_for_analysis(library_id, aligner, version, analysis_type):
    '''
    Given a library, check if library is included in analysis and has all data imported.
    If so, check for existing analysis. Otherwise create analysis jira ticket.

    Args:
        library_id (str): Library/pool id
        aligner (str): Either BWA_ALN_0_5_7 or BWA_MEM_0_7_6A (as 03/01/2019)
        version (str): Version of scpipeline
    '''

    log.info("Checking library {}".format(library_id))

    library_info = colossus_api.get('library', pool_id=library_id)

    if library_info['exclude_from_analysis']:
        log.info('Library {} is excluded from analysis; skipping'.format(library_id))
        return None

    taxonomy_id_map = {
        '9606': 'HG19',
        '10090': 'MM10',
    }

    aligner_map = {
        'A': 'BWA_ALN_0_5_7',
        'M': 'BWA_MEM_0_7_6A',
    }

    taxonomy_id = library_info['sample']['taxonomy_id']
    reference_genome = taxonomy_id_map[taxonomy_id]
    aligner = aligner_map[aligner]

    input_datasets = search_input_datasets(library_id, aligner, reference_genome)
    lanes = set()
    for input_dataset in input_datasets:
        dataset = tantalus_api.get('sequence_dataset', id=input_dataset)
        for sequence_lane in dataset['sequence_lanes']:
            lane = "{}_{}".format(sequence_lane['flowcell_id'], sequence_lane['lane_number'])
            lanes.add(lane)

    lanes = ", ".join(sorted(lanes))
    lanes = hashlib.md5(lanes.encode('utf-8'))
    lanes_hashed = "{}".format(lanes.hexdigest()[:8])

    # Previously we were only running QC analyses and so we need to check
    # whether the library has already been ran under QC and ignore if so
    qc_analysis_name = templates.SC_QC_ANALYSIS_NAME_TEMPLATE.format(
        analysis_type='qc',
        aligner=aligner,
        ref_genome=reference_genome,
        library_id=library_id,
        lanes_hashed=lanes_hashed,
    )

    qc_analysis = list(tantalus_api.list("analysis", name=qc_analysis_name, version=version))
    if len(qc_analysis) == 1:
        log.info(f"Data for {library_id} has already been ran under QC")
        analysis_info = dict(
            name=qc_analysis_name,
            library_id=library_id,
            jira_ticket=qc_analysis[0]["jira_ticket"],
            analysis_created=True,
        )
        return analysis_info

    # Idea: Given a set of lanes, a single jira ticket will be used to run each analysis on those lanes.
    # Since the pipeline begins with align, we need to check if the align analysis has already
    # been created and use that ticket if so. Otherwise, throw an error saying to run align first.
    align_analysis_name = templates.SC_QC_ANALYSIS_NAME_TEMPLATE.format(
        analysis_type='align',
        aligner=aligner,
        ref_genome=reference_genome,
        library_id=library_id,
        lanes_hashed=lanes_hashed,
    )

    analysis_name = templates.SC_QC_ANALYSIS_NAME_TEMPLATE.format(
        analysis_type=analysis_type,
        aligner=aligner,
        ref_genome=reference_genome,
        library_id=library_id,
        lanes_hashed=lanes_hashed,
    )

    if analysis_type != "align":
        try:
            align_analysis = tantalus_api.get("analysis", name=align_analysis_name, status="complete")
            jira_ticket = align_analysis["jira_ticket"]
            log.info("Analysis ticket {} already exists for {};".format(
                jira_ticket,
                library_id,
            ))

        except NotFoundError:
            log.error(f"a completed align analysis is required to run before {analysis_type} runs")
            return None

        try:
            analysis = tantalus_api.get("analysis", name=analysis_name)
            analysis_info = dict(
                name=analysis_name,
                library_id=library_id,
                jira_ticket=analysis["jira_ticket"],
                analysis_created=True,
            )
        except NotFoundError:
            analysis_info = dict(
                name=analysis_name,
                library_id=library_id,
                jira_ticket=jira_ticket,
                analysis_created=False,
            )

    else:
        try:
            align_analysis = tantalus_api.get("analysis", name=align_analysis_name)
            jira_ticket = align_analysis["jira_ticket"]
            log.info("Analysis ticket {} already exists for {}".format(
                jira_ticket,
                library_id,
            ))

            analysis_info = dict(
                name=analysis_name,
                library_id=library_id,
                jira_ticket=jira_ticket,
                analysis_created=True,
            )

        except NotFoundError:
            log.info("JIRA ticket needs to be created for align analysis")
            jira_ticket = create_analysis_jira_ticket(library_id)

            analysis_info = dict(
                name=analysis_name,
                library_id=library_id,
                jira_ticket=jira_ticket,
                analysis_created=False,
            )

    return analysis_info


def create_analysis_jira_ticket(library_id):
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

    library = colossus_api.get('library', pool_id=library_id)
    sample_id = library['sample']['sample_id']

    library_jira_ticket = library['jira_ticket']
    issue = jira_api.issue(library_jira_ticket)

    log.info('Creating analysis JIRA ticket as sub task for {}'.format(library_jira_ticket))

    # In order to search for library on Jira,
    # Jira ticket must include spaces
    sub_task = {
        'project': {
            'key': 'SC'
        },
        'summary': 'Analysis of {} - {}'.format(sample_id, library_id),
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
    jira_api.add_watcher(analysis_jira_ticket, 'jedwards')
    jira_api.add_watcher(analysis_jira_ticket, 'jbiele')
    jira_api.add_watcher(analysis_jira_ticket, 'jbwang')
    jira_api.add_watcher(analysis_jira_ticket, 'elaks')

    # Assign task to myself
    analysis_issue = jira_api.issue(analysis_jira_ticket)
    analysis_issue.update(assignee={'name': JIRA_USER})

    log.info('Created analysis ticket {} for library {}'.format(analysis_jira_ticket, library_id))

    return analysis_jira_ticket


def create_tantalus_analysis(name, jira_ticket, library_id, analysis_type, version):
    '''
    Create analysis objects on Tantalus

    Args:
        name (str): Name of analysis in form
            sc_<analysis_type>_<aligner>_<ref_genome>_<library_id>_<hashed_lanes>
        jira_ticket (str): Jira ticket id (ex. SC-1234)
        version (str): Version of pipeline
        analysis_type (str)

    Returns:
        analysis_id (int): pk of analysis object on Tantalus
    '''

    try:
        analysis = tantalus_api.get('analysis', name=name)

    except NotFoundError:
        log.info('Creating analysis object {} on tantalus'.format(name))

        # todo: refactor
        analysis_name_parsed = name.split("_")
        aligner = "_".join(analysis_name_parsed[2:7])
        reference_genome = analysis_name_parsed[-3]

        args = dict(
            aligner=aligner,
            library_id=library_id,
            ref_genome=reference_genome,
        )
        data = dict(
            name=name,
            analysis_type=analysis_type,
            jira_ticket=jira_ticket,
            version=version,
            status='idle',
            args=args,
        )
        analysis, _ = tantalus_api.create('analysis', data, ['name'])

        log.info('Created analysis {} on tantalus'.format(analysis['id']))

    analysis_id = analysis['id']

    return analysis_id


def create_colossus_analysis(library_id, jira_ticket, version, aligner):
    '''
    Create analysis objects on Colossus

    Args:
        library_id (str): Library/Pool id
        jira_ticket (str): Jira ticket id (ex. SC-1234)
        version (str): Version of pipeline

    Returns:
        analysis_id (int): pk of analysis information object on Colossus
    '''

    try:
        analysis = colossus_api.get("analysis_information", analysis_jira_ticket=jira_ticket)

    except NotFoundError:
        taxonomy_id_map = {
            '9606': 1, # grch37
            '10090': 2, # mm10
        }

        library_info = colossus_api.get('library', pool_id=library_id)
        taxonomy_id = library_info['sample']['taxonomy_id']
        ref_genome_key = taxonomy_id_map[taxonomy_id]
        sequencings = get_sequencings(library_info)
        lanes = get_lanes_from_sequencings(sequencings)

        log.info("Creating analysis information object for {}_{} on Colossus".format(
            library_info['sample']['sample_id'], library_id))

        analysis = colossus_api.create(
            'analysis_information',
            library=library_info['id'],
            version=version,
            sequencings=sequencings,
            reference_genome=ref_genome_key,
            aligner=aligner,
            analysis_jira_ticket=jira_ticket,
            lanes=lanes,
        )

        # FIXME: Find out why lanes can't be added using create
        analysis_run = colossus_api.create(
            'analysis_run',
            run_status='idle',
            dlpanalysisinformation=analysis['id'],
            blob_path=jira_ticket,
        )

        colossus_api.update('analysis_information', analysis['id'], analysis_run=analysis_run['id'])

        log.info('Created analysis {} on colossus'.format(analysis['id']))

    analysis_id = analysis['id']

    return analysis_id


def run_screens(analyses_to_run, version):
    '''
    Mass run of analyses in screens when saltant is down

    Args:
        analyses_to_run (dict): jira tickets sorted by analysis type
    '''

    python_cmd = os.environ.get("HEADNODE_AUTOMATION_PYTHON")
    run_file = os.path.join(os.environ.get("HEADNODE_AUTOMATION_DIR"), "workflows", "run.py")

    # FIXME: Find out how to pass command to screen to create new windows
    # Right now a screen is being created for each analysis -- needs cleanup

    for analysis_type, ticket_library in analyses_to_run.items():
        for ticket in ticket_library:
            library_id = ticket_library["library"]
            analysis_screen = screenutils.Screen(ticket, initialize=True)
            stdout_file = "logs/{}_{}.out".format(ticket, analysis_type)
            stderr_file = "logs/{}_{}.err".format(ticket, analysis_type)

            cmd_str = "{} {} {} {} {} --update > {} 2> {}".format(
                python_cmd,
                run_file,
                ticket,
                version,
                analysis_type,
                stdout_file,
                stderr_file,
            )

        analysis_screen.send_commands(cmd_str)


def check_running_analysis(jira_ticket, analysis_type):
    '''
    Given a jira ticket, check if analysis is already running

    Args:
        jira_ticket (str)

    Return:
        bool
    '''

    analysis = tantalus_api.get("analysis", jira_ticket=jira_ticket, analysis_type__name=analysis_type)

    if analysis["status"] == "running":
        log.info("Analysis {} already running".format(jira_ticket))
        return True

    return False


@click.command()
@click.argument('version')
@click.argument('aligner')
@click.option('--check', is_flag=True)
@click.option('--override_contamination', is_flag=True)
@click.option('--screen', is_flag=True)
@click.option('--ignore_status', is_flag=True)
@click.option('--skip', "-s", multiple=True)
def main(version, aligner, check=False, override_contamination=False, screen=False, ignore_status=False, skip=None):

    config_path = os.path.join(os.environ['HEADNODE_AUTOMATION_DIR'], 'workflows/config/normal_config.json')
    config = file_utils.load_json(config_path)

    log.info('version: {}, aligner: {}'.format(version, aligner))

    analyses_to_run = get_analyses_to_run(version, aligner, check=check)

    log.info("Analyses to run {}".format(analyses_to_run))

    for skip_analysis in skip:
        if not templates.JIRA_ID_RE.match(skip_analysis):
            raise Exception('Invalid Jira ticket to be skipped: {}'.format(skip_analysis))

        log.info("Skipping analysis on {}".format(skip_analysis))
        for analysis_type, ticket_library in analyses_to_run.items():
            if skip_analysis in ticket_library:
                del analyses_to_run[analysis_type][skip_analysis]

    # If saltant is down, run analysis in screens
    if screen:
        log.info("Running analyses in screens")
        run_screens(analyses_to_run, version)
        return "Running in screens"

    for analysis_type, ticket_library in analyses_to_run.items():
        for ticket in ticket_library:
            library_id = ticket_library[ticket]
            if not check_running_analysis(ticket, analysis_type):
                log.info(f"Running {analysis_type} for {ticket}")
                saltant_utils.run_analysis(
                    analysis_type,
                    ticket,
                    version,
                    library_id,
                    aligner,
                    config,
                    override_contamination=override_contamination,
                )


if __name__ == '__main__':
    main()
