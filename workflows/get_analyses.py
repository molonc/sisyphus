import os
import click
import logging
import hashlib

from jira import JIRA, JIRAError
from datetime import datetime
from collections import defaultdict

import unanalyzed_data
from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from dbclients.basicclient import NotFoundError
from workflows.utils import saltant_utils
from workflows.utils import file_utils

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False


def get_sequencings(library_id):
    '''
    Given library id (str), return list of sequencings
    '''
    library_info = colossus_api.get('library', pool_id=library_id)
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
    Find libraries needed for analysis and create JIRA tickets and analysis objects on tantalus and colossus

    Args:
        version (str): Version of pipeline
        aligner (str): Either BWA_ALN_0_5_7 or BWA_MEM_0_7_6A (as 03/01/2019)
        check (bool): If true, find libraries to analyze only; no new creations

    Returns:
        analyses_tickets (list): List of JIRA tickets
    '''
    unaligned_data_libraries = unanalyzed_data.search_for_unaligned_data()
    no_hmmcopy_data_libraries = unanalyzed_data.search_for_no_hmmcopy_data()

    analyses_tickets = dict(
        align = [], 
        hmmcopy = [],
    )

    if check:
        log.info("Finding analyses to run only.")
        log.info("Unaligned data: {}".format(unaligned_data_libraries))
        log.info("No hmmcopy data: {}".format(no_hmmcopy_data_libraries))

    else:
        log.info("Checking unaligned data")
        for library_id in unaligned_data_libraries:
            analysis_info = check_library_for_analysis(library_id, aligner, 'align')

            if analysis_info is not None:
                jira_ticket = analysis_info['jira_ticket']
                analyses_tickets['align'].append(jira_ticket)     

                if analysis_info['analysis_created'] == False:
                    tantalus_analysis = create_tantalus_analysis(
                        analysis_info['name'], 
                        jira_ticket, 
                        analysis_info['library_id'], 
                        'align', 
                        version
                    )

                    colossus_analysis = create_colossus_analysis(analysis_info['library_id'], jira_ticket, version)

        log.info("Checking no hmmcopy data")
        for library_id in no_hmmcopy_data_libraries:
            analysis_info = check_library_for_analysis(library_id, aligner, 'hmmcopy')

            if analysis_info is not None:
                jira_ticket = analysis_info['jira_ticket']
                analyses_tickets['hmmcopy'].append(jira_ticket)     

                if analysis_info['analysis_created'] == False:
                    tantalus_analysis = create_tantalus_analysis(
                        analysis_info['name'], 
                        jira_ticket, 
                        analysis_info['library_id'], 
                        'hmmcopy', 
                        version,
                    )

                    colossus_analysis = create_colossus_analysis(analysis_info['library_id'], jira_ticket, version)

    return analyses_tickets


def check_library_for_analysis(library_id, aligner, analysis_type):
    '''
    Given a library, check if library is included in analysis and has all data imported. 
    If so, check for existing analysis. Otherwise create analysis jira ticket.

    Args: 
        library_id (str): Library/pool id
        aligner (str): Either BWA_ALN_0_5_7 or BWA_MEM_0_7_6A (as 03/01/2019)
        analysis_type (str): Either align or hmmcopy (as 03/01/2019)
    '''

    log.info("Checking library {}".format(library_id))

    library_info = colossus_api.get('library', pool_id=library_id)

    if library_info['exclude_from_analysis']:
        log.info('Library {} is excluded from analysis; skipping'.format(library_id))
        return None

    taxonomy_id_map = {
        '9606':      'HG19',
        '10090':     'MM10',
    }

    taxonomy_id = library_info['sample']['taxonomy_id']
    reference_genome = taxonomy_id_map[taxonomy_id]

    sequencing_ids = get_sequencings(library_id)

    if not sequencing_ids:
        log.info('Library {} has no sequencings; skipping'.format(library_id))
        pass

    lanes = set()
    for sequencing_id in sequencing_ids:
        sequencing = colossus_api.get('sequencing', id=sequencing_id)

        # Check if all lanes have been imported
        if sequencing['number_of_lanes_requested'] != 0 and len(sequencing['dlplane_set']) < sequencing['number_of_lanes_requested']:
            log.info("Either no lanes requested or not all data has been imported; skipping")
            return None

        for lane in sequencing['dlplane_set']:
            lanes.add(lane['flow_cell_id']) 

    lanes = ", ".join(sorted(lanes))
    lanes = hashlib.md5(lanes)
    lanes_hashed = "{}".format(lanes.hexdigest()[:8])

    try:
        if analysis_type == "align":
            analysis_name = "sc_{}_{}_{}_{}_{}".format(
                analysis_type, 
                aligner, 
                reference_genome, 
                library_id,
                lanes_hashed,
            )
            analysis = tantalus_api.get('analysis', name=analysis_name)
            jira_ticket = analysis['jira_ticket']

        elif analysis_type == "hmmcopy":
            align_analysis_name = "sc_align_{}_{}_{}_{}".format(
                aligner, 
                reference_genome, 
                library_id,
                lanes_hashed,
            )

            analysis_name = "sc_{}_{}_{}_{}_{}".format(
                analysis_type,
                aligner, 
                reference_genome, 
                library_id,
                lanes_hashed,
            )

            jira_ticket = check_existing_align_analysis(align_analysis_name)

        if jira_ticket is not None:
            log.info("Analysis ticket {} already exists for {}; tantalus analysis name: {}".format(jira_ticket, library_id, analysis_name))
            analysis_info = dict(
                name = analysis_name,
                library_id = library_id,
                jira_ticket = jira_ticket,
                analysis_created = True,
            )
        
    except NotFoundError:
        # Create jira ticket
        log.info("JIRA ticket needs to be created for {} analysis".format(analysis_type))   
        jira_ticket = create_analysis_jira_ticket(library_id) 
        analysis_info = dict(
            name = analysis_name,
            library_id = library_id,
            jira_ticket = jira_ticket,
            analysis_created = False,
        )

    return analysis_info


def check_existing_align_analysis(align_analysis_name):
    '''
    Checks if existing align analysis object exists on Tantalus

    Args:
        align_analysis_name (str): Name of analysis object to be searched

    Returns:
        jira_ticket (str or None)
    '''
    log.info("Searching for align analysis {}".format(align_analysis_name)) 
    align_analysis = tantalus_api.get("analysis", name=align_analysis_name)

    if align_analysis["status"] == "complete":
        log.info("Completed analysis exists with ticket {}".format(align_analysis["jira_ticket"]))
        jira_ticket = align_analysis["jira_ticket"]
        return jira_ticket

    if align_analysis["status"] == "error":
        log.info("Analysis {} failed with error; running again".format(align_analysis["jira_ticket"]))
        return jira_ticket

    # FIX: if analysis is idle, align should've been ran and cause error since analysis info wont be defined
    return None


def create_analysis_jira_ticket(library_id):
    '''
    Create analysis jira ticket as subtask of library jira ticket

    Args:
        info (dict): Keys: library_id

    Returns:
        analysis_jira_ticket: jira ticket id (ex. SC-1234)
    '''

    JIRA_USER = os.environ['JIRA_USER']
    JIRA_PASSWORD = os.environ['JIRA_PASSWORD']
    jira_api = JIRA('https://www.bcgsc.ca/jira/', basic_auth=(JIRA_USER, JIRA_PASSWORD))

    library = colossus_api.get('library', pool_id=library_id)
    sample_id = library['sample']['sample_id']

    library_jira_ticket = library['jira_ticket']
    issue = jira_api.issue(library_jira_ticket)
    
    log.info('Creating analysis JIRA ticket as sub task for {}'.format(library_jira_ticket))

    sub_task = {
        'project': {'key': 'SC'},
        'summary': 'Analysis of LIB_{}_{}'.format(sample_id, library_id),
        'issuetype' : { 'name' : 'Sub-task' },
        'parent': {'id': issue.key}
    }

    sub_task_issue = jira_api.create_issue(fields=sub_task)
    analysis_jira_ticket = sub_task_issue.key

    # Add watchers
    jira_api.add_watcher(analysis_jira_ticket, JIRA_USER)
    jira_api.add_watcher(analysis_jira_ticket, 'jedwards')
    jira_api.add_watcher(analysis_jira_ticket,'jbiele')
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
        name (str): Name of analysis (should be in form sc_<analysis_type>_<aligner>_<ref_genome>_<library_id>_<hashed_lanes>)
        jira_ticket (str): Jira ticket id (ex. SC-1234)
        version (str): Version of pipeline
        analysis_type (str): Either align or hmmcopy (as of 03/01/2019)

    Returns:
        analysis_id (int): pk of analysis object on Tantalus
    '''
    log.info('Creating analysis object {} on tantalus'.format(name))

    analysis_name_parsed = name.split("_")
    aligner = "_".join(analysis_name_parsed[2:7])
    reference_genome = analysis_name_parsed[-3]

    args = dict(
        aligner = aligner,
        library_id = library_id,
        ref_genome = reference_genome,
    )
    data = dict(
        name = name,
        analysis_type = analysis_type,
        jira_ticket = jira_ticket,
        version = version,
        status = 'idle',
        args = args,
    )
    analysis = tantalus_api.create('analysis', **data)

    log.info('Created analysis {} on tantalus'.format(analysis['id']))
    analysis_id = analysis['id']

    return analysis_id


def create_colossus_analysis(library_id, jira_ticket, version):
    '''
    Create analysis objects on Colossus

    Args:
        library_id (str): Library/Pool id
        jira_ticket (str): Jira ticket id (ex. SC-1234)
        version (str): Version of pipeline
    
    Returns:
        analysis_id (int): pk of analysis information object on Colossus
    '''

    taxonomy_id_map = {
        '9606':      1, # grch37
        '10090':     2, # mm10
    }

    library_info = colossus_api.get('library', pool_id=library_id)

    taxonomy_id = library_info['sample']['taxonomy_id']
    ref_genome_key = taxonomy_id_map[taxonomy_id]
    sequencings = [sequencing['id'] for sequencing in library_info['dlpsequencing_set']]
    lanes = get_lanes_from_sequencings(sequencings)

    log.info("Creating analysis information object for {}_{} on Colossus".format(library_info['sample']['sample_id'], library_id))

    analysis = colossus_api.create('analysis_information',
        library=library_info['id'],
        version=version,
        sequencings=sequencings,
        reference_genome=ref_genome_key,
        aligner='A',
        analysis_jira_ticket=jira_ticket,
    )

    # FIXME: Find out why lanes can't be added using create 
    colossus_api.update('analysis_information', analysis['id'], lanes=lanes)

    analysis_run = colossus_api.create('analysis_run',
        run_status='idle',
        dlpanalysisinformation=analysis['id'],
        blob_path=jira_ticket,
    )

    colossus_api.update('analysis_information', analysis['id'], analysis_run=analysis_run['id'])

    log.info('Created analysis {} on colossus'.format(analysis['id']))
    analysis_id = analysis['id']

    return analysis_id


@click.command()
@click.argument('version')
@click.argument('aligner')
@click.option('--check', is_flag=True)
def main(version, aligner, check=False):
    aligner_map = {
        'A': 'BWA_ALN_0_5_7',
        'M': 'BWA_MEM_0_7_6A',
    }

    if aligner not in aligner_map.keys():
        raise Exception('Invalid aligner; choose A or M')

    aligner = aligner_map[aligner]

    config_path = os.path.join(os.environ['HEADNODE_AUTOMATION_DIR'], 'workflows/config/normal_config.json')
    config = file_utils.load_json(config_path)

    log.info('version: {}, aligner: {}'.format(version, aligner))

    analyses_to_run = get_analyses_to_run(version, aligner, check=check)

    for align_analysis in analyses_to_run['align']:
        log.info("Running align for {}".format(align_analysis))
        saltant_utils.run_align(align_analysis, version, config)

    # MAYBE: Add completed align analyses to hmmcopy analysis list
    
    for hmmcopy_analysis in analyses_to_run['hmmcopy']:
        log.info("Running hmmcopy for {}".format(hmmcopy_analysis))
        saltant_utils.run_hmmcopy(hmmcopy_analysis, version, config)

if __name__ == '__main__':
    main()

