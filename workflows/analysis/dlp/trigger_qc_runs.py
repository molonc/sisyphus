#!/usr/bin/env python
import os
import re
import sys
import time
import yaml
import click
import logging

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from workflows import get_analyses
from workflows.utils import saltant_utils, file_utils

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

def create_analysis_from_library(library_id):
    config = file_utils.load_json(config_filename)

    # taxonomy id map
    taxonomy_id_map = {
        '9606': 'HG19',
        '10090': 'MM10',
    }

    # create ticket
    jira_ticket = get_analyses.create_analysis_jira_ticket(library_id)

    # get library info from colossus
    library = colossus_api.get('library', pool_id=library_id)
    taxonomy_id = library['sample']['taxonomy_id']

    args = {}
    args['library_id'] = library_id
    args['aligner'] = "BWA_MEM_0_7_6A"
    args['ref_genome'] = taxonomy_id_map[taxonomy_id]
    args['gsc_lanes'] = None
    args['brc_flowcell_ids'] = None

    alignment.create_analysis(jira_ticket, version, args)

    # delete arguments not needed for hmmcopy and annotation
    del args['gsc_lanes']
    del args['brc_flowcell_ids']

    hmmcopy.create_analysis(jira_ticket, version, args)
    annotation.create_analysis(jira_ticket, version, args)

# TODO: find new method to get unanalyzed data
# or maybe no necessary if after every new import, the analyses are already created and set to ready


@click.command()
@click.argument('version')
@click.argument('aligner')
@click.argument('--override_contamination', is_flag=True))
def main(version, aligner, override_contamination=True):
    # analysis types for qc
    analysis_types = ['align', 'hmmcopy', 'annotation']

    # map of type of analyses required before particular analysis can run
    required_analyses_map = {
        'align': [],
        'hmmcopy': ['align'],
        'annotation': [
            'align',
            'hmmcopy',
        ],
    }

    # collect qc analysis objects with ready status
    ready_analyses = []
    for analysis_type in analysis_types:
        # get list of analyses with status set to ready given the analysis type
        analyses = list(tantalus_api.list(
            'analysis',
            status="ready",
            analysis_type__name=analysis_type,
        ))

        # add to list of ready analyses
        ready_analyses += analyses

    for analysis in ready_analyses:
        analysis_type = analysis['analysis_type']
        jira_ticket = analysis['jira_ticket']
        library_id = analysis_type['args']['library_id']
        # check if the required analysis exist and is complete
        for required_analysis in required_analyses_map[analysis_type]:
            # get analysis name
            required_analysis_name = analysis['name'].replace(
                analysis_type,
                required_analysis,
            )

            # check if complete analysis exists
            try:
                analysis = tantalus_api.get('analysis', name=required_analysis_name, status='complete')
            # skip analysis if required analysis doesn't exist
            except:
                logging.error(
                    f"a completed {required_analysis} analysis is required to run before {analysis_type} for {jira_ticket}"
                )
                continue

        

        # run analysis on saltant
        saltant_utils.run_analysis(
            analysis['id'],
            analysis_type,
            jira_ticket,
            version,
            library_id,
            aligner,
            config,
            override_contamination=override_contamination,
        )

    