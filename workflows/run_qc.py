#!/usr/bin/env python
import os
import logging

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from workflows.analysis.dlp.alignment import AlignmentAnalysis
from workflows.analysis.dlp.annotation import AnnotationAnalysis
from workflows.analysis.dlp.hmmcopy import HMMCopyAnalysis
from workflows.utils import saltant_utils, file_utils

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)

tantalus_api = TantalusApi()
colossus_api = ColossusApi()


def main():
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

    # analysis classes for qc
    analysis_type_classes = {
        "align": AlignmentAnalysis,
        "hmmcopy": HMMCopyAnalysis,
        "annotation": AnnotationAnalysis,
    }

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
    for analysis_type in analysis_type_classes:
        # get list of analyses with status set to ready given the analysis type
        analyses = list(tantalus_api.list(
            'analysis',
            status="ready",
            analysis_type__name=analysis_type,
        ))

        # add to list of ready analyses
        ready_analyses += analyses

    for analysis in ready_analyses:
        # set boolean determining trigger of run
        is_ready = True

        # get analysis info
        analysis_type = analysis['analysis_type']
        jira_ticket = analysis['jira_ticket']
        library_id = analysis['args']['library_id']

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
                log.error(
                    f"a completed {required_analysis} analysis is required to run before {analysis_type} runs for {jira_ticket}"
                )
                is_ready = False
                continue

        if is_ready:
            # use create_from_args method from Analysis class to update input datasets and results
            analysis_type_classes[analysis_type].create_from_args(
                tantalus_api,
                jira_ticket,
                config["scp_version"],
                analysis["args"],
                update=True,
            )

            # run analysis on saltant
            log.info(f"running {analysis_type} on {library_id} using ticket {jira_ticket}")
            saltant_utils.run_analysis(
                analysis['id'],
                analysis_type,
                jira_ticket,
                config["scp_version"],
                library_id,
                config["default_aligner"],
                config,
            )


if __name__ == "__main__":
    main()