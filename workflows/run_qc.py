#!/usr/bin/env python
import os
import logging
from datetime import date, timedelta

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

from workflows.analysis.dlp.alignment import AlignmentAnalysis
from workflows.analysis.dlp.annotation import AnnotationAnalysis
from workflows.analysis.dlp.hmmcopy import HMMCopyAnalysis
from workflows.utils import saltant_utils, file_utils, tantalus_utils
from workflows.tantalus_utils import create_qc_analyses_from_library

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

    # map of type of analyses required before particular analysis can run
    required_analyses_map = {
        'annotation': [
            'align',
            'hmmcopy',
        ],
        'hmmcopy': ['align'],
        'align': [],
    }

    # get colossus analysis information objects with status last updated this year
    analyses = colossus_api.list(
        "analysis_information",
        analysis_run__last_updated_0="2020-01-01",
        analysis_run__last_updated_1=datetime.date.today() + timedelta(days=1),
        aligner=config["default_aligner"],
    )

    analyses_to_create = []
    for analysis in analyses:
        # skip analysis if marked as complete
        status = analysis["analysis_run"]["run_status"]
        if status == "complete":
            continue

        jira_ticket = analysis["analysis_jira_ticket"]

        for analysis_type in required_analyses_map:
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
                    continue

                # otherwise run analysis
                saltant_utils.run_analysis(
                    tantalus_analysis['id'],
                    analysis_type,
                    jira_ticket,
                    config["scp_version"],
                    library_id,
                    config["default_aligner"],
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
                    create_qc_analyses_from_library(
                        analysis["library"]["pool_id"],
                        jira_ticket,
                        version,
                        analysis_type,
                    )
                    tantalus_analysis = tantalus_api.get(
                        'analysis',
                        jira_ticket=jira_ticket,
                        analysis_type__name=analysis_type,
                    )

                    saltant_utils.run_analysis(
                        tantalus_analysis['id'],
                        analysis_type,
                        jira_ticket,
                        config["scp_version"],
                        library_id,
                        config["default_aligner"],
                        config,
                    )


if __name__ == "__main__":
    main()