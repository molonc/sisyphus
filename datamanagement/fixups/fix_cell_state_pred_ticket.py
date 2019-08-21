import sys
import yaml
import io
import logging
import click
import itertools

import dbclients.tantalus
import dbclients.basicclient
from datamanagement.utils.constants import LOGGING_FORMAT


@click.command()
@click.option('--jira_ticket')
@click.option('--dry_run')
def fix(jira_ticket=None, dry_run=False):
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    logger = logging.getLogger("azure.storage")
    logger.setLevel(logging.ERROR)

    tantalus_api = dbclients.tantalus.TantalusApi()

    if jira_ticket is not None:
        hmmcopy_results = tantalus_api.get('results', results_type='hmmcopy', analysis__jira_ticket=jira_ticket)
        analysis_iter = tantalus_api.list('analysis', analysis_type__name='cell_state_classifier', input_results__id=hmmcopy_results['id'])

    else:
        analysis_iter = tantalus_api.list('analysis', analysis_type__name='cell_state_classifier')

    for analysis in analysis_iter:
        if not len(analysis['input_results']) == 1:
            logging.error(f'analsysis {analysis["id"]} has {len(analysis["input_results"])} input results')
            continue

        hmmcopy_results = tantalus_api.get('results', id=analysis['input_results'][0])
        hmmcopy_analysis = tantalus_api.get('analysis', id=hmmcopy_results['analysis'])

        if analysis['jira_ticket'] == hmmcopy_analysis['jira_ticket']:
            logging.info(f'cell cycle analsysis {analysis["id"]}, {analysis["name"]} already has jira ticket {hmmcopy_analysis["jira_ticket"]}')
            continue

        logging.info(f'updating cell cycle analsysis {analysis["id"]}, {analysis["name"]} to have jira ticket {hmmcopy_analysis["jira_ticket"]}')
        if not dry_run:
            tantalus_api.update('analysis', id=analysis['id'], jira_ticket=hmmcopy_analysis['jira_ticket'])


if __name__ == '__main__':
    fix()
