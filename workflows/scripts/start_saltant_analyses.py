import os
import logging
import io
import yaml
import click
import time
import pandas as pd

import dbclients.tantalus
import dbclients.basicclient
import workflows.utils.saltant_utils
from datamanagement.utils.constants import LOGGING_FORMAT


def run_analysis(analysis, delay=None):
    analysis_id = analysis['id']
    analysis_status = analysis['status']
    jira_ticket = analysis['jira_ticket']

    if analysis_status in ('running', 'complete'):
        logging.info(f'skipping analysis {analysis_id} with status {analysis_status}')
        return

    if delay is not None:
        logging.info(f'sleeping for {delay}s')
        time.sleep(delay)

    logging.info(f'starting analysis {analysis_id} with status {analysis_status} from jira ticket {jira_ticket}')

    workflows.utils.saltant_utils.get_or_create_task_instance(
        'run_{}'.format(analysis_id),
        'andrew', # TODO: cli
        {
            'analysis_id': analysis_id,
            'jira': jira_ticket
        },
        14, # TODO: by name, cli
        'andrew-pseudobulk',
    ) # TODO: cli


@click.group()
def cli():
    pass


@cli.command()
@click.argument('jira_ticket_file')
@click.argument('analysis_type')
@click.option('--delay', type=int)
def from_table(jira_ticket_file, analysis_type, delay=None):
    tantalus_api = dbclients.tantalus.TantalusApi()

    analyses = pd.read_csv(jira_ticket_file)

    for jira_ticket in analyses['jira_id'].unique():
        try:
            analyses = tantalus_api.list('analysis', jira_ticket=jira_ticket, analysis_type__name=analysis_type)

        except dbclients.basicclient.NotFoundError:
            logging.error(f'unable to find analysis for jira ticket {jira_ticket}, analysis {analysis_type}')
            continue

        for analysis in analyses:
            try:
                run_analysis(analysis, delay=delay)

            except KeyboardInterrupt:
                raise

            except:
                logging.exception(f'start analysis failed for {analysis_id}')


@cli.command()
@click.argument('analysis_id', type=int)
@click.option('--delay', type=int)
def from_id(analysis_id, delay=None):
    tantalus_api = dbclients.tantalus.TantalusApi()

    analysis = tantalus_api.get('analysis', id=analysis_id)

    run_analysis(analysis, delay=delay)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
    cli()
