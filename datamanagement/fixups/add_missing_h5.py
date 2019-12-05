import os
import logging
import io
import itertools
import yaml
import click
import pandas as pd

import dbclients.tantalus
import dbclients.basicclient
import datamanagement.transfer_files
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.basicclient import NotFoundError


@click.command()
@click.argument('jira_ticket')
@click.option('--dry_run', is_flag=True)
def add_h5(jira_ticket, dry_run=False):
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    tantalus_api = dbclients.tantalus.TantalusApi()
    storage_client = tantalus_api.get_storage_client('singlecellresults')

    for blob_name in storage_client.list(jira_ticket):
        if blob_name.endswith('_snv_annotations.h5') or blob_name.endswith('_snv_counts.h5'):
            blob_filepath = os.path.join(storage_client.prefix, blob_name)

            logging.info(f'adding file {blob_filepath}')

            if not dry_run:
                file_resource, file_instance = tantalus_api.add_file('singlecellresults', blob_filepath)
                results = tantalus_api.get('resultsdataset', results_type='pseudobulk', analysis__jira_ticket=jira_ticket)
                results = tantalus_api.update('resultsdataset', id=results['id'], file_resources=list(results['file_resources']) + [file_resource['id']])


if __name__ == '__main__':
    add_h5()
