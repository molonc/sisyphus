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
@click.option('--jira_ticket')
@click.option('--update', is_flag=True)
def add_counts(jira_ticket=None, update=False):
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    tantalus_api = dbclients.tantalus.TantalusApi()
    storage_client = tantalus_api.get_storage_client('singlecellresults')

    pseudobulk_results = list(tantalus_api.list('resultsdataset', results_type='pseudobulk', analysis__jira_ticket=jira_ticket))

    for results in pseudobulk_results:
        anno_file_resources = list(tantalus_api.get_dataset_file_resources(results['id'], 'resultsdataset', filters={'filename__endswith': '_snv_annotations.h5'}))

        for anno_file_resource in anno_file_resources:
            anno_filename = anno_file_resource['filename']
            counts_filename = anno_filename.replace('_snv_annotations.h5', '_snv_counts.h5')

            try:
                counts_file_resource = tantalus_api.get('file_resource', filename=counts_filename)
            except NotFoundError:
                counts_file_resource = None

            # if counts_file_resource is not None:
            #     continue

            logging.info(f'no file {counts_filename}')

            counts_filepath = os.path.join(storage_client.prefix + counts_filename)
            file_resource, file_instance = tantalus_api.add_file('singlecellresults', counts_filepath)

            results = tantalus_api.update('resultsdataset', id=results['id'], file_resources=list(results['file_resources']) + [file_resource['id']])

if __name__ == '__main__':
    add_counts()
