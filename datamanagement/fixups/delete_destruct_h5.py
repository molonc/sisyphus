import logging
import pandas as pd

import dbclients.tantalus
import dbclients.basicclient
from datamanagement.utils.constants import LOGGING_FORMAT

logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

tantalus_api = dbclients.tantalus.TantalusApi()
storage_client = tantalus_api.get_storage_client('singlecellresults')

pseudobulk_results = list(tantalus_api.list('resultsdataset', results_type='pseudobulk'))

for results in pseudobulk_results:
    file_resources = tantalus_api.get_dataset_file_resources(results['id'], 'resultsdataset', filters={'filename__endswith': '_destruct.h5'})
    for file_resource in file_resources:
        filename = file_resource['filename']
        url = storage_client.get_url(filename)
        if filename.endswith('_cell_counts_destruct.h5'):
            try:
                data = pd.read_csv(url, nrows=5)
            except UnicodeDecodeError:
                logging.info(f'file {filename} appears to be h5')
                continue
            if 'cluster_id' not in data.columns:
                raise ValueError(f'expected cluster_id in dataframe columns {data.columns}')
            logging.info(f'file {filename} appears to be counts csv')
        else:
            try:
                data = pd.read_csv(url, sep='\t', nrows=5)
            except UnicodeDecodeError:
                logging.info(f'file {filename} appears to be h5')
                continue
            if 'prediction_id' not in data.columns:
                raise ValueError(f'expected prediction_id in dataframe columns {data.columns}')
            logging.info(f'file {filename} appears to be destruct tsv')
        fixed_filename = filename.replace('_destruct.h5', '_destruct.csv.gz')
        try:
            fixed_file_resource = tantalus_api.get('file_resource', filename=fixed_filename, resultsdataset__id=results['id'])
        except dbclients.basicclient.NotFoundError:
            logging.info(f'did not find replacement file {fixed_filename}')
        else:
            logging.info(f'found replacement file {fixed_filename}')
        logging.info(f'deleting file {filename}')
        tantalus_api.delete_file(file_resource)
