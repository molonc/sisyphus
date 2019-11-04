import click
import logging
import io
import itertools
import yaml
import sys
import pandas as pd

import dbclients.tantalus
import dbclients.basicclient
import datamanagement.transfer_files
from datamanagement.utils.constants import LOGGING_FORMAT


destruct_cols = [
    'prediction_id',
    'chromosome_1',
    'strand_1',
    'position_1',
    'chromosome_2',
    'strand_2',
    'position_2',
    'homology',
    'num_split',
    'inserted',
    'mate_score',
    'template_length_1',
    'log_cdf',
    'template_length_2',
    'log_likelihood',
    'template_length_min',
    'num_reads',
    'num_unique_reads',
    'type',
    'num_inserted',
    'sequence',
    'gene_id_1',
    'gene_name_1',
    'gene_location_1',
    'gene_id_2',
    'gene_name_2',
    'gene_location_2',
    'dgv_ids',
    'is_germline',
    'is_dgv',
    'num_patients',
    'is_filtered',
    'dist_filtered',
    'balanced',
    'rearrangement_type',
]

cell_counts_cols = [
    'cluster_id',
    'cell_id',
    'read_count',
]

destruct_yaml = '''columns:
- dtype: int
  name: prediction_id
- dtype: str
  name: chromosome_1
- dtype: str
  name: strand_1
- dtype: int
  name: position_1
- dtype: str
  name: chromosome_2
- dtype: str
  name: strand_2
- dtype: int
  name: position_2
- dtype: int
  name: homology
- dtype: int
  name: num_split
- dtype: str
  name: inserted
- dtype: float
  name: mate_score
- dtype: int
  name: template_length_1
- dtype: float
  name: log_cdf
- dtype: int
  name: template_length_2
- dtype: float
  name: log_likelihood
- dtype: int
  name: template_length_min
- dtype: int
  name: num_reads
- dtype: int
  name: num_unique_reads
- dtype: str
  name: type
- dtype: int
  name: num_inserted
- dtype: str
  name: sequence
- dtype: str
  name: gene_id_1
- dtype: str
  name: gene_name_1
- dtype: str
  name: gene_location_1
- dtype: str
  name: gene_id_2
- dtype: str
  name: gene_name_2
- dtype: str
  name: gene_location_2
- dtype: float
  name: dgv_ids
- dtype: bool
  name: is_germline
- dtype: bool
  name: is_dgv
- dtype: int
  name: num_patients
- dtype: bool
  name: is_filtered
- dtype: float
  name: dist_filtered
- dtype: bool
  name: balanced
- dtype: str
  name: rearrangement_type
header: false
sep: "\\t"
'''

destruct_library_yaml = '''columns:
- dtype: int
  name: prediction_id
- dtype: int
  name: num_reads
- dtype: int
  name: num_unique_reads
- dtype: str
  name: library
- dtype: bool
  name: is_normal
- dtype: float
  name: patient_id
header: false
sep: "\\t"
'''

cell_counts_yaml = '''columns:
- dtype: int
  name: cluster_id
- dtype: str
  name: cell_id
- dtype: int
  name: read_count
header: false
sep: ','
'''


@click.command()
@click.argument('storage_name')
@click.option('--ticket_id')
@click.option('--commit', is_flag=True)
def fix_destruct_header(storage_name, ticket_id=None):
    ''' Fix destruct headers
    '''
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    tantalus_api = dbclients.tantalus.TantalusApi()
    storage_client = tantalus_api.get_storage_client(storage_name)

    if ticket_id is not None:
        pseudobulk_results = list(tantalus_api.list('resultsdataset', results_type='pseudobulk', analysis__jira_ticket=ticket_id))
    else:
        pseudobulk_results = list(tantalus_api.list('resultsdataset', results_type='pseudobulk'))

    for results in pseudobulk_results:
        file_resources1 = tantalus_api.get_dataset_file_resources(results['id'], 'resultsdataset', filters={'filename__endswith': '_destruct.csv.gz.yaml'})
        file_resources2 = tantalus_api.get_dataset_file_resources(results['id'], 'resultsdataset', filters={'filename__endswith': '_destruct_library.csv.gz.yaml'})

        for file_resource in itertools.chain(file_resources1, file_resources2):
            filename = file_resource['filename']
            url = storage_client.get_url(filename)

            if filename.endswith('cell_counts_destruct.csv.gz.yaml'):
                yaml_text = cell_counts_yaml
                yaml_type = 'cell_counts_yaml'
                continue

            elif filename.endswith('destruct_library.csv.gz.yaml'):
                yaml_text = destruct_library_yaml
                yaml_type = 'destruct_library_yaml'
                continue

            else:
                yaml_text = destruct_yaml
                yaml_type = 'destruct_yaml'

            metadata = yaml.load(storage_client.open_file(filename))
            if '\t' in metadata['columns'][0]['name']:
                logging.info(f'file {filename} appears is corrupt')
                logging.info(f'replaceing {filename} contents with {yaml_type}')
                stream = io.BytesIO()
                stream.write(yaml_text.encode('utf-8'))
                storage_client.write_data(filename, stream)



if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    fix_destruct_header()

