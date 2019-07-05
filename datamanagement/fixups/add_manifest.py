import sys
import yaml
import io
import logging

import dbclients.tantalus
import dbclients.basicclient
from datamanagement.utils.constants import LOGGING_FORMAT

logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

logger = logging.getLogger("azure.storage")
logger.setLevel(logging.ERROR)


tantalus_api = dbclients.tantalus.TantalusApi()

analysis_dir_templates = {
    ('align', 'v0.2.10'): '{ticket_id}/results/results/alignment/',
    ('align', 'v0.2.11'): '{ticket_id}/results/results/alignment/',
    ('align', 'v0.2.13'): '{ticket_id}/results/results/alignment/',
    ('align', 'v0.2.15'): '{ticket_id}/results/results/alignment/',
    ('align', 'v0.2.19'): '{ticket_id}/results/results/alignment/',
    ('align', 'v0.2.20'): '{ticket_id}/results/results/alignment/',
    ('align', 'v0.2.7'): '{ticket_id}/results/results/alignment/',
    ('align', 'v0.2.9'): '{ticket_id}/results/results/alignment/',

    ('annotation', 'v0.2.25'): '{ticket_id}/results/results/QC/annotation/',

    ('hmmcopy', 'v0.0.0'): '{ticket_id}/results/results/hmmcopy_autoploidy/',
    ('hmmcopy', 'v0.1.5'): '{ticket_id}/results/results/hmmcopy_autoploidy/',
    ('hmmcopy', 'v0.2.10'): '{ticket_id}/results/results/hmmcopy_autoploidy/',
    ('hmmcopy', 'v0.2.11'): '{ticket_id}/results/results/hmmcopy_autoploidy/',
    ('hmmcopy', 'v0.2.15'): '{ticket_id}/results/results/hmmcopy_autoploidy/',
    ('hmmcopy', 'v0.2.19'): '{ticket_id}/results/results/hmmcopy_autoploidy/',
    ('hmmcopy', 'v0.2.20'): '{ticket_id}/results/results/hmmcopy_autoploidy/',
    ('hmmcopy', 'v0.2.7'): '{ticket_id}/results/results/hmmcopy_autoploidy/',
    ('hmmcopy', 'v0.2.9'): '{ticket_id}/results/results/hmmcopy_autoploidy/',

    ('pseudobulk', 'v0.2.11'): '{ticket_id}/results/',
    ('pseudobulk', 'v0.2.12'): '{ticket_id}/results/',
    ('pseudobulk', 'v0.2.13'): '{ticket_id}/results/',
    ('pseudobulk', 'v0.2.15'): '{ticket_id}/results/',
    ('pseudobulk', 'v0.2.20'): '{ticket_id}/results/',
    ('pseudobulk', 'v0.2.25'): '{ticket_id}/results/',
}


def get_pseudobulk_info(analysis):
    info = {}
    info['normal_sample'] = {}
    info['normal_sample']['sample_id'] = analysis['args']['matched_normal_sample']
    info['normal_sample']['library_id'] = analysis['args']['matched_normal_library']
    info['tumour_samples'] = []
    for dataset_id in analysis['input_datasets']:
        dataset = tantalus_api.get('sequencedataset', id=dataset_id)
        info['tumour_samples'].append({
            'sample_id': dataset['sample']['sample_id'],
            'library_id': dataset['library']['library_id'],
        })
    return info


client = tantalus_api.get_storage_client('singlecellblob_results')


for results in tantalus_api.list('results'):
    try:
        if (results['results_type'], results['results_version']) in analysis_dir_templates:
            if results['analysis'] is None:
                continue

            analysis = tantalus_api.get('analysis', id=results['analysis'])

            analysis_dir = analysis_dir_templates[results['results_type'], results['results_version']].format(
                ticket_id=analysis['jira_ticket'])

            manifest_filename = analysis_dir + 'manifest.yaml'
            manifest_filepath = tantalus_api.get_filepath('singlecellblob_results', manifest_filename)

            if client.exists(manifest_filename):
                logging.info(f'manifest {manifest_filename} exists')
                continue

            file_resources = tantalus_api.get_dataset_file_resources(
                results['id'], 'resultsdataset')
            filenames = [f['filename'] for f in file_resources]

            for filename in filenames:
                if not filename.startswith(analysis_dir):
                    logging.info(results['results_type'], results['results_version'])
                    logging.info(filenames[0])
                    raise

            manifest = {}
            if results['results_type'] in ('align', 'hmmcopy', 'annotation'):
                assert len(results['samples']) >= 1
                assert len(results['libraries']) == 1
                manifest['meta'] = {}
                manifest['meta']['type'] = results['results_type']
                manifest['meta']['version'] = results['results_version']
                manifest['meta']['sample_ids'] = [a['sample_id'] for a in results['samples']]
                manifest['meta']['library_id'] = results['libraries'][0]['library_id']
                manifest['filenames'] = filenames

            elif results['results_type'] == 'pseudobulk':
                assert len(results['samples']) >= 1
                assert len(results['libraries']) >= 1
                manifest['meta'] = {}
                manifest['meta']['type'] = results['results_type']
                manifest['meta']['version'] = results['results_version']
                manifest['meta'].update(get_pseudobulk_info(analysis))
                manifest['filenames'] = filenames

            manifest_io = io.BytesIO()
            manifest_io.write(yaml.dump(manifest, default_flow_style=False).encode())

            client.write_data(manifest_filename, manifest_io)

            tantalus_api.add_file('singlecellblob_results', manifest_filepath)

    except (ValueError, AssertionError, KeyError, dbclients.basicclient.FieldMismatchError):
        logging.exception(f'failed for {results["results_type"]}, {results["results_version"]}, {manifest_filename}')


