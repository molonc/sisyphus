import logging
import packaging.version
import pandas as pd


def get_passed_cell_ids(tantalus_api, results_id, storage_name):
    # Find the metrics file in the annotation results
    results = tantalus_api.get('results', id=results_id)
    assert len(results['libraries']) == 1
    library_id = results['libraries'][0]['library_id']
    file_instances = tantalus_api.get_dataset_file_instances(
        results_id, 'resultsdataset', storage_name,
        filters={'filename__endswith': f'{library_id}_metrics.csv.gz'})
    assert len(file_instances) == 1
    file_instance = file_instances[0]

    storage_client = tantalus_api.get_storage_client(file_instance['storage']['name'])
    f = storage_client.open_file(file_instance['file_resource']['filename'])
    data = pd.read_csv(f, compression='gzip')

    # Recalculate the is_contaminated flag for results prior to v0.5.17
    assert results['results_type'] == 'annotation'
    annotation_version = results['results_version']
    if packaging.version.parse(annotation_version) < packaging.version.parse('v0.5.17') or annotation_version in ('v0.6.3', 'v0.6.4'):
        logging.info(f'recalculating is_contaminated for annotation results version {annotation_version}')

        data['fastqscreen_grch37_exclusive'] = data['fastqscreen_grch37'] - data['fastqscreen_grch37_multihit']
        data['fastqscreen_mm10_exclusive'] = data['fastqscreen_mm10'] - data['fastqscreen_mm10_multihit']
        data['fastqscreen_salmon_exclusive'] = data['fastqscreen_salmon'] - data['fastqscreen_salmon_multihit']

        data['proportion_grch37'] = data['fastqscreen_grch37_exclusive'] / data['total_reads']
        data['proportion_mm10'] = data['fastqscreen_mm10_exclusive'] / data['total_reads']
        data['proportion_salmon'] = data['fastqscreen_salmon_exclusive'] / data['total_reads']

        data['is_contaminated'] = (
            (data['proportion_mm10'] > 0.05) |
            (data['proportion_salmon'] > 0.05)
        )

    else:
        logging.info(f'using existing is_contaminated for annotation results version {annotation_version}')

    # Filter cells marked as contaminated
    data = data[~data['is_contaminated']]

    return set(data['cell_id'].values)


