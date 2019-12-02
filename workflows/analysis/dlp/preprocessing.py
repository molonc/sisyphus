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
    alignment_metrics = pd.read_csv(f, compression='gzip')

    # Filter cells marked as contaminated
    alignment_metrics = alignment_metrics[~alignment_metrics["is_contaminated"]]

    return set(alignment_metrics['cell_id'].values)


