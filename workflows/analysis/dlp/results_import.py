import os
import yaml
import logging


qc_results_name_template = '{jira_ticket}_{analysis_type}_{library_id}'

pseudobulk_results_name_template = '{jira_ticket}_{analysis_type}_{library_id}_{sample_id}'


def create_dlp_results(
        tantalus_api,
        results_dir,
        analysis_id,
        name,
        samples,
        libraries,
        storage_name,
        update=False,
        skip_missing=False,
    ):
    logging.info('Searching for existing results {}'.format(name))
    storage_client = tantalus_api.get_storage_client(storage_name)

    # Load the metadata.yaml file, assumed to exist in the root of the results directory
    metadata_filename = os.path.join(results_dir, "metadata.yaml")
    metadata = yaml.safe_load(storage_client.open_file(metadata_filename))

    # Add all files to tantalus including the metadata.yaml file
    file_resource_ids = set()
    for filename in metadata["filenames"] + ['metadata.yaml']:
        filename = os.path.join(results_dir, filename)
        filepath = os.path.join(storage_client.prefix, filename)

        if not storage_client.exists(filename) and skip_missing:
            logging.warning('skipping missing file: {}'.format(filename))
            continue

        file_resource, _ = tantalus_api.add_file(
            storage_name=storage_name,
            filepath=filepath,
            update=update,
        )

        file_resource_ids.add(file_resource["id"])

    data = {
        'name': name,
        'results_type': metadata["meta"]["type"],
        'results_version': f"v{metadata['meta']['version']}",
        'analysis': analysis_id,
        'file_resources': list(file_resource_ids),
        'samples': samples,
        'libraries': libraries,
    }

    keys = [
        'name',
        'results_type',
    ]

    results, _ = tantalus_api.create('results', data, keys, get_existing=True, do_update=update)

    return results

