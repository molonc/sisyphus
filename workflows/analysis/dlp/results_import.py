import os
import yaml
import logging

from workflows.scripts.low_complexity_filter import filter_reads
import gzip
from io import BytesIO
#import pandas as pd

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

def filter_low_complexity_region(
    tantalus_api,
    results_dir,
    library_id,
    storage_name,
    ):
    """
    Filter "low complexity region" in reads.csv file
    """
    blacklist_file = '/home/dmin/blacklist_2018.10.23.txt'

    storage_client = tantalus_api.get_storage_client(storage_name)

    reads_filename = os.path.join(results_dir, "_".join([library_id, "reads.csv.gz"]))
    blob_client = storage_client.blob_service.get_blob_client(storage_client.storage_container, reads_filename)
    reads_data_raw = blob_client.download_blob()
    reads_bytes_io = BytesIO(reads_data_raw.content_as_bytes())

    with gzip.open(reads_bytes_io) as reads_file:
        filtered_masked_df, filtered_mseg_df = filter_reads(reads_file, blacklist_file)

    filtered_mseg_out = filtered_mseg_df.to_csv(index=False, encoding='utf-8')
    filtered_masked_out = filtered_masked_df.to_csv(index=False, encoding="utf-8")

    # gzip output
    gzipped_filtered_mseg = gzip.compress(bytes(filtered_mseg_out, 'utf-8'))
    gzipped_filtered_masked = gzip.compress(bytes(filtered_masked_out, 'utf-8'))

    filtered_mseg_blobname = os.path.join(results_dir, "_".join([library_id, "segments_filtered.csv.gz"]))
    filtered_masked_blobname = os.path.join(results_dir, "_".join([library_id, "reads_filtered.csv.gz"]))

    # upload to Azure
    storage_client.write_data_raw(filtered_mseg_blobname, gzipped_filtered_mseg)
    storage_client.write_data_raw(filtered_masked_blobname, gzipped_filtered_masked)

    # update metadata entries
    metadata_filename = os.path.join(results_dir, "metadata.yaml")
    metadata = yaml.safe_load(storage_client.open_file(metadata_filename))

    filenames_to_add = [os.path.basename(filtered_mseg_blobname), os.path.basename(filtered_masked_blobname)]
    metadata["filenames"].extend(filenames_to_add)
    stream = yaml.dump(metadata)
    storage_client.write_data_raw(metadata_filename, stream)
