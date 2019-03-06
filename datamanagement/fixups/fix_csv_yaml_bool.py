import itertools
import logging
import click
import os
import pandas as pd

from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi


remote_storage_name = "singlecellblob_results"


if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    tantalus_api = TantalusApi()

    remote_storage_client = tantalus_api.get_storage_client(remote_storage_name)

    file_resources = tantalus_api.list(
        'file_resource',
        filename__endswith='.csv.gz.yaml',
    )

    for file_resource in file_resources:
        file_instance = tantalus_api.get_file_instance(file_resource, remote_storage_name)

        url = remote_storage_client.get_url(file_resource['filename'])

        # Read the yaml from the url, replace 'boolean' with 'bool'

        remote_storage_client.write_data()

        tantalus_api.update_file(file_instance)

