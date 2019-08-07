import itertools
import logging
import click
import os
import errno
import io
import yaml
import sys
import click
import pandas as pd

from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi
from dbclients.tantalus import BlobStorageClient

remote_storage_name = "singlecellresults"
PATTERN = ".csv.gz.yaml"


@click.command()
@click.option("--dry_run", is_flag=True)
def edit_yaml(dry_run=False):

    file_resources = tantalus_api.list("file_resource", filename__endswith=PATTERN)
    os.chdir("/Users/miyuen/fix_csv_tmp/")
    flags = os.O_CREAT | os.O_EXCL

    for file_resource in file_resources:
        stream = io.BytesIO()
        edited = False
        f_path = os.path.join("/singlecelldata/results", file_resource["filename"])

        sentinel_file = "_".join(file_resource["filename"].split("/"))
        if os.path.exists(sentinel_file):
            continue
        else:
            file_instance = tantalus_api.get_file_instance(
                file_resource, remote_storage_name
            )
            yaml_file = blob_storage_client.open_file(file_resource["filename"])
            file = yaml.safe_load(yaml_file)

            if file.keys() != ["header", "columns"]:
                logging.info("Formatting error: {}".format(f_path))
                continue
            for dtype in file["columns"]:
                if dtype["dtype"] == "boolean":
                    dtype["dtype"] = "bool"
                    edited = True
            if (not dry_run) and edited:
                stream.write(yaml.dump(file, default_flow_style=False))
                blob_storage_client.write_data(file_resource["filename"], stream)
                tantalus_api.update_file(file_instance)
                #exit()
            with open(sentinel_file, "w"):
                pass

if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)

    tantalus_api = TantalusApi()
    blob_storage_client = BlobStorageClient(
        "singlecelldata", "results", "singlecelldata/results"
    )

    edit_yaml()
