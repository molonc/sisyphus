import os
import click
import subprocess

from dbclients.tantalus import TantalusApi

tantalus_api = TantalusApi()


@click.command()
@click.argument("data_dir")
@click.argument("library")
def download_data(data_dir, library):
    # init directory to download data to
    data_dir = os.path.join(data_dir, library)
    # check if destination path exists
    if not os.path.exists(data_dir):
        os.makedirs(data_dir)

    # init storage client
    storage_client = tantalus_api.get_storage_client("scrna_fastq")

    # list all blobs for library
    blobs = storage_client.list(library)

    for blob in blobs:
        # get flowcell from path
        flowcell = os.path.basename(os.path.dirname(blob))
        # get fastq filename
        filename = os.path.basename(blob)

        # join destination path with flowcell name and create path
        flowcell_path = os.path.join(data_dir, flowcell)
        if not os.path.exists(flowcell_path):
            os.makedirs(flowcell_path)

        # format filepath
        filepath = os.path.join(flowcell_path, filename)
        # check if file already exists with same size from blob storage
        if os.path.exists(filepath) and os.path.getsize(filepath) == storage_client.get_size(blob):
            continue

        # download blob to path
        print(f"downloading {blob} to {filepath}")
        #blob = storage_client.blob_service.get_blob_to_path(container_name="rnaseq", blob_name=blob, file_path=filepath)
        blob_client = storage_client.blob_service.get_blob_client("rnaseq", blob)
        with open(filepath, "wb") as my_blob:
            download_stream = blob_client.download_blob()
            my_blob.write(download_stream.readall())


if __name__ == "__main__":
    download_data()