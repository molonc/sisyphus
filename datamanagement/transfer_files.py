#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import datetime
import logging
import os
import subprocess
import sys
import time
import traceback
from azure.storage.blob import BlockBlobService, ContainerPermissions
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi
from datamanagement.utils.utils import make_dirs
import click


# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stdout, level=logging.INFO)

# Configure the azure.storage logger.
# For more verbose output as required by microsoft set loglevel
# to DEBUG
logger = logging.getLogger("azure.storage")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)-20s %(levelname)-5s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)


class DataCorruptionError(Exception):
    """An error when corrupt data is found.

    Raised when MD5 calculated does not match the saved database md5 for
    the file resource.
    """

    pass


class FileDoesNotExist(Exception):
    """An error for when a file can't be found.

    Raised when the file does not actually exist, although there is a
    FileInstance object in the database that says the file exists.
    """

    pass


class FileAlreadyExists(Exception):
    """An error for when a file already exists.

    Raised when the file already exists, although there is no
    FileInstance object in the database that says the file exists.
    """

    pass


def _as_gb(num_bytes):
    return round(num_bytes / (1024.0 * 1024.0 * 1024.0), 2)


class TransferProgress(object):
    def __init__(self):
        self._start = time.time()
        self._interval = 10
        self._last_print = self._start - self._interval * 2

    def print_progress(self, current, total):
        current_time = time.time()
        if current_time < self._last_print + self._interval:
            return
        self._last_print = current_time
        elapsed = current_time - self._start
        percent = "NA"
        if total > 0:
            percent = "{:.2f}".format(100.0 * float(current) / total)

        logging.info(
            "{}/{} ({}%) in {}s".format(
                _as_gb(current), _as_gb(total), percent, elapsed
            )
        )


def _check_file_same_blob(block_blob_service, file_resource, container, blobname):
    properties = block_blob_service.get_blob_properties(container, blobname)
    blobsize = properties.properties.content_length
    if file_resource["size"] != blobsize:
        return False
    return True


class AzureBlobServerDownload(object):
    """ Blob Download class.

    Note: this class works with a tantalus storage or directory as destination.
    """

    def __init__(self, tantalus_api, from_storage, to_storage_name, to_storage_prefix):
        self.block_blob_service = tantalus_api.get_storage_client(from_storage["name"]).blob_service
        self.from_storage = from_storage
        self.to_storage_name = to_storage_name
        self.to_storage_prefix = to_storage_prefix

    def download_from_blob(self, file_instance):
        """ Transfer a file from blob to a server.

        This should be called on the from server.
        """

        file_resource = file_instance["file_resource"]

        cloud_filepath = file_instance["filepath"]
        if not cloud_filepath.startswith(self.from_storage["prefix"]):
            raise Exception("{} does not have storage prefix {}".format(
                cloud_filepath, self.from_storage["prefix"]))

        cloud_blobname = file_resource["filename"]
        cloud_container = self.from_storage["storage_container"]

        local_filepath = os.path.join(self.to_storage_prefix, file_resource["filename"])

        make_dirs(os.path.dirname(local_filepath))

        if not self.block_blob_service.exists(cloud_container, cloud_blobname):
            error_message = "source file {filepath} does not exist on {storage} for file instance with pk: {pk}".format(
                filepath=cloud_filepath,
                storage=file_instance["storage"]["name"],
                pk=file_instance["id"],
            )
            raise FileDoesNotExist(error_message)

        if os.path.isfile(local_filepath):
            if _check_file_same_blob(
                self.block_blob_service,
                file_resource, cloud_container, cloud_blobname,
            ):
                logging.info(
                    "skipping transfer of file resource {} that matches existing file".format(
                        file_resource["filename"]))
                return

            error_message = "target file {filepath} already exists on {storage}".format(
                filepath=local_filepath, storage=self.to_storage_name
            )
            raise FileAlreadyExists(error_message)

        self.block_blob_service.get_blob_to_path(
            cloud_container,
            cloud_blobname,
            local_filepath,
            progress_callback=TransferProgress().print_progress,
            max_connections=16,
        )

        os.chmod(local_filepath, 0o444)


class AzureBlobServerUpload(object):
    """ Blob Upload class.

    Note: this class only works with tantalus storage as destination.
    """

    def __init__(self, tantalus_api, to_storage):
        self.block_blob_service = tantalus_api.get_storage_client(self.to_storage["name"]).blob_service
        self.to_storage = to_storage

    def upload_to_blob(self, file_instance):
        """Transfer a file from a server to blob.

        This should be called on the from server.
        """
        file_resource = file_instance["file_resource"]

        local_filepath = file_instance["filepath"]

        cloud_filepath = os.path.join(self.to_storage["prefix"], file_resource["filename"])
        if not cloud_filepath.startswith(self.to_storage["prefix"]):
            raise Exception("{} does not have storage prefix {}".format(
                cloud_filepath, self.to_storage["prefix"]))

        cloud_blobname = file_resource["filename"]
        cloud_container = self.to_storage["storage_container"]

        if not os.path.isfile(local_filepath):
            error_message = "source file {filepath} does not exist on {storage} for file instance with pk: {pk}".format(
                filepath=local_filepath,
                storage=file_instance["storage"]["name"],
                pk=file_instance["id"],
            )
            raise FileDoesNotExist(error_message)

        if self.block_blob_service.exists(cloud_container, cloud_blobname):
            if _check_file_same_blob(
                self.block_blob_service,
                file_resource, cloud_container, cloud_blobname,
            ):
                logging.info(
                    "skipping transfer of file resource {} that matches existing file".format(
                        file_resource["filename"]))
                return

            error_message = "target file {filepath} already exists on {storage}".format(
                filepath=cloud_filepath, storage=self.to_storage["name"]
            )
            raise FileAlreadyExists(error_message)

        self.block_blob_service.create_blob_from_path(
            cloud_container,
            cloud_blobname,
            local_filepath,
            progress_callback=TransferProgress().print_progress,
            max_connections=16,
            timeout=10 * 60 * 64,
        )


def blob_to_blob_transfer_closure(tantalus_api, source_storage, destination_storage):
    """Returns a function for transfering blobs between Azure containers.

    Note that this will *not* create new containers that don't already
    exist. This is a useful note because for development the container
    names are changed to "{container name}-test", and these "test
    containers" are unlikely to exist.
    """
    # Start BlockBlobService for source and destination accounts
    source_account = tantalus_api.get_storage_client(
        source_storage).blob_service
    destination_account = tantalus_api.get_storage_client(
        destination_storage).blob_service

    # Get a shared access signature for the source account so that we
    # can read its private files
    shared_access_sig = source_account.generate_container_shared_access_signature(
        container_name=source_storage["storage_container"],
        permission=ContainerPermissions.READ,
        expiry=(datetime.datetime.utcnow() + datetime.timedelta(hours=200)),
    )

    def transfer_function(source_file):
        """Transfer function aware of source and destination Azure storages.

        Using non-local source_account and destination_account. This
        isn't Python 3, so no nonlocal keyword :(
        """
        # Copypasta validation from AzureTransfer.download_from_blob
        source_filepath = source_file["filepath"]
        source_container, blobname = source_filepath.split("/", 1)
        assert source_container == source_file["storage"]["storage_container"]

        if not source_account.exists(source_container, blobname):
            error_message = "source file {filepath} does not exist on {storage} for file instance with pk: {pk}".format(
                filepath=source_filepath,
                storage=source_file["storage"]["name"],
                pk=source_file["id"],
            )
            raise FileDoesNotExist(error_message)

        # Copypasta validation from AzureTransfer.upload_to_blob
        if destination_account.exists(
            destination_storage["storage_container"], blobname
        ):
            # Check if the file already exist. If the file does already
            # exist, don't re-transfer this file. If the file does exist
            # but has a different size, then raise an exception.

            # Size check
            destination_blob_size = destination_account.get_blob_properties(
                container_name=destination_storage["storage_container"],
                blob_name=blobname,
            )

            if source_file["size"] == destination_blob_size:
                # Don't retransfer
                logging.info(
                    "skipping transfer of file resource {} that matches existing file".format(
                        source_file["filename"]))
                return
            else:
                # Raise an exception and report that a blob with this
                # name already exists!
                error_message = "target filepath {filepath} already exists on {storage} but with different filesize".format(
                    filepath=source_filepath,
                    storage=destination_storage["storage_account"],
                )
                raise FileAlreadyExists(error_message)

        # Finally, transfer the file between the blobs
        source_sas_url = source_account.make_blob_url(
            container_name=source_file["storage"]["storage_container"],
            blob_name=blobname,
            sas_token=shared_access_sig,
        )

        destination_account.copy_blob(
            container_name=destination_storage["storage_container"],
            blob_name=blobname,
            copy_source=source_sas_url,
        )

    # Return the transfer function
    return transfer_function


def check_file_same_local(file_resource, filepath):
    # TODO: define 'size' for folder
    if file_resource["is_folder"]:
        return True

    if file_resource["size"] != os.path.getsize(filepath):
        return False

    return True


class RsyncTransfer(object):
    """ Blob Upload class.

    Note: this class works with a tantalus storage or directory as destination.
    """

    def __init__(self, tantalus_api, to_storage_name, to_storage_prefix, to_storage_server_ip):
        self.to_storage = to_storage
        self.to_storage_name = to_storage_name
        self.to_storage_prefix = to_storage_prefix
        self.to_storage_server_ip = to_storage_server_ip

    def rsync_file(file_instance):
        """ Rsync a single file from one storage to another
        """

        file_resource = file_instance["file_resource"]

        local_filepath = os.path.join(self.to_storage_prefix, file_resource["filename"])

        remote_filepath = file_instance["filepath"]

        if file_instance["file_resource"]["is_folder"]:
            local_filepath = local_filepath + "/"
            remote_filepath = remote_filepath + "/"

        if os.path.isfile(local_filepath):
            if check_file_same_local(file_instance["file_resource"], local_filepath):
                logging.info(
                    "skipping transfer of file resource {} that matches existing file".format(
                        source_file["filename"]))
                return

            error_message = "target file {filepath} already exists on {storage} with different size".format(
                filepath=local_filepath, storage=self.to_storage_name)

            raise FileAlreadyExists(error_message)

        if file_instance["storage"]["server_ip"] == self.to_storage_server_ip:
            remote_location = remote_filepath
        else:
            remote_location = file_instance["storage"]["server_ip"] + ":" + remote_filepath

        make_dirs(os.path.dirname(local_filepath))

        subprocess_cmd = [
            "rsync",
            "--progress",
            # '--info=progress2',
            "--chmod=D555",
            "--chmod=F444",
            "--times",
            "--copy-links",
            remote_location,
            local_filepath,
        ]

        if file_instance["file_resource"]["is_folder"]:
            subprocess_cmd.insert(1, "-r")

        sys.stdout.flush()
        sys.stderr.flush()
        subprocess.check_call(subprocess_cmd, stdout=sys.stdout, stderr=sys.stderr)

        if not check_file_same_local(file_instance["file_resource"], local_filepath):
            error_message = "transfer to {filepath} on {storage} failed".format(
                filepath=local_filepath, storage=self.to_storage_name
            )
            raise Exception(error_message)


def get_file_transfer_function(tantalus_api, from_storage, to_storage):
    if from_storage["storage_type"] == "blob" and to_storage["storage_type"] == "blob":
        return blob_to_blob_transfer_closure(tantalus_api, from_storage, to_storage)

    elif from_storage["storage_type"] == "server" and to_storage["storage_type"] == "blob":
        return AzureBlobServerUpload(
            tantalus_api, to_storage).upload_to_blob

    elif from_storage["storage_type"] == "blob" and to_storage["storage_type"] == "server":
        return AzureBlobServerDownload(
            tantalus_api, from_storage, to_storage["name"], to_storage["prefix"]).download_from_blob

    elif from_storage["storage_type"] == "server" and to_storage["storage_type"] == "server":
        return RsyncTransfer(
            tantalus_api, to_storage["name"], to_storage["prefix"], to_storage["server_ip"]).rsync_file


def get_cache_function(tantalus_api, from_storage, cache_directory):
    if from_storage["storage_type"] == "blob":
        return AzureBlobServerDownload(
            tantalus_api, from_storage, 'localcache', cache_directory).download_from_blob

    elif from_storage["storage_type"] == "server":
        return RsyncTransfer(
            tantalus_api, 'localcache', cache_directory, to_storage_server_ip).rsync_file

    else:
        raise ValueError('unaccepted storage type {}'.format(from_storage["storage_type"]))


@click.group()
def cli():
    pass


@cli.command()
@click.argument("tag_name")
@click.argument("from_storage_name")
@click.argument("to_storage_name")
def transfer_tagged_datasets(tag_name, from_storage_name, to_storage_name):
    """ Transfer a set of tagged datasets
    """

    tantalus_api = TantalusApi()

    tag = tantalus_api.get("tag", name=tag_name)

    for dataset_id in tag['sequencedataset_set']:
        transfer_dataset(tantalus_api, dataset_id, "sequencedataset", from_storage_name, to_storage_name)

    for dataset_id in tag['resultsdataset_set']:
        transfer_dataset(tantalus_api, dataset_id, "resultsdataset", from_storage_name, to_storage_name)


@cli.command()
@click.argument("tag_name")
@click.argument("from_storage_name")
@click.argument("cache_directory")
def cache_tagged_datasets(tag_name, from_storage_name, cache_directory):
    """ Cache a set of tagged datasets
    """

    tantalus_api = TantalusApi()

    tag = tantalus_api.get("tag", name=tag_name)

    for dataset_id in tag['sequencedataset_set']:
        cache_dataset(tantalus_api, dataset_id, "sequencedataset", from_storage_name, cache_directory)

    for dataset_id in tag['resultsdataset_set']:
        cache_dataset(tantalus_api, dataset_id, "resultsdataset", from_storage_name, cache_directory)


RETRIES = 3
def _transfer_files_with_retry(f_transfer, file_instance):
    for retry in range(RETRIES):
        try:
            f_transfer(file_instance)
            break
        except Exception as e:
            logging.error("Transfer failed. Retrying.")

            if retry < RETRIES - 1:
                traceback.print_exc()
            else:
                logging.error("Failed all retry attempts")
                raise


def transfer_dataset(tantalus_api, dataset_id, dataset_model, from_storage_name, to_storage_name):
    """ Transfer a dataset
    """
    assert dataset_model in ("sequencedataset", "resultsdataset")

    dataset = tantalus_api.get(dataset_model, id=dataset_id)

    to_storage = tantalus_api.get("storage", name=to_storage_name)
    from_storage = tantalus_api.get("storage", name=from_storage_name)

    f_transfer = get_file_transfer_function(tantalus_api, from_storage, to_storage)

    file_instances = tantalus_api.get_dataset_file_instances(dataset_id, dataset_model, from_storage_name)

    for file_instance in file_instances:
        file_resource = file_instance["file_resource"]
        other_file_instances = file_resource["file_instances"]

        storage_names = set([f["storage"]["name"] for f in other_file_instances])

        if to_storage["name"] in storage_names:
            logging.info(
                "skipping file resource {} that already exists on storage {}".format(
                    file_resource["filename"], to_storage["name"]
                )
            )
            continue

        logging.info(
            "starting transfer {} to {}".format(
                file_resource["filename"], to_storage["name"]))

        _transfer_files_with_retry(f_transfer, file_instance)

        tantalus_api.add_instance(file_resource, to_storage)


def cache_dataset(tantalus_api, dataset_id, dataset_model, from_storage_name, cache_directory):
    """ Cache a dataset
    """
    assert dataset_model in ("sequencedataset", "resultsdataset")

    dataset = tantalus_api.get(dataset_model, id=dataset_id)

    from_storage = tantalus_api.get("storage", name=from_storage_name)

    f_transfer = get_cache_function(tantalus_api, from_storage, cache_directory)

    file_instances = tantalus_api.get_dataset_file_instances(dataset_id, dataset_model, from_storage_name)

    for file_instance in file_instances:
        logging.info(
            "starting caching {} to {}".format(
                file_instance["file_resource"]["filename"], cache_directory))

        _transfer_files_with_retry(f_transfer, file_instance)


if __name__ == "__main__":
    cli()
