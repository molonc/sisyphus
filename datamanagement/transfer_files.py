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
import shutil
import tempfile
from azure.storage.blob import BlockBlobService, ContainerPermissions
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi, NotFoundError
from datamanagement.utils.utils import make_dirs
import click



# Configure the azure.storage logger.
# For more verbose output as required by microsoft set loglevel
# to DEBUG
logger = logging.getLogger("azure.storage")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)-20s %(levelname)-5s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.ERROR)

# Use azcopy if we have it
if shutil.which('azcopy') is not None:
    azcopy = True
else:
    azcopy = False


def run_azcopy(src, dest):
    with tempfile.TemporaryDirectory() as azcopy_temp:
        os.environ['AZCOPY_LOG_LOCATION'] = azcopy_temp
        os.environ['AZCOPY_JOB_PLAN_LOCATION'] = azcopy_temp
        with open(os.devnull, 'w') as devnull:
            subprocess.check_call(
                ['azcopy', 'copy', '--log-level', 'NONE', src, dest],
                stdout=devnull)


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
        logging.info(
            "blob {} in container {} has size {} which mismatches recorded size {} for {} in tantalus".format(
                blobname, container, blobsize, file_resource["size"], file_resource["filename"]))
        return False
    return True


def _check_file_same_local(file_resource, filepath):
    # TODO: define 'size' for folder
    if file_resource["is_folder"]:
        return True
    filesize = os.path.getsize(filepath)
    if file_resource["size"] != filesize:
        logging.info(
            "file {} has size {} which mismatches recorded size {} for {} in tantalus".format(
                filepath, filesize, file_resource["size"], file_resource["filename"]))
        return False
    return True


class AzureBlobServerDownload(object):
    """ Blob Download class.

    Note: this class works with a tantalus storage or directory as destination.
    """

    def __init__(self, tantalus_api, from_storage, to_storage_name, to_storage_prefix):
        self.tantalus_api = tantalus_api
        self.storage_client = self.tantalus_api.get_storage_client(from_storage['name'])
        self.from_storage = from_storage
        self.to_storage_name = to_storage_name
        self.to_storage_prefix = to_storage_prefix

    def download_from_blob(self, file_instance, overwrite=False):
        """ Download file from blob to a server.

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

        self.tantalus_api.check_file(file_instance)

        if os.path.isfile(local_filepath):
            if overwrite:
                logging.info(f'removing existing file {local_filepath}')
                os.remove(local_filepath)

            else:
                if _check_file_same_local(file_resource, local_filepath):
                    logging.info(
                        "skipping transfer of file resource {} that matches existing file".format(
                            file_resource["filename"]))
                    return
                error_message = "target file {filepath} already exists on {storage} with different size".format(
                    filepath=local_filepath,
                    storage=self.to_storage_name,
                )
                raise FileAlreadyExists(error_message)

        if azcopy:
            blob_url = self.storage_client.get_url(cloud_blobname)
            run_azcopy(blob_url, local_filepath)

        else:
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
        self.tantalus_api = tantalus_api
        self.block_blob_service = tantalus_api.get_storage_client(to_storage["name"]).blob_service
        self.to_storage = to_storage

    def upload_to_blob(self, file_instance, overwrite=False):
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

        # Check if file instance to be uploaded exists and size matches
        self.tantalus_api.check_file(file_instance)

        # Check any existing file unless we intend to overwrite
        if self.block_blob_service.exists(cloud_container, cloud_blobname) and not overwrite:
            if _check_file_same_blob(
                    self.block_blob_service,
                    file_resource, cloud_container, cloud_blobname):
                logging.info(
                    "skipping transfer of file resource {} that matches existing file".format(
                        file_resource["filename"]))
                return
            error_message = "target file {filepath} already exists on {storage} with different size".format(
                filepath=cloud_filepath,
                storage=self.to_storage["name"],
            )
            raise FileAlreadyExists(error_message)

        if azcopy:
            storage_client = self.tantalus_api.get_storage_client(self.to_storage['name'])
            blob_url = storage_client.get_url(cloud_blobname, write_permission=True)
            run_azcopy(local_filepath, blob_url)

        else:
            self.block_blob_service.create_blob_from_path(
                cloud_container,
                cloud_blobname,
                local_filepath,
                progress_callback=TransferProgress().print_progress,
                max_connections=16,
                timeout=10 * 60 * 64,
            )


class AzureBlobBlobTransfer(object):
    """ Blob Upload class.

    Note: this class only works with tantalus storage as destination.
    """

    def __init__(self, tantalus_api, source_storage, destination_storage):
        self.tantalus_api = tantalus_api
        self.source_storage = source_storage
        self.destination_storage = destination_storage

        # Start BlockBlobService for source and destination accounts
        self.source_account = tantalus_api.get_storage_client(source_storage["name"]).blob_service
        self.destination_account = tantalus_api.get_storage_client(destination_storage["name"]).blob_service

        # Get a shared access signature for the source account so that we
        # can read its private files
        self.shared_access_sig = self.source_account.generate_container_shared_access_signature(
            container_name=source_storage["storage_container"],
            permission=ContainerPermissions.READ,
            expiry=(datetime.datetime.utcnow() + datetime.timedelta(hours=200)),
        )

    def transfer(self, file_instance, overwrite=False):
        """ Transfer function aware of source and destination Azure storages.
        """
        file_resource = file_instance["file_resource"]

        blobname = file_resource["filename"]
        source_container = file_instance["storage"]["storage_container"]
        destination_container = self.destination_storage["storage_container"]

        assert self.source_storage["storage_container"] == source_container

        # Check if file instance exists and size matches
        self.tantalus_api.check_file(file_instance)

        # Check any existing file unless we intend to overwrite
        if self.destination_account.exists(destination_container, blobname) and not overwrite:
            if _check_file_same_blob(
                    self.destination_account,
                    file_resource, destination_container, blobname):
                logging.info(
                    "skipping transfer of file resource {} that matches existing file".format(
                        file_resource["filename"]))
                return
            error_message = "target blob {blobname} in container {container} already exists on {storage} with different size".format(
                filepath=blobname,
                container=destination_container,
                storage=self.destination_storage["name"],
            )
            raise FileAlreadyExists(error_message)

        # Finally, transfer the file between the blobs
        source_sas_url = self.source_account.make_blob_url(
            container_name=file_instance["storage"]["storage_container"],
            blob_name=blobname,
            sas_token=self.shared_access_sig,
        )

        self.destination_account.copy_blob(
            container_name=self.destination_storage["storage_container"],
            blob_name=blobname,
            copy_source=source_sas_url,
        )


class RsyncTransfer(object):
    """ Blob Upload class.

    Note: this class works with a tantalus storage or directory as destination.
    """

    def __init__(self, to_storage_name, to_storage_prefix, local_transfer=False):
        self.to_storage_name = to_storage_name
        self.to_storage_prefix = to_storage_prefix
        self.local_transfer = local_transfer

    def rsync_file(self, file_instance, overwrite=False):
        """ Rsync a single file from one storage to another
        """
        file_resource = file_instance["file_resource"]

        local_filepath = os.path.join(self.to_storage_prefix, file_resource["filename"])

        remote_filepath = file_instance["filepath"]

        if file_instance["file_resource"]["is_folder"]:
            local_filepath = local_filepath + "/"
            remote_filepath = remote_filepath + "/"

        if os.path.isfile(local_filepath):
            if overwrite:
                logging.info(f'removing existing file {local_filepath}')
                os.remove(local_filepath)

            else:
                if _check_file_same_local(file_instance["file_resource"], local_filepath):
                    logging.info(
                        "skipping transfer of file resource {} that matches existing file".format(
                            file_resource["filename"]))
                    return

                error_message = "target file {filepath} already exists on {storage} with different size".format(
                    filepath=local_filepath, storage=self.to_storage_name)

                raise FileAlreadyExists(error_message)

        if file_instance["storage"]["server_ip"] == self.local_transfer:
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

        if not _check_file_same_local(file_instance["file_resource"], local_filepath):
            error_message = "transfer to {filepath} on {storage} failed".format(
                filepath=local_filepath, storage=self.to_storage_name
            )
            raise Exception(error_message)


def get_file_transfer_function(tantalus_api, from_storage, to_storage):
    if from_storage["storage_type"] == "blob" and to_storage["storage_type"] == "blob":
        return AzureBlobBlobTransfer(tantalus_api, from_storage, to_storage).transfer

    elif from_storage["storage_type"] == "server" and to_storage["storage_type"] == "blob":
        return AzureBlobServerUpload(
            tantalus_api, to_storage).upload_to_blob

    elif from_storage["storage_type"] == "blob" and to_storage["storage_type"] == "server":
        return AzureBlobServerDownload(
            tantalus_api, from_storage, to_storage["name"], to_storage["prefix"]).download_from_blob

    elif from_storage["storage_type"] == "server" and to_storage["storage_type"] == "server":
        local_transfer = (to_storage["server_ip"] == from_storage["server_ip"])
        return RsyncTransfer(
            to_storage["name"], to_storage["prefix"], local_transfer=local_transfer).rsync_file


def get_cache_function(tantalus_api, from_storage, cache_directory):
    if from_storage["storage_type"] == "blob":
        return AzureBlobServerDownload(
            tantalus_api, from_storage, 'localcache', cache_directory).download_from_blob

    elif from_storage["storage_type"] == "server":
        return RsyncTransfer(
            'localcache', cache_directory).rsync_file

    else:
        raise ValueError('unaccepted storage type {}'.format(from_storage["storage_type"]))


@click.group()
def cli():
    pass


@cli.command("transfer_tag")
@click.argument("tag_name")
@click.argument("from_storage_name")
@click.argument("to_storage_name")
def transfer_tagged_datasets_cmd(tag_name, from_storage_name, to_storage_name):
    transfer_tagged_datasets(tag_name, from_storage_name, to_storage_name)


def transfer_tagged_datasets(tag_name, from_storage_name, to_storage_name):
    """ Transfer a set of tagged datasets
    """

    tantalus_api = TantalusApi()

    tag = tantalus_api.get("tag", name=tag_name)

    for dataset_id in tag['sequencedataset_set']:
        transfer_dataset(tantalus_api, dataset_id, "sequencedataset", from_storage_name, to_storage_name)

    for dataset_id in tag['resultsdataset_set']:
        transfer_dataset(tantalus_api, dataset_id, "resultsdataset", from_storage_name, to_storage_name)


@cli.command("cache_tag")
@click.argument("tag_name")
@click.argument("from_storage_name")
@click.argument("cache_directory")
@click.option("--suffix_filter", required=False)
def cache_tagged_datasets_cmd(tag_name, from_storage_name, cache_directory, suffix_filter=None):
    cache_tagged_datasets(tag_name, from_storage_name, cache_directory, suffix_filter=suffix_filter)


def cache_tagged_datasets(tag_name, from_storage_name, cache_directory, suffix_filter=None):
    """ Cache a set of tagged datasets
    """

    tantalus_api = TantalusApi()

    tag = tantalus_api.get("tag", name=tag_name)

    for dataset_id in tag['sequencedataset_set']:
        cache_dataset(
            tantalus_api, dataset_id, "sequencedataset", from_storage_name,
            cache_directory, suffix_filter=suffix_filter)

    for dataset_id in tag['resultsdataset_set']:
        cache_dataset(
            tantalus_api, dataset_id, "resultsdataset", from_storage_name,
            cache_directory, suffix_filter=suffix_filter)


RETRIES = 3
def _transfer_files_with_retry(f_transfer, file_instance, overwrite=False):
    for retry in range(RETRIES):
        try:
            f_transfer(file_instance, overwrite=overwrite)
            break
        except Exception as e:
            logging.error("Transfer failed. Retrying.")

            if retry < RETRIES - 1:
                traceback.print_exc()
            else:
                logging.error("Failed all retry attempts")
                raise


@cli.command("transfer")
@click.argument("dataset_id", type=int)
@click.argument("dataset_model", type=click.Choice(["sequencedataset", "resultsdataset"]))
@click.argument("from_storage_name")
@click.argument("to_storage_name")
def transfer_dataset_cmd(dataset_id, dataset_model, from_storage_name, to_storage_name):
    tantalus_api = TantalusApi()
    transfer_dataset(tantalus_api, dataset_id, dataset_model, from_storage_name, to_storage_name)


def transfer_dataset(tantalus_api, dataset_id, dataset_model, from_storage_name, to_storage_name):
    """ Transfer a dataset
    """
    assert dataset_model in ("sequencedataset", "resultsdataset")

    if tantalus_api.is_dataset_on_storage(dataset_id, dataset_model, to_storage_name):
        logging.info(f'{dataset_model} {dataset_id} already on {to_storage_name}')
        return

    dataset = tantalus_api.get(dataset_model, id=dataset_id)

    to_storage = tantalus_api.get("storage", name=to_storage_name)
    from_storage = tantalus_api.get("storage", name=from_storage_name)

    f_transfer = get_file_transfer_function(tantalus_api, from_storage, to_storage)

    file_instances = tantalus_api.get_dataset_file_instances(dataset_id, dataset_model, from_storage_name)

    for file_instance in file_instances:
        file_resource = file_instance["file_resource"]

        try:
            other_file_instance = tantalus_api.get(
                "file_instance", file_resource=file_resource["id"], storage__name=to_storage["name"])
        except NotFoundError:
            other_file_instance = None

        if other_file_instance is not None and not other_file_instance['is_deleted']:
            logging.info(
                "skipping file resource {} that already exists on storage {}".format(
                    file_resource["filename"], to_storage["name"]
                )
            )
            continue

        overwrite = (other_file_instance is not None and other_file_instance['is_deleted'])

        logging.info(
            "starting transfer {} to {}".format(
                file_resource["filename"], to_storage["name"]))

        _transfer_files_with_retry(f_transfer, file_instance, overwrite=overwrite)

        tantalus_api.add_instance(file_resource, to_storage)


@cli.command("cache")
@click.argument("dataset_id", type=int)
@click.argument("dataset_model", type=click.Choice(["sequencedataset", "resultsdataset"]))
@click.argument("from_storage_name")
@click.argument("cache_directory")
@click.option("--suffix_filter", required=False)
def cache_dataset_cmd(dataset_id, dataset_model, from_storage_name, cache_directory, suffix_filter=None):
    tantalus_api = TantalusApi()
    cache_dataset(tantalus_api, dataset_id, dataset_model, from_storage_name, cache_directory, suffix_filter=suffix_filter)


def cache_dataset(tantalus_api, dataset_id, dataset_model, from_storage_name, cache_directory, suffix_filter=None):
    """ Cache a dataset
    """
    cache_client = tantalus_api.get_cache_client(cache_directory)

    assert dataset_model in ("sequencedataset", "resultsdataset")

    dataset = tantalus_api.get(dataset_model, id=dataset_id)

    from_storage = tantalus_api.get("storage", name=from_storage_name)

    f_transfer = get_cache_function(tantalus_api, from_storage, cache_directory)

    file_instances = tantalus_api.get_dataset_file_instances(dataset_id, dataset_model, from_storage_name)

    filepaths = []

    for file_instance in file_instances:
        filename = file_instance["file_resource"]["filename"]

        if suffix_filter is not None and not filename.endswith(suffix_filter):
            logging.info("skipping caching of {}".format(filename))
            continue

        logging.info("starting caching {} to {}".format(
                filename, cache_directory))

        _transfer_files_with_retry(f_transfer, file_instance)

        filepath = cache_client.get_url(file_instance['file_resource']['filename'])

        filepaths.append(filepath)

    return filepaths


def cache_file(tantalus_api, file_instance, cache_directory):
    """ Cache a single file.
    """
    f_transfer = get_cache_function(tantalus_api, file_instance['storage'], cache_directory)

    logging.info("starting caching {} from {} to {}".format(
        file_instance['file_resource']['filename'], file_instance['storage']['name'], cache_directory))

    _transfer_files_with_retry(f_transfer, file_instance)

    return os.path.join(cache_directory, file_instance['file_resource']['filename'])


if __name__ == "__main__":
    # Set up the root logger
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    cli()
