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
from utils.constants import LOGGING_FORMAT
from utils.runtime_args import parse_runtime_args
from utils.tantalus import TantalusApi
from utils.utils import make_dirs

# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stdout, level=logging.INFO)

# Configure the azure.storage logger. TODO(mwiens91): is this code
# necessary? Apparently some Microsoft people asked us to put it in, but
# so far I've seen zero output from it, even as we were having errors.
logger = logging.getLogger("azure.storage")
handler = logging.StreamHandler()
formatter = logging.Formatter("%(asctime)s %(name)-20s %(levelname)-5s %(message)s")
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.DEBUG)


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


def get_new_filepath(storage, file_resource):
    """Emulate Tantalus' get_filepath method logic."""
    if storage["storage_type"] == "server":
        return os.path.join(
            storage["storage_directory"], file_resource["filename"].strip("/")
        )
    elif storage["storage_type"] == "blob":
        return "/".join(
            [storage["storage_container"], file_resource["filename"].strip("/")]
        )
    else:
        # Storage type not supported!
        raise NotImplementedError


class AzureTransfer(object):
    """A class useful for server-blob interactions.

    Not so much blob-to-blob interactions in its present form.
    """

    def __init__(self, storage):
        self.block_blob_service = BlockBlobService(
            account_name=storage["storage_account"],
            account_key=storage["credentials"]["storage_key"],
        )
        self.block_blob_service.MAX_BLOCK_SIZE = 64 * 1024 * 1024

    def download_from_blob(self, file_instance, to_storage, tantalus_api):
        """ Transfer a file from blob to a server.

        This should be called on the from server.
        """

        cloud_filepath = file_instance["filepath"]
        cloud_container, cloud_blobname = cloud_filepath.split("/", 1)
        assert cloud_container == file_instance["storage"]["storage_container"]

        # Get the file resource associated with the file instance
        file_resource = tantalus_api.get(
            "file_resource", id=file_instance["file_resource"]
        )

        local_filepath = get_new_filepath(to_storage, file_resource)

        make_dirs(os.path.dirname(local_filepath))

        if not self.block_blob_service.exists(cloud_container, cloud_blobname):
            error_message = "source file {filepath} does not exist on {storage} for file instance with pk: {pk}".format(
                filepath=cloud_filepath,
                storage=file_instance["storage"]["name"],
                pk=file_instance["id"],
            )
            raise FileDoesNotExist(error_message)

        if os.path.isfile(local_filepath):
            error_message = "target file {filepath} already exists on {storage}".format(
                filepath=local_filepath, storage=to_storage["name"]
            )
            raise FileAlreadyExists(error_message)

        self.block_blob_service.get_blob_to_path(
            cloud_container,
            cloud_blobname,
            local_filepath,
            progress_callback=TransferProgress().print_progress,
            max_connections=1,
        )

        os.chmod(local_filepath, 0o444)

    def _check_file_same_blob(self, file_resource, container, blobname):
        properties = self.block_blob_service.get_blob_properties(container, blobname)
        blobsize = properties.properties.content_length
        if file_resource["size"] != blobsize:
            return False
        return True

    def upload_to_blob(self, file_instance, to_storage, tantalus_api):
        """Transfer a file from a server to blob.

        This should be called on the from server.
        """
        local_filepath = file_instance["filepath"]

        # Get the file resource associated with the file instance
        file_resource = tantalus_api.get(
            "file_resource", id=file_instance["file_resource"]
        )

        cloud_filepath = get_new_filepath(to_storage, file_resource)
        cloud_container, cloud_blobname = cloud_filepath.split("/", 1)
        assert cloud_container == to_storage["storage_container"]

        if not os.path.isfile(local_filepath):
            error_message = "source file {filepath} does not exist on {storage} for file instance with pk: {pk}".format(
                filepath=local_filepath,
                storage=file_instance["storage"]["name"],
                pk=file_instance["id"],
            )
            raise FileDoesNotExist(error_message)

        if self.block_blob_service.exists(cloud_container, cloud_blobname):
            if self._check_file_same_blob(
                file_resource, cloud_container, cloud_blobname
            ):
                return

            error_message = "target file {filepath} already exists on {storage}".format(
                filepath=cloud_filepath, storage=to_storage["name"]
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


def blob_to_blob_transfer_closure(source_storage, destination_storage):
    """Returns a function for transfering blobs between Azure containers.

    Note that this will *not* create new containers that don't already
    exist. This is a useful note because for development the container
    names are changed to "{container name}-test", and these "test
    containers" are unlikely to exist.
    """
    # Start BlockBlobService for source and destination accounts
    source_account = BlockBlobService(
        account_name=source_storage["storage_account"],
        account_key=source_storage["credentials"]["storage_key"],
    )
    destination_account = BlockBlobService(
        account_name=destination_storage["storage_account"],
        account_key=destination_storage["credentials"]["storage_key"],
    )

    # Get a shared access signature for the source account so that we
    # can read its private files
    shared_access_sig = source_account.generate_container_shared_access_signature(
        container_name=source_storage["storage_container"],
        permission=ContainerPermissions.READ,
        expiry=(datetime.datetime.utcnow() + datetime.timedelta(hours=200)),
    )

    def transfer_function(source_file, *_):
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


def rsync_file(file_instance, to_storage, tantalus_api):
    """ Rsync a single file from one storage to another
    """

    # Get the file resource associated with the file instance
    file_resource = tantalus_api.get("file_resource", id=file_instance["file_resource"])

    local_filepath = get_new_filepath(to_storage, file_resource)

    remote_filepath = file_instance["filepath"]

    if file_instance["file_resource"]["is_folder"]:
        local_filepath = local_filepath + "/"
        remote_filepath = remote_filepath + "/"

    if os.path.isfile(local_filepath):
        if check_file_same_local(file_instance["file_resource"], local_filepath):
            return
        error_message = "target file {filepath} already exists on {storage} with different size".format(
            filepath=local_filepath, storage=to_storage["name"]
        )
        raise FileAlreadyExists(error_message)

    if file_instance["storage"]["server_ip"] == to_storage["server_ip"]:
        remote_location = remote_filepath
    else:
        remote_location = (
            file_instance["storage"]["username"]
            + "@"
            + file_instance["storage"]["server_ip"]
            + ":"
            + remote_filepath
        )

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
            filepath=local_filepath, storage=to_storage["name"]
        )
        raise Exception(error_message)


def get_file_transfer_function(from_storage, to_storage):
    if from_storage["storage_type"] == "blob" and to_storage["storage_type"] == "blob":
        return blob_to_blob_transfer_closure(from_storage, to_storage)
    elif (
        from_storage["storage_type"] == "server"
        and to_storage["storage_type"] == "blob"
    ):
        return AzureTransfer(to_storage).upload_to_blob
    elif (
        from_storage["storage_type"] == "blob"
        and to_storage["storage_type"] == "server"
    ):
        return AzureTransfer(from_storage).download_from_blob
    elif (
        from_storage["storage_type"] == "server"
        and to_storage["storage_type"] == "server"
    ):
        return rsync_file


def transfer_files(tag_name, from_storage_name, to_storage_name):
    """ Transfer a set of files
    """
    # Connect to the Tantalus API (this requires appropriate environment
    # variables defined)
    tantalus_api = TantalusApi()

    # Get the storage details, sans credentials
    to_storage = tantalus_api.get("storage", name=to_storage_name)
    from_storage = tantalus_api.get("storage", name=from_storage_name)

    # For blob storages, get credentials for each storage by
    # transforming from credential ID to the credential itself
    if to_storage["storage_type"] == "blob":
        to_storage["credentials"] = tantalus_api.get(
            "storage_azure_blob_credentials", id=to_storage["credentials"]
        )

    if from_storage["storage_type"] == "blob":
        from_storage["credentials"] = tantalus_api.get(
            "storage_azure_blob_credentials", id=from_storage["credentials"]
        )

    f_transfer = get_file_transfer_function(from_storage, to_storage)

    for dataset in tantalus_api.list("sequence_dataset", tags__name=tag_name):
        for file_resource_id in dataset["file_resources"]:
            # Get the file resource corresponding to the ID
            file_resource = tantalus_api.get("file_resource", id=file_resource_id)

            storage_names = []

            for file_instance in file_resource["file_instances"]:
                storage_id = file_instance["storage"]["id"]

                storage_name = tantalus_api.get("storage", id=int(storage_id))["name"]
                storage_names.append(storage_name)

            if to_storage_name in storage_names:
                logging.info(
                    "skipping file resource {} that already exists on storage {}".format(
                        file_resource["filename"], to_storage_name
                    )
                )

                # Skip this file resource
                continue

            from_file_instance = None

            for file_instance in file_resource["file_instances"]:
                # TODO(mwiens91): We're querying Tantalus for storage
                # names above and here. Two requests for identical
                # information. We only really need to do it once. Fix
                # this by saving the storage name in the file_instance
                # dictionary after the first set of API requests.
                storage_id = file_instance["storage"]["id"]
                storage_name = tantalus_api.get("storage", id=int(storage_id))["name"]

                if storage_name == from_storage_name:
                    from_file_instance = file_instance

            if from_file_instance is None:
                raise FileDoesNotExist(
                    "file instance for file resource {} does not exist on source storage {}".format(
                        file_resource["filename"], from_storage_name
                    )
                )

            # Get a "nicer" version of the file instance with more
            # nested model relationships
            from_file_instance = tantalus_api.get(
                "file_instance", id=from_file_instance["id"]
            )

            logging.info(
                "starting transfer {} to {}".format(
                    file_resource["filename"], to_storage["name"]
                )
            )

            RETRIES = 3

            for retry in range(RETRIES):
                try:
                    f_transfer(from_file_instance, to_storage, tantalus_api)
                    break
                except Exception as e:
                    logging.error("Transfer failed. Retrying.")

                    if retry < RETRIES - 1:
                        traceback.print_exc()
                    else:
                        logging.error("Failed all retry attempts")
                        raise e

            tantalus_api.get_or_create(
                "file_instance",
                file_resource=file_resource["id"],
                storage=to_storage["id"],
            )


if __name__ == "__main__":
    # Parse the incoming arguments
    args = parse_runtime_args()

    # Transfer some files
    transfer_files(
        tag_name=args["tag_name"],
        from_storage_name=args["from_storage"],
        to_storage_name=args["to_storage"],
    )
