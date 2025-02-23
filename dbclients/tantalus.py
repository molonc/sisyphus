"""Contains a Tantalus API class to make requests to Tantalus.

This class makes no attempt at being all-encompassing. It covers a
subset of Tantalus API features to meet the needs of the automation
scripts.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import json
import logging
import os
import shutil

import azure.storage.blob as azureblob
#import azure.storage.blob.shared_access_signature as blob_sas
from azure.storage.blob import generate_blob_sas
import datetime
import pandas as pd
import time
from azure.identity import ClientSecretCredential
from azure.core.exceptions import ResourceNotFoundError
try:
    from urllib.request import urlopen
except ImportError:
    from urllib2 import urlopen

# async packages
import asyncio
from azure.storage.blob.aio import BlobServiceClient as AsyncBlobServiceClient
from azure.identity.aio import ClientSecretCredential as AsyncClientSecretCredential

from datamanagement.utils.django_json_encoder import DjangoJSONEncoder
from datamanagement.utils.utils import make_dirs
from dbclients.basicclient import BasicAPIClient, FieldMismatchError, NotFoundError

from dbclients.utils.dbclients_utils import get_tantalus_base_url

log = logging.getLogger('sisyphus')
# changed 3 to 20
class AsyncBlobStorageClient(object):
    def __init__(self, storage_account, storage_container, prefix, concurrency=20):
        self.storage_account = storage_account
        self.storage_container = storage_container
        self.prefix = prefix

        self.storage_account_url = "https://{}.blob.core.windows.net".format(
            self.storage_account
        )

        self.queue = asyncio.Queue()
        # used to control number of concurrent tasks
        self.concurrency = concurrency

    async def get_secret_token(self):
        client_id = os.environ["CLIENT_ID"]
        secret_key = os.environ["SECRET_KEY"]
        tenant_id = os.environ["TENANT_ID"]

        return AsyncClientSecretCredential(tenant_id, client_id, secret_key)

    async def upload_blob(self, blob_service_client, update=False):
        while not self.queue.empty():
            data = await self.queue.get()
            blobname = data[0]
            source_file = data[1]

            # check if blob already exists
            if(await self.exists(blob_service_client, blobname)):
                log.info(f"{blobname} already exists on {self.prefix}")

                blobsize = await self.get_size(blob_service_client, blobname)
                filesize = os.path.getsize(source_file)

                if blobsize == filesize:
                    log.info(f"{blobname} has the same size as {source_file}")
                    self.queue.task_done()
                    continue
                elif update:
                    log.info(f"{blobname} updating from {source_file}")
                else:
                    raise Exception(f"blob size is {blobsize} but local file size is {filesize}")

            # Instantiate a new BlobClient
            blob_client = blob_service_client.get_blob_client(self.storage_container, blobname)

            try:
                log.info(f"starting upload for {source_file}")
                async with blob_client:
                    with open(source_file, 'rb') as data:
                        await blob_client.upload_blob(data, overwrite=True)
                log.info(f"upload done for {source_file}!")
            except KeyboardInterrupt:
                raise
            except:
                raise
                # await self.queue.put(source_file)
            finally:
                self.queue.task_done()

    async def delete_blob(self, blob_service_client):
        while not self.queue.empty():
            blobname = await self.queue.get()
            # Instantiate a new BlobClient
            blob_client = blob_service_client.get_blob_client(self.storage_container, blobname)

            try:
                log.info(f"starting delete for {blobname}")
                async with blob_client:
                    await blob_client.delete_blob()
                log.info(f"delete done for {blobname}!")
            except KeyboardInterrupt:
                raise
            except:
                raise
                # await self.queue.put(source_file)
            finally:
                self.queue.task_done()

    async def exists(self, blob_service_client, blobname):
        blob_client = blob_service_client.get_blob_client(self.storage_container, blobname)

        try:
            async with blob_client:
                await blob_client.get_blob_properties()
                return True
        except ResourceNotFoundError:
            return False

    async def get_size(self,blob_service_client, blobname):
        blob_client = blob_service_client.get_blob_client(self.storage_container, blobname)

        async with blob_client:
            blob = await blob_client.get_blob_properties()

        return blob.size

    async def batch_upload_files(self, data):
        """
        Batch upload files to Blob storage account asynchronously

        Args:
            data (list of tuple): list of tuple containing (blobname, source_file)
        """
        credential_token = await self.get_secret_token()

        blob_service_client = AsyncBlobServiceClient(
            self.storage_account_url,
            credential_token,
        )

        # set max size to be 64MB
        blob_service_client.MAX_BLOCK_SIZE = 64 * 1024 * 1024

        for upload_info in data:
            await self.queue.put(upload_info)

        # create concurrent tasks
        tasks = [asyncio.create_task(self.upload_blob(blob_service_client)) for _ in range(self.concurrency)]
        # execute tasks
        await asyncio.gather(*tasks)
        # close secret credential client connection
        await blob_service_client.close()
        await credential_token.close()

    async def batch_delete_files(self, data):
        """
        Batch upload files to Blob storage account asynchronously

        Args:
            data (list): list containing blobnames to delete
        """
        credential_token = await self.get_secret_token()

        blob_service_client = AsyncBlobServiceClient(
            self.storage_account_url,
            credential_token,
        )

        # set max size to be 64MB
        blob_service_client.MAX_BLOCK_SIZE = 64 * 1024 * 1024

        for blobname in data:
            await self.queue.put(blobname)

        # create concurrent tasks
        tasks = [asyncio.create_task(self.delete_blob(blob_service_client)) for _ in range(self.concurrency)]
        # execute tasks
        await asyncio.gather(*tasks)
        # close secret credential client connection
        await blob_service_client.close()
        await credential_token.close()

class BlobStorageClient(object):
    def __init__(self, storage_account, storage_container, prefix):
        self.storage_account = storage_account
        self.storage_container = storage_container
        self.prefix = prefix

        client_id = os.environ["CLIENT_ID"]
        secret_key = os.environ["SECRET_KEY"]
        tenant_id = os.environ["TENANT_ID"]

        storage_account_token = ClientSecretCredential(tenant_id, client_id, secret_key)

        storage_account_url = "https://{}.blob.core.windows.net".format(
            self.storage_account
        )

        self.blob_service = azureblob.BlobServiceClient(
            storage_account_url,
            storage_account_token)

        self.blob_service.MAX_BLOCK_SIZE = 64 * 1024 * 1024

    def get_size(self, blobname):
        blob_client = self.blob_service.get_blob_client(self.storage_container, blobname)
        blob = blob_client.get_blob_properties()
        return blob.size

    def get_created_time(self, blobname):
        blob_client = self.blob_service.get_blob_client(self.storage_container, blobname)
        blob = blob_client.get_blob_properties()
        created_time = blob.last_modified.isoformat()
        return created_time

    def get_url(self, blobname, write_permission=False):

        if write_permission:
            permissions = azureblob.BlobSasPermissions(read=True, write=True, create=True, delete=True)
        else:
            permissions = azureblob.BlobSasPermissions(read=True)

        start_time = datetime.datetime.utcnow()
        expiry_time = datetime.datetime.utcnow() + datetime.timedelta(hours=12)

        token = self.blob_service.get_user_delegation_key(
            key_start_time=start_time,
            key_expiry_time=expiry_time
        )

        sas_token = generate_blob_sas(
        account_name=self.storage_account,
        container_name=self.storage_container,
        blob_name=blobname,
        user_delegation_key=token,
        permission=permissions,
        start=start_time,
        expiry=expiry_time,
)

        #sas_token = blob_sas.generate_blob_sas(
        #    self.storage_account,
        #    self.storage_container,
        #    blobname,
        #    user_delegation_key=token,
        #    permission=permissions,
        #    start=start_time,
        #    expiry=expiry_time,
        #)

        protocol = "https"
        primary_endpoint = "{}.blob.core.windows.net".format(self.storage_account)

        url = '{}://{}/{}/{}'.format(
            protocol,
            primary_endpoint,
            self.storage_container,
            blobname,
        )

        url += '?' + sas_token
        return url

    def delete(self, blobname):
        blob_client = self.blob_service.get_blob_client(self.storage_container, blobname)
        blob_client.delete_blob()

    def open_file(self, blobname):
        url = self.get_url(blobname)
        return urlopen(url)

    def exists(self, blobname):
        blob_client = self.blob_service.get_blob_client(self.storage_container, blobname)

        try:
            blob_client.get_blob_properties()
            return True
        except ResourceNotFoundError:
            return False

    def list(self, prefix):
        blob_client = self.blob_service.get_container_client(self.storage_container)
        container_blobs = blob_client.list_blobs(name_starts_with=prefix)

        for blob in container_blobs:
            yield blob.name

    def write_data(self, blobname, stream):
        stream.seek(0)
        blob_client = self.blob_service.get_blob_client(self.storage_container, blobname)
        return blob_client.upload_blob(stream, overwrite=True)

    def write_data_raw(self, blobname, data):
        blob_client = self.blob_service.get_blob_client(self.storage_container, blobname)
        return blob_client.upload_blob(data, overwrite=True)

    def create(self, blobname, filepath, update=False, max_concurrency=200, timeout=345600):
        kwargs = {}
        if max_concurrency:
            kwargs['max_concurrency'] = max_concurrency
        if timeout:
            kwargs['timeout'] = timeout

        if self.exists(blobname):
            log.info("{} already exists on {}".format(blobname, self.prefix))

            blobsize = self.get_size(blobname)
            filesize = os.path.getsize(filepath)

            if blobsize == filesize:
                log.info("{} has the same size as {}".format(blobname, filepath))
                return
            elif update:
                log.info("{} updating from {}".format(blobname, filepath))
            else:
                raise Exception("blob size is {} but local file size is {}".format(blobsize, filesize))

        log.info("Creating blob {} from path {}".format(blobname, filepath))

        blob_client = self.blob_service.get_blob_client(self.storage_container, blobname)

        with open(filepath, "rb") as stream:
            blob_client.upload_blob(stream, overwrite=True)

    def copy(self, blobname, new_blobname, wait=False):
        url = self.get_url(blobname)
        blob_client = self.blob_service.get_blob_client(self.storage_container, new_blobname)
        copy_props = blob_client.start_copy_from_url(url)
        if wait:
            while copy_props.status != 'success':
                time.sleep(1)
                blob = blob_client.get_blob_properties(self.storage_container, new_blobname)
                copy_props = blob.properties.copy

    def download(
            self, blob_name, destination_file_path, max_concurrency=None, timeout=None
    ):
        """
        download data from blob storage
        :param container_name: blob container name
        :param blob_name: blob path
        :param destination_file_path: path to download the file to
        :return: azure.storage.blob.baseblobservice.Blob instance with content properties and metadata
        """
        kwargs = {}
        if max_concurrency:
            kwargs['max_concurrency'] = max_concurrency
        if timeout:
            kwargs['timeout'] = timeout

        try:
            blob_client = self.blob_service.get_blob_client(self.storage_container, blob_name)
            with open(destination_file_path, "wb") as my_blob:
                download_stream = blob_client.download_blob(**kwargs)
                my_blob.write(download_stream.readall())
            blob = blob_client.get_blob_properties()
        except Exception as exc:
            print("Error downloading {} from {}".format(blob_name, self.storage_container))
            raise exc

        if not blob:
            raise Exception('Blob download failure')

        return blob



class ServerStorageClient(object):
    def __init__(self, storage_directory, prefix):
        self.storage_directory = storage_directory
        self.prefix = prefix

    def get_size(self, filename):
        filepath = os.path.join(self.storage_directory, filename)
        return os.path.getsize(filepath)

    def get_created_time(self, filename):
        filepath = os.path.join(self.storage_directory, filename)
        # TODO: this is currently fixed at pacific time
        return pd.Timestamp(time.ctime(os.path.getmtime(filepath)), tz="Canada/Pacific").isoformat()

    def get_url(self, filename):
        filepath = os.path.join(self.storage_directory, filename)
        return filepath

    def delete(self, filename):
        os.remove(self.get_url(filename))

    def open_file(self, filename):
        filepath = os.path.join(self.storage_directory, filename)
        return open(filepath)

    def exists(self, filename):
        filepath = os.path.join(self.storage_directory, filename)
        return os.path.exists(filepath)

    def list(self, prefix):
        for root, dirs, files in os.walk(os.path.join(self.storage_directory, prefix)):
            for filename in files:
                yield os.path.join(root, filename)

    def write_data(self, filename, stream):
        stream.seek(0)
        filepath = os.path.join(self.storage_directory, filename)
        dirname = os.path.dirname(filepath)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        with open(filepath, "wb") as f:
            f.write(stream.getvalue())

    def create(self, filename, filepath, update=False):
        if self.exists(filename):
            log.info("{} already exists on {}".format(filename, self.prefix))

            storagefilesize = self.get_size(filename)
            filesize = os.path.getsize(filepath)

            if storagefilesize == filesize:
                log.info("{} has the same size as {}".format(filename, filepath))
                return
            elif update:
                log.info("{} updating from {}".format(filename, filepath))
            else:
                raise Exception("storage file size is {} but local file size is {}".format(storagefilesize, filesize))

        log.info("Creating storage file {} from path {}".format(filename, filepath))
        tantalus_filepath = os.path.join(self.storage_directory, filename)
        if not os.path.samefile(filepath, tantalus_filepath):
            shutil.copy(filepath, tantalus_filepath)

    def copy(self, filename, new_filename, wait=None):
        filepath = os.path.join(self.storage_directory, filename)
        new_filepath = os.path.join(self.storage_directory, new_filename)
        if not os.path.exists(os.path.dirname(new_filepath)):
            make_dirs(os.path.dirname(new_filepath))
        os.link(filepath, new_filepath)


class DataError(Exception):
    """ An general data error.
    """
    pass


class DataCorruptionError(DataError):
    """ An error raised for data file corruption.
    """
    pass


class DataMissingError(DataError):
    """ An error raised when data is missing from the expected location.
    """
    pass


class DataNotOnStorageError(Exception):
    """ An error when data is not on the requested storage.
    """
    pass


class TantalusApi(BasicAPIClient):
    """Tantalus API class."""

    def __init__(self):
        """Set up authentication using basic authentication.

        Expects to find valid environment variables
        TANTALUS_API_USERNAME and TANTALUS_API_PASSWORD. Also looks for
        an optional TANTALUS_API_URL.
        """
        TANTALUS_BASE_URL = get_tantalus_base_url()

        super(TantalusApi, self).__init__(
            TANTALUS_BASE_URL,
            username=os.environ.get("TANTALUS_API_USERNAME"),
            password=os.environ.get("TANTALUS_API_PASSWORD"),
        )

        self.cached_storages = {}
        self.cached_storage_clients = {}

    def get_list_pagination_initial_params(self, params):
        """ Get initial pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["page_size"] = 1000
        params["page"] = 1

    def get_list_pagination_next_page_params(self, params):
        """ Get next page pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["page_size"] = 1000
        params["page"] += 1

    def get_file_resource_filename(self, storage_name, filepath):
        """ Strip the storage directory from a filepath to create a tantalus filename.

        Args:
            storage_name: storage in which the file resides
            filepath: abs path of the file

        Returns:
            filename: relative filename of the file
        """
        storage = self.get_storage(storage_name)

        if not filepath.startswith(storage['prefix']):
            raise ValueError('file {} not in storage {} with prefix {}'.format(
                filepath,
                storage['name'],
                storage['prefix'],
            ))

        filename = filepath[len(storage['prefix']):]
        filename = filename.lstrip('/')

        return filename

    def get_filepath(self, storage_name, filename):
        """ Prefix the filename with the storage prefix to create a full filepath.

        Args:
            storage_name: storage in which the file resides
            filename: relative filename of the file

        Returns:
            filepath: abs path of the file
        """
        storage = self.get_storage(storage_name)

        if filename.startswith('/') or '..' in filename:
            raise ValueError('expected relative path got {}'.format(filename))

        return os.path.join(storage['prefix'], filename)

    def get_storage(self, storage_name):
        """ Retrieve a storage object with caching.

        Args:
            storage_name: storage in which the file resides

        Returns:
            storage details (dict)
        """
        if storage_name in self.cached_storages:
            return self.cached_storages[storage_name]

        storage = self.get('storage', name=storage_name)

        self.cached_storages[storage_name] = storage

        return storage

    def get_cache_client(self, storage_directory):
        """ Retrieve a client for the given cache

        Args:
            storage_directory: directory in which the files are cached

        Returns:
            storage client object
        """
        return ServerStorageClient(storage_directory, storage_directory)

    def get_storage_client(self, storage_name, is_async=False, concurrency=3):
        """ Retrieve a client for the given storage

        Args:
            storage_name: storage in which the file resides

        Returns:
            storage client object
        """
        if storage_name in self.cached_storage_clients:
            return self.cached_storage_clients[storage_name]

        storage = self.get_storage(storage_name)

        if storage['storage_type'] == 'blob':
            if(is_async):
                client = AsyncBlobStorageClient(storage['storage_account'], storage['storage_container'], storage['prefix'], concurrency)
            else:
                client = BlobStorageClient(storage['storage_account'], storage['storage_container'], storage['prefix'])
        elif storage['storage_type'] == 'server':
            client = ServerStorageClient(storage['storage_directory'], storage['prefix'])
        else:
            return ValueError('unsupported storage type {}'.format(storage['storage_type']))

        self.cached_storage_clients[(storage_name, is_async)] = client

        return client

    def _add_or_update_file(self, storage_name, filename, update=False):
        """ Create or update a file resource and file instance in the given storage.

        Args:
            storage_name: storage for file instance
            filename: storage relative filename

        Kwargs:
            update: update the file if exists

        Returns:
            file_resource, file_instance

        For a file that does not exist, create the file resource and
        file instance on the specific storage and return them.

        If the file already exist in tantalus and the file being
        added has the same properties, add_file will ensure an instance
        exists on the given storage.

        If the file already exists in tantalus and the file being added
        has different size, functionality will depend on the
        update kwarg.  If update=False, fail with FieldMismatchError.
        If update=True, update the file resource, create a file instance
        on the given storage, and set all other file instances to
        is_delete=True.
        """
        storage = self.get_storage(storage_name)
        storage_client = self.get_storage_client(storage_name)

        created = storage_client.get_created_time(filename)
        size = storage_client.get_size(filename)

        # Try getting the file resource with the same size
        try:
            file_resource = self.get('file_resource', filename=filename, size=size)
            log.info('file resource has id {}'.format(file_resource['id']))
        except NotFoundError:
            log.info('no identical file resource by size')
            file_resource = None
        if file_resource is not None:
            file_instance = self.add_instance(file_resource, storage)
            return file_resource, file_instance

        # Try getting or creating the file resource, will
        # fail only if exists with different size.  If the
        # file has the same size but different created time
        # the above get will have suceeded.
        try:
            file_resource, _ = self.create(
                'file_resource',
                dict(
                    filename=filename,
                    created=created,
                    size=size,
                ),
                ['filename'],
                get_existing=True,
                do_update=False,
            )
            log.info('file resource has id {}'.format(file_resource['id']))
        except FieldMismatchError:
            if not update:
                log.exception('file resource with filename {} has different properties, not updating'.format(filename))
                raise
            file_resource = None

        # Creating a file did not succeed because it existed but with
        # different properties.  Update the file.
        if file_resource is None:

            # Should have raised above if update=False
            assert update

            # Get existing file resource with different properties
            file_resource = self.get(
                'file_resource',
                filename=filename,
            )
            log.info('updating file resource {}'.format(file_resource['id']))

            # Delete all existing instances
            file_instances = self.list("file_instance", file_resource=file_resource["id"])
            for file_instance in file_instances:
                file_instance = self.update(
                    'file_instance',
                    id=file_instance['id'],
                    is_deleted=True,
                )
                log.info('deleted file instance {}'.format(file_instance['id']))

            # Update the file properties
            file_resource = self.update(
                'file_resource',
                id=file_resource['id'],
                filename=filename,
                created=created,
                size=size,
            )

        file_instance = self.add_instance(file_resource, storage)

        return file_resource, file_instance

    def add_file(self, storage_name, filepath, update=False):
        """ Create a file resource and file instance in the given storage.

        Args:
            storage_name: storage for file instance
            filepath: full path to file

        Kwargs:
            update: update the file if exists

        Returns:
            file_resource, file_instance

        For a file that does not exist, create the file resource and
        file instance on the specific storage and return them.

        If the file already exist in tantalus and the file being
        added has the same properties, add_file will ensure an instance
        exists on the given storage.

        If the file already exists in tantalus and the file being added
        has different properties, functionality will depend on the
        update kwarg.  If update=False, will raise FieldMismatchError.
        If update=True, update the file resource, create a file instance
        on the given storage, and set all other file instances to
        is_delete=True.
        """
        log.info('adding file with path {} in storage {}'.format(filepath, storage_name))

        filename = self.get_file_resource_filename(storage_name, filepath)

        return self._add_or_update_file(storage_name, filename, update=update)

    def update_file(self, file_instance):
        """
        Update a file resource to match the file pointed
        to by the given file instance.

        Args:
            file_instance (dict)

        Returns:
            file_instance (dict)
        """
        filename = file_instance['file_resource']['filename']
        storage_name = file_instance['storage']['name']

        log.info('updating file instance {} with filename {} in storage {}'.format(
            file_instance['id'],
            filename,
            storage_name,
        ))

        file_resource, file_instance = self._add_or_update_file(storage_name, filename, update=True)

        return file_instance

    def check_file(self, file_instance):
        """
        Check a file instance in tantalus exists and has the same size
        on its given storage.

        Args:
            file_instance (dict)

        Raises:
            DataCorruptionError, DataMissingError
        """

        if file_instance['is_deleted']:
            return

        storage_client = self.get_storage_client(file_instance['storage']['name'])

        file_resource = file_instance["file_resource"]

        if not storage_client.exists(file_resource['filename']):
            raise DataMissingError('file instance {} with path {} doesnt exist on storage {}'.format(
                file_instance['id'], file_instance['filepath'], file_instance['storage']['name']))

        size = storage_client.get_size(file_resource['filename'])
        if size != file_resource['size']:
            raise DataCorruptionError(
                'file instance {} with path {} has size {} on storage {} but {} in tantalus'.format(
                    file_instance['id'], file_instance['filepath'], size, file_instance['storage']['name'],
                    file_instance['file_resource']['size']))

    def delete_file(self, file_resource):
        """
        Delete a file and remove from all datasets.

        Args:
            file_resource (dict)
        """

        file_instances = self.list("file_instance", file_resource=file_resource["id"])
        for file_instance in file_instances:
            file_instance = self.update(
                "file_instance",
                id=file_instance["id"],
                is_deleted=True,
            )
            logging.info(f"deleted file instance {file_instance['id']}")

        for dataset_type in ("sequencedataset", "resultsdataset"):
            datasets = self.list(dataset_type, file_resources__id=file_resource["id"])
            for dataset in datasets:
                file_resources = list(set(dataset["file_resources"]))
                file_resources.remove(file_resource["id"])
                logging.info(f"removing file resource {file_resource['id']} from {dataset['id']}")
                self.update(dataset_type, id=dataset["id"], file_resources=file_resources)

    def swap_file(self, file_instance, new_filename):
        """
        Swap with an identical file within the same storage.

        Args:
            file_instance (dict): file instance to rename
            new_filename (str): destination filename

        The file will be swapped to a file within the same storage.  All other instances
        on other storages will be marked as deleted.
        """

        storage_name = file_instance['storage']['name']
        storage_client = self.get_storage_client(storage_name)

        file_resource = file_instance['file_resource']
        filename = file_resource['filename']

        # Check the files are identical by size
        size = storage_client.get_size(filename)
        new_size = storage_client.get_size(new_filename)
        if size != new_size:
            raise ValueError(f'files {filename} and {new_filename} have nonequal sizes {size} and {new_size}')

        # Update the file properties
        file_resource = self.update(
            'file_resource',
            id=file_resource['id'],
            filename=new_filename,
        )
        log.info(f'renamed file from {filename} to {new_filename}')

        # Delete all other instances
        other_file_instances = self.list("file_instance", file_resource=file_resource["id"])
        for other_file_instance in other_file_instances:
            if other_file_instance['id'] == file_instance['id']:
                continue
            other_file_instance = self.update(
                'file_instance',
                id=other_file_instance['id'],
                is_deleted=True,
            )
            log.info('deleted file instance {}'.format(other_file_instance['id']))

    def add_instance(self, file_resource, storage):
        """
        Add a file instance accounting for the possibility that we are replacing a deleted instance.

        Args:
            file_resource (dict)
            storage (dict)

        Returns:
            file_instance (dict)
        """

        file_instance = self.get_or_create(
            'file_instance',
            file_resource=file_resource["id"],
            storage=storage['id'],
        )

        if file_instance['is_deleted']:
            file_instance = self.update(
                'file_instance',
                id=file_instance['id'],
                is_deleted=False,
            )

        return file_instance

    def get_dataset_file_instances(self, dataset_id, dataset_model, storage_name, filters=None):
        """
        Given a dataset get all file instances.

        Note: file_resource and sequence_dataset are added as fields
        to the file_instances

        Args:
            dataset_id (int): primary key of sequencedataset or resultsdataset
            dataset_model (str): model type, sequencedataset or resultsdataset
            storage_name (str): name of the storage for which to retrieve file instances

        KwArgs:
            filters (dict): additional filters such as filename extension

        Returns:
            file_instances (list)
        """

        if filters == None:
            filters = {}

        file_resources = self.get_dataset_file_resources(dataset_id, dataset_model, filters)

        if dataset_model == 'sequencedataset':
            file_instances = self.list(
                'file_instance',
                file_resource__sequencedataset__id=dataset_id,
                storage__name=storage_name,
                is_deleted=False,
            )

        elif dataset_model == 'resultsdataset':
            file_instances = self.list(
                'file_instance',
                file_resource__resultsdataset__id=dataset_id,
                storage__name=storage_name,
                is_deleted=False,
            )

        else:
            raise ValueError('unrecognized dataset model {}'.format(dataset_model))

        file_instances = dict([(f['file_resource']['id'], f) for f in file_instances])

        # Each file resource should have a file instance on the given
        # storage unless not all files have been copied to the requested
        # storage. File instances may be a superset of file resources
        # if filters were added to the request.

        # Check if file resources have a file instance
        # return only file instances in the set file of
        # file resources
        filtered_file_instances = []
        for file_resource in file_resources:
            if file_resource['id'] not in file_instances:
                raise DataNotOnStorageError('file resource {} with filename {} not on {}'.format(
                    file_resource['id'], file_resource['filename'], storage_name))
            filtered_file_instances.append(file_instances[file_resource['id']])

        return filtered_file_instances

    def get_dataset_file_resources(self, dataset_id, dataset_model, filters=None):
        """
        Given a dataset get all file resources.

        Args:
            dataset_id (int): primary key of sequencedataset or resultsdataset
            dataset_model (str): model type, sequencedataset or resultsdataset

        KwArgs:
            filters (dict): additional filters such as filename extension

        Returns:
            file_resources (list)
        """
        if filters is None:
            filters = {}

        if dataset_model == 'sequencedataset':
            file_resources = self.list('file_resource', sequencedataset__id=dataset_id, **filters)

        elif dataset_model == 'resultsdataset':
            file_resources = self.list('file_resource', resultsdataset__id=dataset_id, **filters)

        else:
            raise ValueError('unrecognized dataset model {}'.format(dataset_model))

        return file_resources

    def is_dataset_on_storage(self, dataset_id, dataset_model, storage_name):
        """
        Given a dataset test if all files are on a specific storage.

        Args:
            dataset_id (int): primary key of sequencedataset or resultsdataset
            dataset_model (str): model type, sequencedataset or resultsdataset

        Returns:
            bool
        """

        try:
            self.get_dataset_file_instances(dataset_id, dataset_model, storage_name)

        except DataNotOnStorageError:
            return False

        return True

    def tag(self, name, sequencedataset_set=(), resultsdataset_set=()):
        """
        Tag datasets.

        Args:
            name (str)
            sequencedataset_set (list)
            resultsdataset_set (list)

        Returns:
            tag (dict)
        """
        endpoint_url = self.join_urls(self.base_api_url, 'tag')

        fields = {
            'name': name,
            'sequencedataset_set': sequencedataset_set,
            'resultsdataset_set': resultsdataset_set,
        }
        payload = json.dumps(fields, cls=DjangoJSONEncoder)

        r = self.session.post(endpoint_url, data=payload)

        if not r.ok:
            raise Exception('failed with error: "{}", reason: "{}"'.format(r.reason, r.text))

        return r.json()
