"""Contains a Tantalus API class to make requests to Tantalus.

This class makes no attempt at being all-encompassing. It covers a
subset of Tantalus API features to meet the needs of the automation
scripts.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import json
import os
import time
import datetime
import urllib2
import logging
import pandas as pd

from django.core.serializers.json import DjangoJSONEncoder
from dbclients.basicclient import BasicAPIClient, FieldMismatchError, NotFoundError

import azure.storage.blob
from azure.keyvault import KeyVaultClient, KeyVaultAuthentication
from azure.common.credentials import ServicePrincipalCredentials


log = logging.getLogger('sisyphus')

TANTALUS_API_URL = "http://tantalus.bcgsc.ca/api/"


def get_storage_account_key(
        accountname, client_id, secret_key, tenant_id, keyvault_account
):
    def auth_callback(server, resource, scope):
        credentials = ServicePrincipalCredentials(
            client_id=client_id,
            secret=secret_key,
            tenant=tenant_id,
            resource="https://vault.azure.net"
        )
        token = credentials.token
        return token['token_type'], token['access_token']

    client = KeyVaultClient(KeyVaultAuthentication(auth_callback))
    keyvault = "https://{}.vault.azure.net/".format(keyvault_account)

    # passing in empty string for version returns latest key
    secret_bundle = client.get_secret(keyvault, accountname, "")
    return secret_bundle.value


class BlobStorageClient(object):
    def __init__(self, storage):
        self.storage_account = storage['storage_account']
        self.storage_container = storage['storage_container']

        client_id = os.environ["CLIENT_ID"]
        secret_key = os.environ["SECRET_KEY"]
        tenant_id = os.environ["TENANT_ID"]
        keyvault_account = os.environ['AZURE_KEYVAULT_ACCOUNT']

        storage_key = get_storage_account_key(
            self.storage_account, client_id, secret_key,
            tenant_id, keyvault_account)

        self.blob_service = azure.storage.blob.BlockBlobService(
            account_name=self.storage_account,
            account_key=storage_key)
        self.blob_service.MAX_BLOCK_SIZE = 64 * 1024 * 1024

        self.prefix = storage['prefix']

    def get_size(self, blobname):
        properties = self.blob_service.get_blob_properties(self.storage_container, blobname)
        blobsize = properties.properties.content_length
        return blobsize

    def get_created_time(self, blobname):
        properties = self.blob_service.get_blob_properties(self.storage_container, blobname)
        created_time = properties.properties.last_modified.isoformat()
        return created_time

    def get_url(self, blobname):
        sas_token = self.blob_service.generate_blob_shared_access_signature(
            self.storage_container,
            blobname,
            permission=azure.storage.blob.BlobPermissions.READ,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=12),
        )
        blob_url = self.blob_service.make_blob_url(
            container_name=self.storage_container,
            blob_name=blobname,
            protocol="https",
            sas_token=sas_token,
        )
        return blob_url

    def delete(self, blobname):
        self.blob_service.delete_blob(self.storage_container, blob_name=blobname)

    def open_file(self, blobname):
        url = self.get_url(blobname)
        return urllib2.urlopen(url)

    def exists(self, blobname):
        return self.blob_service.exists(self.storage_container, blob_name=blobname)

    def list(self, prefix):
        for blob in self.blob_service.list_blobs(self.storage_container, prefix=prefix):
            yield blob.name

    def write_data(self, blobname, stream):
        stream.seek(0)
        return self.blob_service.create_blob_from_stream(
            self.storage_container, 
            blob_name=blobname,
            stream=stream)


class ServerStorageClient(object):
    def __init__(self, storage):
        self.storage_directory = storage['storage_directory']
        self.prefix = storage['prefix']

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

class TantalusApi(BasicAPIClient):
    """Tantalus API class."""

    # Parameters used for pagination
    pagination_param_names = ("limit", "offset")

    def __init__(self):
        """Set up authentication using basic authentication.

        Expects to find valid environment variables
        TANTALUS_API_USERNAME and TANTALUS_API_PASSWORD. Also looks for
        an optional TANTALUS_API_URL.
        """

        super(TantalusApi, self).__init__(
            os.environ.get("TANTALUS_API_URL", TANTALUS_API_URL),
            username=os.environ.get("TANTALUS_API_USERNAME"),
            password=os.environ.get("TANTALUS_API_PASSWORD"),
        )

        self.cached_storages = {}
        self.cached_storage_clients = {}

    def get_list_pagination_initial_params(self, params):
        """Get initial pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["limit"] = 100
        params["offset"] = 0

    def get_list_pagination_next_page_params(self, params):
        """Get next page pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["offset"] += params["limit"]

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
                filepath, storage['name'], storage['prefix']))

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

    def get_storage_client(self, storage_name):
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
            client = BlobStorageClient(storage)
        elif storage['storage_type'] == 'server':
            client = ServerStorageClient(storage)
        else:
            return ValueError('unsupported storage type {}'.format(storage['storage_type']))

        self.cached_storage_clients[storage_name] = client

        return client

    def get_file_compression(self, filepath):
        compression_choices = {
            ".gz":      "GZIP",
            ".bzip2":   "BZIP2",
            ".spec":    "SPEC",
        }

        extension = os.path.splitext(filepath)[1]
        try:
            return compression_choices[extension]
        except KeyError:
            return "UNCOMPRESSED"


    def add_file(self, storage_name, filepath, update=False):
        """ Create a file resource and file instance in the given storage.

        Args:
            storage_name: storage for file instance
            filepath: full path to file
            file_type: type for file_resource
            fields: additional fields for file_resource

        Kwargs:
            update: update the file if exists

        Returns:
            file_resource, file_instance
        """
        storage = self.get_storage(storage_name)
        storage_client = self.get_storage_client(storage_name)

        filename = self.get_file_resource_filename(storage_name, filepath)

        compression = self.get_file_compression(filename)
        file_type = filename.split(".")[1].upper()

        try:
            file_resource = self.get_or_create(
                'file_resource',
                filename=filename,
                file_type=file_type,
                created=storage_client.get_created_time(filename),
                size=storage_client.get_size(filename),
                compression=compression,
            )

            log.info('file resource has id {}'.format(file_resource['id']))
        except FieldMismatchError as e:
            if not update:
                log.info('file resource has different fields, not updating')
                raise
            file_resource = None

        # File resource will be none if fields mismatched and
        # we are given permission to update
        if file_resource is None:
            log.warning('updating existing file resource with filename {}'.format(filename))

            file_resource = self.get(
                'file_resource',
                filename=filename,
            )

            file_instances = self.list(
                'file_instance',
                file_resource=file_resource['id'],
            )

            # Cannot update if there are other instances
            for file_instance in file_instances:
                if file_instance['storage']['id'] != storage['id']:
                    raise Exception('file {} also exists on {}, cannot update'.format(
                        filename, file_instance['storage']))

            file_resource = self.update(
                'file_resource',
                id=file_resource['id'],
                filename=filename,
                file_type=file_type,
                created=storage_client.get_created_time(filename),
                size=storage_client.get_size(filename),
            )

        file_instance = self.get_or_create(
            'file_instance',
            file_resource=file_resource['id'],
            storage=storage['id'],
        )

        return file_resource, file_instance

    def get_file_instance(self, file_resource, storage_name):
        """
        Given a file resource and a storage name, return the matching file instance.

        Args:
            file_resource (dict)
            storage_name (str)

        Returns:
            file_instance (dict)
        """
        for file_instance in file_resource['file_instances']:
            if file_instance['storage']['name'] == storage_name:
                return file_instance

        raise NotFoundError

    def get_sequence_dataset_file_instances(self, dataset, storage_name):
        """
        Given a dataset get all file instances.

        Note: file_resource and sequence_dataset are added as fields
        to the file_instances

        Args:
            dataset (dict)
            storage_name (str)

        Returns:
            file_instances (list)
        """
        file_instances = []

        for file_resource in self.list('file_resource', sequencedataset__id=dataset['id']):

            file_instance = self.get_file_instance(file_resource, storage_name)
            file_instance['file_resource'] = file_resource
            file_instance['sequence_dataset'] = dataset

            file_instances.append(file_instance)

        return file_instances

    def tag(self, name, sequencedataset_set=(), resultsdataset_set=()):
        """
        Tag datasets.

        Args:
            tag_name (str)
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

        r = self.session.post(
            endpoint_url,
            data=payload)

        if not r.ok:
            raise Exception('failed with error: "{}", reason: "{}"'.format(
                r.reason, r.text))

        return r.json()

