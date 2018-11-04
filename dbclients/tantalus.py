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
import pandas as pd
from django.core.serializers.json import DjangoJSONEncoder
from dbclients.basicclient import BasicAPIClient
import azure.storage.blob

TANTALUS_API_URL = "http://tantalus.bcgsc.ca/api/"


class BlobStorageClient(object):
    def __init__(self, storage):
        self.storage_account = storage['storage_account']
        self.storage_container = storage['storage_container']
        self.storage_key = storage['credentials']['storage_key']
        self.prefix = storage['prefix']

        self.blob_service = azure.storage.blob.BlockBlobService(
            account_name=self.storage_account,
            account_key=self.storage_key)

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
            self.storage_account,
            blobname,
            permission=azure.storage.blob.BlobPermissions.READ,
            expiry=datetime.datetime.utcnow() + datetime.timedelta(hours=1),
        )
        blob_url = self.blob_service.make_blob_url(
            container_name=self.container_name,
            blob_name=blobname,
            protocol="http",
            sas_token=sas_token,
        )
        return blob_url


class ServerStorageClient(object):
    def __init__(self, storage):
        self.storage_directory = storage['storage_directory']

    def get_size(self, filename):
        filepath = os.path.join(self.storage_directory, filename)
        return os.path.getsize(filepath)

    def get_created_time(self, filename):
        filepath = os.path.join(self.storage_directory, filename)
        return pd.Timestamp(time.ctime(os.path.getmtime(filepath)), tz="Canada/Pacific")
    
    def get_url(self, filename):
        filepath = os.path.join(self.storage_directory, filename)
        return filepath


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
            storage['credentials'] = self.get(
                'storage_azure_blob_credentials',
                id=storage['credentials'])
            client = BlobStorageClient(storage)
        elif storage['storage_type'] == 'server':
            client = ServerStorageClient(storage)
        else:
            return ValueError('unsupported storage type {}'.format(storage['storage_type']))

        self.cached_storage_clients[storage_name] = client

        return client

    def add_file(self, storage_name, filepath, file_type, **args):
        """ Create a file resource and file instance in the give storage.

        Args:
            storage_name: storage for file instance
            filepath: full path to file
            file_type: type for file_resource
        
        Kwargs:
            additional fields for file_resource

        Returns:
            file_resource, file_instance
        """
        storage = self.get_storage(storage_name)
        storage_client = self.get_storage_client(storage_name)

        filename = self.get_file_resource_filename(storage_name, filepath)

        file_resource = self.get_or_create(
            'file_resource',
            filename=filename,
            file_type=file_type,
            created=storage_client.get_created_time(filename),
            size=storage_client.get_size(filename),
            **args
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

        raise Exception('no file instance in storage {} found for resource {}'.format(
            storage_name,
            file_resource['id'],
        ))

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

    @staticmethod
    def join_urls(*pieces):
        """Join pieces of an URL together safely."""
        return "/".join(s.strip("/") for s in pieces) + "/"

    def sequence_dataset_add(self, model_dictionaries, tag_name=None):
        """POST to the sequence_dataset_add endpoint.

        Args:
            model_dictionaries: A list of dictionaries containing
                information about a model to create.
            tag_name: An optional string (or None) containing the name
                of the tag to associate with the model instances
                represented in the model_dictionaries.

        Raises:
            RuntimeError: The request returned with a non-2xx status
                code.
        """
        endpoint_url = self.join_urls(self.base_api_url, "/sequence_dataset_add/")

        payload = json.dumps(
            {"model_dictionaries": model_dictionaries, "tag": tag_name},
            cls=DjangoJSONEncoder,
        )

        r = self.session.post(endpoint_url, data=payload)

        try:
            # Ensure that the request was successful
            assert 200 <= r.status_code < 300
        except AssertionError:
            msg = (
                "Request to {url} failed with status {status_code}:\n"
                "The reponse from the request was as follows:\n\n"
                "{content}"
            ).format(url=endpoint_url, status_code=r.status_code, content=r.text)

            raise RuntimeError(msg)


