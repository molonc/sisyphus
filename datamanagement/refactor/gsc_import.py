import logging
import asyncio # for async blob upload

from datamanagement.utils.gsc import GSCAPI

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
"""
User supplied internal ID
sequencing object (Colossus)
Colossus GSC ID -> either internal or external
    - need to make sure user supplied internal ID matches Colossus GSC ID

External ID
    - 

dlp_library_id = sequencing["library"]
gsc_library_id = sequencing["gsc_library_id"]
library_info = colossus_api.query_libraries_by_library_id(dlp_library_id)
jira_ticket = library_info["jira_ticket"]

primary_sample_id = library_info['sample']['sample_id']
cell_samples = query_colossus_dlp_cell_info(colossus_api, dlp_library_id)
rev_comp_overrides = query_colossus_dlp_rev_comp_override(colossus_api, dlp_library_id)

external_identifier = f"{primary_sample_id}_{dlp_library_id}"

def upload_to_azure(storage_client, blobname, filepath, update=False):
    if(storage_client.exists(blobname)):
        if(storage_client.get_size(blobname) == os.path.getsize(filepath)):
            logging.info(f"{blobname} already exists and is the same size. Skipping...")

            return
        else:
            if not(update):
                message = f"{blobname} has different size from {filepath}. Please specify --update option to overwrite."
                logging.error(message)
                raise ValueError(message)

    storage_client.create(
        blobname,
        filepath,
        update=update,
    )

def upload_blob_async(blob_infos, storage, tantalus_api):
    concurrency = 3
    async_blob_client = tantalus_api.get_storage_client(storage['name'], is_async=True, concurrency=concurrency)

    async_blob_upload_data = []
    for fastq_path, tantalus_filename, tantalus_path in blob_infos:
        if(storage['storage_type'] == 'server'):
            continue
        elif storage['storage_type'] == 'blob':
            async_blob_upload_data.append((tantalus_filename, fastq_path))
        else:
            raise ValueError("Unexpected storage type. Must be one of 'server' or 'blob'!")

    if(storage['storage_type'] == 'blob'):
        loop = asyncio.get_event_loop()
        loop.run_until_complete(async_blob_client.batch_upload_files(async_blob_upload_data))
"""

class GSC_Import(object):
    def __init__(
        self,
        colossus_sequencing=None,
        gsc_internal_id=None,
    ):
        #self.gsc_api = self._get_GSCApi()
        #self.colossus_api = self._get_ColossusApi()
        #self.tantalus_api = self._get_TantalusApi()

        self.gsc_api = GSCAPI()
        self.colossus_api = ColossusApi()
        self.tantalus_api = TantalusApi()

        self.gsc_internal_id = gsc_internal_id

    def _get_ColossusApi(self):
        return ColossusApi()

    def _get_TantalusApi(self):
        return TantalusApi()

    def _get_GSCApi(self):
        return GSCAPI()

    def upload_blobs(self, data, storage_account_name, is_async=False, concurrency=3, update=False):
        """
        Upload blobs to Azure storage account
        """
        if(is_async):
            self._upload_blobs_async(
                data=data,
                storage_account_name=storage_account_name,
                concurrency=concurrency,
                update=update,
            )
        else:
            self._upload_blobs_sync(
                data=data,
                storage_account_name=storage_account_name,
                update=update,
            )

    def _upload_blobs_async(self, data, storage_account_name, concurrency=3, update=False):
        """
        Asynchronously upload blob to Azure storage account.
        It detects duplicate file (same name, same size).

        Args:
            data (list of tuple): list of tuple (tantalus_filename, fastq_path)
            storage_account_name: name of the Azure Storage Account as tracked in Tantalus
            concurrency: number of concurrent workers to be uesd in the event loop
        """
        async_storage_client = self.tantalus_api.get_storage_client(storage_account_name, is_async=True, concurrency=concurrency)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(async_storage_client.batch_upload_files(data))

    def _upload_blobs_sync(self, data, storage_account_name, update=False):
        """
        Synchronously upload blob to Azure storage account.

        Args:
            data (list of tuple): list of tuple (tantalus_filename, fastq_path)
            storage_account_name: name of the Azure Storage Account as tracked in Tantalus
        """
        sync_storage_client = self.tantalus_api.get_storage_client(storage_account_name)

        for blobname, filepath in data:
            if(storage_client.exists(blobname)):
                if(storage_client.get_size(blobname) == os.path.getsize(filepath)):
                    logging.info(f"{blobname} already exists and is the same size. Skipping...")

                    continue
            else:
                if not(update):
                    message = f"{blobname} has different size from {filepath}. Please specify --update option to overwrite."
                    logging.error(message)
                    raise ValueError(message)

            storage_client.create(
                blobname,
                filepath,
                update=update,
            )

class GSC_DLP_Import(GSC_Import):
    def __init__(self):
        super().__init__(colossus_sequencing, gsc_internal_id)

    def fetch_data(self):
        """
        Fetch data from GSC
        """
        pass

    def process_data(self):
        """
        Process fetched data from GSC
        """
        pass

class GSC_TenX_Import(GSC_Import):
    def __init__(self):
        pass


