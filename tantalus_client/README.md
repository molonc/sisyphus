# tantalus_client

This library acts as a direct interface with the Tantalus REST API.
It uses coreapi for extracting and pushing data to and from Tantalus.


tantalus_client is divided into three parts: tasks related to Analysis objects, tasks related to Results objects and all other tasks.


## tantalus_analysis

**create_analysis**

Checks if an analysis object with the given name exists in Tantalus and 
creates it if it doesn't exist.

Args:
* name: name of the analysis object
* jira_ticket: Jira ticket associated with the analysis
* last_updated: date and time the analysis object was last updated (only used when creating an analysis object)
* args: string of command line arguments for the analysis
* status: run status of the analysis

Returns:
* Analysis with the given parameters

**analysis_update**

Update the analysis object with the given ID with the given field

Args:
* id: the ID of the analysis object to update
* fields: analysis object fields to update
Returns:
* Analysis with the updated parameters


## tantalus_results

**query_for_sample**

Get the sample object from Tantalus for the given sample ID

Args:
* sample_id: sample ID to query (e.g. SA501)
Returns:
* Sample with the given sample ID

**create_results**

Checks if an results object with the given name exists in Tantalus and 
creates it if it doesn't exist.

Args:
* name: name of the results object
* results_type: type of analysis that generated the results
* results_version: pipeline version that generated the results
* analysis: ID of the associated analysis object
* sample_ids: IDs of the associated samples (primary IDs in Tantalus, not SA501)
* file_resources: IDs of the associated file resources

Returns:
* Reults with the given parameters


## tantalus

**get_storage**

Get a storage by name

Args:
* storage_name: storage name (e.g., singlecellblob, shahlab, gsc)

Returns:
* Storage object with the given name

**get_storage_id**

Get a storage by name and return its ID

Args:
* storage_name: storage name (e.g., singlecellblob, shahlab, gsc)

Returns:
* ID of the storage object with the given name

**get_sequence_datasets**

Get all sequence datasets for a library and sample

Args:
* library_id: DLP library ID
* sample_id: sample ID

Returns:
* List of sequence datasets that match the provided arguments

**get_file_resource**

Get a file resource from its ID

Args:
* resource_id: file resource ID

Returns:
* File resource object with the given ID

**tag_datasets**

Tag a list of datasets for later reference

Args:
* datasets: list of sequence dataset IDs
* tag_name: name with which to tag the datasets

Returns:
* Tag object with the given parameters

**push_bcl2fastq_paths**

Push paths of fastqs generated from bcl2fastq

Args:
* outputs: dictionary of flowcell IDs and paths to fastqs
* storage: name of the storage of the fastqs

Returns:
* ID of the import tasks

**push_bams**

Push paths of bams

Args:
* bams: list of bam paths
* storage: ID of the storage
* name: name of the task

Returns:
* Import DLP bam task with the given parameters

**transfer_files**

Transfer sequence datasets

Args:
* source_storage: ID of the source of the files
* destination_storage: ID of the destination of the files
* tag_name: tag name of the files to be transferred
* transfer_name: name of the transfer

Returns:
* File transfer object with the given parameters

**query_gsc_for_dlp_fastqs**

Queries the GSC for fastqs and returns the ID of the query instance

Args:
* dlp_library_id: DLP library ID
* gsc_library_id: GSC library ID

Returns:
* Query GSC object with the given parameters

**query_gsc_for_wgs_bams**

Queries the GSC for WGS bams and returns the ID of the query instance

Args:
* library_ids: list of GSC libraries
* name: name of the query

Returns:
* Query GSC object with the given parameters

**create_file_resource**

Creates file resource object in Tantalus

Args:
* filename: path to the file
* file_type: type of file (BAM, BAI, LOG, YAML, DIR)
* is_folder: whether the file resource is a folder

Returns:
* File resource object with the given parameters


### Requirements

```
pip install coreapi openapi_codec
```

`TANTALUS_API_USER` and `TANTALUS_API_PASSWORD` must also be set an environment variables.

By default, tantalus_client uses `http://tantalus.bcgsc.ca/api/` as the API URL, but it can be set as the environment variable `TANTALUS_API_URL`.
The OpenAPI document can be found at `http://tantalus.bcgsc.ca/api/swagger/?format=openapi`.
A human-readable version is in `document.txt`.


