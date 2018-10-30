import os
import datetime
from generic_tasks import get_or_create, wait_for_finish, make_tantalus_query


def get_storage(storage_name):
    """
    Get a storage by name
    Args:
        storage_name: storage name (e.g., singlecellblob, shahlab, gsc)
    Returns:
        Storage object with the given name
    """

    storage = make_tantalus_query('storage', {'name': storage_name})

    if len(storage) != 1:
        raise Exception('Found {} storages with name {}'.format(len(storage), storage))

    return storage.pop()


def get_storage_id(storage_name):
    """
    Get a storage by name and return its ID
    Args:
        storage_name: storage name (e.g., singlecellblob, shahlab, gsc)
    Returns:
        ID of the storage object with the given name
    """

    return get_storage(storage_name)['id']


def get_sequence_datasets(name=None, library_id=None, sample_id=None, tag=None, dataset_type=None):
    """
    Get all sequence datasets for a library and sample
    Args:
        name: name of the dataset
        library_id: DLP library ID
        sample_id: sample ID
        tag: tags on the datasets
    Returns:
        List of sequence datasets that match the provided arguments
    """
    
    table = 'sequence_dataset'
    params = {}

    if name is not None:
        params['name'] = name
    if tag is not None:
        params['tags__name'] = tag
    if library_id is not None:
        params['library__library_id'] = library_id
    if sample_id is not None:
        params['sample__sample_id'] = sample_id
    if dataset_type is not None:
	params['dataset_type'] = dataset_type

    return make_tantalus_query(table, params)


def get_file_resource(resource_id):
    """
    Get a file resource from its ID
    Args:
        resource_id: file resource ID
    Returns:
        File resource object with the given ID
    """

    return make_tantalus_query('file_resource', params = {'id': resource_id}).pop()


def tag_datasets(datasets, tag_name):
    """
    Tag a list of datasets for later reference
    Args:
        datasets: list of sequence dataset IDs
        tag_name: name with which to tag the datasets
    Returns:
        Tag object with the given parameters
    """

    return get_or_create('sequence_dataset_tag',
        name=tag_name, sequencedataset_set=list(datasets))


def push_bcl2fastq_paths(outputs, storage):
    """
    Push paths of fastqs generated from bcl2fastq
    Args:
        outputs: dictionary of flowcell IDs and paths to fastqs
        storage: name of the storage of the fastqs
    Returns:
        ID of the import tasks
    """

    task_ids = []

    for flowcell_id, output in outputs.items():
        if output.startswith('/genesis'):
            output = output[len('/genesis'):]

        response = get_or_create(
            'brc_import_fastqs',
            output_dir=output,
            storage=storage,
            flowcell_id=flowcell_id,
            name='{flowcell_id}_{storage}'.format(flowcell_id=flowcell_id, storage=storage)
        )

        tasks_ids.append(response['id'])

    wait_for_finish('brc_import_fastqs', task_ids)

    return task_ids


def push_bams(bams, storage, name):
    """
    Push paths of bams
    Args:
        bams: list of bam paths
        storage: ID of the storage
        name: name of the task
    Returns:
        Import DLP bam task with the given parameters
    """

    response = get_or_create(
        'import_dlp_bam',
        bam_paths=list(bams),
        storage=storage,
        name=name
    )
    wait_for_finish('import_dlp_bam', [response['id']])

    return response


def transfer_files(source_storage, destination_storage, tag_name, transfer_name):
    """
    Transfer sequence datasets
    Args:
        source_storage: ID of the source of the files
        destination_storage: ID of the destination of the files
        tag_name: tag name of the files to be transferred
        transfer_name: name of the transfer
    Returns:
        File transfer object with the given parameters
    """

    response = get_or_create(
        'file_transfer',
        name=transfer_name, tag_name=tag_name, from_storage=source_storage, to_storage=destination_storage)

    if not response['running'] and response['finished'] and not response['success']:
        # File transfer finished but did not succeed
        raise Exception('File transfer started but did not finish. \
            Try restarting it at http://tantalus.bcgsc.ca/filetransfers/{}'.format(response['id']))

    wait_for_finish('file_transfer', [response['id']])

    return response


def query_gsc_for_dlp_fastqs(dlp_library_id, gsc_library_id):
    """
    Queries the GSC for fastqs and returns the ID of the query instance
    Args:
        dlp_library_id: DLP library ID
        gsc_library_id: GSC library ID
    Returns:
        Query GSC object with the given parameters
    """

    response = get_or_create(
        'query_gsc_dlp_paired_fastqs',
        dlp_library_id=dlp_library_id,
        gsc_library_id=gsc_library_id,
        name=dlp_library_id
    )
    wait_for_finish('query_gsc_dlp_paired_fastqs', [response['id']])

    return response


def query_gsc_for_wgs_bams(library_ids, name):
    """
    Queries the GSC for WGS bams and returns the ID of the query instance
    Args:
        library_ids: list of GSC libraries
        name: name of the query
    Returns:
        Query GSC object with the given parameters
    """

    response = get_or_create(
        'query_gsc_wgs_bams',
        library_ids=list(library_ids),
        name=name
     )
    wait_for_finish('query_gsc_wgs_bams', [response['id']])

    return response


def create_file_resource(source_file, filename, file_type, is_folder=False):
    """
    Creates file resource object in Tantalus
    Args:
        filename: path to the file
        file_type: type of file (BAM, BAI, LOG, YAML, DIR)
        is_folder: whether the file resource is a folder
    Returns:
        File resource object with the given parameters
    """

    if filename.endswith('.gz'):
        compression = 'GZIP'
    elif filename.endswith('.bz2'):
        compression = 'BZIP2'
    elif filename.endswith('.spec'):
        compression = 'SPEC'
    else:
        compression = 'UNCOMPRESSED'

    created = datetime.datetime.fromtimestamp(os.path.getmtime(source_file)).isoformat()

    size = os.path.getsize(source_file)

    response = get_or_create(
        'file_resource',
        filename=filename,
        compression=compression,
        created=created,
        file_type=file_type,
        size=size
    )

    return response
