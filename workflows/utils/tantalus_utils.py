import os
from tantalus_client import tantalus, generic_tasks
import workflows.templates


def check_gsc_lane_id(lane_id):
    if not templates.LANE_ID_RE.match(lane_id):
        raise Exception("Invalid GSC lane {}".format(lane_id))


def get_dlp_bams(library_id, analysis_id, lane_ids):
    """
    Return a set of ids for bam sequence datasets from a particular library
    that match the provided arguments.
    """
    assert analysis_id is not None and analysis_id
    assert lane_ids is not None and lane_ids

    sequence_datasets = set()
    file_resources = set()

    datasets = generic_tasks.tantalus_list(
        'sequence_dataset',
        library__library_id=library_id,
        dataset_type='BAM',
    )

    for dataset in datasets:
        if (lane_ids is not None) and (not sequence_dataset_match_lanes(dataset, lane_ids)):
            continue

        if dataset['analysis'] is None:
            generic_tasks.tantalus_update('sequence_dataset', dataset['id'], analysis=analysis_id)
            dataset['analysis'] = analysis_id

        if dataset['analysis'] != analysis_id:
            raise Exception('Sequence dataset {} is associated with analysis {} and not analysis'.format(
                dataset['id'], dataset['analysis'], analysis_id))

        sequence_datasets.add(dataset['id'])
        file_resources.update(dataset['file_resources'])

    if len(file_resources) == 0:
       raise Exception('No sequence datasets that match lanes {}'.format(lane_ids))

    return sequence_datasets


def sequence_dataset_match_lanes(dataset, lane_ids):
    if lane_ids is None:
        return True

    dataset_lanes = get_lanes_from_dataset(dataset)
    return set(lane_ids) == set(dataset_lanes)


def get_flowcell_lane(lane):
    if lane['lane_number'] == '':
        return lane['flowcell_id']
    else:
        return '{}_{}'.format(lane['flowcell_id'], lane['lane_number'])


def get_lanes_from_dataset(dataset):
    """
    Return a list of lanes given a dataset, where each lane has the format
    [flowcell_id]_[lane_number].
    Args:
        dataset (dict)
    Returns:
        lanes (list)
    """
    lanes = set()
    for lane in dataset['sequence_lanes']:
        lanes.add(get_flowcell_lane(lane))
    return lanes


def get_sequencing_centre_from_dataset(dataset):
    """
    Return a sequencing centre (e.g. GSC, BRC) given a dataset.
    An error is thrown if more than one sequencing centre is found.
    Args:
        dataset (dict)
    Returns:
        sequencing_centre (str)
    """

    sequencing_centres = {lane['sequencing_centre'] for lane in dataset['sequence_lanes']}

    if len(sequencing_centres) != 1:
        raise Exception('{} sequencing centers found for dataset {}'.format(
            len(sequencing_centres),
            dataset['id'])
        )

    return list(sequencing_centres).pop()


def get_sequencing_instrument_from_dataset(dataset):
    """
    Return a sequencing instrument given a dataset.
    An error is thrown if more than one sequencing instrument is found.
    Args:
        dataset (dict)
    Returns:
        sequencing_instrument (str)
    """

    sequencing_instruments = {lane['sequencing_instrument'] for lane in dataset['sequence_lanes']}

    if len(sequencing_instruments) != 1:
        raise Exception('{} sequencing instruments found for dataset {}'.format(
            len(sequencing_instruments),
            dataset['id'])
        )

    return list(sequencing_instruments).pop()


def file_resource_match_location(file_resource, storage_name):
    """
    Given a file resource and location, checks if any file instances
    for that file resource exist in the location. Data must be
    transferred if this returns False.
    Args:
        file_resource (dict)
        storage_name (str)
    """
    for file_instance in file_resource['file_instances']:
        if file_instance['storage']['name'] == storage_name:
            return True

    raise Exception('File resource {} expected in storage {}'.format(file_resource['id'], storage_name))


def get_file_instance(file_resource, storage_name):
    """
    Given a file resource and a storage name, return the matching instance.
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
        file_resource['id']
    ))


def get_storage_prefix(storage):
    """
    Get the prefix for the storage.
    Args:
        storage (dict)
    """

    storage_type = storage['storage_type']
    if storage_type == 'blob':
        return os.path.join(storage['storage_account'], storage['storage_container'])
    elif storage_type == 'server':
        return storage['storage_directory']

    raise Exception('Unrecognized storage type')


def get_file_instance_path(file_resource, storage_name):
    """
    Get the file path for a file instance based on a specific storage.
    Args:
        file_resource (dict)
        storage_name (str)
    """

    file_instance = get_file_instance(file_resource, storage_name)

    if file_instance['filename_override']:
        return file_instance['filename_override']

    prefix = get_storage_prefix(file_instance['storage'])
    return os.path.join(prefix, file_resource['filename'])
