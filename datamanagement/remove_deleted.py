import sys
import os
import json
import click
import logging
from dbclients.tantalus import TantalusApi
from utils.constants import LOGGING_FORMAT


logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)


@click.command()
@click.argument('storage_name', nargs=1)
@click.option('--dry_run', is_flag=True)
@click.option('--check_remote')
def main(
    storage_name,
    dry_run=False,
    check_remote=None,
):
    tantalus_api = TantalusApi()

    storage_client = tantalus_api.get_storage_client(storage_name)

    remote_storage_client = None
    if check_remote is not None:
        remote_storage_client = tantalus_api.get_storage_client(check_remote)

    file_instances = tantalus_api.list(
        'file_instance',
        storage__name=storage_name,
        is_deleted=True)

    # DEBUG: check whether we are getting back
    # consistent ordered results from tantalus
    file_instances = list(file_instances)
    file_instance_ids = set([f['id'] for f in file_instances])
    if len(file_instances) != len(file_instance_ids):
        raise Exception('received duplicate results from tantalus')

    logging.info('processing {} file instances'.format(
        len(file_instance_ids)))
    logging.info('processing the following file instances: {}'.format(
        str(file_instance_ids)))

    for file_instance in file_instances:
        file_resource = tantalus_api.get('file_resource', id=file_instance['file_resource']['id'])
        all_file_instances = list(tantalus_api.list('file_instance', file_resource=file_resource['id']))

        logging.info('checking file instance {}, file resource {}, filepath {}'.format(
            file_instance['id'], file_resource['id'], file_instance['filepath']))

        sequencedatasets = tantalus_api.list('sequencedataset', file_resources__id=file_resource['id'])
        resultsdatasets = tantalus_api.list('resultsdataset', file_resources__id=file_resource['id'])

        sequencedataset_ids = list(set([a['id'] for a in sequencedatasets]))
        resultsdataset_ids = list(set([a['id'] for a in resultsdatasets]))

        logging.info('file resource {} belongs to sequencedataset {} and resultsdatasets {}'.format(
            file_resource['id'], sequencedataset_ids, resultsdataset_ids))

        # Optionally check for a remote version
        if remote_storage_client:
            remote_instance = None
            for other_instance in file_resource['file_instances']:
                if other_instance['storage']['name'] == check_remote:
                    remote_instance = other_instance

            if not remote_instance:
                logging.info('not deleting file instance {}, no other instance'.format(
                    file_instance['id']))
                continue

            if remote_instance['is_deleted']:
                logging.info('not deleting file instance {}, other instance {} deleted'.format(
                    file_instance['id'], other_instance['id']))
                continue

            if not remote_storage_client.exists(file_resource['filename']):
                logging.info('not deleting file instance {}, other instance {} doesnt exist'.format(
                    file_instance['id'], other_instance['id']))
                continue

            logging.info('deletion ok for file instance {}, found other instance {}'.format(
                file_instance['id'], other_instance['id']))

        # Delete the file from the filesystem
        logging.info('deleting file {}'.format(file_instance['filepath']))
        if not dry_run:
            try:
                storage_client.delete(file_resource['filename'])
            except FileNotFoundError:
                logging.exception('file already deleted')

        # Delete the instance model from tantalus
        logging.info('deleting file instance {}'.format(file_instance['id']))
        if not dry_run:
            tantalus_api.delete('file_instance', id=file_instance['id'])

        # If this is the only file instance for this file resource, delete the file resource
        if len(all_file_instances) == 1:
            assert file_resource['file_instances'][0]['id'] == file_instance['id']
            logging.info('deleting file resource {}'.format(file_resource['id']))
            if not dry_run:
                tantalus_api.delete('file_resource', id=file_resource['id'])


if __name__ == "__main__":
    main()
