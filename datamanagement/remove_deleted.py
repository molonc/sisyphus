import sys
import os
import json
import click
from dbclients.tantalus import TantalusApi


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

    for file_instance in file_instances:
        file_resource = tantalus_api.get('file_resource', id=file_instance['file_resource'])
        print('deleting file {}'.format(file_instance['filepath']))
        if not dry_run:
            storage_client.delete(file_resource['filename'])
        print('deleting file instance {}'.format(file_instance['id']))
        if not dry_run:
            tantalus_api.delete('file_instance', id=file_instance['id'])
        if remote_storage_client:
            remote_instance = None
            for other_instance in file_resource['file_instances']:
                if other_instance['storage']['name'] == check_remote:
                    remote_instance = other_instance
            if not remote_instance:
                print('not deleting, no other instance')
                continue
            if remote_instance['is_deleted']:
                print('not deleting, other instance deleted')
                continue
            if not remote_storage_client.exists(file_resource['filename']):
                print('not deleting, other instance doesnt exist')
                continue
            print('found other instance')
        if len(file_resource['file_instances']) == 1:
            assert file_resource['file_instances'][0]['id'] == file_instance['id']
            print('deleting file resource {}'.format(file_resource['id']))
            if not dry_run:
                tantalus_api.delete('file_resource', id=file_resource['id'])


if __name__ == "__main__":
    main()
