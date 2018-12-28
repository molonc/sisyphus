import sys
import os
import json
import click
from dbclients.tantalus import TantalusApi


@click.command()
@click.argument('storage_name', nargs=1)
@click.option('--dry_run', is_flag=True)
def main(
    storage_name,
    dry_run=False,
):
    tantalus_api = TantalusApi()

    storage_client = tantalus_api.get_storage_client(storage_name)

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
        if len(file_resource['file_instances']) == 1:
            assert file_resource['file_instances'][0]['id'] == file_instance['id']
            print('deleting file resource {}'.format(file_resource['id']))
            if not dry_run:
                tantalus_api.delete('file_resource', id=file_resource['id'])


if __name__ == "__main__":
    main()
