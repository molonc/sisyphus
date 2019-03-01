from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import logging
import click
import pandas as pd

import datamanagement.transfer_files
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError


def convert_hdf5_table(input_filename, output_filename):
    with pd.HDFStore(input_filename) as inputstore:
        with pd.HDFStore(output_filename, 'w', complevel=9, complib='blosc') as outputstore:
            tablenames = inputstore.keys()
            for tablename in tablenames:
                outputstore.put(tablename, inputstore[tablename], format='table')


@click.command()
@click.argument('prefix', nargs=1)
@click.argument('storage_name', nargs=1)
@click.argument('cache_directory', nargs=1)
def fix(prefix, storage_name, cache_directory):
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    tantalus_api = TantalusApi()

    file_resources = tantalus_api.list(
        'file_resource',
        filename__startswith=prefix,
        filename__endswith='.h5',
        fileinstance__storage__name=storage_name,
    )

    for file_resource in file_resources:
        logging.warning('fixing {}'.format(file_resource['filename']))

        file_instance = tantalus_api.get_file_instance(
            file_resource,
            storage_name,
        )

        filepath = datamanagement.transfer_files.cache_file(
            tantalus_api,
            file_instance,
            cache_directory,
        )

        logging.warning('convert hdf5 {}'.format(file_resource['filename']))

        output_filepath = filepath + '.new'

        convert_hdf5_table(filepath, output_filepath)

        logging.warning('updating tantalus {}'.format(file_resource['filename']))

        client = tantalus_api.get_storage_client(storage_name)
        client.create(file_resource['filename'], output_filepath)
        tantalus_api.update_file(file_instance)


if __name__ == '__main__':
    fix()
