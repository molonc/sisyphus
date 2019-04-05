import docker
import sys
import os
import logging
import pandas as pd

from datamanagement.utils.constants import LOGGING_FORMAT


# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)


def convert_python2_hdf5_to_msgpack(h5_filepath, key, msgpack_filepath):
    h5_filepath = os.path.realpath(h5_filepath)
    msgpack_filepath = os.path.realpath(msgpack_filepath)

    directory = os.path.dirname(h5_filepath)

    client = docker.from_env()
    client.containers.run(
        'amcpherson/py2hdfread:latest', ['to-msgpack', h5_filepath, key, msgpack_filepath],
        volumes={
            directory: {'bind': directory, 'mode': 'rw'}
        }
    )


def convert_python2_hdf5_to_csv(h5_filepath, key, csv_filepath):
    h5_filepath = os.path.realpath(h5_filepath)
    csv_filepath = os.path.realpath(csv_filepath)

    directory = os.path.dirname(h5_filepath)

    client = docker.from_env()
    client.containers.run(
        'amcpherson/py2hdfread:latest', ['to-csv', h5_filepath, key, csv_filepath],
        volumes={
            directory: {'bind': directory, 'mode': 'rw'}
        }
    )


def get_python2_hdf5_keys(h5_filepath):
    h5_filepath = os.path.realpath(h5_filepath)
    key_filepath = h5_filepath + '._temp_keys'

    directory = os.path.dirname(h5_filepath)

    client = docker.from_env()
    client.containers.run(
        'amcpherson/py2hdfread:latest', ['get-keys', h5_filepath, key_filepath],
        volumes={
            directory: {'bind': directory, 'mode': 'rw'}
        }
    )

    with open(key_filepath, 'r') as f:
        keys = [l.rstrip() for l in f.readlines()]

    os.remove(key_filepath)

    return keys


def read_python2_hdf5_dataframe(h5_filepath, key):
    h5_filepath = os.path.realpath(h5_filepath)

    msgpack_filepath = h5_filepath + '.' + key.replace('/', '_') + '.msgpack'

    filepath_time = os.path.getmtime(h5_filepath)

    if not os.path.exists(msgpack_filepath) or filepath_time > os.path.getmtime(msgpack_filepath):
        logging.info('msgpack file {} doesnt exists, creating'.format(msgpack_filepath))
        convert_python2_hdf5_to_msgpack(h5_filepath, key, msgpack_filepath)

    else:
        logging.info('msgpack file {} exists'.format(msgpack_filepath))

    data = pd.read_msgpack(msgpack_filepath)

    # Fix columns names and string columns that are bytes
    data.columns = data.columns.astype(str)
    for col in data:
        try:
            newcol = data[col].str.decode('utf-8')
        except AttributeError:
            continue
        if not newcol.isnull().any():
            data[col] = newcol

    return data


if __name__ == '__main__':
    data = pd.DataFrame({'a': [1, 2], 'b': [1., 2.], 'c': ['1', '2']})
    print(data)
    with pd.HDFStore('test.h5', 'w') as store:
        store.put('test', data, format='table')
    data = read_python2_hdf5_dataframe('test.h5', 'test')
    print(data)
    print(data.dtypes)
    data = read_python2_hdf5_dataframe('test.h5', 'test')
    print(data)


