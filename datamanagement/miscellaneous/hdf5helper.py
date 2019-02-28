import docker
import sys
import os
import logging
import pandas as pd

from datamanagement.utils.constants import LOGGING_FORMAT


# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)


def read_python2_hdf5_dataframe(h5_filepath, key):
    h5_filepath = os.path.realpath(h5_filepath)
    directory = os.path.dirname(h5_filepath)

    msgpack_filepath = h5_filepath + '.' + key.replace('/', '_') + '.msgpack'

    filepath_time = os.path.getmtime(h5_filepath)

    if not os.path.exists(msgpack_filepath) or filepath_time > os.path.getmtime(msgpack_filepath):
        logging.info('msgpack file {} doesnt exists, creating'.format(msgpack_filepath))
        client = docker.from_env()
        client.containers.run(
            'amcpherson/py2hdfread:latest', [h5_filepath, key],
            volumes={
                directory: {'bind': directory, 'mode': 'rw'}
            }
        )
    else:
        logging.info('msgpack file {} exists'.format(msgpack_filepath))
    output = pd.read_msgpack(msgpack_filepath)
    return output


if __name__ == '__main__':
    data = pd.DataFrame({'a': [1, 2], 'b': [1., 2.], 'c': ['1', '2']})
    print(data)
    with pd.HDFStore('test.h5', 'w') as store:
        store.put('test', data, format='table')
    print(read_python2_hdf5_dataframe('test.h5', 'test'))
    print(read_python2_hdf5_dataframe('test.h5', 'test'))


