import pandas as pd
import sys
import os
import click
import yaml

import csv_utils


@click.group()
def convert():
    pass


@convert.command()
@click.argument('h5_filepath')
@click.argument('key')
@click.argument('msgpack_filepath')
def to_msgpack(h5_filepath, key, msgpack_filepath):
    store = pd.HDFStore(h5_filepath, 'r')
    data = store[key]
    data.to_msgpack(msgpack_filepath)


@convert.command()
@click.argument('h5_filepath')
@click.argument('key')
@click.argument('csv_filepath')
def to_csv(h5_filepath, key, csv_filepath):
    store = pd.HDFStore(h5_filepath)
    csv_utils.write_csv_with_types(store[key], csv_filepath)


@convert.command()
@click.argument('h5_filepath')
@click.argument('keys_filepath')
def get_keys(h5_filepath, keys_filepath):
    store = pd.HDFStore(h5_filepath)
    with open(keys_filepath, 'w') as f:
        for key in store:
            f.write(key + '\n')


if __name__ == '__main__':
    convert()


