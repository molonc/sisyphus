import pandas as pd
import sys
import os
import click
import yaml


pandas_to_std_types = {
    "bool": "boolean",
    "int64": "int",
    "float64": "float",
    "object": "str",
}


def write_csv_with_types(data, filename):
    """ Write data frame to csv with types in accompanying yaml.
    Args:
        data: DataFrame
        filename: csv filename ends with '.csv' or '.csv.gz'
    """

    if len(data.columns) != len(data.columns.unique()):
        raise ValueError('duplicate columns not supported')

    data.to_csv(filename, compression='gzip', index=False)

    typeinfo = {}
    for column, dtype in data.dtypes.iteritems():
        typeinfo[column] = pandas_to_std_types[str(dtype)]

    yaml_filename = filename + '.yaml'
    with open(yaml_filename, 'w') as f:
        yaml.dump(typeinfo, f, default_flow_style=False)


@click.group()
def convert():
    pass


@convert.command()
@click.argument('h5_filepath')
@click.argument('key')
def to_msgpack(h5_filepath, key):
    msgpack_filepath = h5_filepath + '.' + key.replace('/', '_') + '.msgpack'
    store = pd.HDFStore(h5_filepath, 'r')
    data = store[key]
    data.to_msgpack(msgpack_filepath)


h5_key_name_map = {
    '_destruct.h5': {
        '/breakpoint': '_destruct_breakpoint',
        '/breakpoint_library': '_destruct_breakpoint_library',
    },
    '_snv_counts.h5': {
        '/snv_allele_counts': '_snv_union_counts',
    },
    '_snv_annotations.h5': {
        '/snv_allele_counts': '_snv_allele_counts',
        '/museq/vcf': '_snv_museq',
        '/snv/cosmic_status': '_snv_cosmic_status',
        '/snv/dbsnp_status': '_snv_dbsnp_status',
        '/snv/mappability': '_snv_mappability',
        '/snv/snpeff': '_snv_snpeff',
        '/snv/tri_nucleotide_context': '_snv_trinuc',
        '/strelka/vcf': '_snv_strelka',
    }
}


@convert.command()
@click.argument('h5_filepath')
def to_csv(h5_filepath):
    key_name_map = None
    for suffix in h5_key_name_map:
        if h5_filepath.endswith(suffix):
            key_name_map = h5_key_name_map[suffix]
            h5_prefix = h5_filepath[:-len(suffix)]
    if key_name_map is None:
        raise Exception('unknown suffix')

    store = pd.HDFStore(h5_filepath)
    for key in store:
        if key.endswith('meta'):
            continue

        csv_filepath = h5_prefix + key_name_map[key] + '.csv.gz'

        write_csv_with_types(store[key], csv_filepath)


if __name__ == '__main__':
    convert()


