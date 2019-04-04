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


def write_csv_with_types(data, filename, header=True):
    """ Write data frame to csv with types in accompanying yaml.
    Args:
        data (DataFrame): data to serialize
        filename (str): gzipped csv filename

    KwArgs:
        header (boolean): write header into csv
    """

    if len(data.columns) != len(data.columns.unique()):
        raise ValueError('duplicate columns not supported')

    data.to_csv(filename, compression='gzip', index=False, header=header)

    metadata = {}
    metadata['header'] = header
    metadata['columns'] = []
    for column, dtype in data.dtypes.iteritems():
        metadata['columns'].append({
            'name': column,
            'dtype': pandas_to_std_types[str(dtype)],
        })

    yaml_filename = filename + '.yaml'
    with open(yaml_filename, 'w') as f:
        yaml.dump(metadata, f, default_flow_style=False)


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
    },
    '_alignment_metrics.h5': {
        '/alignment/metrics': '_alignment_metrics',
        '/alignment/gc_metrics': '_gc_metrics',
    },
    '_hmmcopy.h5': {
        '/hmmcopy/segments/0': '_multiplier0_segments',
        '/hmmcopy/segments/1': '_multiplier1_segments',
        '/hmmcopy/segments/2': '_multiplier2_segments',
        '/hmmcopy/segments/3': '_multiplier3_segments',
        '/hmmcopy/segments/4': '_multiplier4_segments',
        '/hmmcopy/segments/5': '_multiplier5_segments',
        '/hmmcopy/segments/6': '_multiplier6_segments',
        '/hmmcopy/reads/0': '_multiplier0_reads',
        '/hmmcopy/reads/1': '_multiplier1_reads',
        '/hmmcopy/reads/2': '_multiplier2_reads',
        '/hmmcopy/reads/3': '_multiplier3_reads',
        '/hmmcopy/reads/4': '_multiplier4_reads',
        '/hmmcopy/reads/5': '_multiplier5_reads',
        '/hmmcopy/reads/6': '_multiplier6_reads',
        '/hmmcopy/params/0': '_multiplier0_params',
        '/hmmcopy/params/1': '_multiplier1_params',
        '/hmmcopy/params/2': '_multiplier2_params',
        '/hmmcopy/params/3': '_multiplier3_params',
        '/hmmcopy/params/4': '_multiplier4_params',
        '/hmmcopy/params/5': '_multiplier5_params',
        '/hmmcopy/params/6': '_multiplier6_params',
        '/hmmcopy/metrics/0': '_multiplier0_metrics',
        '/hmmcopy/metrics/1': '_multiplier1_metrics',
        '/hmmcopy/metrics/2': '_multiplier2_metrics',
        '/hmmcopy/metrics/3': '_multiplier3_metrics',
        '/hmmcopy/metrics/4': '_multiplier4_metrics',
        '/hmmcopy/metrics/5': '_multiplier5_metrics',
        '/hmmcopy/metrics/6': '_multiplier6_metrics',
    },
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


