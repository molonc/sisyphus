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
            'name': str(column),
            'dtype': str(pandas_to_std_types[str(dtype)]),
        })

    yaml_filename = filename + '.yaml'
    with open(yaml_filename, 'w') as f:
        yaml.dump(metadata, f, default_flow_style=False)
