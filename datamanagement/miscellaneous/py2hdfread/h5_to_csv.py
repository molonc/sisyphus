import time
import os
import logging
import pandas as pd
import numpy as np

import ruamel.yaml as yaml
from pandas.testing import assert_frame_equal
from ruamel.yaml.scanner import ScannerError
import logging


logging.basicConfig(
    format="%(levelname)s:%(asctime)s:%(message)s",
    datefmt="%y%m%d-%H%M%S",
    level=logging.INFO,
)
logger = logging.getLogger("")

root = "/path/to/data/files"

pandas2std_types = {
    "bool": "boolean",
    "int64": "int",
    "float64": "float",
    "object": "str",
}

std2pandas_types = {
    "boolean": "bool",
    "int": "int64",
    "float": "float64",
    "str": "object",
}

yaml_pref = """
__PIPELINE_INFO__:
  data_type: generic
__HEADER__:
  caller: single_cell_hmmcopy_bin
  sample_id: SAMPLE_ID_HERE
  file_format: csv
"""

gz = ".gz"
csv = ".csv"
suff_old = ".h5"
suff_new = "%s%s" % (csv, gz)
suff_yaml = ".yaml"


class Timeit:
    def __init__(self, name=None):
        self.name = " '" + name + "'" if name else ""

    def __enter__(self):
        self.start = time.clock()

    def __exit__(self, exc_type, exc_value, traceback):
        self.t = time.clock() - self.start  # * 1000.0
        logger.info("Block %s; t=%f (s)" % (self.name, self.t))


def diffs(x):
    return "{}+".format(x[0]) if x[0] == x[1] else "{}; {}".format(x[0], x[0] - x[1])


# https://stackoverflow.com/questions/17095101/outputting-difference-in-two-pandas-dataframes-side-by-side-highlighting-the-d
def show_diff(df0, df1):
    """
    service function
    """
    df0.set_index("cell_id")
    df1.set_index("cell_id")
    df = pd.concat([df0, df1], axis="columns", keys=["0", "1"], join="outer")
    res = df.swaplevel(axis="columns")[df0.columns[1:]]
    x = res.groupby(level=0, axis=1).apply(lambda x: x.apply(diffs, axis=1))
    print(x)


def compare_df(df0, df1):
    """
    compares 2 dataframes first column names sets after this values column by column,
    return:
        True if df0 == df1 else False
    """
    cols = df0.dtypes[:]  # fillna could change datatype

    # panrdas imports hdf null values into a dataframe as null,
    # after saving to csv and reading they become nan and could not be compared to null.
    df0.fillna(
        value=0, inplace=True
    )  # probably will fail if nan-s are in non numeric fields
    df1.fillna(value=0, inplace=True)

    ##compare set of column names, types will be compared later
    l = set(df0.dtypes.keys()) == set(df1.dtypes.keys())
    if l:
        for name, type in cols.iteritems():
            if "float64" == type.name:
                l &= np.allclose(df0[name], df1[name])
            else:
                l &= list(df0[name]) == list(df1[name])
            logger.info("tested: %s; type=%s; isOk=%i;" % (name, type.name, l))
            if not l:
                logger.info(
                    "\n{};\n{}\n".format(list(df0[name])[:11], list(df1[name])[:11])
                )
                break
    logger.info("etc: {};".format(l))
    return l


def read_csv_with_types(filename):
    """
    Args:
        filename(str): filename ends with '.csv' or '.csv.gz'
    Returns:
        DataFrame
    """
    fnm = (
        filename[: -len(csv)] if filename.endswith(csv) else filename[: -len(suff_new)]
    )
    ynm = "%s%s" % (fnm, suff_yaml)
    print("yaml: %s" % (ynm))
    with open(ynm, "r") as f:
        l = "\n".join(f.readlines())
    yml = yaml.load(l)
    print(yml)
    if "__HEADER__" in yml:
        yml = yml["__HEADER__"]
    types = {k: std2pandas_types[v] for (k, v) in yml["field_types"].iteritems()}
    df = pd.read_csv(filename, dtype=types, index_col=False)
    df.to_csv(filename + ".xx", sep=",", encoding="utf-8", index=False)  # debug
    return df


def write_csv_with_types(df, filename, index=False, encoding="utf-8"):
    """
    writes df to gzipped or raw csv depending on extension
    see pandas to_csv()
    Args:
        df: DataFrame
        filename: str -- csv filename ends with '.csv' or '.csv.gz'
        rest as in pandas to_csv()
    Returns:

    """
    csv = ".csv"
    yaml = "%s.yaml" % (
        filename[: -len(csv)]
        if filename.endswith(csv)
        else filename[: -len(csv + ".gz")]
    )
    df.to_csv(filename, sep=",", encoding=encoding, index=index)  # ,compression='gzip')
    with open(yaml, "w") as fo:
        fo.write("field_types:\n")
        try:
            for k, v in df.dtypes.iteritems():
                print(k, v, pandas2std_types[v.name])
                fo.write("    %s        : %s\n" % (k, pandas2std_types[v.name]))
        except Exception as e:
            print(e)
            pass


def convert1(hdf5_filename):
    """
    args:
        full file name of the file to convert
    """
    ds = pd.HDFStore(path=hdf5_filename, mode="r")
    keys = ds.keys()
    fnm_old = hdf5_filename[: -len(suff_old)]
    print("all: %s;" % (keys))
    for k in keys:
        print("trying: %s;" % (k))
        fnm = "%s%s" % (fnm_old, k.replace("/", "_"))
        csv_filename = "%s%s" % (fnm, suff_new)
        with Timeit("select({})".format(fnm)):
            df = ds.select(k)
        with Timeit("convert-write; {}".format(csv_filename)):
            write_csv_with_types(df, csv_filename)
        df_test = read_csv_with_types(csv_filename)
        with Timeit("compare; {}".format(csv_filename)):
            assert compare_df(df, df_test)


def main():
    # convert1(os.path.join(root,'_alignment_metrics.h5'))
    convert1(os.path.join(root, "_hmmcopy.h5"))


def compare_df_test(isOk, a, b, msg):
    dfa = pd.DataFrame(a)
    dfb = pd.DataFrame(b)
    l = compare_df(dfa, dfb)
    res = bool(isOk) == bool(l)
    logging.info(
        "df_test();\ntesting {};\n{};\n{};      res={}; {}".format(
            msg, dfa, dfb, l, "OK" if res else "ERROR"
        )
    )
    return res


def main_test():
    a = {"f1": [1, 2, 3], "f2": [1.0, 2.0, 3.0], "f3": ["a", "b", "c"]}
    xx = {
        "different int": {
            "f1": [1, 2, 4],
            "f2": [1.0, 2.0, 3.0],
            "f3": ["a", "b", "c"],
        },
        "different float": {
            "f1": [1, 2, 4],
            "f2": [1.0, 2.5, 3.0],
            "f3": ["a", "b", "c"],
        },
        "different string": {
            "f1": [1, 2, 4],
            "f2": [1.0, 2.5, 3.0],
            "f3": ["a", "b", "d"],
        },
        "different field name": {
            "f1": [1, 2, 4],
            "f2": [1.0, 2.5, 3.0],
            "f4": ["a", "b", "c"],
        },
        "different field types 1": {
            "f2": [1, 2, 4],
            "f1": [1.0, 2.5, 3.0],
            "f3": ["a", "b", "c"],
        },
        "different field types 2": {
            "f3": [1, 2, 4],
            "f2": [1.0, 2.5, 3.0],
            "f1": ["a", "b", "c"],
        },
    }
    res = compare_df_test(1, a, a, "equal")
    for k, v in xx.iteritems():
        res &= compare_df_test(0, a, v, k)
    logging.info("============== test: {}".format("OK" if res else "ERROR"))


main_test()
