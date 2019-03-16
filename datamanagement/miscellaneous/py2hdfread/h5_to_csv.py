import time
import os
import logging
import pandas as pd
import numpy as np

# import ruamel.yaml as yaml
import yaml as yaml
from pandas.testing import assert_frame_equal
from ruamel.yaml.scanner import ScannerError
import logging
import errno

import pprint as pp
import traceback

logging.basicConfig(
    format="%(levelname)s:%(asctime)s:%(message)s",
    datefmt="%y%m%d-%H%M%S",
    level=logging.INFO,
)
logger = logging.getLogger("")

# root = "/path/to/data/files"
# root = "~/w/bccrc/tt"
root = "/Users/ogolovko/w/bccrc/tt"

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


def logerr(msg=""):
    logger.error(
        "%s; %s;" % (msg, pp.pformat(traceback.format_list(traceback.extract_stack())))
    )


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


def read_csv_with_types(dir, basefilename):
    """
    Args:
        basefilename(str): filename ends with '.csv' or '.csv.gz'
    Returns:
        DataFrame
    """
    filename = os.path.join(dir, basefilename)
    ynm = "%s%s" % (filename, suff_yaml)
    logger.info("yaml: %s" % (ynm))
    with open(ynm, "r") as f:
        l = "\n".join(f.readlines())
    yml = yaml.load(l)
    if "__HEADER__" in yml:
        yml = yml["__HEADER__"]
    types = {k: std2pandas_types[v] for (k, v) in yml["field_types"].iteritems()}
    df = pd.read_csv(filename, dtype=types, index_col=False)
    # creating uncompressed file for testing
    # df.to_csv(filename + ".xx", sep=",", encoding="utf-8", index=False)  # debug
    return df


def write_csv_with_types(data, dir, filename, index=False, encoding="utf-8"):
    """
    writes df to gzipped or raw csv depending on extension
    see pandas to_csv()
    if filename contains directories then creates them
    Args:
        data: DataFrame
        dir: str -- output directory
        filename: str -- csv filename ends with '.csv' or '.csv.gz'
        rest as in pandas to_csv()
    Returns:
        (error,yaml,csv) : (int,str,str)
            error -- error code, if OK then 0 else 1
            yamlfile  -- yaml file name without dir
            csv   -- csv file name without dir
    """

    if len(data.columns) != len(data.columns.unique()):
        raise ValueError("duplicate columns not supported")

    fullname = os.path.join(dir, filename)
    yamlfile = filename + suff_yaml  # file name without dir

    logger.info(
        "write_csv_with_types(); dir: %s; file: %s; full: %s;"
        % (dir, filename, fullname)
    )
    if not os.path.exists(os.path.dirname(fullname)):
        logger.info(
            "write_csv_with_types(); dir: %s; full: %s; creating directory: %s;"
            % (dir, fullname, os.path.dirname(fullname))
        )
        try:
            os.makedirs(os.path.dirname(fullname))
        except OSError as e:  # Guard against race condition
            if e.errno != errno.EEXIST:
                raise
    data.to_csv(
        fullname, sep=",", encoding=encoding, index=index
    )  # ,compression='gzip')

    error = 1
    try:
        # could be exception
        typeinfo = {
            "field_types": {
                column: pandas2std_types[str(dtype)]
                for (column, dtype) in data.dtypes.iteritems()
            }
        }
    except Exception as e:
        # logerr(e)
        raise e
    with open(os.path.join(dir, yamlfile), "w") as f:
        yaml.dump(typeinfo, f, default_flow_style=False)
    error = 0
    return (error, yamlfile, filename)


def create_filename(isHmm, dir_up, file_pref, key):
    """
    returns
        SC-1037/results/results +
            /alignment/A96224A_xxx.csv.gz if key == /alighnment/xxx  -- case alignment_metrics
            /multiplier_0/A96224A_yyy.csv.gz if key == /hmmcopy/yyy/3   -- case hmmcopy
    """
    a = key.split("/")

    s = (
        os.path.join(
            dir_up, "hmmcopy_autoploidy", "multiplier_" + a[-1], file_pref + "_" + a[-2]
        )
        if isHmm
        else os.path.join(dir_up, "alignment", file_pref + "_" + a[-1])
    ) + suff_new
    return s


def convert1(dir, hdf5_filename):
    """
    converts, writes and tests one hdf5 file
    _hmmcopy.h5 and _alignment_metrics.h5 processed differently
    _hmmcopy.h5 tree structure:
       /
           |--hmmcopy
               |--reads
               |   |--0   # these nodes contain data
               |   |--1
               |   ...
               |--segments
               |   |--0   # these nodes contains data
               |   |--1
               |   ...
               |--metrics
               |   |--0   # these nodes contains data
               |   |--1
               |   ...

       i.e. data is in /hmmcopy/reads/0

    _alignment_metrics.h5 tree structure:
       /
           |--alignment
               |--metrics     # data is here
               |--gc_metrics  # data is here

    output example:
    --------------------
    singlecelldata -> results -> SC-1037 -> results -> results -> alignment -> A96224A_alignment_metrics.h5

    maps to

    singlecelldata -> results -> SC-1037 -> results -> results -> alignment -> A96224A_alignment_metrics.csv.gz
    singlecelldata -> results -> SC-1037 -> results -> results -> alignment -> A96224A_alignment_metrics.yaml
    singlecelldata -> results -> SC-1037 -> results -> results -> alignment -> A96224A_gc_metrics.csv.gz
    singlecelldata -> results -> SC-1037 -> results -> results -> alignment -> A96224A_gc_metrics.yaml

    --------------------
    singlecelldata -> results -> SC-1037 -> results -> results -> alignment -> A96224A_hmmcopy.h5

    maps to

    singlecelldata -> results -> SC-1037 -> results -> results -> hmmcopy_autoploidy -> multiplier_0 -> A96224A_reads.csv.gz
    singlecelldata -> results -> SC-1037 -> results -> results -> hmmcopy_autoploidy -> multiplier_0 -> A96224A_reads.csv.gz.yaml
    singlecelldata -> results -> SC-1037 -> results -> results -> hmmcopy_autoploidy -> multiplier_0 -> A96224A_segments.csv.gz
    singlecelldata -> results -> SC-1037 -> results -> results -> hmmcopy_autoploidy -> multiplier_0 -> A96224A_segments.csv.gz.yaml
    ...
    singlecelldata -> results -> SC-1037 -> results -> results -> hmmcopy_autoploidy -> multiplier_1 -> A96224A_reads.csv.gz
    singlecelldata -> results -> SC-1037 -> results -> results -> hmmcopy_autoploidy -> multiplier_1 -> A96224A_reads.csv.gz.yaml
    ...
    --------------------

    Args:
        dir -- root directory
        hdf5_filename -- file name of the file to convert with path starting from the root directory
    Returns:
        (error,filenames) : (int,set[str])
            error -- error code, if OK then 0 else 1
            filenames -- union of yaml and csv file names without dir
    """
    fullname = os.path.join(dir, hdf5_filename)

    # one step up from SC-1497/results/results/alignment : cd ../
    dir_up = os.path.dirname(hdf5_filename)[: -len("/alignment")]

    pref = os.path.basename(hdf5_filename).split("_")[0]
    logging.info(
        "convert1(); full: %s; dir_up: %s; pref: %s;" % (fullname, dir_up, pref)
    )
    isHmm = hdf5_filename.endswith("_hmmcopy.h5")

    ds = pd.HDFStore(path=fullname, mode="r")
    keys = ds.keys()

    fnm_old = hdf5_filename[: -len(suff_old)]
    filenames = set({})

    for k in keys:
        logger.info("trying: %s; %s;" % (hdf5_filename, k))
        with Timeit("{}; select({})".format(hdf5_filename, k)):
            df = ds.select(k)
        # create filename without path
        csv_filename = create_filename(isHmm, dir_up, pref, k)
        with Timeit("convert-write; file: {};".format(csv_filename)):
            # returning filenames are relative to dir
            (rc, yml, csv) = write_csv_with_types(df, dir, csv_filename)

        if 0 == rc:
            # testing if result is equivalent to the original
            # csv is filename relative to dir
            df_test = read_csv_with_types(dir, csv)
            with Timeit("compare; {}".format(csv_filename)):
                rc = not compare_df(df, df_test)
                if 0 != rc:
                    break
            filenames.add(csv)
            filenames.add(yml)
    return (rc, filenames)


def main():
    # convert1(os.path.join(root,'_alignment_metrics.h5'))
    # convert1(root, "_hmmcopy.h5")
    # convert1(root, "SC-1497/results/results/alignment/A96199B_hmmcopy.h5")
    convert1(root, "SC-1497/results/results/alignment/A96199B_alignment_metrics.h5")


def compare_df_test(isOk, a, b, msg):
    """
    tests compare_df function, success depends on isOk,
    if isOk True then DataFrames are equal and comparison result should be True
    if isOk False then DataFrames are not equal and comparison result should be False
    Args:
        isOk: bool
        a,b : DataFrame -- DataFrames to compare
        msg : string -- logging message
    Returns:
        bool -- True if isOk == compare_result else False
    """
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


def df_rw_test(a):
    dfa = pd.DataFrame(a)
    datfile = "xx/yy/a.csv.gz"
    rc = write_csv_with_types(dfa, root, datfile)
    df = read_csv_with_types(root, datafile)
    l = compare_df(dfa, df)
    logging.info("df_rw_test(); res={}; {}".format(l, "OK" if l else "ERROR"))
    return l


def main_test():
    """
    entry point for tests
    """
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
    res &= df_rw_test(a)
    logging.info("============== test: {}".format("OK" if res else "ERROR"))
    return res


if __name__ == "__main__":
    # assert(main_test())
    main()
