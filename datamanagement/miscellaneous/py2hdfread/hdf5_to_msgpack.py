import pandas as pd
import sys
import os

h5_filepath = sys.argv[1]
key = sys.argv[2]
msgpack_filepath = h5_filepath + '.' + key.replace('/', '_') + ".msgpack"
store = pd.HDFStore(h5_filepath, 'r')
data = store[key]
data.to_msgpack(msgpack_filepath)
