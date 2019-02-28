import pandas as pd
import sys
import os

output_msgpack = sys.argv[1] + "." + sys.argv[2] + ".msgpack"
store = pd.HDFStore(sys.argv[1], 'r')
store[sys.argv[2]].to_msgpack(output_msgpack)
