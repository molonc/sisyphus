import pandas as pd
import sys

output_msgpack = "/mount/" + sys.argv[1] + "." + sys.argv[2] + ".msgpack"
print output_msgpack
output = open(output_msgpack, 'w')
store = pd.HDFStore("/mount/" + sys.argv[1])
store[ sys.argv[2]].to_msgpack(output_msgpack)
