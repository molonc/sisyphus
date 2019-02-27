import pandas
import docker
import sys
import os


def convert_h5_to_msgpack(filepath, key):
    directory = os.path.dirname(filepath)
    filename = os.path.basename(filepath)
    output_msgpack = str(directory) + "/" + str(filename) + "." + key + ".msgpack"

    if not os.path.exists(output_msgpack):
        print("FILE PATH DOES NOT EXIST, CREATING")
        client = docker.from_env()
        client.containers.run("simongsong/shahlab_python:latest",
                              environment=["TABLE=" + str(filename), "STORAGE=" + key],
                              volumes={str(directory):
                                           {"bind": "/mount",
                                            "mode": "rw",
                                            }
                                       })
    else:
        print("FILEPATH EXIST")
    output = pandas.read_msgpack(output_msgpack)
    print(output)
    return output