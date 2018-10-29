import os
import json
import yaml

def load_json(path):
    with open(path) as infile:
        return json.load(infile)


def load_yaml(path):
    with open(path) as infile:
        return yaml.load(infile)


def walk_dir(path):
    for root, _, files in os.walk(path):
        for f in files:
            yield os.path.join(root, f)
