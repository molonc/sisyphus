#!/usr/bin/env python
from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys
import click
import json
import ast
import pandas as pd

from utils.constants import LOGGING_FORMAT
from utils.runtime_args import parse_runtime_args
from dbclients.tantalus import TantalusApi
import datamanagement.templates as templates

logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stdout, level=logging.INFO)

REQUIRED_FIELDS = [
    'filepaths',
    'sample_id',
    'library_id',
    'storage_name',
    'dataset_name',
    'dataset_type',
]

OPTIONAL_FIELDS = [
    'tag_name',
    'aligner',
    'sequence_lane_pks',
    'reference_genome'
]


class ListParameter(click.Option):
    def type_cast_value(self, ctx, value):
        try:
            return ast.literal_eval(value)
        except:
            raise click.BadParameter(value)


@click.group()
def input_type():
    pass


@input_type.command()
@click.argument('json_file')
@click.option('--update', is_flag=True)
def json_input(**kwargs):
    missing_input = False
    
    #Parse the input json file
    try:
        with open(kwargs['json_file']) as f:
            inputs = json.load(f)
    except:
        inputs = json.loads(kwargs['json_file'])

    #Check that arguments have the right name
    for key, val in inputs.iteritems():
        if key not in REQUIRED_FIELDS + OPTIONAL_FIELDS:
            raise Exception("Unrecognized input for {}".format(key))

    #Check if all required arguments are present
    for key in REQUIRED_FIELDS:
        if key not in inputs:
            logging.error("Missing input for {}".format(key))
            missing_input = True
    
    if missing_input:
        raise Exception("Please add missing inputs")

    for key in OPTIONAL_FIELDS:
        if key not in inputs:
            if key == 'sequence_lane_pks':
                inputs[key] = []
            else:
                inputs[key] = None

    inputs["update"] = kwargs['update']

    #Call main with these arguments
    add_generic_dataset(**inputs)


@input_type.command()
@click.argument('filepaths', nargs=-1)
@click.argument('sample_id', nargs=1)
@click.argument('library_id', nargs=1)
@click.option('--storage_name')
@click.option('--dataset_name')
@click.option('--dataset_type')
@click.option('--tag_name')
@click.option('--aligner')
@click.option('--sequence_lane_pks', cls=ListParameter, default='[]')
@click.option('--reference_genome', type=click.Choice(['HG18', 'HG19']))
@click.option('--update', is_flag=True)
def command_line(**kwargs):
    missing_input = False

    #Check if all required arguments are present
    for key, val in kwargs.iteritems():
        if not val and key in REQUIRED_FIELDS:
            logging.error("Missing input for {}".format(key))
            missing_input = True

    if missing_input:
        raise Exception("Please add missing inputs")
    
    #Call main with these arguments
    add_generic_dataset(**kwargs)


def add_generic_dataset(**kwargs):
    tantalus_api = TantalusApi()

    file_resource_pks = []

    sample = tantalus_api.get(
        "sample",
        sample_id=kwargs['sample_id']
    )

    library = tantalus_api.get(
        "dna_library",
        library_id=kwargs['library_id']
    )

    #Add the file resource to tantalus
    for filepath in kwargs['filepaths']:
        logging.info("Adding file resource for {} to Tantalus".format(filepath))
        resource, instance = tantalus_api.add_file(
            storage_name=kwargs['storage_name'],
            filepath=filepath,
            update=kwargs['update']
        )
        file_resource_pks.append(resource["id"])

    if "tag_name" in kwargs:
        tag = tantalus_api.get("tag", name=kwargs["tag_name"])
        tags = [tag["id"]]
    else:
        tags = []

    ref_genome = kwargs.get("reference_genome")
    aligner = kwargs.get("aligner")

    if "sequence_lane_pks" in kwargs:
        sequence_pks = map(str, kwargs["sequence_lane_pks"])

    #Add the dataset to tantalus
    sequence_dataset = tantalus_api.get_or_create(
            "sequence_dataset",
            name=kwargs['dataset_name'],
            dataset_type=kwargs['dataset_type'],
            sample=sample["id"],
            library=library["id"],
            sequence_lanes=sequence_pks,
            file_resources=file_resource_pks,
            reference_genome=ref_genome,
            aligner=aligner,
            tags=tags,
    )

    logging.info("Succesfully created sequence dataset with ID {}".format(sequence_dataset["id"]))


if __name__=='__main__':
    input_type()
