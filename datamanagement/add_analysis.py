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

logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

REQUIRED_FIELDS = [
    'name',
    'jira_id',
    'type',
    'version',
]


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
        if key not in REQUIRED_FIELDS:
            raise Exception("Unrecognized input for {}".format(key))

    #Check if all required arguments are present
    for key in REQUIRED_FIELDS:
        if key not in inputs:
            logging.error("Missing input for {}".format(key))
            missing_input = True
    
    if missing_input:
        raise Exception("Please add missing inputs")

    inputs["update"] = kwargs['update']

    #Call main with these arguments
    add_analysis(**inputs)


@input_type.command()
@click.argument('name', nargs=1)
@click.argument('jira_id', nargs=1)
@click.argument('type', nargs=1)
@click.argument('version', nargs=1)
@click.option('--update', is_flag=True)
def command_line(**kwargs): 
    #Call main with these arguments
    add_analysis(**kwargs)


def add_analysis(**kwargs):
    tantalus_api = TantalusApi()

    #Create new analysis object
    analysis = tantalus_api.get_or_create(
            "analysis",
            name=kwargs['name'],
            jira_ticket=kwargs['jira_id'],
            analysis_type=kwargs['type'],
            version=kwargs['version']
    )

    logging.info("Successfully created analysis with ID {}".format(analysis["id"]))


if __name__ == '__main__':
    input_type()