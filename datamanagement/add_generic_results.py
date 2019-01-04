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


@click.command()
@click.argument('filepaths', nargs=-1)
@click.option('--sample_ids', multiple=True)
@click.option('--storage_name')
@click.option('--results_name')
@click.option('--results_type')
@click.option('--results_version')
@click.option('--tag_name')
@click.option('--analysis_pk')
@click.option('--update', is_flag=True)
def add_generic_results_cmd(
        filepaths, sample_ids, storage_name, results_name,
        results_type, results_version, analysis_pk=None,
        tag_name=None, update=False):

    add_generic_results(
        filepaths, sample_ids, storage_name, results_name,
        results_type, results_version, analysis_pk=analysis_pk,
        tag_name=tag_name, update=update)


def add_generic_results(
        filepaths, sample_ids, storage_name, results_name,
        results_type, results_version, analysis_pk=None,
        tag_name=None, update=False):

    tantalus_api = TantalusApi()

    sample_pks = []
    for sample_id in sample_ids:
        samples = tantalus_api.get(
            "sample",
            sample_id=sample_id,
        )
        sample_pks.append(samples['id'])

    #Add the file resource to tantalus
    file_resource_pks = []
    for filepath in filepaths:
        logging.info("Adding file resource for {} to Tantalus".format(filepath))
        resource, instance = tantalus_api.add_file(
            storage_name=storage_name,
            filepath=filepath,
            update=update,
        )
        file_resource_pks.append(resource["id"])

    if tag_name is not None:
        tag = tantalus_api.get("tag", name=tag_name)
        tags = [tag["id"]]
    else:
        tags = []

    #Add the dataset to tantalus
    results_dataset = tantalus_api.get_or_create(
            "results",
            name=results_name,
            results_type=results_type,
            results_version=results_version,
            analysis=analysis_pk,
            samples=sample_pks,
            file_resources=file_resource_pks,
            tags=tags,
    )

    logging.info("Succesfully created sequence dataset with ID {}".format(results_dataset["id"]))


if __name__=='__main__':
    add_generic_results_cmd()
