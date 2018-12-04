#!/usr/bin/env python

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import re
import sys
import warnings
import time
import subprocess
import pandas as pd
from dbclients.colossus import get_colossus_sublibraries_from_library_id
from dbclients.tantalus import TantalusApi
from utils.constants import LOGGING_FORMAT
from utils.dlp import create_sequence_dataset_models, fastq_paired_end_check
from utils.runtime_args import parse_runtime_args
from utils.filecopy import rsync_file
from utils.utils import make_dirs
import datamanagement.templates as templates
import filecmp


if __name__ == "__main__":

    args = parse_runtime_args()
    # variables defined)
    tantalus_api = TantalusApi()

    storage = tantalus_api.get("storage_server", name=args["storage_name"])

    datasets = list(tantalus_api.list(
        "sequence_dataset",
        sequence_lanes__flowcell_id=args["flowcell_id"],
        dataset_type="FQ"))

    dataset_ids = [str(d["id"]) for d in datasets]

    file_resources = []

    Collect file resource values of each data set
    for i in range(len(datasets)):
    	file_resources.append(datasets[i]["file_resources"])

    for f in file_resources:

    file = tantalus_api.get("file_resource", id=1758610)
   	# print(file, '\n')

    file_path = file["file_instances"][0]["filepath"]

   	print(file_path)

    # print(file_resources)

    # print(tantalus_api.get("sequence_dataset", id=dataset_ids[0], sequence_lanes__flowcell_id=args["flowcell_id"]))



    # for ids in dataset_ids:
    # 	(tantalus_api.get("sequence_dataset", flowcell_id=args["flowcell_id"]))

    # if len(datasets) > 0:
    #     warnings.warn("found dataset {}".format(','.join([str(d["id"]) for d in datasets])))

