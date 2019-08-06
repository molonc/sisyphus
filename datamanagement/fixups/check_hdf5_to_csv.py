"""
Check hdf5 based datasets for conversion.

As a convenience you can do the following to run conversions in parallel

python datamanagement/fixups/check_hdf5_to_csv.py --results_type hmmcopy \
  | parallel --max-args 1 -j 10 \
  "python datamanagement/fixups/convert_hdf5_to_csv.py \
  /home/ubuntu/convert_cache/ --dataset_id {} \
  2> /home/ubuntu/convert_cache/convert_hmmcopy_{}_$(date +%Y_%m_%d_%H_%M_%S).err \
  > /home/ubuntu/convert_cache/convert_hmmcopy_{}_$(date +%Y_%m_%d_%H_%M_%S).out"

"""

import itertools
import logging
import click
import os
import sys
import pandas as pd

import datamanagement.transfer_files
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi
from datamanagement.miscellaneous.hdf5helper import get_python2_hdf5_keys
from datamanagement.miscellaneous.hdf5helper import convert_python2_hdf5_to_csv
from dbclients.basicclient import NotFoundError


remote_storage_name = "singlecellresults"


@click.command()
@click.option('--results_type')
def run_h5_convert(results_type=None):
    tantalus_api = TantalusApi()

    remote_storage_client = tantalus_api.get_storage_client(remote_storage_name)

    if results_type is not None:
        results_list = tantalus_api.list("resultsdataset", results_type=results_type)
        logging.info('converting results with results type {}'.format(results_type))

    else:
        results_list = tantalus_api.list("resultsdataset")
        logging.info('converting all results')

    for result in results_list:
        logging.info('processing results dataset {}'.format(result['id']))

        try:
            file_instances = tantalus_api.get_dataset_file_instances(
                result["id"],
                "resultsdataset",
                remote_storage_name,
            )

            existing_filenames = set([i['file_resource']['filename'] for i in file_instances])

            found_csv_yaml = False
            for existing_filename in existing_filenames:
                if existing_filename.endswith('.csv.gz.yaml'):
                    found_csv_yaml = True
                    break

            if found_csv_yaml:
                logging.info('found filename {}, skipping conversion'.format(existing_filename))

            else:
                print(result["id"])
                logging.info('no yaml found')

        except NotFoundError:
            logging.exception('no files found for conversion')

        except KeyboardInterrupt:
            raise

        except Exception:
            logging.exception('conversion failed')


if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    run_h5_convert()
