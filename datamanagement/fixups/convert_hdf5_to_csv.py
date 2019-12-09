"""
Convert h5 files to a set of csv.gz/yaml files.

Steps:
   1)   query for all hmmcopy and align results from the resultsdataset table
   2)   query for all file resources using get_dataset_file_instances,
          using a filter on filename__endswith to specify h5
   3)   cache the file using datamanagement.transfer_files.cache_file
   4)   convert the files using the key to filename map to provide names for the csv.gz/yaml files
   5)   create the files in blob using client.create
   6)   add the files using tantalus_api.add_file
   7)   update the file_resources on the results (resultsdataset) with the new file resource ids from add_file
"""

import itertools
import logging
import click
import os
import pandas as pd

import datamanagement.transfer_files
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi
from datamanagement.miscellaneous.hdf5helper import get_python2_hdf5_keys
from datamanagement.miscellaneous.hdf5helper import convert_python2_hdf5_to_csv
from dbclients.basicclient import NotFoundError


logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)


remote_storage_name = "singlecellresults"


h5_key_name_map = {
    '_destruct.h5': {
        '/breakpoint': '_destruct_breakpoint',
        '/breakpoint_library': '_destruct_breakpoint_library',
    },
    '_snv_counts.h5': {
        '/snv_allele_counts': '_snv_union_counts',
    },
    '_snv_annotations.h5': {
        '/snv_allele_counts': '_snv_allele_counts',
        '/museq/vcf': '_snv_museq',
        '/snv/cosmic_status': '_snv_cosmic_status',
        '/snv/dbsnp_status': '_snv_dbsnp_status',
        '/snv/mappability': '_snv_mappability',
        '/snv/snpeff': '_snv_snpeff',
        '/snv/tri_nucleotide_context': '_snv_trinuc',
        '/strelka/vcf': '_snv_strelka',
    },
    '_alignment_metrics.h5': {
        '/alignment/metrics': '_alignment_metrics',
        '/alignment/gc_metrics': '_gc_metrics',
    },
    '_hmmcopy.h5': {
        '/hmmcopy/segments/0': '_multiplier0_segments',
        '/hmmcopy/segments/1': '_multiplier1_segments',
        '/hmmcopy/segments/2': '_multiplier2_segments',
        '/hmmcopy/segments/3': '_multiplier3_segments',
        '/hmmcopy/segments/4': '_multiplier4_segments',
        '/hmmcopy/segments/5': '_multiplier5_segments',
        '/hmmcopy/segments/6': '_multiplier6_segments',
        '/hmmcopy/reads/0': '_multiplier0_reads',
        '/hmmcopy/reads/1': '_multiplier1_reads',
        '/hmmcopy/reads/2': '_multiplier2_reads',
        '/hmmcopy/reads/3': '_multiplier3_reads',
        '/hmmcopy/reads/4': '_multiplier4_reads',
        '/hmmcopy/reads/5': '_multiplier5_reads',
        '/hmmcopy/reads/6': '_multiplier6_reads',
        '/hmmcopy/params/0': '_multiplier0_params',
        '/hmmcopy/params/1': '_multiplier1_params',
        '/hmmcopy/params/2': '_multiplier2_params',
        '/hmmcopy/params/3': '_multiplier3_params',
        '/hmmcopy/params/4': '_multiplier4_params',
        '/hmmcopy/params/5': '_multiplier5_params',
        '/hmmcopy/params/6': '_multiplier6_params',
        '/hmmcopy/metrics/0': '_multiplier0_metrics',
        '/hmmcopy/metrics/1': '_multiplier1_metrics',
        '/hmmcopy/metrics/2': '_multiplier2_metrics',
        '/hmmcopy/metrics/3': '_multiplier3_metrics',
        '/hmmcopy/metrics/4': '_multiplier4_metrics',
        '/hmmcopy/metrics/5': '_multiplier5_metrics',
        '/hmmcopy/metrics/6': '_multiplier6_metrics',
    },
}


def get_h5_info(h5_filepath):
    key_name_map = None
    for suffix in h5_key_name_map:
        if h5_filepath.endswith(suffix):
            key_name_map = h5_key_name_map[suffix]
            h5_prefix = h5_filepath[:-len(suffix)]
            return key_name_map, h5_prefix
    if 'variant_calling_rawdata' in h5_filepath:
        logging.warning(f'skipping {h5_filepath}')
        return None, None
    raise Exception(f'unknown suffix for {h5_filepath}')


def get_h5_csv_info(h5_filepath):
    key_name_map, h5_prefix = get_h5_info(h5_filepath)

    if key_name_map is None:
        return

    for key in get_python2_hdf5_keys(h5_filepath):
        if key.endswith('meta'):
            continue

        csv_filepath = h5_prefix + key_name_map[key] + '.csv.gz'

        yield key, csv_filepath


def convert_h5(h5_filepath, key, csv_filepath):
    convert_python2_hdf5_to_csv(h5_filepath, key, csv_filepath)


@click.command()
@click.argument('cache_dir')
@click.option('--dataset_id', type=int)
@click.option('--results_type')
@click.option('--redo', is_flag=True)
@click.option('--dry_run', is_flag=True)
@click.option('--check_done', is_flag=True)
def run_h5_convert(cache_dir, dataset_id=None, results_type=None, redo=False, dry_run=False, check_done=False):
    tantalus_api = TantalusApi()

    local_cache_client = tantalus_api.get_cache_client(cache_dir)
    remote_storage_client = tantalus_api.get_storage_client(remote_storage_name)

    if dataset_id is not None:
        results_list = [tantalus_api.get("resultsdataset", id=dataset_id)]
        logging.info('converting results with id {}'.format(dataset_id))

    elif results_type is not None:
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
                # Destruct outputs csv.yaml directly, check non destruct files
                if 'destruct' in existing_filename:
                    continue
                if existing_filename.endswith('.csv.gz.yaml'):
                    found_csv_yaml = True
                    break

            if found_csv_yaml and check_done:
                logging.info('found filename {}, skipping conversion'.format(existing_filename))
                continue

            file_resource_ids = []

            filepaths_to_clean = []

            for file_instance in file_instances:
                if not file_instance['file_resource']['filename'].endswith('.h5'):
                    continue

                datamanagement.transfer_files.cache_file(tantalus_api, file_instance, cache_dir)

                h5_filepath = local_cache_client.get_url(file_instance['file_resource']['filename'])

                filepaths_to_clean.append(h5_filepath)

                logging.info('converting {}'.format(h5_filepath))

                for key, csv_filepath in get_h5_csv_info(h5_filepath):
                    if not csv_filepath.startswith(cache_dir):
                        raise Exception('unexpected csv path {}'.format(csv_filepath))

                    csv_filename = csv_filepath[len(cache_dir):]
                    csv_filename = csv_filename.lstrip('/')

                    if csv_filename in existing_filenames and not redo:
                        logging.info('file {} already exists, not converting'.format(csv_filename))
                        continue

                    if dry_run:
                        logging.info('would convert {}, key {} to {}'.format(
                            h5_filepath, key, csv_filepath))
                        continue

                    logging.info('converting {}, key {} to {}'.format(
                        h5_filepath, key, csv_filepath))
                    convert_h5(h5_filepath, key, csv_filepath)

                    yaml_filename = csv_filename + '.yaml'
                    yaml_filepath = csv_filepath + '.yaml'

                    fileinfo_to_add = [
                        (csv_filename, csv_filepath),
                        (yaml_filename, yaml_filepath),                        
                    ]

                    for filename, filepath in fileinfo_to_add:
                        logging.info('creating file {} from path {}'.format(
                            filename, filepath))

                        remote_storage_client.create(filename, filepath, update=redo)
                        remote_filepath = os.path.join(remote_storage_client.prefix, filename)

                        logging.info('adding file {} from path {}'.format(
                            filename, remote_filepath))

                        (file_resource, file_instance) = tantalus_api.add_file(
                            remote_storage_name, remote_filepath, update=True)#redo)

                        file_resource_ids.append(file_resource["id"])
                        filepaths_to_clean.append(filepath)

            if len(file_resource_ids) == 0:
                logging.warning('no files added')
                continue

            logging.info('adding file resources {} to dataset {}'.format(
                file_resource_ids, result["id"]))

            tantalus_api.update(
                "resultsdataset",
                result["id"],
                file_resources=result["file_resources"] + file_resource_ids,
            )

            for filepath in filepaths_to_clean:
                logging.info('removing file {}'.format(filepath))
                os.remove(filepath)

        except NotFoundError:
            logging.exception('no files found for conversion')

        except KeyboardInterrupt:
            raise

        except Exception:
            logging.exception('conversion failed')


if __name__ == "__main__":
    run_h5_convert()
