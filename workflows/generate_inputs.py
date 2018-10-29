#!/usr/bin/env python
import json
import pandas as pd
import yaml
from collections import defaultdict
import logging
import os

import templates
from tantalus_client import tantalus
from utils import tantalus_utils, colossus_utils

log = logging.getLogger('sisyphus')


def generate_sample_info(library_id):
    """ Generate a DataFrame of sample information for the single_cell_pipeline
    Args:
        library_id: the library_id of the associated library
    Returns:
        pandas DataFrame of sample information
    """

    data = colossus_utils.query_colossus_library(library_id)
    pool_id = data['pool_id']
    sublibraries = colossus_utils.query_colossus_for_sublibraries(library_id)
    sample_ids = set()

    rows = []
    #Loop through sublibs
    for sublib in sublibraries:
        row = str(sublib['row']).zfill(2)
        col = str(sublib['column']).zfill(2)

        sample_id = sublib['sample_id']['sample_id']
        cell_id = '-'.join([sample_id, pool_id, 'R' + row, 'C' + col])

        row = {
            'library_id':       library_id,
            'cell_id':          cell_id,
            'pick_met':         sublib['pick_met'],
            'condition':        sublib['condition'],
            'sample_type':      sublib['sample_id']['sample_type'],
            'img_col':          sublib['img_col'],
            'row':              sublib['row'],
            'column':           sublib['column'],
            'primer_i5':        sublib['primer_i5'],
            'index_i5':         sublib['index_i5'],
            'primer_i7':        sublib['primer_i7'],
            'index_i7':         sublib['index_i7'],
            'index_sequence':   sublib['primer_i7'] + '-' + sublib['primer_i5'],
            'sample_id':        sublib['sample_id']['sample_id'],
            'pick_met':         sublib['pick_met'],
        }

        rows.append(row)
        sample_ids.add(sample_id)

    sample_info = pd.DataFrame(rows)

    return sample_info


def get_fastq_paths(sample_df, location, fastqs, lane_ids):
    """ Queries Tantalus for fastq info
    Args:
        sample_df: DataFrame of sample information. Used in determining which fastq files to keep for a chip
        fastqs: List of fastqs from Tantalus
        location: Which tantalus storage to look at
    Returns:
        fastq_df: DataFrame of fastq information
    """

    cell_list = zip(sample_df.cell_id, sample_df.index_sequence)

    rows = []

    for cell_id, index_sequence in cell_list:
        for lane_id in lane_ids:
            try:
                fastq_1 = fastqs[(lane_id, index_sequence, 1)]
                fastq_2 = fastqs[(lane_id, index_sequence, 2)]
            except KeyError:
                log.debug('missing fastqs for lane {}, index sequence {}'.format(lane_id, index_sequence))

            fastq_1_path = tantalus_utils.get_file_instance_path(fastq_1, location)
            fastq_2_path = tantalus_utils.get_file_instance_path(fastq_2, location)

            if not (fastq_1_path and fastq_2_path):
                raise Exception('fastq_1 is {}, fastq_2 is {}'.format(fastq_1_path, fastq_2_path))

            rows.append(
                {
                    'fastq_1':                  fastq_1_path,
                    'fastq_2':                  fastq_2_path,
                    'cell_id':                  cell_id,
                    'lane_id':                  lane_id,
                    'sequencing_center':        fastq_1['sequencing_centre'],
                    'sequencing_instrument':    fastq_1['sequencing_instrument'],
                }
            )

    fastq_df = pd.DataFrame(rows)
    for idx in fastq_df.duplicated(['cell_id', 'lane_id']).nonzero()[0]:
        raise Exception('duplicate fastqs for cell_id {} lane_id: {}'.format(
            fastq_df.loc[idx, 'cell_id'], 
            fastq_df.loc[idx, 'lane_id'])
        )

    return fastq_df
