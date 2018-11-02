#!/usr/bin/env python
import json
import pandas as pd
import yaml
from collections import defaultdict
import logging
import os

import datamanagement.templates as templates
import dbclients.colossus
from utils import colossus_utils

colossus_api = dbclients.colossus.ColossusApi()

log = logging.getLogger('sisyphus')


def generate_sample_info(library_id):
    """ Generate a DataFrame of sample information for the single_cell_pipeline
    Args:
        library_id: the library_id of the associated library
    Returns:
        pandas DataFrame of sample information
    """

    data = colossus_api.get('library', pool_id=library_id)
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

