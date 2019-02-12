#!/usr/bin/env python
import json
import pandas as pd
import yaml
from collections import defaultdict
import logging
import os

import datamanagement.templates as templates
import dbclients.colossus

colossus_api = dbclients.colossus.ColossusApi()

log = logging.getLogger('sisyphus')


def generate_sample_info(library_id, test_run=False):
    """ Generate a DataFrame of sample information for the single_cell_pipeline
    Args:
        library_id: the library_id of the associated library

    KwArgs:
        test_run: boolean set to true if running tests

    Returns:
        pandas DataFrame of sample information
    """
    query_library_id = library_id
    if test_run:
        query_library_id = library_id.strip('TEST')

    data = colossus_api.get('library', pool_id=query_library_id)
    sublibraries = colossus_api.list('sublibraries', library__pool_id=query_library_id)
    sample_ids = set()

    rows = []
    for sublib in sublibraries:
        row = str(sublib['row']).zfill(2)
        col = str(sublib['column']).zfill(2)

        sample_id = sublib['sample_id']['sample_id']

        if test_run:
            sample_id += 'TEST'

        cell_id = '-'.join([sample_id, library_id, 'R' + row, 'C' + col])

        row = {
            'library_id':       library_id,
            'sample_id':        sample_id,
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
            'pick_met':         sublib['pick_met'],
        }

        rows.append(row)
        sample_ids.add(sample_id)

    sample_info = pd.DataFrame(rows)

    return sample_info

