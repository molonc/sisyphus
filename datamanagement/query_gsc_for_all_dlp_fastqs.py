from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
import string
import sys
import time
import click
import pandas as pd
from datamanagement.query_gsc_for_dlp_fastqs import import_gsc_dlp_paired_fastqs
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi


@click.command()
@click.argument('storage_name')
@click.option('--all', is_flag=True)
@click.option('--tag_name')
@click.option('--dry_run', is_flag=True)
def main(storage_name, all, dry_run, tag_name=None):
    # Set up the root logger
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    # Connect to the Tantalus API (this requires appropriate environment
    colossus_api = ColossusApi()
    tantalus_api = TantalusApi()

    storage = tantalus_api.get("storage", name=storage_name)

    sequencing_list_all = list(colossus_api.list('sequencing'))
    sequencing_list = list()

    if all:
        sequencing_list = sequencing_list_all

    else:
        for sequencing in sequencing_list_all:
            if sequencing['number_of_lanes_requested'] != len(sequencing['dlplane_set']):
                sequencing_list.append(sequencing)

    for sequencing in sequencing_list:

        # Query GSC for FastQs
        try:
            import_info = import_gsc_dlp_paired_fastqs(
                colossus_api,
                tantalus_api,
                sequencing["library"],
                storage,
                tag_name,
                dry_run=dry_run)
        except Exception as e:
            logging.warning(("Library {} failed to import: {}".format(sequencing["library"], e)))
            continue

        if import_info is None:
            continue

        # Re-get the sequencing, may be unnecessary but safer
        # given that it is nested in sequencing and may have been changed
        # in a previous iteration of this loop
        sequencing = colossus_api.get('sequencing', id=sequencing['id'])

        if sequencing['gsc_library_id'] is not None:
            if sequencing['gsc_library_id'] != import_info['gsc_library_id']:
                raise Exception('gsc library id mismatch in sequencing {} '.format(sequencing['id']))

        else:
            colossus_api.update(
                'sequencing',
                sequencing['id'],
                gsc_library_id=import_info['gsc_library_id'])

        for flowcell in import_info['flowcells_to_be_created']:
            colossus_api.get_or_create("lane", sequencing=sequencing['id'], flow_cell_id=flowcell)


if __name__ == "__main__":
    main()
