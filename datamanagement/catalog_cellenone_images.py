import sys
import os
import glob
import logging
import shutil
import click
import collections
import re
import pandas as pd

from dbclients.basicclient import NotFoundError
from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from datamanagement.add_generic_results import add_generic_results
from utils.constants import LOGGING_FORMAT


# library_id = "A96146A"
# source_dir = '/shahlab/archive/single_cell_indexing/Cellenone/Cellenone_images/20180831_A96146A'
# source_dir = '/shahlab/archive/single_cell_indexing/Cellenone/Cellenone_images/20171019_A95664B'
# destination_dir = '/shahlab/amcpherson/temp123'


def clean_filename(filename):
    prefix = '=HYPERLINK("'
    suffix = '")'

    if not filename.startswith(prefix) or not filename.endswith(suffix):
        raise ValueError(f'unknown format for filename {filename}')

    filename = filename[len(prefix):-len(suffix)]

    return filename


def generate_new_filename(row):
    library_id = row['library_id']
    chip_row = row['chip_row']
    chip_column = row['chip_column']

    new_filename = f'{library_id}_R{chip_row:02d}_C{chip_column:02d}.png'

    return new_filename


def read_cellenone_isolated_files(source_dir):
    """Read the isolated.xls files in a cellenone directory.
    
    Args:
        source_dir (str): Path to the cellenone output
    """
    logging.info(f'processing directory {source_dir}')

    isolated_filenames = glob.glob(os.path.join(source_dir, '*', '*isolated.xls'))

    catalog = []

    for isolated_filename in isolated_filenames:
        logging.info(f'processing {isolated_filename}')

        data = pd.read_csv(isolated_filename, sep='\t')

        data = data.dropna(subset=['XPos', 'YPos', 'X', 'Y'])

        if data.empty:
            logging.info(f'no useful data in {isolated_filename}')
            continue

        # Clean spaces from column names
        data.columns = [a.strip() for a in data.columns]

        # Images subdirectory for the run given by this isolated.xls
        images_dir = os.path.basename(os.path.dirname(isolated_filename))

        # Clean hyperlink tag from original filename
        data['original_filename'] = data['ImageFile'].apply(clean_filename)

        # Original filename relative to root cellenone directory
        data['original_filename'] = [os.path.join(images_dir, a) for a in data['original_filename']]

        # Chip row and column as YPos and XPos
        data['chip_row'] = data['YPos']
        data['chip_column'] = data['XPos']

        catalog.append(data)

    if len(catalog) == 0:
        return pd.DataFrame()

    catalog = pd.concat(catalog, ignore_index=True)

    logging.info(f'found {len(catalog.index)} entries for {source_dir}')

    return catalog


def catalog_images(library_id, source_dir, destination_dir):
    """ Catalog cellenone images and organize into a new directory
    
    Args:
        library_id (str): DLP Library ID
        source_dir (str): Source Cellenone directory
        destination_dir (str): Destination catalogued images directory
    """

    catalog = read_cellenone_isolated_files(source_dir)

    if catalog.empty:
        raise ValueError(f'empty catalog, cellenone data incompatible')

    # Add library id to catalog, required for pretty filename
    catalog['library_id'] = library_id

    # Generate a pretty filename
    catalog['filename'] = catalog.apply(generate_new_filename, axis=1)

    # Report duplicate chip wells
    cols = ['chip_row', 'chip_column']
    dup = catalog[cols].duplicated()
    if dup.any():
        dup_values = list(catalog.loc[dup, cols].values)
        logging.error(f'column {cols} has duplicates {dup_values}')
        logging.error(f'removing {len(dup_values)} duplicate wells')

    # Remove duplicate chip wells
    catalog = catalog[~catalog[['chip_row', 'chip_column']].duplicated(keep=False)]

    # Fail on any other duplicate columns
    assert not catalog[['original_filename']].duplicated().any()
    assert not catalog[['filename']].duplicated().any()

    # List of filepaths of newly created files
    filepaths = []

    # Move the image files into the source directory
    for idx in catalog.index:
        new_filename = catalog.loc[idx, 'filename']
        new_filepath = os.path.join(destination_dir, new_filename)

        original_filename = catalog.loc[idx, 'original_filename']
        original_filepath = os.path.join(source_dir, original_filename)

        shutil.copyfile(original_filepath, new_filepath)

        filepaths.append(new_filepath)

    # Save the catalog
    catalog_filepath = os.path.join(destination_dir, 'catalog.csv')
    catalog.to_csv(catalog_filepath, index=False)

    filepaths.append(catalog_filepath)

    return filepaths


def process_cellenone_images(
        library_id,
        source_dir,
        storage_name,
        tag_name=None,
        update=False,
        remote_storage_name=None,
    ):

    tantalus_api = TantalusApi()

    results_name = 'CELLENONE_IMAGES_{}'.format(library_id)
    results_type = 'CELLENONE_IMAGES'
    results_version = 'v1'

    try:
        existing_results = tantalus_api.get('results', name=results_name)
    except NotFoundError:
        existing_results = None

    if existing_results is not None and not update:
        logging.info(f'results for {library_id} exist, not processing')
        return

    storage = tantalus_api.get('storage', name=storage_name)
    storage_directory = storage['storage_directory']

    destination_dir = os.path.join(
        storage_directory,
        'single_cell_indexing',
        'Cellenone',
        'Cellenone_processed',
        library_id,
        results_version,
    )

    try:
        os.makedirs(destination_dir)
    except:
        pass

    filepaths = catalog_images(library_id, source_dir, destination_dir)

    results_dataset = add_generic_results(
        filepaths=filepaths,
        storage_name=storage_name,
        results_name=results_name,
        results_type=results_type,
        results_version=results_version,
        library_ids=[library_id],
        recursive=False,
        tag_name=tag_name,
        update=update,
        remote_storage_name=remote_storage_name,
    )


@click.group()
def cli():
    pass


@cli.command()
@click.argument('filepaths', nargs=-1)
@click.option('--storage_name')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
@click.option('--remote_storage_name')
def glob_cellenone_data(filepaths, storage_name, tag_name=None, update=False, remote_storage_name=None):

    tantalus_api = TantalusApi()

    for filepath in filepaths:
        match = re.match(r".*/single_cell_indexing/Cellenone/Cellenone_images/(\d+)_(A\d+[A-Z]*)", filepath)
        if match is None:
            logging.warning('skipping malformed {}'.format(filepath))
            continue

        fields = match.groups()
        date = fields[0]
        library_id = fields[1]

        try:
            tantalus_api.get('dna_library', library_id=library_id)
        except NotFoundError:
            logging.warning('skipping file with unknown library {}'.format(filepath))
            continue

        try:
            process_cellenone_images(
                library_id,
                filepath,
                storage_name,
                tag_name=tag_name,
                update=update,
                remote_storage_name=remote_storage_name,
            )
        except ValueError:
            logging.exception(f'unable to process {library_id}, {filepath}')


@cli.command()
@click.argument('library_id')
@click.argument('cellenone_filepath')
@click.argument('storage_name')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
@click.option('--remote_storage_name')
def add_cellenone_data(
        library_id,
        cellenone_filepath,
        storage_name,
        tag_name=None,
        update=False,
        remote_storage_name=None):

    tantalus_api = TantalusApi()

    process_cellenone_images(
        library_id,
        cellenone_filepath,
        storage_name,
        tag_name=tag_name,
        update=update,
        remote_storage_name=remote_storage_name,
    )


if __name__=='__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    cli()

