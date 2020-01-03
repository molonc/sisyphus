import click
import sys
import json
import pandas as pd
import yaml
from collections import defaultdict
import logging
import os
import yaml
import subprocess
import dbclients.colossus
import dbclients.tantalus
import datamanagement.transfer_files
import datamanagement.templates as templates
from datamanagement.utils.filecopy import rsync_file
from datamanagement.utils.constants import LOGGING_FORMAT
from datamanagement.utils.utils import get_lane_str
import argparse
import pickle

tantalus_api = dbclients.tantalus.TantalusApi()


regions = [
    '6:11000000-13000000',
    '8:3000000-5000000',
    '17:45000000-47000000',
]


cell_ids = [
    'SA1090-A96213A-R20-C28',
    'SA1090-A96213A-R20-C62',
    'SA1090-A96213A-R22-C43',
    'SA1090-A96213A-R22-C44',
    'SA1090-A96213A-R24-C12',
    'SA1090-A96213A-R24-C19',
    'SA1090-A96213A-R24-C20',
    'SA1090-A96213A-R24-C58',
    'SA1090-A96213A-R25-C13',
    'SA1090-A96213A-R25-C14',
    'SA1090-A96213A-R25-C22',
    'SA1090-A96213A-R25-C40',
    'SA1090-A96213A-R25-C64',
    'SA1090-A96213A-R26-C49',
    'SA1090-A96213A-R26-C50',
    'SA1090-A96213A-R26-C61',
    'SA1090-A96213A-R26-C64',
    'SA1090-A96213A-R27-C10',
    'SA1090-A96213A-R27-C14',
    'SA1090-A96213A-R27-C21',
    'SA1090-A96213A-R27-C45',
    'SA1090-A96213A-R28-C12',
    'SA1090-A96213A-R28-C20',
    'SA1090-A96213A-R28-C23',
    'SA1090-A96213A-R28-C39',
    'SA1090-A96213A-R28-C55',
    'SA1090-A96213A-R28-C64',
    'SA1090-A96213A-R29-C18',
    'SA1090-A96213A-R29-C47',
    'SA1090-A96213A-R29-C59',
    'SA1090-A96213A-R29-C62',
    'SA1090-A96213A-R29-C68',
    'SA1090-A96213A-R30-C14',
    'SA1090-A96213A-R30-C15',
    'SA1090-A96213A-R30-C35',
    'SA1090-A96213A-R30-C44',
    'SA1090-A96213A-R30-C55',
    'SA1090-A96213A-R31-C09',
    'SA1090-A96213A-R31-C37',
    'SA1090-A96213A-R32-C39',
    'SA1090-A96213A-R32-C41',
    'SA1090-A96213A-R32-C65',
    'SA1090-A96213A-R32-C66',
    'SA1090-A96213A-R33-C31',
    'SA1090-A96213A-R33-C38',
    'SA1090-A96213A-R33-C66',
    'SA1090-A96213A-R35-C18',
    'SA1090-A96213A-R35-C24',
    'SA1090-A96213A-R35-C25',
    'SA1090-A96213A-R35-C28',
    'SA1090-A96213A-R35-C38',
    'SA1090-A96213A-R35-C47',
]


LIBRARY_ID = 'A96213A'
TEST_LANE_FLOWCELL = "HHCJ7CCXY"
TEST_LANE_NUMBER = "5"
TUMOUR_DATASET_NAME = 'BAM-SA1090-SC_WGS-A96213A-lanes_6300f0e4-BWA_MEM_0_7_6A-HG19'
NORMAL_DATASET_NAME = 'BAM-DAH370N-WGS-A41086-lanes_52f27595-BWA_MEM_0_7_10-HG19'
NORMAL_LANES_STR = 'lanes_52f27595'
REMOTE_STORAGE_NAME = 'singlecellblob'
LOCAL_CACHE_DIRECTORY = os.environ['TANTALUS_CACHE_DIR']


def gzip_file(uncompressed, compressed):
    cmd = [
        'gzip',
        uncompressed,
        '-c',
    ]

    logging.info('command -> ' + ' '.join(cmd))

    with open(compressed, 'wb') as f:
        subprocess.check_call(cmd, stdout=f)


def run_filter_cmd(filtered_bam, source_bam):
    if os.path.exists(filtered_bam):
        logging.info(f'filtered bam {filtered_bam} already exists, skipping')
        return

    cmd = [
        'samtools',
        'view',
        '-b',
        '-o', filtered_bam,
        source_bam,
    ]

    cmd.extend(regions)

    logging.info('command -> ' + ' '.join(cmd))
    subprocess.check_call(cmd)

    try:
        os.remove(filtered_bam + '.bai')
    except:
        pass

    cmd = [
        'samtools',
        'index',
        filtered_bam,
    ]

    logging.info('command -> ' + ' '.join(cmd))
    subprocess.check_call(cmd)


def run_bam_fastq(end_1_fastq, end_2_fastq, source_bam):
    if os.path.exists(end_1_fastq) and os.path.exists(end_2_fastq):
        logging.info(f'fastqs for {source_bam} exist, skipping')
        return

    cmd = [
        'samtools',
        'sort',
        '-n',
        source_bam,
        '-o',
        source_bam + '.sorted.bam',
    ]

    logging.info('command -> ' + ' '.join(cmd))
    subprocess.check_call(cmd)

    cmd = [
        'samtools',
        'fastq',
        '-t',
        '-1',
        end_1_fastq,
        '-2',
        end_2_fastq,
        source_bam + '.sorted.bam',
    ]

    logging.info('command -> ' + ' '.join(cmd))
    subprocess.check_call(cmd)


def get_tumour_bams():
    tumour_dataset = tantalus_api.get('sequence_dataset', name=TUMOUR_DATASET_NAME)

    file_resources = tantalus_api.get_dataset_file_resources(
        tumour_dataset['id'], 'sequencedataset', filters={'filename__endswith': '.bam'})

    colossus_api = dbclients.colossus.ColossusApi()
    sublibs = colossus_api.get_sublibraries_by_index_sequence(LIBRARY_ID)

    tumour_bam_info = {
        'dataset': tumour_dataset,
        'cells': {},
    }

    for file_resource in file_resources:
        assert file_resource['filename'].endswith('.bam')

        index_sequence = str(file_resource['sequencefileinfo']['index_sequence'])
        sublib = sublibs[index_sequence]
        cell_id = sublib['cell_id']

        if cell_id not in cell_ids:
            continue

        filepath = os.path.join(LOCAL_CACHE_DIRECTORY, file_resource['filename'])

        tumour_bam_info['cells'][cell_id] = {
            'bam': filepath,
            'sublib': sublib,
        }

    return tumour_bam_info


def get_normal_bam():
    normal_dataset = tantalus_api.get('sequence_dataset', name=NORMAL_DATASET_NAME)

    file_resources = list(tantalus_api.get_dataset_file_resources(
        normal_dataset['id'], 'sequencedataset', filters={'filename__endswith': '.bam'}))
    assert len(file_resources) == 1
    file_resource = file_resources[0]

    normal_bam_filepath = os.path.join(LOCAL_CACHE_DIRECTORY, file_resource['filename'])

    bam_info = {
        'bam': normal_bam_filepath,
        'dataset': normal_dataset,
    }

    return bam_info


def create_tumour_fastqs(fastq_dir, temp_dir):
    """ Create a filterd fastq dataset
    """
    tumour_bam_info = get_tumour_bams()

    dataset = tumour_bam_info['dataset']
    lane_ids = [get_lane_str(l) for l in dataset['sequence_lanes']]
    sample_id = dataset['sample']['sample_id']
    library_id = dataset['library']['library_id']

    FASTQ_TEMPLATE = '{cell_id}_{read_end}.fastq.gz'

    tumour_fastq_metadata = {
        'filenames': [],
        'meta': {
            'type': 'cellfastqs',
            'version': 'v0.0.1',
            'cell_ids': [],
            'lane_ids': lane_ids,
            'sample_id': sample_id,
            'library_id': library_id,
            'fastqs': {
                'template': FASTQ_TEMPLATE,
                'instances': []
            },
        }
    }

    for cell_id in tumour_bam_info['cells']:
        bam_path = tumour_bam_info['cells'][cell_id]['bam']
        sublib = tumour_bam_info['cells'][cell_id]['sublib']

        logging.info('creating paired end fastqs for bam {}'.format(bam_path))

        # Filter bams
        cell_filtered_bam = os.path.join(fastq_dir, f'{cell_id}.bam')
        run_filter_cmd(cell_filtered_bam, bam_path)

        # Convert bams to fastq, uncompressed
        cell_end_1_fastq = os.path.join(fastq_dir, f'{cell_id}_1.fastq')
        cell_end_2_fastq = os.path.join(fastq_dir, f'{cell_id}_2.fastq')
        run_bam_fastq(cell_end_1_fastq, cell_end_2_fastq, cell_filtered_bam)

        # Gzip final fastq
        cell_end_1_fastq_filename = FASTQ_TEMPLATE.format(cell_id=cell_id, read_end='1')
        cell_end_2_fastq_filename = FASTQ_TEMPLATE.format(cell_id=cell_id, read_end='2')
        cell_end_1_fastq_gz = os.path.join(fastq_dir, cell_end_1_fastq_filename)
        cell_end_2_fastq_gz = os.path.join(fastq_dir, cell_end_2_fastq_filename)
        gzip_file(cell_end_1_fastq, cell_end_1_fastq_gz)
        gzip_file(cell_end_2_fastq, cell_end_2_fastq_gz)

        tumour_fastq_metadata['filenames'].append(cell_end_1_fastq_filename)
        tumour_fastq_metadata['filenames'].append(cell_end_2_fastq_filename)

        tumour_fastq_metadata['meta']['cell_ids'].append(cell_id)

        for read_end in ('1', '2'):
            tumour_fastq_metadata['meta']['fastqs']['instances'].append({
                'cell_id': cell_id,
                'read_end': read_end,
                'condition': sublib['condition'],
                'img_col': sublib['img_col'],
                'index_i5': sublib['index_i5'],
                'index_i7': sublib['index_i7'],
                'pick_met': sublib['pick_met'],
                'primer_i5': sublib['primer_i5'],
                'primer_i7': sublib['primer_i7'],
                'row': sublib['row'],
                'column': sublib['column'],
            })

    metadata_yaml_filename = os.path.join(fastq_dir, 'metadata.yaml')

    with open(metadata_yaml_filename, 'w') as meta_yaml:
        yaml.safe_dump(tumour_fastq_metadata, meta_yaml, default_flow_style=False)


def create_normal_bam(bam_dir):
    """
    Create a normal bam dataset.
    """
    try: os.makedirs(bam_dir)
    except: pass

    normal_bam_info = get_normal_bam()
    normal_filepath = normal_bam_info['bam']

    normal_dataset = normal_bam_info['dataset']
    lane_ids = [get_lane_str(l) for l in normal_dataset['sequence_lanes']]
    sample_id = normal_dataset['sample']['sample_id']
    library_id = normal_dataset['library']['library_id']

    normal_bam_metadata = {
        'filenames': [],
        'meta': {
            'type': 'wgsbam',
            'version': 'v0.0.1',
            'lane_ids': lane_ids,
            'sample_id': sample_id,
            'library_id': library_id,
        },
    }

    normal_filtered_bam_filename = f'{sample_id}_{library_id}.bam'
    normal_filtered_bam_filepath = os.path.join(bam_dir, normal_filtered_bam_filename)
    run_filter_cmd(normal_filtered_bam_filepath, normal_filepath)

    normal_bam_metadata['filenames'].append(normal_filtered_bam_filename)
    normal_bam_metadata['filenames'].append(normal_filtered_bam_filename + '.bai')

    metadata_yaml_filename = os.path.join(bam_dir, 'metadata.yaml')

    with open(metadata_yaml_filename, 'w') as meta_yaml:
        yaml.safe_dump(normal_bam_metadata, meta_yaml, default_flow_style=False)


def pull_source_datasets():
    tantalus_api = dbclients.tantalus.TantalusApi()

    for dataset_name in (TUMOUR_DATASET_NAME, NORMAL_DATASET_NAME):
        dataset = tantalus_api.get('sequence_dataset', name=dataset_name)
        datamanagement.transfer_files.cache_dataset(
            tantalus_api, dataset['id'], 'sequencedataset', REMOTE_STORAGE_NAME, LOCAL_CACHE_DIRECTORY)


def create_align_input_yaml(fastqs_dir, input_yaml_filepath):
    """ Prepare input yaml for align pipeline
    """

    fastqs_metadata_filepath = os.path.join(fastqs_dir, 'metadata.yaml')
    fastqs_metadata = yaml.load(open(fastqs_metadata_filepath))

    input_info = {}

    lane_id = '.'.join(fastqs_metadata['meta']['lane_ids'])
    cell_ids = fastqs_metadata['meta']['cell_ids']

    fastq_template = fastqs_metadata['meta']['fastqs']['template']
    fastqs_df = pd.DataFrame(fastqs_metadata['meta']['fastqs']['instances'])
    assert not fastqs_df.drop('read_end', axis=1).drop_duplicates()[['cell_id']].duplicated().any()
    fastqs_df = fastqs_df.set_index('cell_id', drop=False)

    for cell_id in cell_ids:
        cell_info = fastqs_df.loc[cell_id].set_index('read_end', drop=False)
        input_info[cell_id] = {
            'condition': cell_info.loc['1']['condition'],
            'column': int(cell_info.loc['1']['column']),
            'img_col': int(cell_info.loc['1']['img_col']),
            'index_i5': cell_info.loc['1']['index_i5'],
            'index_i7': cell_info.loc['1']['index_i7'],
            'pick_met': cell_info.loc['1']['pick_met'],
            'primer_i5': cell_info.loc['1']['primer_i5'],
            'primer_i7': cell_info.loc['1']['primer_i7'],
            'row': int(cell_info.loc['1']['row']),
        }
        fastq_1_filepath = os.path.join(fastqs_dir, fastq_template.format(**cell_info.loc['1'].to_dict()))
        fastq_2_filepath = os.path.join(fastqs_dir, fastq_template.format(**cell_info.loc['2'].to_dict()))
        input_info[cell_id]['fastqs'] = {
            lane_id: {
                'fastq_1': fastq_1_filepath,
                'fastq_2': fastq_2_filepath,
                'sequencing_center': 'TEST',
                'sequencing_instrument': 'TEST',
            }
        }

    with open(input_yaml_filepath, 'w') as meta_yaml:
        yaml.safe_dump(input_info, meta_yaml, default_flow_style=False)


def create_hmmcopy_input_yaml(fastqs_dir, bams_dir, input_yaml_filepath):
    """ Prepare input yaml for hmmcopy pipeline
    """

    fastqs_metadata_filepath = os.path.join(fastqs_dir, 'metadata.yaml')
    fastqs_metadata = yaml.load(open(fastqs_metadata_filepath))

    fastq_template = fastqs_metadata['meta']['fastqs']['template']
    fastqs_df = pd.DataFrame(fastqs_metadata['meta']['fastqs']['instances'])
    assert not fastqs_df.drop('read_end', axis=1).drop_duplicates()[['cell_id']].duplicated().any()
    fastqs_df = fastqs_df.set_index('cell_id', drop=False)

    bams_metadata_filepath = os.path.join(bams_dir, 'metadata.yaml')
    bams_metadata = yaml.load(open(bams_metadata_filepath))

    input_info = {}

    bam_template = bams_metadata['meta']['bams']['template']

    for instance in bams_metadata['meta']['bams']['instances']:
        cell_id = instance['cell_id']

        cell_info = fastqs_df.loc[cell_id].set_index('read_end', drop=False)
        input_info[cell_id] = {
            'condition': cell_info.loc['1']['condition'],
            'column': int(cell_info.loc['1']['column']),
            'img_col': int(cell_info.loc['1']['img_col']),
            'index_i5': cell_info.loc['1']['index_i5'],
            'index_i7': cell_info.loc['1']['index_i7'],
            'pick_met': cell_info.loc['1']['pick_met'],
            'primer_i5': cell_info.loc['1']['primer_i5'],
            'primer_i7': cell_info.loc['1']['primer_i7'],
            'row': int(cell_info.loc['1']['row']),
        }

        bam_filename = bam_template.format(**instance)
        bam_filepath = os.path.join(bams_dir, bam_filename)
        input_info[cell_id]['bam'] = bam_filepath

    with open(input_yaml_filepath, 'w') as meta_yaml:
        yaml.safe_dump(input_info, meta_yaml, default_flow_style=False)


def _read_region_bams(bams_dir):
    bam_paths = {}

    metadata_filepath = os.path.join(bams_dir, 'metadata.yaml')
    metadata = yaml.load(open(metadata_filepath))

    bam_template = metadata['meta']['bams']['template']

    for instance in metadata['meta']['bams']['instances']:
        region_id = instance['region']

        bam_filename = bam_template.format(**instance)
        bam_filepath = os.path.join(bams_dir, bam_filename)
        bam_paths[region_id] = {'bam': bam_filepath}
    
    return bam_paths


def create_variant_calling_input_yaml(normal_bams_dir, tumour_bams_dir, input_yaml_filepath):
    """ Prepare input yaml for variant calling pipeline
    """

    input_info = {
        'normal': _read_region_bams(normal_bams_dir),
        'tumour': _read_region_bams(tumour_bams_dir),
    }

    with open(input_yaml_filepath, 'w') as meta_yaml:
        yaml.safe_dump(input_info, meta_yaml, default_flow_style=False)


def _get_wgs_bam_filepath(bam_dir):
    metadata_filepath = os.path.join(bam_dir, 'metadata.yaml')
    metadata = yaml.load(open(metadata_filepath))
    bam_filenames = list(filter(lambda a: a.endswith('.bam'), metadata['filenames']))
    assert len(bam_filenames) == 1
    bam_filename = bam_filenames[0]
    bam_filepath = os.path.join(bam_dir, bam_filename)

    return bam_filepath


def _read_cell_bams(bams_dir):
    bam_paths = {}

    metadata_filepath = os.path.join(bams_dir, 'metadata.yaml')
    metadata = yaml.load(open(metadata_filepath))

    bam_template = metadata['meta']['bams']['template']

    for instance in metadata['meta']['bams']['instances']:
        cell_id = instance['cell_id']

        bam_filename = bam_template.format(**instance)
        bam_filepath = os.path.join(bams_dir, bam_filename)
        bam_paths[cell_id] = {'bam': bam_filepath}
    
    return bam_paths


def create_breakpoint_calling_input_yaml(normal_bam_dir, tumour_bams_dir, input_yaml_filepath):
    """ Prepare input yaml for breakpoint calling pipeline
    """

    input_info = {
        'normal': {'bam': _get_wgs_bam_filepath(normal_bam_dir)},
        'tumour': _read_cell_bams(tumour_bams_dir),
    }

    with open(input_yaml_filepath, 'w') as meta_yaml:
        yaml.safe_dump(input_info, meta_yaml, default_flow_style=False)


@click.command()
@click.argument('data_dir')
@click.option('--skip_download', is_flag=True)
def create_dlp_test_data(data_dir, skip_download):
    if not skip_download:
        pull_source_datasets()

    tumour_fastq_dir = os.path.join(data_dir, 'tumour_fastqs')
    normal_bam_dir = os.path.join(data_dir, 'normal_bam')
    temp_dir = os.path.join(data_dir, 'temp')

    for d in (tumour_fastq_dir, normal_bam_dir, temp_dir):
        try: os.makedirs(d)
        except: pass

    tumour_metadata = os.path.join(tumour_fastq_dir, 'metadata.yaml')
    if os.path.exists(tumour_metadata):
        logging.info(f'skipping create_tumour_fastqs, found {tumour_metadata}')
    else:
        create_tumour_fastqs(tumour_fastq_dir, temp_dir)

    normal_metadata = os.path.join(normal_bam_dir, 'metadata.yaml')
    if os.path.exists(normal_metadata):
        logging.info(f'skipping create_normal_bam, found {normal_metadata}')
    else:
        create_normal_bam(normal_bam_dir)

    align_input_yaml_filepath = os.path.join(data_dir, 'align', 'inputs.yaml')
    create_align_input_yaml(tumour_fastq_dir, align_input_yaml_filepath)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    logging.info('test')
    create_dlp_test_data()

