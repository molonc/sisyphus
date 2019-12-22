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
import argparse
import pickle

tantalus_api = dbclients.tantalus.TantalusApi()


regions = [
    '6:11000000-13000000',
    '8:3000000-5000000',
    '17:45000000-47000000',
]

cell_ids = [
    'SA922-A90554B-R34-C14', 'SA922-A90554B-R32-C12',
    'SA922-A90554B-R34-C42', 'SA922-A90554B-R33-C38',
    'SA922-A90554B-R26-C40', 'SA922-A90554B-R28-C42',
    'SA922-A90554B-R26-C16', 'SA922-A90554B-R30-C14',
    'SA922-A90554B-R31-C29', 'SA922-A90554B-R34-C67',
    'SA922-A90554B-R31-C34', 'SA922-A90554B-R34-C61',
    'SA922-A90554B-R24-C17', 'SA922-A90554B-R24-C27',
    'SA922-A90554B-R27-C21', 'SA922-A90554B-R26-C22',
    'SA922-A90554B-R35-C29', 'SA922-A90554B-R29-C37',
    'SA922-A90554B-R25-C25', 'SA922-A90554B-R28-C38',
    'SA922-A90554B-R24-C14', 'SA922-A90554B-R26-C26',
    'SA922-A90554B-R27-C48', 'SA922-A90554B-R25-C19',
    'SA922-A90554B-R26-C25', 'SA922-A90554B-R33-C34',
    'SA921-A90554A-R08-C17', 'SA921-A90554A-R14-C34',
    'SA921-A90554A-R05-C58', 'SA921-A90554A-R04-C40',
    'SA921-A90554A-R11-C09', 'SA921-A90554A-R04-C35',
    'SA921-A90554A-R04-C64', 'SA921-A90554A-R05-C28',
    'SA921-A90554A-R13-C20', 'SA921-A90554A-R14-C25',
    'SA921-A90554A-R04-C10', 'SA921-A90554A-R06-C57',
    'SA921-A90554A-R09-C30', 'SA921-A90554A-R11-C52',
    'SA921-A90554A-R07-C59', 'SA921-A90554A-R10-C14',
    'SA921-A90554A-R08-C48', 'SA921-A90554A-R06-C22',
    'SA921-A90554A-R04-C44', 'SA921-A90554A-R03-C36',
    'SA921-A90554A-R04-C66', 'SA921-A90554A-R14-C65',
    'SA921-A90554A-R05-C14', 'SA921-A90554A-R06-C13',
    'SA921-A90554A-R06-C14', 'SA921-A90554A-R05-C35',
    'SA921-A90554A-R10-C11', 'SA921-A90554A-R14-C40',
    'SA1090-A96213A-R24-C58', 'SA1090-A96213A-R25-C22',
    'SA1090-A96213A-R30-C35', 'SA1090-A96213A-R33-C66',
    'SA1090-A96213A-R26-C49', 'SA1090-A96213A-R31-C37',
    'SA1090-A96213A-R27-C14', 'SA1090-A96213A-R35-C18',
    'SA1090-A96213A-R26-C61', 'SA1090-A96213A-R30-C55',
    'SA1090-A96213A-R24-C20', 'SA1090-A96213A-R26-C50',
    'SA1090-A96213A-R35-C24', 'SA1090-A96213A-R20-C28',
    'SA1090-A96213A-R33-C31', 'SA1090-A96213A-R27-C21',
    'SA1090-A96213A-R24-C12', 'SA1090-A96213A-R32-C66',
    'SA1090-A96213A-R26-C64', 'SA1090-A96213A-R29-C59',
    'SA1090-A96213A-R28-C64', 'SA1090-A96213A-R32-C41',
    'SA1090-A96213A-R32-C39', 'SA1090-A96213A-R25-C64',
    'SA1090-A96213A-R33-C38', 'SA1090-A96213A-R28-C23',
    'SA1090-A96213A-R35-C38', 'SA1090-A96213A-R22-C44',
    'SA1090-A96213A-R29-C47', 'SA1090-A96213A-R31-C09',
    'SA1090-A96213A-R27-C45', 'SA1090-A96213A-R29-C18',
    'SA1090-A96213A-R20-C62', 'SA1090-A96213A-R29-C62',
    'SA1090-A96213A-R25-C14', 'SA1090-A96213A-R25-C40',
    'SA1090-A96213A-R30-C15', 'SA1090-A96213A-R32-C65',
    'SA1090-A96213A-R35-C47', 'SA1090-A96213A-R22-C43',
    'SA1090-A96213A-R28-C39'
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

    with open(commpressed, 'wb') as f:
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
        'bedtools',
        'bamtofastq',
        '-i',
        source_bam + '.sorted.bam',
        '-fq',
        end_1_fastq,
        '-fq2',
        end_2_fastq,
    ]

    logging.info('command -> ' + ' '.join(cmd))
    subprocess.check_call(cmd)


def get_tumour_bams():
    storage = tantalus_api.get('storage', name=LOCAL_STORAGE_NAME)
    tumour_dataset = tantalus_api.get('sequence_dataset', name=TUMOUR_DATASET_NAME)

    sample_id = str(tumour_dataset['sample']['sample_id'])

    file_resources = tantalus_api.get_dataset_file_resources(
        tumour_dataset['id'], 'sequencedataset', filters={'filename__endswith': '.bam'})

    sublibs = colossus.get_sublibraries_by_index_sequence(LIBRARY_ID)

    tumour_bam_info = {}

    for file_resource in file_resources:
        assert file_resource['filename'].endswith('.bam')

        index_sequence = str(file_resource['sequencefileinfo']['index_sequence'])
        sublib = sublibs[index_sequence]
        cell_id = sublib['cell_id']

        if cell_id not in cell_ids:
            continue

        filepath = os.path.join(LOCAL_CACHE_DIRECTORY, file_resource['filename'])

        tumour_bam_info[cell_ids] = {
            'bam': filepath,
            'sublib': sublib,
        }

    return tumour_bam_info


def get_normal_bam():
    normal_dataset = tantalus_api.get('sequence_dataset', name=NORMAL_DATASET_NAME)
    file_instances = tantalus_api.get_dataset_file_resources(
        normal_dataset['id'], 'sequencedataset', LOCAL_STORAGE_NAME, filters={'filename__endswith': '.bam'})

    for file_instance in file_instances:
        file_resource = file_instance['file_resource']

        assert file_resource['filename'].endswith('.bam')

        normal_filepath = file_instance['filepath']

        return normal_filepath


def create_tumour_fastqs(fastq_dir, temp_dir):
    """ Create a filterd fastq dataset
    """
    tumour_bam_info = get_tumour_bams()

    try: os.makedirs(fastq_dir)
    except: pass
    try: os.makedirs(temp_dir)
    except: pass

    FASTQ_TEMPLATE = '{cell_id}_{read_end}.fastq'

    tumour_fastq_metadata = {
        'filenames': [],
        'meta': {
            'type': 'cellfastqs'
            'version': 'v0.0.1'
            'cell_ids': [],
            'lane_ids': [],
            'fastqs': {
                'template': FASTQ_TEMPLATE,
                'instances': []
            },
        }
    }

    metadata_yaml_filename = os.path.join(fastq_dir, 'metadata.yaml')

    for cell_id in tumour_bam_info:
        bam_path = tumour_bam_info[cell_id]['bam']
        sublib = tumour_bam_info[cell_id]['sublib']

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
            tumour_fastq_metadata['meta']['fastqs'].append({
                'cell_id': cell_id,
                'read_end': read_end,
                'img_col': sublib['img_col']
                'index_i5': sublib['index_i5']
                'index_i7': sublib['index_i7']
                'pick_met': sublib['pick_met']
                'primer_i5': sublib['primer_i5']
                'primer_i7': sublib['primer_i7']
                'row': sublib['row']
                'column': sublib['column']
            })

    with open(metadata_yaml_filename, 'w') as meta_yaml:
        yaml.safe_dump(tumour_fastq_metadata, meta_yaml, default_flow_style=False)


def create_normal_dataset(data_dir, update=False):
    """
    Create a sequence dataset for the test normal in Tantalus, along with 
    the corresponding library, sample, and sequence lane.
    """
    normal_filepath = get_normal_bam()

    temp_normal_filtered_bam = os.path.join(data_dir, 'DAH370N_filtered.bam')
    run_filter_cmd(temp_normal_filtered_bam, normal_filepath)

    normal_dataset = tantalus_api.get('sequence_dataset', name=NORMAL_DATASET_NAME)
    storage = tantalus_api.get("storage", name=LOCAL_STORAGE_NAME)

    test_sample_id = normal_dataset["sample"]["sample_id"] + 'TEST'
    test_sample = tantalus_api.get_or_create(
        "sample",
        sample_id=test_sample_id,
    )
    sample_pk = test_sample["id"]
    logging.info("test normal sample has id {}".format(sample_pk))

    test_library_id = normal_dataset["library"]["library_id"] + 'TEST'
    test_library = tantalus_api.get_or_create(
        "dna_library",
        library_id=test_library_id,
        library_type=normal_dataset["library"]["library_type"],
        index_format=normal_dataset["library"]["index_format"],
    )
    library_pk = test_library["id"]
    logging.info("test normal library has id {}".format(library_pk))

    normal_bam_name = templates.WGS_BAM_NAME_TEMPLATE.format(
        sample_id=test_sample_id,
        library_id=test_library_id,
        library_type='WGS',
        lanes_str=NORMAL_LANES_STR,
    )
    logging.info("test normal dataset has name {}".format(normal_bam_name))

    tantalus_bam_filename = WGS_BAM_TEMPLATE.format(
        sample_id=test_sample_id,
        library_id=test_library_id,
        ref_genome=normal_dataset['reference_genome'],
        aligner_name=normal_dataset['aligner'],
        number_lanes=len(normal_dataset['sequence_lanes']),
    )
    tantalus_bam_filepath = os.path.join(
        storage['storage_directory'],
        'test_datasets',
        tantalus_bam_filename,
    )

    rsync_file(temp_normal_filtered_bam, tantalus_bam_filepath)  
    rsync_file(temp_normal_filtered_bam+'.bai', tantalus_bam_filepath+'.bai')  

    bam_resource, bam_instance = tantalus_api.add_file(LOCAL_STORAGE_NAME, tantalus_bam_filepath, update=update)
    bai_resource, bai_instance = tantalus_api.add_file(LOCAL_STORAGE_NAME, tantalus_bam_filepath + ".bai", update=update)

    sequence_dataset = tantalus_api.get_or_create(
        'sequence_dataset',
        name=normal_bam_name,
        dataset_type="BAM",
        sample=sample_pk,
        library=library_pk,
        sequence_lanes=[a['id'] for a in normal_dataset['sequence_lanes']],
        file_resources=[
            bam_resource['id'],
            bai_resource['id'],
        ],
    )

    return sequence_dataset


def create_sequence_dataset(tumour_fastq_paths, update=None):
    """
    Create a sequence dataset for the test dataset in Tantalus, along with 
    the corresponding library, sample, and sequence lane.
    """
    tumour_dataset = tantalus_api.get('sequence_dataset', name=TUMOUR_DATASET_NAME)
    storage = tantalus_api.get("storage", name=LOCAL_STORAGE_NAME)

    test_sample_id = tumour_dataset["sample"]["sample_id"] + 'TEST'
    test_library_id = tumour_dataset["library"]["library_id"] + 'TEST'

    lane = tantalus_api.get(
        "sequencing_lane", flowcell_id=TEST_LANE_FLOWCELL, lane_number=TEST_LANE_NUMBER)

    test_lane = tantalus_api.get_or_create(
        "sequencing_lane",
        flowcell_id=lane["flowcell_id"] + "TEST",
        lane_number=lane["lane_number"],
        sequencing_centre=lane["sequencing_centre"],
        sequencing_instrument=lane["sequencing_instrument"],
        read_type=lane["read_type"],
        dna_library=lane["dna_library"],
    )
    
    test_lanes_str = test_lane["flowcell_id"] + "_" + test_lane["lane_number"]

    test_dataset_name = templates.SC_WGS_FQ_NAME_TEMPLATE.format(
        sample_id=test_sample_id,
        library_id=test_library_id,
        dataset_type="FQ",
        library_type=tumour_dataset["library"]["library_type"],
        lane=test_lanes_str,
    )

    logging.info(test_dataset_name)

    test_sample = tantalus_api.get_or_create(
        "sample",
        sample_id=test_sample_id,
    )

    sample_pk = test_sample["id"]
    logging.info("test sample has id {}".format(sample_pk))

    test_library = tantalus_api.get_or_create(
        "dna_library",
        library_id=test_library_id,
        library_type=tumour_dataset["library"]["library_type"],
        index_format=tumour_dataset["library"]["index_format"],
    )
    library_pk = test_library["id"]
    logging.info("test library has id {}".format(library_pk))

    # Create a sequence dataset
    sequence_dataset = dict(
        name=test_dataset_name,
        dataset_type="FQ",
        sample=sample_pk,
        library=library_pk,
        sequence_lanes=[test_lane['id']],
        file_resources=[],
    )

    for cell_id in tumour_fastq_paths:
        for read_end in (1, 2):
            key = 'fastq_{}'.format(read_end)
            fastq_path = tumour_fastq_paths[cell_id][key]
            index_sequence = tumour_fastq_paths[cell_id]['index_sequence']

            tantalus_filename = templates.SC_WGS_FQ_TEMPLATE.format(
                primary_sample_id=test_sample_id,
                dlp_library_id=test_library_id,
                flowcell_id=test_lanes_str,
                lane_number='N',
                cell_sample_id=test_sample_id,
                index_sequence=index_sequence,
                read_end=read_end,
                extension='.gz',
            )

            tantalus_path = os.path.join(
                storage["storage_directory"],
                'test_datasets',
                tantalus_filename)

            rsync_file(fastq_path, tantalus_path)  

            logging.info("adding {} to Tantalus".format(tantalus_path))
            file_resource, file_instance = tantalus_api.add_file(
                LOCAL_STORAGE_NAME,
                tantalus_path,
                update=update,
            )

            sequence_dataset["file_resources"].append(file_resource["id"])

            sequence_file_info = tantalus_api.get_or_create(
                "sequence_file_info",
                file_resource=file_resource["id"],
                index_sequence=index_sequence,
                genome_region=" ".join(regions),
                read_end=read_end,
            )

            tantalus_api.get_or_create(
                "file_instance",
                storage=storage["id"],
                file_resource=file_resource["id"],
            )

    sequence_dataset = tantalus_api.get_or_create("sequence_dataset", **sequence_dataset)

    return sequence_dataset


def pull_source_datasets():
    tantalus_api = dbclients.tantalus.TantalusApi()

    for dataset_name in (TUMOUR_DATASET_NAME, NORMAL_DATASET_NAME):
        dataset = tantalus_api.get('sequence_dataset', name=dataset_name)
        datamanagement.transfer_files.cache_dataset(
            tantalus_api, dataset['id'], 'sequencedataset', REMOTE_STORAGE_NAME, LOCAL_CACHE_DIRECTORY)


@click.command()
@click.argument('data_dir')
@click.option('--update', is_flag=True)
def create_dlp_test_fastq(data_dir, update=False):
    pull_source_datasets()

    tumour_fastq_paths = create_tumour_fastqs(data_dir)
    tumour_dataset = create_sequence_dataset(tumour_fastq_paths, update=update)

    normal_dataset = create_normal_dataset(data_dir, update=update)

    tantalus_api.tag(
        TAG_NAME, sequencedataset_set=[
            tumour_dataset['id'],
            normal_dataset['id'],
        ]
    )

    datamanagement.transfer_files.transfer_dataset(
        tantalus_api, tumour_dataset['id'], 'sequencedataset', LOCAL_STORAGE_NAME, REMOTE_STORAGE_NAME) 

    datamanagement.transfer_files.transfer_dataset(
        tantalus_api, normal_dataset['id'], 'sequencedataset', LOCAL_STORAGE_NAME, REMOTE_STORAGE_NAME) 


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    logging.info('test')
    create_dlp_test_fastq()

