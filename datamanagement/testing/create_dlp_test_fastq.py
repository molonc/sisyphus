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
TUMOUR_DATASET_NAME = 'BAM-SA1090-SC_WGS-A96213A-lanes_341dd558-BWA_ALN_0_5_7-HG19'
NORMAL_DATASET_NAME = 'BAM-DAH370N-WGS-A41086-lanes_52f27595-BWA_ALN_0_5_7-HG19'
NORMAL_LANES_STR = 'lanes_52f27595'
LOCAL_STORAGE_NAME = 'andrewvm'
REMOTE_STORAGE_NAME = 'singlecellblob'
TAG_NAME = 'OV_PseudoBulk_Test'

# TODO: add to templates.py
WGS_BAM_TEMPLATE = os.path.join(
	'wgs',
	'{sample_id}',
	'{library_id}',
	'{ref_genome}',
	'{aligner_name}',
	'numlanes_{number_lanes}',
	'{sample_id}_{library_id}_{ref_genome}_{aligner_name}_numlanes_{number_lanes}.bam'
)


selected_indices = {}
for sublib in dbclients.colossus.get_colossus_sublibraries_from_library_id(LIBRARY_ID):
    cell_id = '{}-{}-R{}-C{}'.format(
        sublib['sample_id']['sample_id'],
        sublib['library']['pool_id'],
        sublib['row'],
        sublib['column'],
    )
    if cell_id not in cell_ids:
        continue
    index_sequence = sublib['primer_i7'] + '-' + sublib['primer_i5']
    selected_indices[index_sequence] = cell_id


def gzip_file(uncompressed):
    cmd = [
        'gzip',
        uncompressed,
    ]

    print ' '.join(cmd)
    subprocess.check_call(cmd)


def run_filter_cmd(filtered_bam, source_bam):
    if os.path.exists(filtered_bam):
        print('filtered bam {} already exists, skipping'.format(filtered_bam))
        return

    cmd = [
        'samtools',
        'view',
        '-f',
        '-b',
        '-o', filtered_bam,
        source_bam,
    ]

    cmd.extend(regions)

    print ' '.join(cmd)
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

    print ' '.join(cmd)
    subprocess.check_call(cmd)


def run_bam_fastq(end_1_fastq, end_2_fastq, source_bam):
    if os.path.exists(end_1_fastq) and os.path.exists(end_2_fastq):
        print('fastqs for {} exist, skipping'.format(source_bam))
        return

    cmd = [
        'samtools',
        'sort',
        '-n',
        source_bam,
        '-o',
        source_bam + '.sorted.bam',
    ]

    print ' '.join(cmd)
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

    print ' '.join(cmd)
    subprocess.check_call(cmd)
    subprocess.check_call(cmd)


def get_tumour_bams():
    storage = tantalus_api.get('storage', name=LOCAL_STORAGE_NAME)
    tumour_dataset = tantalus_api.get('sequence_dataset', name=TUMOUR_DATASET_NAME)

    sample_id = str(tumour_dataset['sample']['sample_id'])

    file_instances = tantalus_api.get_dataset_file_instances(
        tumour_dataset['id'], 'sequencedataset', LOCAL_STORAGE_NAME, filters={'filename__endswith': '.bam'})

    tumour_bam_paths = {}
    tumour_file_resources = {}

    for file_instance in file_instances:
        file_resource = file_instance['file_resource']

        assert file_resource['filename'].endswith('.bam')

        index_sequence = str(file_resource['sequencefileinfo']['index_sequence'])

        if index_sequence not in selected_indices:
            continue

        tumour_bam_paths[index_sequence] = {'bam': str(file_instance['filepath'])}
        tumour_file_resources[index_sequence] = file_instance['file_resource']

    return tumour_bam_paths, tumour_file_resources


def get_normal_bam():
    normal_dataset = tantalus_api.get('sequence_dataset', name=NORMAL_DATASET_NAME)
    file_instances = tantalus_api.get_dataset_file_instances(
        normal_dataset['id'], 'sequencedataset', LOCAL_STORAGE_NAME, filters={'filename__endswith': '.bam'})

    for file_instance in file_instances:
        file_resource = file_instance['file_resource']

        assert file_resource['filename'].endswith('.bam')

        normal_filepath = file_instance['filepath']

        return normal_filepath


def create_tumour_fastqs(data_dir):

    tumour_fastq_paths_pk = os.path.join(data_dir, "tumour_fastq_paths.pk")
    if os.path.exists(tumour_fastq_paths_pk):
        with open(tumour_fastq_paths_pk, "rb") as handle:
            return pickle.load(handle)

    try: os.makedirs(data_dir)
    except: pass

    tumour_bam_paths = get_tumour_bams()[0]

    bam_dir = os.path.join(data_dir, "bam")
    fastq_dir = os.path.join(data_dir, "fastq")

    try: os.makedirs(bam_dir)
    except: pass
    try: os.makedirs(fastq_dir)
    except: pass

    tumour_fastq_paths = {}
    for index_sequence in tumour_bam_paths:
        bam_path = tumour_bam_paths[index_sequence]['bam']
        print('creating paired end fastqs for bam {}'.format(bam_path))
        cell_id = selected_indices[index_sequence]

        cell_filtered_bam = os.path.join(bam_dir, '{cell_id}.bam'.format(cell_id=cell_id))

        cell_end_1_fastq = os.path.join(fastq_dir, '{cell_id}_1.fastq'.format(cell_id=cell_id))
        cell_end_2_fastq = os.path.join(fastq_dir, '{cell_id}_2.fastq'.format(cell_id=cell_id))
        cell_end_1_fastq_gz = os.path.join(fastq_dir, '{cell_id}_1.fastq.gz'.format(cell_id=cell_id))
        cell_end_2_fastq_gz = os.path.join(fastq_dir, '{cell_id}_2.fastq.gz'.format(cell_id=cell_id))
        
        run_filter_cmd(cell_filtered_bam, bam_path)
        run_bam_fastq(cell_end_1_fastq, cell_end_2_fastq, cell_filtered_bam)

        if os.path.exists(cell_end_1_fastq_gz) and os.path.exists(cell_end_2_fastq_gz):
            print('gzipped fastqs exist for {}, skipping'.format(cell_id))
        else:
            gzip_file(cell_end_1_fastq)
            gzip_file(cell_end_2_fastq)

        tumour_fastq_paths[cell_id] = {
            'fastq_1': str(cell_end_1_fastq_gz),
            'fastq_2': str(cell_end_2_fastq_gz),
            'index_sequence': index_sequence,
        }

    return tumour_fastq_paths


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
    print("test normal sample has id {}".format(sample_pk))

    test_library_id = normal_dataset["library"]["library_id"] + 'TEST'
    test_library = tantalus_api.get_or_create(
        "dna_library",
        library_id=test_library_id,
        library_type=normal_dataset["library"]["library_type"],
        index_format=normal_dataset["library"]["index_format"],
    )
    library_pk = test_library["id"]
    print("test normal library has id {}".format(library_pk))

    normal_bam_name = templates.WGS_BAM_NAME_TEMPLATE.format(
        sample_id=test_sample_id,
        library_id=test_library_id,
        library_type='WGS',
        lanes_str=NORMAL_LANES_STR,
    )
    print("test normal dataset has name {}".format(normal_bam_name))

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

    print(test_dataset_name)

    test_sample = tantalus_api.get_or_create(
        "sample",
        sample_id=test_sample_id,
    )

    sample_pk = test_sample["id"]
    print("test sample has id {}".format(sample_pk))

    test_library = tantalus_api.get_or_create(
        "dna_library",
        library_id=test_library_id,
        library_type=tumour_dataset["library"]["library_type"],
        index_format=tumour_dataset["library"]["index_format"],
    )
    library_pk = test_library["id"]
    print("test library has id {}".format(library_pk))

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

            print("adding {} to Tantalus".format(tantalus_path))
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
        datamanagement.transfer_files.transfer_dataset(
            tantalus_api, dataset['id'], 'sequencedataset', REMOTE_STORAGE_NAME, LOCAL_STORAGE_NAME)


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
    create_dlp_test_fastq()

