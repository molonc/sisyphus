from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
from workflows import generate_inputs
from datetime import datetime
import yaml
import os


tantalus_api = TantalusApi()
colossus_api = ColossusApi()


def define_metadata_yaml():
    return {
        'filenames': [],
        'meta': {
            'type': '',
            'version': '',
            'cell_ids': set(),  # converted to list once populated
            'lane_ids': set(),  # converted to list once populated
            'sequencing_centre': '',
            'sequencing_instrument': '',
            'fastqs': {
                'template': '',
                'instances': []
            }
        }
    }


def construct_instance_info(index_sequence, sample_info, filename):
    instance_info = sample_info[sample_info['index_sequence'] == index_sequence]

    return {
        'sample_id':      instance_info['sample_id'].values[0],
        'library_id':     instance_info['library_id'].values[0],
        'cell_id':        instance_info['cell_id'].values[0],
        'read_end':       int(filename[filename.rfind('_') + 1:filename.find('.')]),
        'img_col':        int(instance_info['img_col'].values[0]),
        'index_i5':       instance_info['index_i5'].values[0],
        'index_i7':       instance_info['index_i7'].values[0],
        'pick_met':       instance_info['pick_met'].values[0],
        'primer_i5':      instance_info['primer_i5'].values[0],
        'primer_i7':      instance_info['primer_i7'].values[0],
        'index_sequence': instance_info['index_sequence'].values[0],
        'row':            int(instance_info['row'].values[0]),
        'column':         int(instance_info['column'].values[0]),
    }


def determine_fastq_template(filename, index_sequence):
    filename_templates = [
        '{sample_id}_{library_id}_{index_sequence}_{read_end}.fastq.gz',
        '{cell_id}_R{read_end}_001.fastq.gz'
    ]
    filename_parts = filename.split('_')

    if filename_parts[2] == index_sequence:
        return filename_templates[0]
    else:
        return filename_templates[1]


def construct_lane_info(lanes):
    lane_info = {'lane_ids': set(), 'sequencing_centre': '', 'sequencing_instrument': ''}
    centre_and_instrument_determined = False

    for lane in lanes:
        lane_info['lane_ids'].add(lane['flowcell_id'])
        if not centre_and_instrument_determined:
            lane_info['sequencing_centre']     = lane['sequencing_centre']
            lane_info['sequencing_instrument'] = lane['sequencing_instrument']
            centre_and_instrument_determined = True

    return lane_info


def construct_fastq_metadata_yaml(library_id):
    fq_dirs = {}

    datasets = tantalus_api.list("sequencedataset", dataset_type='FQ', library__library_id=library_id)
    for dataset in datasets:

        sample_info = generate_inputs.generate_sample_info(library_id)
        file_resources = tantalus_api.list('file_resource', sequencedataset__id=dataset['id'])
        for file_resource in file_resources:

            fq_dir         = os.path.dirname(file_resource['filename'])
            index_sequence = file_resource['sequencefileinfo']['index_sequence']
            filename       = os.path.basename(file_resource['filename'])
            lane_info      = construct_lane_info(dataset['sequence_lanes'])

            if fq_dir not in fq_dirs.keys():
                fq_dirs[fq_dir]                                  = define_metadata_yaml()
                fq_dirs[fq_dir]['meta']['type']                  = dataset['library']['library_type']
                fq_dirs[fq_dir]['meta']['version']               = 'v.0.0.1'  # hardcoded for now
                fq_dirs[fq_dir]['meta']['sequencing_centre']     = lane_info['sequencing_centre']
                fq_dirs[fq_dir]['meta']['sequencing_instrument'] = lane_info['sequencing_instrument']
                fq_dirs[fq_dir]['meta']['fastqs']['template']    = determine_fastq_template(filename, index_sequence)

            fq_dirs[fq_dir]['meta']['lane_ids'] = fq_dirs[fq_dir]['meta']['lane_ids'].union(lane_info['lane_ids'])
            instance_info = construct_instance_info(index_sequence, sample_info, filename)
            fq_dirs[fq_dir]['meta']['fastqs']['instances'].append(instance_info)
            fq_dirs[fq_dir]['meta']['cell_ids'].add(instance_info['cell_id'])
            fq_dirs[fq_dir]['filenames'].append(filename)

            print(f"{library_id} - {dataset['id']} - {index_sequence} - {file_resource['filename']}")

    count = 0
    for fq_dir in fq_dirs.keys():
        fq_dirs[fq_dir]['meta']['cell_ids'] = list(fq_dirs[fq_dir]['meta']['cell_ids'])  # convert set to list
        fq_dirs[fq_dir]['meta']['lane_ids'] = list(fq_dirs[fq_dir]['meta']['lane_ids'])  # convert set to list
        with open(f"metadata_{count}.yaml", 'w') as outfile:
            yaml.dump(fq_dirs[fq_dir], outfile, default_flow_style=False, sort_keys=False)
            count = count + 1


if __name__ == "__main__":
    # spectrum_libs = ['A108851A', 'A108833A', 'A108832A', 'A108867A', 'A96185B', 'A96167A', 'A96167B', 'A96123A',
    #                 'A98245B', 'A98177A', 'A98179A', 'A98177B', 'A96121B', 'A96253']

    spectrum_libs = ['A96123A']

    for library in spectrum_libs:
        construct_fastq_metadata_yaml(library)

