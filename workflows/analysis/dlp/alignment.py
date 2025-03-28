import os
import yaml
import logging
import click
import sys
import collections
import pandas as pd

import dbclients.tantalus
import workflows.analysis.base
import workflows.analysis.dlp.launchsc
import datamanagement.templates as templates
from datamanagement.utils.utils import get_lane_str, get_datasets_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
from datamanagement.utils.dlp import create_sequence_dataset_models
import workflows.analysis.dlp.results_import as results_import
from workflows.generate_inputs import generate_sample_info

reference_genome_map = {
    'HG19': 'grch37',
    'MM10': 'mm10',
    'AT10': 'at10',
}


def get_flowcell_lane(lane):
    if lane['lane_number'] == '':
        return lane['flowcell_id']
    else:
        return '{}_{}'.format(lane['flowcell_id'], lane['lane_number'])


class AlignmentAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'align'

    def __init__(self, *args, **kwargs):
        super(AlignmentAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type)
        #self.out_dir = os.path.join(self.jira, "results")
        self.bams_dir = self._get_bams_dir(self.jira, self.args)

    def _get_lanes(self):
        lanes = dict()
        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequence_dataset', id=dataset_id)
            for lane in dataset['sequence_lanes']:
                lanes[get_lane_str(lane)] = lane
        return lanes

    def _get_bams_dir(self, jira, args):
        lanes = self._get_lanes()

        # TODO: control aligner vocabulary elsewhere
        assert args['aligner'] in ('BWA_ALN', 'BWA_MEM')

        bams_dir = templates.SC_WGS_BAM_DIR_TEMPLATE.format(
            library_id=args["library_id"],
            ref_genome=reference_genome_map[args["ref_genome"]],
            aligner_name=args["aligner"],
            number_lanes=len(lanes),
            jira_ticket=jira,
        )

        return bams_dir

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        filter_lanes = []
        if args['gsc_lanes'] is not None:
            filter_lanes += args['gsc_lanes']
        if args['brc_flowcell_ids'] is not None:
            # Each BRC flowcell has 4 lanes
            filter_lanes += ['{}_{}'.format(args['brc_flowcell_ids'], i + 1) for i in range(4)]

        datasets = tantalus_api.list(
            'sequence_dataset',
            library__library_id=args['library_id'],
            dataset_type='FQ',
        )

        if not datasets:
            raise Exception('no sequence datasets matching library_id {}'.format(args['library_id']))

        dataset_ids = set()

        for dataset in datasets:
            if len(dataset['sequence_lanes']) != 1:
                raise Exception('sequence dataset {} has {} lanes'.format(
                    dataset['id'],
                    len(dataset['sequence_lanes']),
                ))

            lane_id = '{}_{}'.format(
                dataset['sequence_lanes'][0]['flowcell_id'],
                dataset['sequence_lanes'][0]['lane_number'],
            )

            if filter_lanes and (lane_id not in filter_lanes):
                continue

            dataset_ids.add(dataset['id'])

        return list(dataset_ids)

    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        lanes_hashed = get_datasets_lanes_hash(tantalus_api, input_datasets)

        # TODO: control aligner vocabulary elsewhere
        assert args['aligner'] in ('BWA_ALN', 'BWA_MEM')

        name = templates.SC_QC_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=cls.analysis_type_,
            aligner=args['aligner'],
            ref_genome=args['ref_genome'],
            library_id=args['library_id'],
            lanes_hashed=lanes_hashed,
        )

        return name

    def check_inputs_yaml(self, inputs_yaml_filename):
        inputs_dict = yaml.load(open(inputs_yaml_filename))

        lanes = list(self._get_lanes().keys())
        input_lanes = list(inputs_dict.values())[0]['fastqs'].keys()

        num_cells = len(inputs_dict)
        fastq_1_set = set()
        fastq_2_set = set()

        for cell_id, cell_info in inputs_dict.items():
            fastq_1 = list(cell_info["fastqs"].values())[0]["fastq_1"]
            fastq_2 = list(cell_info["fastqs"].values())[0]["fastq_2"]

            fastq_1_set.add(fastq_1)
            fastq_2_set.add(fastq_2)

        if not (num_cells == len(fastq_1_set) == len(fastq_2_set)):
            raise Exception(
                "number of cells is {} but found {} unique fastq_1 and {} unique fastq_2 in inputs yaml".format(
                    num_cells,
                    len(fastq_1_set),
                    len(fastq_2_set),
                ))

        if set(lanes) != set(input_lanes):
            raise Exception('lanes in input datasets: {}\nlanes in inputs yaml: {}'.format(lanes, input_lanes))

    def _generate_cell_metadata(self, storage_name):
        """ Generates per cell metadata

        Args:
            storage_name: Which tantalus storage to look at
        """
        logging.info('Generating cell metadata')

        sample_info = generate_sample_info(self.args["library_id"])

        if sample_info['index_sequence'].duplicated().any():
            raise Exception('Duplicate index sequences in sample info.')

        if sample_info['cell_id'].duplicated().any():
            raise Exception('Duplicate cell ids in sample info.')

        lanes = self._get_lanes()

        # Sort by index_sequence, lane id, read end
        fastq_filepaths = dict()

        # Lane info
        lane_info = dict()

        tantalus_index_sequences = set()
        colossus_index_sequences = set()
        storage_client = self.tantalus_api.get_storage_client(storage_name)
        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequence_dataset', id=dataset_id)

            if len(dataset['sequence_lanes']) != 1:
                raise ValueError('unexpected lane count {} for dataset {}'.format(
                    len(dataset['sequence_lanes']),
                    dataset_id,
                ))

            lane_id = get_flowcell_lane(dataset['sequence_lanes'][0])

            lane_info[lane_id] = {
                'sequencing_centre': dataset['sequence_lanes'][0]['sequencing_centre'],
                'sequencing_instrument': dataset['sequence_lanes'][0]['sequencing_instrument'],
                'read_type': dataset['sequence_lanes'][0]['read_type'],
            }

            file_instances = self.tantalus_api.get_dataset_file_instances(
                dataset['id'],
                'sequencedataset',
                storage_name,
            )

            for file_instance in file_instances:
                # skip metadata.yaml
                if os.path.basename(file_instance['file_resource']['filename']) == "metadata.yaml":
                    continue

                file_resource = file_instance['file_resource']
                read_end = file_resource['sequencefileinfo']['read_end']
                index_sequence = file_resource['sequencefileinfo']['index_sequence']
                tantalus_index_sequences.add(index_sequence)
                fastq_filepaths[(index_sequence, lane_id, read_end)] = str(file_instance['filepath'])

                # check if file exists on storage
                error_msg = f"{file_instance['file_resource']['filename']} does not exist on {storage_name}"
                assert storage_client.exists(file_instance['file_resource']['filename']), error_msg

        input_info = {}

        for idx, row in sample_info.iterrows():
            index_sequence = row['index_sequence']

            colossus_index_sequences.add(index_sequence)
            
            print(fastq_filepaths)

            lane_fastqs = collections.defaultdict(dict)
            for lane_id, lane in lanes.items():
                lane_fastqs[lane_id]['fastq_1'] = fastq_filepaths[(index_sequence, lane_id, 1)]
                lane_fastqs[lane_id]['fastq_2'] = fastq_filepaths[(index_sequence, lane_id, 2)]
                lane_fastqs[lane_id]['sequencing_center'] = lane_info[lane_id]['sequencing_centre']
                lane_fastqs[lane_id]['sequencing_instrument'] = lane_info[lane_id]['sequencing_instrument']

            if len(lane_fastqs) == 0:
                raise Exception('No fastqs for cell_id {}, index_sequence {}'.format(
                    row['cell_id'], row['index_sequence']))

            # sample ID and library ID required as of scpipeline alignment v0.8.0
            input_info[str(row['cell_id'])] = {
                'fastqs': dict(lane_fastqs),
                'pick_met': str(row['pick_met']),
                'condition': str(row['condition']),
                'primer_i5': str(row['primer_i5']),
                'index_i5': str(row['index_i5']),
                'primer_i7': str(row['primer_i7']),
                'index_i7': str(row['index_i7']),
                'img_col': int(row['img_col']),
                'column': int(row['column']),
                'row': int(row['row']),
                'sample_id': str(row['sample_id']),
                'library_id': str(row['library_id']),
                'is_control': bool(row['is_control']),
                'sample_type': 'null' if (row['sample_type'] == 'X') else str(row['sample_type']),
            }

        if colossus_index_sequences != tantalus_index_sequences:
            raise Exception("index sequences in Colossus and Tantalus do not match")

        return input_info

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        """ Generates a YAML file of input information

        Args:
            inputs_yaml_filename: the directory to which the YAML file should be saved
            storage_name: Which tantalus storage to look at
        """

        input_info = self._generate_cell_metadata(storages['working_inputs'])

        with open(inputs_yaml_filename, 'w') as inputs_yaml:
            yaml.safe_dump(input_info, inputs_yaml, default_flow_style=False)

        self.check_inputs_yaml(inputs_yaml_filename)

    def run_pipeline(
            self,
            scpipeline_dir,
            tmp_dir,
            inputs_yaml,
            context_config_file,
            docker_env_file,
            docker_server,
            dirs,
            run_options,
            storages,
    ):
        out_storage_client = self.tantalus_api.get_storage_client(storages["working_results"])
        out_path = os.path.join(out_storage_client.prefix, self.out_dir)
        bams_storage_client = self.tantalus_api.get_storage_client(storages["working_inputs"])
        bams_path = os.path.join(bams_storage_client.prefix, self.bams_dir)

        # get scp configuration i.e. specifies aligner and reference genome
        scp_config = self.get_config(self.args)
        run_options['config_override'] = scp_config

        return workflows.analysis.dlp.launchsc.run_pipeline(
            analysis_type='alignment',
            version=self.version,
            run_options=run_options,
            scpipeline_dir=scpipeline_dir,
            tmp_dir=tmp_dir,
            inputs_yaml=inputs_yaml,
            context_config_file=context_config_file,
            docker_env_file=docker_env_file,
            docker_server=docker_server,
            output_dirs={
                'output_prefix': out_path+"/",
                'bams_dir': bams_path,
            },
            cli_args=[
                '--library_id',
                self.args['library_id'],
            ],
            max_jobs='400',
            dirs=dirs,
        )

    def create_output_datasets(self, storages, update=False):
        """ Create BAM datasets in tantalus.
        """
        storage_client = self.tantalus_api.get_storage_client(storages["working_inputs"])
        metadata_yaml_path = os.path.join(self.bams_dir, "metadata.yaml")
        metadata_yaml = yaml.safe_load(storage_client.open_file(metadata_yaml_path))

        colossus_api = dbclients.colossus.ColossusApi()
        cell_sublibraries = colossus_api.get_sublibraries_by_cell_id(self.args['library_id'])

        sequence_lanes = []

        for lane_id, lane in self._get_lanes().items():
            sequence_lanes.append(
                dict(
                    flowcell_id=lane["flowcell_id"],
                    lane_number=lane["lane_number"],
                    sequencing_centre=lane["sequencing_centre"],
                    read_type=lane["read_type"],
                ))

        bam_cell_ids = metadata_yaml["meta"]["cell_ids"]
        bam_template = metadata_yaml["meta"]["bams"]["template"]
        output_file_info = []
        for cell_id in bam_cell_ids:
            bam_filename = bam_template.format(cell_id=cell_id)
            bam_filepath = os.path.join(
                storage_client.prefix,
                self.bams_dir,
                bam_filename,
            )
            bai_filepath = os.path.join(
                storage_client.prefix,
                self.bams_dir,
                f'{bam_filename}.bai',
            )

            for filepath in (bam_filepath, bai_filepath):
                file_info = dict(
                    analysis_id=self.analysis['id'],
                    dataset_type='BAM',
                    sample_id=cell_sublibraries[cell_id]['sample_id']['sample_id'],
                    library_id=self.args['library_id'],
                    library_type='SC_WGS',
                    index_format='D',
                    sequence_lanes=sequence_lanes,
                    ref_genome=self.args['ref_genome'],
                    aligner_name='BWA_MEM_0_7_17', #HACK!! self.args['aligner'],
                    index_sequence=cell_sublibraries[cell_id]['index_sequence'],
                    filepath=filepath,
                )
                output_file_info.append(file_info)

        logging.info('creating sequence dataset models for output bams')

        output_datasets = create_sequence_dataset_models(
            file_info=output_file_info,
            storage_name=storages["working_inputs"],
            tag_name=None,
            tantalus_api=self.tantalus_api,
            analysis_id=self.get_id(),
            update=update,
        )

        logging.info("created sequence datasets {}".format(output_datasets))

    def create_output_results(self, storages,version=1, update=False, skip_missing=False):
        """
        Create the set of output results produced by this analysis.
        """
        results_name = results_import.qc_results_name_template.format(
            jira_ticket=self.jira,
            analysis_type=self.analysis_type,
            library_id=self.args['library_id'],
        )

        results = results_import.create_dlp_results(
            self.tantalus_api,
            self.out_dir,
            self.get_id(),
            results_name,
            self.get_input_samples(),
            self.get_input_libraries(),
            storages['working_results'],
            update=update,
            skip_missing=skip_missing,
        )

        return [results['id']]

    @classmethod
    def create_analysis_cli(cls):
        cls.create_cli([
            'library_id',
            'aligner',
            'ref_genome',
        ], [
            ('gsc_lanes', None),
            ('brc_flowcell_ids', None),
        ])


workflows.analysis.base.Analysis.register_analysis(AlignmentAnalysis)

if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    AlignmentAnalysis.create_analysis_cli()
