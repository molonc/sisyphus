import os
import yaml
import logging
import click
import sys
import pandas as pd

import dbclients.tantalus
import workflows.analysis.base
import workflows.analysis.dlp.launchsc
import datamanagement.templates as templates
from datamanagement.utils.utils import get_lane_str, get_datasets_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import


reference_genome_map = {
    'HG19': 'grch37',
    'MM10': 'mm10',
}


class AlignmentAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'alignment'

    def __init__(self, *args, **kwargs):
        super(AlignmentAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type)
        self.bams_dir = self._get_bams_dir(jira, args)

    def _get_lanes(self):
        lanes = set()
        for dataset_id in dataset_ids:
            dataset = tantalus_api.get('sequence_dataset', id=dataset_id)
            for lane in dataset['sequence_lanes']:
                lanes.add(get_lane_str(lane))
        return lanes

    def _get_bams_dir(self, jira, args):
        lanes = self._get_lanes()

        bams_dir = templates.SC_WGS_BAM_DIR_TEMPLATE.format(
            library_id=args["library_id"],
            ref_genome=reference_genome_map[args["ref_genome"]],
            aligner_name=args["aligner"],
            number_lanes=len(lanes),
            jira_ticket=jira,
        )

        return os.path.join(storage_prefix, bams_dir)

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

        name = templates.SC_QC_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=cls.analysis_type_,
            aligner=args['aligner'],
            ref_genome=args['ref_genome'],
            library_id=args['library_id'],
            lanes_hashed=lanes_hashed,
        )

       return name

    def check_inputs_yaml(self, inputs_yaml_filename):
        inputs_dict = file_utils.load_yaml(inputs_yaml_filename)

        lanes = list(self.get_lanes().keys())
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
        log.info('Generating cell metadata')

        sample_info = generate_sample_info(self.args["library_id"], test_run=self.run_options.get("is_test_run", False))

        if sample_info['index_sequence'].duplicated().any():
            raise Exception('Duplicate index sequences in sample info.')

        if sample_info['cell_id'].duplicated().any():
            raise Exception('Duplicate cell ids in sample info.')

        lanes = self.get_lanes()

        # Sort by index_sequence, lane id, read end
        fastq_filepaths = dict()

        # Lane info
        lane_info = dict()

        tantalus_index_sequences = set()
        colossus_index_sequences = set()

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequence_dataset', id=dataset_id)

            if len(dataset['sequence_lanes']) != 1:
                raise ValueError('unexpected lane count {} for dataset {}'.format(
                    len(dataset['sequence_lanes']),
                    dataset_id,
                ))

            lane_id = tantalus_utils.get_flowcell_lane(dataset['sequence_lanes'][0])

            lane_info[lane_id] = {
                'sequencing_centre': dataset['sequence_lanes'][0]['sequencing_centre'],
                'sequencing_instrument': dataset['sequence_lanes'][0]['sequencing_instrument'],
                'read_type': dataset['sequence_lanes'][0]['read_type'],
            }

            file_instances = tantalus_api.get_dataset_file_instances(dataset['id'], 'sequencedataset', storage_name)

            for file_instance in file_instances:
                file_resource = file_instance['file_resource']
                read_end = file_resource['sequencefileinfo']['read_end']
                index_sequence = file_resource['sequencefileinfo']['index_sequence']
                tantalus_index_sequences.add(index_sequence)
                fastq_filepaths[(index_sequence, lane_id, read_end)] = str(file_instance['filepath'])

        input_info = {}

        for idx, row in sample_info.iterrows():
            index_sequence = row['index_sequence']

            if self.run_options.get("is_test_run", False) and (index_sequence not in tantalus_index_sequences):
                # Skip index sequences that are not found in the Tantalus dataset, since
                # we need to refer to the original library in Colossus for metadata, but
                # we don't want to iterate through all the cells present in that library
                continue

            colossus_index_sequences.add(index_sequence)

            lane_fastqs = collections.defaultdict(dict)
            for lane_id, lane in lanes.items():
                lane_fastqs[lane_id]['fastq_1'] = fastq_filepaths[(index_sequence, lane_id, 1)]
                lane_fastqs[lane_id]['fastq_2'] = fastq_filepaths[(index_sequence, lane_id, 2)]
                lane_fastqs[lane_id]['sequencing_center'] = lane_info[lane_id]['sequencing_centre']
                lane_fastqs[lane_id]['sequencing_instrument'] = lane_info[lane_id]['sequencing_instrument']

            if len(lane_fastqs) == 0:
                raise Exception('No fastqs for cell_id {}, index_sequence {}'.format(
                    row['cell_id'], row['index_sequence']))

            sample_id = row['sample_id']
            if self.run_options.get("is_test_run", False):
                assert 'TEST' in sample_id

            input_info[str(row['cell_id'])] = {
                'bam': None,
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
                'sample_type': 'null' if (row['sample_type'] == 'X') else str(row['sample_type']),
            }

        if colossus_index_sequences != tantalus_index_sequences:
            raise Exception("index sequences in Colossus and Tantalus do not match")

        return input_info

    def generate_inputs_yaml(self, inputs_yaml_filename):
        """ Generates a YAML file of input information

        Args:
            inputs_yaml_filename: the directory to which the YAML file should be saved
            storage_name: Which tantalus storage to look at
        """

        input_info = self._generate_cell_metadata(self.storages['working_inputs'])

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
        storage_client = self.tantalus_api.get_storage_client(storages["working_results"])
        out_path = os.path.join(storage_client.prefix, self.out_dir)
        bams_path = os.path.join(storage_client.prefix, self.bams_dir)

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
                'out_dir': out_path,
                'bams_dir': bams_path,
            },
            max_jobs='400',
            dirs=dirs,
        )

    def create_output_datasets(self, tag_name=None, update=False):
        """ Create BAM datasets in tantalus.
        """
        storage_client = tantalus_api.get_storage_client(self.storages["working_results"])
        metadata_yaml_path = os.path.join(self.bams_dir, "metadata.yaml")
        metadata_yaml = yaml.safe_load(storage_client.open_file(metadata_yaml_path))

        cell_sublibraries = colossus_api.get_sublibraries_by_cell_id(self.args['library_id'])

        sequence_lanes = []

        for lane_id, lane in self.get_lanes().items():
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
                    aligner_name=self.args['aligner'],
                    index_sequence=cell_sublibraries[cell_id]['index_sequence'],
                    filepath=filepath,
                )
                output_file_info.append(file_info)

        log.info('creating sequence dataset models for output bams')

        output_datasets = dlp.create_sequence_dataset_models(
            file_info=output_file_info,
            storage_name=self.storages["working_results"],
            tantalus_api=tantalus_api,
            analysis_id=self.get_id(),
            update=update,
        )

        log.info("created sequence datasets {}".format(output_datasets))

    def create_output_results(self, storages, update=False, skip_missing=False):
        """
        Create the set of output results produced by this analysis.
        """
        results = results_import.create_dlp_results(
            self.tantalus_api,
            self.out_dir,
            self.get_id(),
            '{}_{}'.format(self.jira, self.analysis_type),
            self.get_input_samples(),
            self.get_input_libraries(),
            storages['working_results'],
            update=False,
            skip_missing=False,
        )

        return [results['id']]


workflows.analysis.base.Analysis.register_analysis(AlignmentAnalysis)


def create_analysis(jira_id, version, args):
    tantalus_api = dbclients.tantalus.TantalusApi()

    analysis = AlignmentAnalysis.create_from_args(tantalus_api, jira_id, version, args)

    logging.info(f'created analysis {analysis.get_id()}')

    if analysis.status.lower() in ('error', 'unknown'):
        analysis.set_ready_status()

    else:
        logging.warning(f'analysis {analysis.get_id()} has status {analysis.status}')


@click.group()
def analysis():
    pass


@analysis.command()
@click.argument('jira_id')
@click.argument('version')
@click.argument('library_id')
@click.argument('aligner')
@click.argument('ref_genome')
@click.option('--gsc_lanes')
@click.option('--brc_flowcell_ids')
def create_single_analysis(jira_id, version, library_id, aligner, ref_genome, **kwargs):
    args = {}
    args['library_id'] = library_id
    args['aligner'] = aligner
    args['ref_genome'] = ref_genome
    args['gsc_lanes'] = kwargs.get('gsc_lanes')
    args['brc_flowcell_ids'] = kwargs.get('brc_flowcell_ids')

    create_analysis(jira_id, version, args)


@analysis.command()
@click.argument('version')
@click.argument('info_table')
def create_multiple_analyses(version, info_table):
    info = pd.read_csv(info_table)

    for idx, row in info.iterrows():
        jira_id = row['jira_id']

        args = {}
        args['library_id'] = row['library_id']
        args['aligner'] = row['aligner']
        args['ref_genome'] = row['ref_genome']
        args['gsc_lanes'] = row.get('gsc_lanes')
        args['brc_flowcell_ids'] = row.get('brc_flowcell_ids')

        try:
            create_analysis(jira_id, version, args)
        except:
            logging.exception(f'create analysis failed for {jira_id}')


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    analysis()
