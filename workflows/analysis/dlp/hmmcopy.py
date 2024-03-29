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
from datamanagement.utils.utils import get_datasets_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import
from workflows.generate_inputs import generate_sample_info

class HMMCopyAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'hmmcopy'

    def __init__(self, *args, **kwargs):
        super(HMMCopyAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type)
    def _check_input_datasets_completeness(tantalus_api, args, input_datasets):
        # get all fastqs datasets of library
        fastq_datasets = tantalus_api.list(
            "sequence_dataset",
            library__library_id=args["library_id"],
            dataset_type="FQ",
        )

        # get list of unique samples from fastq datasets
        library_samples = set([dataset["sample"]["sample_id"] for dataset in fastq_datasets])

        # get list of unique samples from bam input datasets
        input_dataset_samples = collections.defaultdict(list)
        for dataset in input_datasets:
            # get sample id of dataset
            sample_id = dataset["sample"]["sample_id"]

            # check if bam was not produced by merge cell pipeline
            # bams produced by this pipeline will have a non null value in region_split_length field
            if not dataset["region_split_length"]:
                input_dataset_samples[sample_id].append(dataset["id"])

        # check if every sample of the library has a bam input dataset associated with it
        for sample in library_samples:
            if sample not in input_dataset_samples:
                raise Exception(f"no input dataset for sample {sample}")
            logging.info(f"sample {sample} has a dataset in the input datasets")

        logging.info(f"every sample in the library {args['library_id']} has an input dataset")

        # check if every sample from the input datasets is a sample of the library
        for sample in input_dataset_samples:
            if sample not in library_samples:
                raise Exception(
                    f"input dataset(s) {input_dataset_samples['sample']} has sample {sample} not belonging to library {args['library_id']}"
                )

        # check if there exists only one dataset for each sample
        if not all([len(datsets) == 1 for datsets in input_dataset_samples.values()]):
            raise Exception("at least one sample has more than one bam input dataset")

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        datasets = list(
            tantalus_api.list(
                'sequence_dataset',
                dataset_type='BAM',
                analysis__jira_ticket=jira,
                library__library_id=args['library_id'],
                aligner__name__startswith=args['aligner'],
                reference_genome__name=args['ref_genome'],
            ))

        # check if complete set of datasets were fetched
        cls._check_input_datasets_completeness(tantalus_api, args, datasets)

        return [dataset["id"] for dataset in datasets]

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

        # Sort by index_sequence
        bam_filepaths = dict()

        tantalus_index_sequences = set()
        colossus_index_sequences = set()

        storage_client = self.tantalus_api.get_storage_client(storage_name)

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequence_dataset', id=dataset_id)

            file_instances = self.tantalus_api.get_dataset_file_instances(
                dataset['id'],
                'sequencedataset',
                storage_name,
                filters={'filename__endswith': '.bam'},
            )

            for file_instance in file_instances:
                file_resource = file_instance['file_resource']
                index_sequence = file_resource['sequencefileinfo']['index_sequence']
                tantalus_index_sequences.add(index_sequence)
                bam_filepaths[index_sequence] = str(file_instance['filepath'])

                # check if file exists on storage
                error_msg = f"{file_instance['file_resource']['filename']} does not exist on {storage_name}"
                assert storage_client.exists(file_instance['file_resource']['filename']), error_msg

        input_info = {}

        for idx, row in sample_info.iterrows():
            index_sequence = row['index_sequence']

            colossus_index_sequences.add(index_sequence)

            sample_id = row['sample_id']

            input_info[str(row['cell_id'])] = {
                'bam': bam_filepaths[index_sequence],
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

        # get scp configuration i.e. specifies aligner and reference genome
        scp_config = self.get_config(self.args)
        run_options['config_override'] = scp_config

        return workflows.analysis.dlp.launchsc.run_pipeline(
            analysis_type='hmmcopy',
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
            },
            cli_args=[
                '--library_id',
                self.args['library_id'],
            ],
            max_jobs='400',
            dirs=dirs,
        )

    def create_output_results(self, storages, update=False, skip_missing=False):
        """
        Create the set of output results produced by this analysis.
        """
        results_name = results_import.qc_results_name_template.format(
            jira_ticket=self.jira,
            analysis_type=self.analysis_type,
            library_id=self.args['library_id'],
        )

        # filter low complexity region
        self.filter_low_complexity_region(storages)

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

    def filter_low_complexity_region(self, storages):
        """
        Filter "low complexity region" in reads.csv file
        """
        # TODO: filter low complexity region
        library_id = self.args['library_id']

        results_import.filter_low_complexity_region(
            tantalus_api=self.tantalus_api,
            results_dir=self.out_dir,
            library_id=library_id,
            storage_name=storages['working_results']
        )
        # TODO: edit metadata.yml file
        

    @classmethod
    def create_analysis_cli(cls):
        cls.create_cli([
            'sample_id',
            'library_id',
            'aligner',
            'ref_genome',
        ])


workflows.analysis.base.Analysis.register_analysis(HMMCopyAnalysis)

if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    HMMCopyAnalysis.create_analysis_cli()
