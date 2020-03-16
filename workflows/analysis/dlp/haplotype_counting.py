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
from datamanagement.utils.utils import get_lanes_hash, get_datasets_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import
import workflows.analysis.dlp.preprocessing as preprocessing


class HaplotypeCountingAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'count_haps'

    def __init__(self, *args, **kwargs):
        super(HaplotypeCountingAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type, 'sample_{}'.format(self.args['sample_id']))

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        dataset = tantalus_api.get(
            'sequence_dataset',
            analysis__jira_ticket=jira,
            library__library_id=args['library_id'],
            sample__sample_id=args['sample_id'],
            dataset_type='BAM',
            aligner__name__startswith=args['aligner'],
            reference_genome__name=args['ref_genome'],
            region_split_length=None,
        )

        return [dataset["id"]]

    @classmethod
    def search_input_results(cls, tantalus_api, jira, version, args):
        annotation_results = tantalus_api.get(
            'resultsdataset',
            analysis__jira_ticket=jira,
            libraries__library_id=args['library_id'],
            results_type='annotation',
        )

        infer_haps_results = tantalus_api.get(
            'resultsdataset',
            analysis__jira_ticket=args['infer_haps_jira_id'],
            libraries__library_id=args['normal_library_id'],
            samples__sample_id=args['normal_sample_id'],
            results_type='infer_haps',
        )

        return [annotation_results['id'], infer_haps_results['id']]

    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        lanes_hashed = get_datasets_lanes_hash(tantalus_api, input_datasets)

        name = templates.SC_PSEUDOBULK_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=cls.analysis_type_,
            aligner=args['aligner'],
            ref_genome=args['ref_genome'],
            library_id=args['library_id'],
            sample_id=args['sample_id'],
            lanes_hashed=lanes_hashed,
        )

        return name

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        # Separate the results into annotation and infer haps
        for results_id in self.analysis['input_results']:
            results = self.tantalus_api.get('results', id=results_id)

            if results['results_type'] == 'annotation':
                annotation_results = results
            elif results['results_type'] == 'infer_haps':
                infer_haps_results = results
            else:
                raise ValueError('unrecognized results type {results["results_type"]}')

        # Get the haplotypes filepath
        file_instances = self.tantalus_api.get_dataset_file_instances(
            infer_haps_results['id'], 'resultsdataset', storages['working_results'],
            filters={'filename__endswith': 'haplotypes.tsv'})
        assert len(file_instances) == 1
        haplotypes_filepath = file_instances[0]['filepath']

        input_info = {
            'haplotypes': haplotypes_filepath,
            'tumour': {},
        }

        colossus_api = dbclients.colossus.ColossusApi()

        # Get a list of bam filepaths for passed cells
        assert len(self.analysis['input_datasets']) == 1
        dataset_id = self.analysis['input_datasets'][0]
        file_instances = self.tantalus_api.get_dataset_file_instances(
            dataset_id, 'sequencedataset', storages['working_inputs'],
            filters={'filename__endswith': '.bam'})

        index_sequence_sublibraries = colossus_api.get_sublibraries_by_index_sequence(self.args['library_id'])

        for file_instance in file_instances:
            file_resource = file_instance['file_resource']

            index_sequence = file_resource['sequencefileinfo']['index_sequence']
            cell_id = index_sequence_sublibraries[index_sequence]['cell_id']

            input_info['tumour'][cell_id] = {}
            input_info['tumour'][cell_id]['bam'] = str(file_instance['filepath'])

        assert len(input_info['tumour']) > 0

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

        return workflows.analysis.dlp.launchsc.run_pipeline(
            analysis_type='count_haps',
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
            },
            max_jobs='400',
            dirs=dirs,
        )

    def create_output_results(self, storages, update=False, skip_missing=False):
        """
        Create the set of output results produced by this analysis.
        """
        results_name = results_import.pseudobulk_results_name_template.format(
            jira_ticket=self.jira,
            analysis_type=self.analysis_type,
            library_id=self.args['library_id'],
            sample_id=self.args['sample_id'],
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
            'sample_id',
            'library_id',
            'normal_sample_id',
            'normal_library_id',
            'infer_haps_jira_id',
            'aligner',
            'ref_genome',
        ])


workflows.analysis.base.Analysis.register_analysis(HaplotypeCountingAnalysis)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    HaplotypeCountingAnalysis.create_analysis_cli()
