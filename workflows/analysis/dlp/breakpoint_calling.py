import os
import yaml
import logging
import click
import sys
import pandas as pd

import dbclients.tantalus
import dbclients.colossus
import workflows.analysis.base
import workflows.analysis.dlp.launchsc
import workflows.analysis.dlp.utils
import datamanagement.templates as templates
from datamanagement.utils.utils import get_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.preprocessing as preprocessing
import workflows.analysis.dlp.results_import as results_import


class BreakpointCallingAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'breakpoint_calling'

    def __init__(self, *args, **kwargs):
        super(BreakpointCallingAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(
            self.jira,
            "results",
            self.analysis_type,
            'sample_{}'.format(self.args['sample_id']),
        )

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        tumour_dataset = workflows.analysis.dlp.utils.get_most_recent_dataset(
            tantalus_api,
            dataset_type='BAM',
            analysis__jira_ticket=jira,
            library__library_id=args['library_id'],
            sample__sample_id=args['sample_id'],
            aligner__name__startswith=args['aligner'],
            reference_genome__name=args['ref_genome'],
            region_split_length=None,
        )

        normal_dataset = workflows.analysis.dlp.utils.get_most_recent_dataset(
            tantalus_api,
            dataset_type='BAM',
            sample__sample_id=args['normal_sample_id'],
            library__library_id=args['normal_library_id'],
            aligner__name__startswith=args['aligner'],
            reference_genome__name=args['ref_genome'],
            region_split_length=None,
        )

        return [tumour_dataset['id'], normal_dataset['id']]

    @classmethod
    def search_input_results(cls, tantalus_api, jira, version, args):
        results = tantalus_api.get(
            'resultsdataset',
            analysis__jira_ticket=jira,
            libraries__library_id=args['library_id'],
            results_type='annotation',
        )

        return [results["id"]]

    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        assert len(input_datasets) == 2
        for dataset_id in input_datasets:
            dataset = tantalus_api.get('sequence_dataset', id=dataset_id)
            if dataset['sample']['sample_id'] == args['sample_id'] and dataset['library']['library_id'] == args[
                    'library_id']:
                tumour_dataset = dataset

        assert tumour_dataset['aligner'].startswith(args['aligner'])
        assert tumour_dataset['reference_genome'] == args['ref_genome']
        assert tumour_dataset['library']['library_id'] == args['library_id']
        assert tumour_dataset['sample']['sample_id'] == args['sample_id']

        name = templates.SC_PSEUDOBULK_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=cls.analysis_type_,
            aligner=args['aligner'],
            ref_genome=args['ref_genome'],
            library_id=args['library_id'],
            sample_id=args['sample_id'],
            lanes_hashed=get_lanes_hash(tumour_dataset["sequence_lanes"]),
        )

        return name

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        assert len(self.analysis['input_datasets']) == 2

        storage_client = self.tantalus_api.get_storage_client(storages['working_inputs'])

        input_info = {}
        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequence_dataset', id=dataset_id)

            if dataset['sample']['sample_id'] == self.args['normal_sample_id']:
                assert 'normal' not in input_info

                input_info['normal'] = {}

                if dataset['library']['library_type'] == 'SC_WGS':
                    input_info['normal'] = workflows.analysis.dlp.utils.get_cell_bams(
                        self.tantalus_api,
                        dataset,
                        storages,
                    )

                elif dataset['library']['library_type'] == 'WGS':
                    file_instances = self.tantalus_api.get_dataset_file_instances(
                        dataset_id,
                        'sequencedataset',
                        storages['working_inputs'],
                        filters={'filename__endswith': '.bam'})

                    assert len(file_instances) == 1
                    assert storage_client.exists(file_instances[0]['file_resource']['filename'])
                    input_info['normal']['bam'] = str(file_instances[0]['filepath'])

                else:
                    raise ValueError(f'unsupported library type for dataset {dataset}')

            elif dataset['sample']['sample_id'] == self.args['sample_id']:
                assert 'tumour' not in input_info

                assert len(self.analysis['input_results']) == 1
                cell_ids = preprocessing.get_passed_cell_ids(
                    self.tantalus_api,
                    self.analysis['input_results'][0],
                    storages['working_results'],
                )

                input_info['tumour'] = workflows.analysis.dlp.utils.get_cell_bams(
                    self.tantalus_api,
                    dataset,
                    storages,
                    passed_cell_ids=cell_ids,
                )

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
            analysis_type='breakpoint_calling',
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
            'aligner',
            'ref_genome',
        ])


workflows.analysis.base.Analysis.register_analysis(BreakpointCallingAnalysis)

if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    BreakpointCallingAnalysis.create_analysis_cli()
