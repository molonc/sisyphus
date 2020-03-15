import os
import yaml
import logging
import click
import sys
import pandas as pd

import dbclients.tantalus
import workflows.analysis.base
import workflows.analysis.dlp.launchsc
import workflows.analysis.dlp.utils
import datamanagement.templates as templates
from datamanagement.utils.utils import get_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import


class VariantCallingAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'variant_calling'

    def __init__(self, *args, **kwargs):
        super(VariantCallingAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type, 'sample_{}'.format(self.args['sample_id']))

    # TODO: Hard coded for now but should be read out of the metadata.yaml files in the future
    region_split_length = 10000000

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        tumour_dataset = tantalus_api.get(
            'sequencedataset',
            dataset_type='BAM',
            analysis__jira_ticket=jira,
            library__library_id=args['library_id'],
            sample__sample_id=args['sample_id'],
            aligner__name__startswith=args['aligner'],
            reference_genome__name=args['ref_genome'],
            region_split_length=cls.region_split_length,
        )

        normal_dataset = workflows.analysis.dlp.utils.get_most_recent_dataset(
            tantalus_api,
            dataset_type='BAM',
            sample__sample_id=args['normal_sample_id'],
            library__library_id=args['normal_library_id'],
            aligner__name__startswith=args['aligner'],
            reference_genome__name=args['ref_genome'],
            region_split_length=cls.region_split_length,
        )

        return [tumour_dataset['id'], normal_dataset['id']]

    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        assert len(input_datasets) == 2
        for dataset_id in input_datasets:
            dataset = tantalus_api.get('sequencedataset', id=dataset_id)
            if dataset['sample']['sample_id'] == args['sample_id']:
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

        input_info = {}

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequencedataset', id=dataset_id)

            storage_client = self.tantalus_api.get_storage_client(storages['working_inputs'])

            # Read the metadata yaml file
            file_instances = self.tantalus_api.get_dataset_file_instances(
                dataset_id, 'sequencedataset', storages['working_inputs'],
                filters={'filename__endswith': 'metadata.yaml'})
            assert len(file_instances) == 1
            file_instance = file_instances[0]
            metadata = yaml.safe_load(storage_client.open_file(file_instance['file_resource']['filename']))

            # All filenames relative to metadata.yaml
            base_dir = file_instance['file_resource']['filename'].replace('metadata.yaml', '')

            bam_info = {}
            template = metadata['meta']['bams']['template']
            for instance in metadata['meta']['bams']['instances']:
                region = instance['region']

                bams_filename = template.format(**instance)
                assert bams_filename in metadata['filenames']
                assert region not in bam_info

                bam_info[region] = {}
                bam_info[region]['bam'] = os.path.join(
                    storage_client.prefix,
                    base_dir,
                    bams_filename)

            if dataset['sample']['sample_id'] == self.args['normal_sample_id']:
                assert 'normal' not in input_info
                input_info['normal'] = bam_info
            elif dataset['sample']['sample_id'] == self.args['sample_id']:
                assert 'tumour' not in input_info
                input_info['tumour'] = bam_info
            else:
                raise Exception(f'unrecognized dataset {dataset_id}')

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
            analysis_type='variant_calling',
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


workflows.analysis.base.Analysis.register_analysis(VariantCallingAnalysis)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    VariantCallingAnalysis.create_analysis_cli()
