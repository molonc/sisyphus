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
from datamanagement.utils.utils import get_lanes_hash, get_datasets_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import


class HaplotypeCallingAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'infer_haps'

    def __init__(self, *args, **kwargs):
        super(HaplotypeCallingAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type)

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        dataset = workflows.analysis.dlp.utils.get_most_recent_dataset(
            tantalus_api,
            sample__sample_id=args["sample_id"],
            library__library_id=args["library_id"],
            aligner__name__startswith=args["aligner"],
            reference_genome__name=args["ref_genome"],
            region_split_length=None,
            dataset_type="BAM",
        )

        return [dataset["id"]]

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

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        assert len(self.analysis['input_datasets']) == 1

        dataset_id = self.analysis['input_datasets'][0]

        dataset = self.tantalus_api.get('sequence_dataset', id=dataset_id)

        input_info = {'normal': {}}

        if dataset['library']['library_type'] == 'SC_WGS':
            input_info['normal'] = workflows.analysis.dlp.utils.get_cell_bams(self.tantalus_api, dataset, storages)

        elif dataset['library']['library_type'] == 'WGS':
            file_instances = self.tantalus_api.get_dataset_file_instances(
                dataset_id,
                'sequencedataset',
                storages['working_inputs'],
                filters={'filename__endswith': '.bam'})

            assert len(file_instances) == 1
            input_info['normal']['bam'] = str(file_instances[0]['filepath'])

        else:
            raise ValueError(f'unsupported library type for dataset {dataset}')

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
        out_dir = os.path.join(storage_client.prefix, self.out_dir)

        # get scp configuration i.e. specifies aligner and reference genome
        scp_config = self.get_config(self.args)
        run_options['config_override'] = scp_config

        return workflows.analysis.dlp.launchsc.run_pipeline(
            analysis_type='infer_haps',
            version=self.version,
            run_options=run_options,
            scpipeline_dir=scpipeline_dir,
            tmp_dir=tmp_dir,
            inputs_yaml=inputs_yaml,
            context_config_file=context_config_file,
            docker_env_file=docker_env_file,
            docker_server=docker_server,
            output_dirs={
                'out_dir': out_dir,
            },
            max_jobs='400',
            dirs=dirs,
        )

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
            update=update,
            skip_missing=skip_missing,
        )

        return [results['id']]

    @classmethod
    def create_analysis_cli(cls):
        cls.create_cli([
            'sample_id',
            'library_id',
            'aligner',
            'ref_genome',
        ])


workflows.analysis.base.Analysis.register_analysis(HaplotypeCallingAnalysis)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    HaplotypeCallingAnalysis.create_analysis_cli()
