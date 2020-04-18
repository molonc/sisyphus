import os
import yaml
import logging
import click
import sys
import pandas as pd

import dbclients.tantalus
import workflows.analysis.base
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import


class MicroscopePreprocessing(workflows.analysis.base.Analysis):
    analysis_type_ = 'microscope_preprocessing'

    def __init__(self, *args, **kwargs):
        super(MicroscopePreprocessing, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type)

    @classmethod
    def search_input_results(cls, tantalus_api, jira, version, args):
        ### Search in colossus for the cell ids for the library given by args['library_id']
        ## Search according to a template for the location in singlecellresults hwere the data sits

        pass

    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        name = '{analysis_type}_{library_id}'.format(
            analysis_type=cls.analysis_type_,
            library_id=args['library_id'])

        return name

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        storage_client = self.tantalus_api.get_storage_client(storages['working_inputs'])

        input_info = {}

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

        # run the pipeline

    def create_output_results(self, storages, update=False, skip_missing=False):
        """
        Create the set of output results produced by this analysis.
        """
        ### Results name 
        results_name = 'microscope_processed_{}'.format(
            self.args['library_id'])

        library_pk = self.tantalus_api.get(
            'dna_library', library_id=self.args['library_id'])['id']

        results = results_import.create_dlp_results(
            self.tantalus_api,
            self.out_dir,
            self.get_id(),
            results_name,
            [],
            [library_pk],
            storages['working_results'],
            update=update,
            skip_missing=skip_missing,
        )

        return [results['id']]

    @classmethod
    def create_analysis_cli(cls):
        cls.create_cli([
            'library_id',
        ])


workflows.analysis.base.Analysis.register_analysis(MicroscopePreprocessing)

if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    MicroscopePreprocessing.create_analysis_cli()
