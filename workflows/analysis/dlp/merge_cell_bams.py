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
import datamanagement.templates as templates
from datamanagement.utils.utils import get_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.preprocessing as preprocessing


class MergeCellBamsAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'merge_cell_bams'

    def __init__(self, *args, **kwargs):
        super(MergeCellBamsAnalysis, self).__init__(*args, **kwargs)
        self.bams_dir = os.path.join(self.jira, "results", self.analysis_type)

        # TODO: Hard coded for now but should be read out of the metadata.yaml files in the future
        self.split_size = 10000000

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        dataset = tantalus_api.get(
            'sequence_dataset',
            analysis__jira_ticket=jira,
            library__library_id=args['library_id'],
            sample__sample_id=args['sample_id'],
            aligner__name__startswith=args["aligner"],
            reference_genome__name=args["ref_genome"],
            dataset_type='BAM',
            region_split_length=None,
        )

        return [dataset["id"]]

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
        assert len(self.analysis['input_datasets']) == 1

        colossus_api = dbclients.colossus.ColossusApi()

        dataset_id = self.analysis['input_datasets'][0]
        file_instances = self.tantalus_api.get_dataset_file_instances(
            dataset_id, 'sequencedataset', storages['working_inputs'],
            filters={'filename__endswith': '.bam'})

        assert len(self.analysis['input_results']) == 1
        cell_ids = preprocessing.get_passed_cell_ids(
            self.tantalus_api,
            self.analysis['input_results'][0],
            storages['working_results'])

        if len(cell_ids) == 0:
            raise Exception('0 cells passed preprocessing')

        index_sequence_sublibraries = colossus_api.get_sublibraries_by_index_sequence(self.args['library_id'])

        input_info = {'cell_bams': {}}
        for file_instance in file_instances:
            file_resource = file_instance['file_resource']

            index_sequence = file_resource['sequencefileinfo']['index_sequence']
            cell_id = index_sequence_sublibraries[index_sequence]['cell_id']

            if not cell_id in cell_ids:
                continue

            input_info['cell_bams'][cell_id] = {}
            input_info['cell_bams'][cell_id]['bam'] = str(file_instance['filepath'])

        assert len(input_info['cell_bams']) > 0

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
        storage_client = self.tantalus_api.get_storage_client(storages["working_inputs"])
        bams_path = os.path.join(storage_client.prefix, self.bams_dir)

        return workflows.analysis.dlp.launchsc.run_pipeline(
            analysis_type='merge_cell_bams',
            version=self.version,
            run_options=run_options,
            scpipeline_dir=scpipeline_dir,
            tmp_dir=tmp_dir,
            inputs_yaml=inputs_yaml,
            context_config_file=context_config_file,
            docker_env_file=docker_env_file,
            docker_server=docker_server,
            output_dirs={
                'out_dir': bams_path,
            },
            max_jobs='400',
            dirs=dirs,
        )

    def create_output_datasets(self, storages, update=False):
        """
        Create the set of output sequence datasets produced by this analysis.
        """
        assert len(self.analysis['input_datasets']) == 1
        input_dataset = self.tantalus_api.get('sequence_dataset', id=self.analysis['input_datasets'][0])

        storage_client = self.tantalus_api.get_storage_client(storages["working_inputs"])
        metadata_yaml_path = os.path.join(self.bams_dir, "metadata.yaml")
        metadata_yaml = yaml.safe_load(storage_client.open_file(metadata_yaml_path))

        name = templates.WGS_SPLIT_BAM_NAME_TEMPLATE.format(
            dataset_type="BAM",
            sample_id=input_dataset["sample"]["sample_id"],
            library_type=input_dataset["library"]["library_type"],
            library_id=input_dataset["library"]["library_id"],
            lanes_hash=get_lanes_hash(input_dataset["sequence_lanes"]),
            aligner=input_dataset['aligner'],
            reference_genome=input_dataset['reference_genome'],
            split_length=self.split_size,
        )

        file_resources = []
        for filename in metadata_yaml["filenames"] + ['metadata.yaml']:
            filepath = os.path.join(
                storage_client.prefix, self.bams_dir, filename)
            file_resource, file_instance = self.tantalus_api.add_file(
                storages["working_inputs"], filepath, update=update)
            file_resources.append(file_resource["id"])

        data = {
            'name': name,
            'version_number': 1,
            'dataset_type': "BAM",
            'sample': input_dataset["sample"]["id"],
            'library': input_dataset["library"]["id"],
            'sequence_lanes': [a["id"] for a in input_dataset["sequence_lanes"]],
            'file_resources': file_resources,
            'aligner': input_dataset["aligner"],
            'reference_genome': input_dataset["reference_genome"],
            'region_split_length': self.split_size,
            'analysis': self.analysis['id'],
        }

        keys = [
            'name',
            'version_number',
        ]

        output_dataset, _ = self.tantalus_api.create(
            'sequencedataset', data, keys, get_existing=True, do_update=update)

        logging.info("Created sequence dataset {}".format(name))

        return [output_dataset]

    @classmethod
    def create_analysis_cli(cls):
        cls.create_cli([
            'sample_id',
            'library_id',
            'aligner',
            'ref_genome',
        ])


workflows.analysis.base.Analysis.register_analysis(MergeCellBamsAnalysis)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    MergeCellBamsAnalysis.create_analysis_cli()

