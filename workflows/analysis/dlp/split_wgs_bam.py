import os
import yaml
import logging

import workflows.analysis.base
import workflows.analysis.dlp.launchsc
import datamanagement.templates as templates
from datamanagement.utils.utils import get_lanes_hash, get_datasets_lanes_hash
import workflows.analysis.dlp.results_import as results_import


class SplitWGSBamAnalysis(workflows.analysis.base.Analysis):
    def __init__(self, jira, version, args, storages, run_options, **kwargs):
        super(SplitWGSBamAnalysis, self).__init__('split_wgs_bam', jira, version, args, storages, run_options, **kwargs)
        self.bams_dir = os.path.join(jira, "results", self.analysis_type)

        # TODO: Hard coded for now but should be read out of the metadata.yaml files in the future
        self.split_size = 10000000

    def search_input_datasets(self, analysis_type, jira, version, args):
        dataset = self.tantalus_api.get(
            "sequencedataset",
            sample__sample_id=args["sample_id"],
            library__library_id=args["library_id"],
            aligner__name__startswith=args["aligner"],
            reference_genome__name=args["ref_genome"],
            region_split_length=None,
            dataset_type="BAM",
        )

        return [dataset["id"]]

    def generate_unique_name(self, analysis_type, jira, version, args, input_datasets, input_results):
        lanes_hashed = get_datasets_lanes_hash(self.tantalus_api, input_datasets)

        name = templates.SC_QC_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=analysis_type,
            aligner=args['aligner'],
            ref_genome=args['ref_genome'],
            library_id=args['library_id'],
            lanes_hashed=lanes_hashed,
        )

        return name

    def generate_inputs_yaml(self, inputs_yaml_filename):
        assert len(self.analysis['input_datasets']) == 1

        dataset_id = self.analysis['input_datasets'][0]
        file_instances = self.tantalus_api.get_dataset_file_instances(
            dataset_id, 'sequencedataset', self.storages['working_inputs'],
            filters={'filename__endswith': '.bam'})

        input_info = {'normal': {}}
        for file_instance in file_instances:
            input_info['normal']['bam'] = str(file_instance['filepath'])

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
        ):
        storage_client = self.tantalus_api.get_storage_client(self.storages["working_inputs"])
        bams_path = os.path.join(storage_client.prefix, self.bams_dir)

        if self.run_options["skip_pipeline"]:
            return workflows.analysis.dlp.launchsc.run_pipeline2()

        else:
            return workflows.analysis.dlp.launchsc.run_pipeline(
                analysis_type='split_wgs_bam',
                version=self.version,
                run_options=self.run_options,
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

    def create_output_datasets(self, update=False):
        """
        Create the set of output sequence datasets produced by this analysis.
        """
        assert len(self.analysis['input_datasets']) == 1
        input_dataset = self.get_dataset(self.analysis['input_datasets'][0])

        storage_client = self.tantalus_api.get_storage_client(self.storages["working_inputs"])
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
                self.storages["working_inputs"], filepath, update=update)
            file_resources.append(file_resource["id"])

        output_dataset = self.tantalus_api.get_or_create(
            "sequencedataset",
            name=name,
            dataset_type="BAM",
            sample=input_dataset["sample"]["id"],
            library=input_dataset["library"]["id"],
            sequence_lanes=[a["id"] for a in input_dataset["sequence_lanes"]],
            file_resources=file_resources,
            aligner=input_dataset["aligner"],
            reference_genome=input_dataset["reference_genome"],
            region_split_length=self.split_size,
            analysis=self.analysis['id'],
        )

        logging.info("Created sequence dataset {}".format(name))

        return [output_dataset]


