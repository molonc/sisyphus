import os
import yaml

import workflows.analysis.base
import workflows.analysis.dlp.launchsc
import datamanagement.templates as templates
from datamanagement.utils.utils import get_lanes_hash
import workflows.analysis.dlp.results_import as results_import


class VariantCallingAnalysis(workflows.analysis.base.Analysis):
    def __init__(self, jira, version, args, storages, run_options, **kwargs):
        super(VariantCallingAnalysis, self).__init__('variant_calling', jira, version, args, storages, run_options, **kwargs)
        self.out_dir = os.path.join(jira, "results", self.analysis_type, 'sample_{}'.format(args['sample_id']))

    # TODO: Hard coded for now but should be read out of the metadata.yaml files in the future
    region_split_length = 10000000

    @classmethod
    def search_input_datasets(cls, tantalus_api, analysis_type, jira, version, args):
        tumour_dataset = tantalus_api.get(
            'sequencedataset',
            dataset_type='BAM',
            analysis__jira_ticket=jira,
            library__library_id=args['library_id'],
            sample__sample_id=args['sample_id'],
            region_split_length=cls.region_split_length,
        )

        # TODO: kludge related to the fact that aligner are equivalent between minor versions
        aligner_name = None
        if tumour_dataset['aligner'].startswith('BWA_MEM'):
            aligner_name = 'BWA_MEM'
        elif tumour_dataset['aligner'].startswith('BWA_ALN'):
            aligner_name = 'BWA_ALN'
        else:
            raise Exception('unknown aligner')

        normal_dataset = tantalus_api.get(
            'sequencedataset',
            dataset_type='BAM',
            sample__sample_id=args['normal_sample_id'],
            library__library_id=args['normal_library_id'],
            aligner__name__startswith=aligner_name,
            reference_genome__name=tumour_dataset['reference_genome'],
            region_split_length=cls.region_split_length,
        )

        return [tumour_dataset['id'], normal_dataset['id']]

    @classmethod
    def generate_unique_name(cls, tantalus_api, analysis_type, jira, version, args, input_datasets, input_results):
        assert len(input_datasets) == 2
        for dataset_id in input_datasets:
            dataset = tantalus_api.get('sequencedataset', id=dataset_id)
            if dataset['sample']['sample_id'] == args['sample_id']:
                tumour_dataset = dataset

        name = templates.SC_PSEUDOBULK_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=analysis_type,
            aligner=tumour_dataset['aligner'],
            ref_genome=tumour_dataset['reference_genome'],
            library_id=tumour_dataset['library']['library_id'],
            sample_id=tumour_dataset['sample']['sample_id'],
            lanes_hashed=get_lanes_hash(tumour_dataset["sequence_lanes"]),
        )

        return name

    def generate_inputs_yaml(self, inputs_yaml_filename):
        assert len(self.analysis['input_datasets']) == 2

        input_info = {}

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequencedataset', id=dataset_id)

            storage_client = self.tantalus_api.get_storage_client(self.storages['working_inputs'])

            # Read the metadata yaml file
            file_instances = self.tantalus_api.get_dataset_file_instances(
                dataset_id, 'sequencedataset', self.storages['working_inputs'],
                filters={'filename__endswith': 'metadata.yaml'})
            assert len(file_instances) == 1
            file_instance = file_instances[0]
            metadata = yaml.safe_load(storage_client.open_file(file_instance['file_resource']['filename']))

            # All filenames relative to metadata.yaml
            base_dir = file_instance['file_resource']['filename'].replace('metadata.yaml', '')

            file_instances = self.tantalus_api.get_dataset_file_instances(
                dataset_id, 'sequencedataset', self.storages['working_inputs'],
                filters={'filename__endswith': '.bam'})

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
        ):
        storage_client = self.tantalus_api.get_storage_client(self.storages["working_results"])
        out_path = os.path.join(storage_client.prefix, self.out_dir)

        if self.run_options["skip_pipeline"]:
            return workflows.analysis.dlp.launchsc.run_pipeline2()

        else:
            return workflows.analysis.dlp.launchsc.run_pipeline(
                analysis_type='variant_calling',
                version=self.version,
                run_options=self.run_options,
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

    def create_output_results(self, update=False, skip_missing=False):
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
            self.storages['working_results'],
            update=False,
            skip_missing=False,
            analysis_type=None,
        )

        return [results['id']]
