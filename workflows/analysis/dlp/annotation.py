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
from datamanagement.utils.utils import get_datasets_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import


class AnnotationAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'annotation'

    def __init__(self, *args, **kwargs):
        super(AnnotationAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type)

    @classmethod
    def search_input_results(cls, tantalus_api, jira, version, args):
        try:
            align_results_dataset = tantalus_api.get(
                'resultsdataset',
                analysis__jira_ticket=jira,
                results_type='alignment',
            )
        except:
            raise Exception("an align results dataset is expected before annotations run")

        try:
            hmmcopy_results_dataset = tantalus_api.get(
                'resultsdataset',
                analysis__jira_ticket=jira,
                results_type='hmmcopy',
            )
        except:
            raise Exception("a hmmcopy results dataset is expected before annotations run")

        return [dataset["id"] for dataset in [align_results_dataset, hmmcopy_results_dataset]]

    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        # Get hash of lane data based on bams from the same ticket
        bam_datasets = tantalus_api.list(
            'sequence_dataset',
            analysis__jira_ticket=jira,
            library__library_id=args['library_id'],
            dataset_type='BAM',
            aligner__name__startswith=args['aligner'],
            reference_genome__name=args['ref_genome'],
        )

        # TODO: check aligner and reference genome against bam dataset

        lanes_hashed = get_datasets_lanes_hash(tantalus_api, [d['id'] for d in bam_datasets])

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

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        storage_client = self.tantalus_api.get_storage_client(storages['working_inputs'])

        assert len(self.analysis['input_results']) == 2

        input_info = {}

        results_suffixes = {
            'hmmcopy': [
                ('hmmcopy_metrics', '_hmmcopy_metrics.csv.gz'),
                ('hmmcopy_reads', '_reads.csv.gz'),
                ('segs_pdf_tar', '_segs.tar.gz'),
            ],
            'alignment': [
                ('alignment_metrics', '_alignment_metrics.csv.gz'),
                ('gc_metrics', '_gc_metrics.csv.gz'),
            ],
        }

        for dataset_id in self.analysis['input_results']:
            dataset = self.tantalus_api.get('resultsdataset', id=dataset_id)

            for file_type, file_suffix in results_suffixes[dataset['results_type']]:
                file_instances = self.tantalus_api.get_dataset_file_instances(
                    dataset_id,
                    'resultsdataset',
                    storages['working_results'],
                    filters={'filename__endswith': file_suffix})
                assert len(file_instances) == 1
                file_instance = file_instances[0]

                # check if file exists on storage
                assert storage_client.exists(file_instance['file_resource']['filename'])

                input_info[file_type] = file_instance['filepath']

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
            analysis_type='annotation',
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
            'library_id',
            'aligner',
            'ref_genome',
        ])


workflows.analysis.base.Analysis.register_analysis(AnnotationAnalysis)

if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    AnnotationAnalysis.create_analysis_cli()
