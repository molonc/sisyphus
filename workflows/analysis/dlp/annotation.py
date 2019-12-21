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
                results_type='align',
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
            analysis__jira_ticket=self.jira,
            library__library_id=args['library_id'],
            dataset_type='BAM',
        )

        # TODO: check aligner and reference genome against bam dataset

        lanes_hashed = get_datasets_lanes_hash(tantalus_api, [d['id'] for d in bam_datasets])

        name = templates.SC_QC_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=cls.analysis_type_,
            aligner=args['aligner'],
            ref_genome=args['ref_genome'],
            library_id=args['library_id'],
            lanes_hashed=lanes_hashed,
        )

       return name

'hmmcopy_metrics: hmmcopy', 'A_hmmcopy_metrics.csv.gz'
'hmmcopy_reads: hmmcopy', '_reads.csv.gz'
'alignment_metrics: alignment', '_alignment_metrics.csv.gz'
'gc_metrics: alignment', '_gc_metrics.csv.gz'
'segs_pdf_tar: hmmcopy', '_segs.tar.gz```'

    def generate_inputs_yaml(self, inputs_yaml_filename):
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
                    dataset_id, 'sequencedataset', storages['working_inputs'],
                    filters={'filename__endswith': file_suffix})
                assert len(file_instances) == 1
                file_instance = file_instances[0]

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
            update=False,
            skip_missing=False,
        )

        return [results['id']]


workflows.analysis.base.Analysis.register_analysis(AnnotationAnalysis)


def create_analysis(jira_id, version, args):
    tantalus_api = dbclients.tantalus.TantalusApi()

    analysis = AnnotationAnalysis.create_from_args(tantalus_api, jira_id, version, args)

    logging.info(f'created analysis {analysis.get_id()}')

    if analysis.status.lower() in ('error', 'unknown'):
        analysis.set_ready_status()

    else:
        logging.warning(f'analysis {analysis.get_id()} has status {analysis.status}')


@click.group()
def analysis():
    pass


@analysis.command()
@click.argument('jira_id')
@click.argument('version')
@click.argument('library_id')
@click.argument('aligner')
@click.argument('ref_genome')
def create_single_analysis(jira_id, version, library_id, aligner, ref_genome):
    args = {}
    args['library_id'] = library_id
    args['aligner'] = aligner
    args['ref_genome'] = ref_genome

    create_analysis(jira_id, version, args)


@analysis.command()
@click.argument('version')
@click.argument('info_table')
def create_multiple_analyses(version, info_table):
    info = pd.read_csv(info_table)

    for idx, row in info.iterrows():
        jira_id = row['jira_id']

        args = {}
        args['library_id'] = row['library_id']
        args['aligner'] = row['aligner']
        args['ref_genome'] = row['ref_genome']

        try:
            create_analysis(jira_id, version, args)
        except:
            logging.exception(f'create analysis failed for {jira_id}')


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    analysis()

