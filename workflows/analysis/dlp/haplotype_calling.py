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
        file_instances = self.tantalus_api.get_dataset_file_instances(
            dataset_id, 'sequencedataset', storages['working_inputs'],
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
            run_options,
            storages,
        ):
        storage_client = self.tantalus_api.get_storage_client(storages["working_results"])
        out_dir = os.path.join(storage_client.prefix, self.out_dir)

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


workflows.analysis.base.Analysis.register_analysis(HaplotypeCallingAnalysis)


def create_analysis(jira_id, version, args, update=False):
    tantalus_api = dbclients.tantalus.TantalusApi()

    analysis = HaplotypeCallingAnalysis.create_from_args(tantalus_api, jira_id, version, args, update=update)

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
@click.argument('sample_id')
@click.argument('library_id')
@click.argument('aligner')
@click.argument('ref_genome')
@click.option('--update', is_flag=True)
def create_single_analysis(jira_id, version, sample_id, library_id, aligner, ref_genome, update=False):
    args = {}
    args['sample_id'] = sample_id
    args['library_id'] = library_id
    args['aligner'] = aligner
    args['ref_genome'] = ref_genome

    create_analysis(jira_id, version, args, update=update)


@analysis.command()
@click.argument('version')
@click.argument('info_table')
@click.option('--update', is_flag=True)
def create_multiple_analyses(version, info_table, update=False):
    info = pd.read_csv(info_table)

    for idx, row in info.iterrows():
        jira_id = row['jira_id']

        args = {}
        args['sample_id'] = row['sample_id']
        args['library_id'] = row['library_id']
        args['aligner'] = row['aligner']
        args['ref_genome'] = row['ref_genome']

        try:
            create_analysis(jira_id, version, args, update=update)
        except KeyboardInterrupt:
            raise
        except:
            logging.exception(f'create analysis failed for {jira_id}')


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    analysis()
