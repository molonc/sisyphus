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
from datamanagement.utils.utils import get_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import
import workflows.analysis.dlp.preprocessing as preprocessing


class HaplotypeCountingAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'count_haps'

    def __init__(self, *args, **kwargs):
        super(HaplotypeCountingAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type, 'sample_{}'.format(self.args['sample_id']))

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        dataset = tantalus_api.get(
            'sequence_dataset',
            analysis__jira_ticket=jira,
            library__library_id=args['library_id'],
            sample__sample_id=args['sample_id'],
            dataset_type='BAM',
            region_split_length=None,
        )

        return [dataset["id"]]

    @classmethod
    def search_input_results(cls, tantalus_api, jira, version, args):
        annotation_results = tantalus_api.get(
            'resultsdataset',
            analysis__jira_ticket=jira,
            libraries__library_id=args['library_id'],
            results_type='annotation',
        )

        infer_haps_results = tantalus_api.get(
            'resultsdataset',
            analysis__jira_ticket=args['infer_haps_jira_id'],
            libraries__library_id=args['normal_library_id'],
            samples__sample_id=args['normal_sample_id'],
            results_type='infer_haps',
        )

        return [annotation_results['id'], infer_haps_results['id']]

    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        assert len(input_datasets) == 1
        dataset = tantalus_api.get('sequence_dataset', id=input_datasets[0])

        name = templates.SC_PSEUDOBULK_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=cls.analysis_type_,
            aligner=dataset['aligner'],
            ref_genome=dataset['reference_genome'],
            library_id=dataset['library']['library_id'],
            sample_id=dataset['sample']['sample_id'],
            lanes_hashed=get_lanes_hash(dataset["sequence_lanes"]),
        )

        return name

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        # Separate the results into annotation and infer haps
        for results_id in self.analysis['input_results']:
            results = self.tantalus_api.get('results', id=results_id)

            if results['results_type'] == 'annotation':
                annotation_results = results
            elif results['results_type'] == 'infer_haps':
                infer_haps_results = results
            else:
                raise ValueError('unrecognized results type {results["results_type"]}')

        # Get the haplotypes filepath
        file_instances = self.tantalus_api.get_dataset_file_instances(
            infer_haps_results['id'], 'resultsdataset', storages['working_results'],
            filters={'filename__endswith': 'haplotypes.tsv'})
        assert len(file_instances) == 1
        haplotypes_filepath = file_instances[0]['filepath']

        input_info = {
            'haplotypes': haplotypes_filepath,
            'tumour': {},
        }

        # Get a list of passed cell ids
        colossus_api = dbclients.colossus.ColossusApi()

        cell_ids = preprocessing.get_passed_cell_ids(
            self.tantalus_api,
            annotation_results['id'],
            storages['working_results'])

        if len(cell_ids) == 0:
            raise Exception('0 cells passed preprocessing')

        # Get a list of bam filepaths for passed cells
        assert len(self.analysis['input_datasets']) == 1
        dataset_id = self.analysis['input_datasets'][0]
        file_instances = self.tantalus_api.get_dataset_file_instances(
            dataset_id, 'sequencedataset', storages['working_inputs'],
            filters={'filename__endswith': '.bam'})

        index_sequence_sublibraries = colossus_api.get_sublibraries_by_index_sequence(self.args['library_id'])

        for file_instance in file_instances:
            file_resource = file_instance['file_resource']

            index_sequence = file_resource['sequencefileinfo']['index_sequence']
            cell_id = index_sequence_sublibraries[index_sequence]['cell_id']

            if not cell_id in cell_ids:
                continue

            input_info['tumour'][cell_id] = {}
            input_info['tumour'][cell_id]['bam'] = str(file_instance['filepath'])

        assert len(input_info['tumour']) > 0

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
            analysis_type='count_haps',
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


workflows.analysis.base.Analysis.register_analysis(HaplotypeCountingAnalysis)


def create_analysis(jira_id, version, args, update=False):
    tantalus_api = dbclients.tantalus.TantalusApi()

    analysis = HaplotypeCountingAnalysis.create_from_args(tantalus_api, jira_id, version, args, update=update)

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
@click.argument('normal_sample_id')
@click.argument('normal_library_id')
@click.argument('infer_haps_jira_id')
@click.option('--update', is_flag=True)
def create_single_analysis(jira_id, version, sample_id, library_id, normal_sample_id, normal_library_id, infer_haps_jira_id, update=False):
    args = {}
    args['sample_id'] = sample_id
    args['library_id'] = library_id
    args['normal_sample_id'] = normal_sample_id
    args['normal_library_id'] = normal_library_id
    args['infer_haps_jira_id'] = infer_haps_jira_id

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
        args['normal_sample_id'] = row['normal_sample_id']
        args['normal_library_id'] = row['normal_library_id']
        args['infer_haps_jira_id'] = row['infer_haps_jira_id']

        try:
            create_analysis(jira_id, version, args, update=update)
        except KeyboardInterrupt:
            raise
        except:
            logging.exception(f'create analysis failed for {jira_id}')


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    analysis()
