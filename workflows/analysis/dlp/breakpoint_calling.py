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
import workflows.analysis.dlp.utils
import datamanagement.templates as templates
from datamanagement.utils.utils import get_lanes_hash
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.preprocessing as preprocessing
import workflows.analysis.dlp.results_import as results_import


class BreakpointCallingAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'breakpoint_calling'

    def __init__(self, *args, **kwargs):
        super(BreakpointCallingAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type, 'sample_{}'.format(self.args['sample_id']))

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        tumour_dataset = workflows.analysis.dlp.utils.get_most_recent(
            tantalus_api,
            'sequencedataset',
            dataset_type='BAM',
            analysis__jira_ticket=jira,
            library__library_id=args['library_id'],
            sample__sample_id=args['sample_id'],
            region_split_length=None,
        )

        # TODO: kludge related to the fact that aligner are equivalent between minor versions
        aligner_name = None
        if tumour_dataset['aligner'].startswith('BWA_MEM'):
            aligner_name = 'BWA_MEM'
        elif tumour_dataset['aligner'].startswith('BWA_ALN'):
            aligner_name = 'BWA_ALN'
        else:
            raise Exception('unknown aligner')

        # TODO: this could also work for normals that are cells
        normal_dataset = workflows.analysis.dlp.utils.get_most_recent(
            tantalus_api,
            'sequencedataset',
            dataset_type='BAM',
            sample__sample_id=args['normal_sample_id'],
            library__library_id=args['normal_library_id'],
            aligner__name__startswith=aligner_name,
            reference_genome__name=tumour_dataset['reference_genome'],
            region_split_length=None,
        )

        return [tumour_dataset['id'], normal_dataset['id']]

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
        assert len(input_datasets) == 2
        for dataset_id in input_datasets:
            dataset = tantalus_api.get('sequence_dataset', id=dataset_id)
            if dataset['sample']['sample_id'] == args['sample_id']:
                tumour_dataset = dataset

        name = templates.SC_PSEUDOBULK_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=cls.analysis_type_,
            aligner=tumour_dataset['aligner'],
            ref_genome=tumour_dataset['reference_genome'],
            library_id=tumour_dataset['library']['library_id'],
            sample_id=tumour_dataset['sample']['sample_id'],
            lanes_hashed=get_lanes_hash(tumour_dataset["sequence_lanes"]),
        )

        return name

    def _get_cell_bams(self, dataset, storages, passed_cell_ids=None):
        colossus_api = dbclients.colossus.ColossusApi()

        index_sequence_sublibraries = colossus_api.get_sublibraries_by_index_sequence(
            dataset['library']['library_id'])

        file_instances = self.tantalus_api.get_dataset_file_instances(
            dataset['id'], 'sequencedataset', storages['working_inputs'],
            filters={'filename__endswith': '.bam'})

        cell_bams = {}

        for file_instance in file_instances:
            file_resource = file_instance['file_resource']

            index_sequence = file_resource['sequencefileinfo']['index_sequence']
            cell_id = index_sequence_sublibraries[index_sequence]['cell_id']

            if passed_cell_ids is not None and cell_id not in passed_cell_ids:
                continue

            cell_bams[cell_id] = {}
            cell_bams[cell_id]['bam'] = str(file_instance['filepath'])

        return cell_bams

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        assert len(self.analysis['input_datasets']) == 2

        input_info = {}

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequence_dataset', id=dataset_id)

            if dataset['sample']['sample_id'] == self.args['normal_sample_id']:
                assert 'normal' not in input_info

                input_info['normal'] = {}

                if dataset['library']['library_type'] == 'SC_WGS':
                    input_info['normal'] = self._get_cell_bams(dataset, storages)

                elif dataset['library']['library_type'] == 'WGS':
                    file_instances = self.tantalus_api.get_dataset_file_instances(
                        dataset_id, 'sequencedataset', storages['working_inputs'],
                        filters={'filename__endswith': '.bam'})

                    assert len(file_instances) == 1
                    input_info['normal']['bam'] = str(file_instances[0]['filepath'])

                else:
                    raise ValueError(f'unsupported library type for dataset {dataset}')

            elif dataset['sample']['sample_id'] == self.args['sample_id']:
                assert 'tumour' not in input_info

                assert len(self.analysis['input_results']) == 1
                cell_ids = preprocessing.get_passed_cell_ids(
                    self.tantalus_api,
                    self.analysis['input_results'][0],
                    storages['working_results'])

                input_info['tumour'] = self._get_cell_bams(dataset, storages, passed_cell_ids=cell_ids)

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
            analysis_type='breakpoint_calling',
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
            update=update,
            skip_missing=skip_missing,
        )

        return [results['id']]


workflows.analysis.base.Analysis.register_analysis(BreakpointCallingAnalysis)


def create_analysis(jira_id, version, args, update=False):
    tantalus_api = dbclients.tantalus.TantalusApi()

    analysis = BreakpointCallingAnalysis.create_from_args(tantalus_api, jira_id, version, args, update=update)

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
@click.option('--update', is_flag=True)
def create_single_analysis(jira_id, version, sample_id, library_id, normal_sample_id, normal_library_id, update=False):
    args = {}
    args['sample_id'] = sample_id
    args['library_id'] = library_id
    args['normal_sample_id'] = normal_sample_id
    args['normal_library_id'] = normal_library_id

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

        try:
            create_analysis(jira_id, version, args, update=update)
        except KeyboardInterrupt:
            raise
        except:
            logging.exception(f'create analysis failed for {jira_id}')


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    analysis()
