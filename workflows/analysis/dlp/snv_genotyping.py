import os
import yaml
import logging
import click
import sys
import pandas as pd
import workflows.analysis.dlp.utils
import dbclients.tantalus
import workflows.analysis.base
import workflows.analysis.dlp.launchsc
from datamanagement.utils.constants import LOGGING_FORMAT
import workflows.analysis.dlp.results_import as results_import
import workflows.analysis.dlp.preprocessing as preprocessing


class SnvGenotypingAnalysis(workflows.analysis.base.Analysis):
    analysis_type_ = 'snv_genotyping'

    def __init__(self, *args, **kwargs):
        super(SnvGenotypingAnalysis, self).__init__(*args, **kwargs)
        self.out_dir = os.path.join(self.jira, "results", self.analysis_type)

    @classmethod
    def search_input_results(cls, tantalus_api, jira, version, args):
        results_ids = set()

        for info in args['inputs']:
            library_jira_id = info['library_jira_id']
            library_id = info['library_id']
            sample_id = info['sample_id']

            results = workflows.analysis.dlp.utils.get_most_recent_result(
                tantalus_api,
                libraries__library_id=info['library_id'],
                samples__sample_id=sample_id,
                results_type="variant_calling")

            results_ids.add(results['id'])

            results = workflows.analysis.dlp.utils.get_most_recent_result(
                tantalus_api,
                libraries__library_id=info['library_id'],
                results_type='annotation'
            )
            results_ids.add(results['id'])

        return list(results_ids)

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        dataset_ids = []

        for info in args['inputs']:
            library_jira_id = info['library_jira_id']
            library_id = info['library_id']
            sample_id = info['sample_id']

            dataset = workflows.analysis.dlp.utils.get_most_recent_dataset(
                tantalus_api,
                library__library_id=library_id,
                sample__sample_id=sample_id,
                dataset_type='BAM',
                region_split_length=None
            )

            dataset_ids.append(dataset['id'])

        return dataset_ids

    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        name = '{group_id}_{analysis_type}'.format(
            group_id=args['group_id'],
            analysis_type=cls.analysis_type_,
        )

        return name

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        input_info = {
            'vcf_files':[],
            'tumour_cells': {},
        }

        tumour_sample_ids = set([a['sample_id'] for a in self.args['inputs']])
        tumour_library_ids = set([a['library_id'] for a in self.args['inputs']])
        normal_sample_ids = set([a['normal_sample_id'] for a in self.args['inputs']])
        normal_library_ids = set([a['normal_library_id'] for a in self.args['inputs']])
        assert len(tumour_sample_ids.intersection(normal_sample_ids)) == 0
        assert len(tumour_library_ids.intersection(normal_library_ids)) == 0

        # Retrieve vcf files for museq and strelka snvs
        for results_id in self.analysis['input_results']:
            results = self.tantalus_api.get('results', id=results_id)

            if results['results_type'] != 'variant_calling':
                continue

            # Tumour and normal samples are linked to each results,
            # remove normal sample to get tumour sample id
            sample_ids = [a['sample_id'] for a in results['samples']]
            sample_ids = list(filter(lambda a: a not in normal_sample_ids, sample_ids))
            assert len(sample_ids) == 1
            sample_id = sample_ids[0]

            # Tumour and normal libraries are linked to each results,
            # remove normal library to get tumour library id
            library_ids = [a['library_id'] for a in results['libraries']]
            library_ids = list(filter(lambda a: a not in normal_library_ids, library_ids))
            assert len(library_ids) == 1
            library_id = library_ids[0]


            snv_inputs = [
                ('museq.vcf.gz', 'museq_vcf'),
                ('strelka_snv.vcf.gz', 'strelka_snv_vcf'),
            ]
            for suffix, filetype in snv_inputs:
                file_instances = self.tantalus_api.get_dataset_file_instances(
                    results_id, 'resultsdataset', storages['working_results'],
                    filters={'filename__endswith': suffix})
                assert len(file_instances) == 1
                file_instance = file_instances[0]

                input_info['vcf_files'].append(file_instance['filepath'])
	
        assert len(input_info['vcf_files']) != 0
        colossus_api = dbclients.colossus.ColossusApi()

        # Retrieve bam files for input datasets
        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequencedataset', id=dataset_id)

            sample_id = dataset['sample']['sample_id']
            library_id = dataset['library']['library_id']
            index_sequence_sublibraries = colossus_api.get_sublibraries_by_index_sequence(library_id)

            file_instances = self.tantalus_api.get_dataset_file_instances(
                dataset_id, 'sequencedataset', storages['working_inputs'],
                filters={'filename__endswith': '.bam'})

            for file_instance in file_instances:
                file_resource = file_instance['file_resource']

                index_sequence = file_resource['sequencefileinfo']['index_sequence']
                cell_id = index_sequence_sublibraries[index_sequence]['cell_id']

                input_info['tumour_cells'][sample_id] = input_info['tumour_cells'].get(sample_id, {})
                input_info['tumour_cells'][sample_id][library_id] = input_info['tumour_cells'][sample_id].get(library_id, {})
                assert cell_id not in input_info['tumour_cells'][sample_id][library_id]
                input_info['tumour_cells'][sample_id][library_id][cell_id] = {'bam': str(file_instance['filepath'])}

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
            analysis_type='snv_genotyping',
            version=self.version,
            run_options=run_options,
            scpipeline_dir=scpipeline_dir,
            tmp_dir=tmp_dir,
            inputs_yaml=inputs_yaml,
            context_config_file=context_config_file,
            docker_env_file=docker_env_file,
            docker_server=docker_server,
            output_dirs={
                'output_prefix': out_path+"/"
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
            update=True,
            skip_missing=skip_missing,
        )

        return [results['id']]


workflows.analysis.base.Analysis.register_analysis(SnvGenotypingAnalysis)


@click.group()
def analysis():
    pass


@analysis.command()
@click.argument('jira_id')
@click.argument('version')
@click.argument('group_id')
@click.option('--library_jira_id', multiple=True)
@click.option('--library_id', multiple=True)
@click.option('--sample_id', multiple=True)
@click.option('--normal_library_id', multiple=True)
@click.option('--normal_sample_id', multiple=True)
@click.option('--update', is_flag=True)
def create_single_analysis(jira_id, version, group_id, library_jira_id, library_id, sample_id, normal_library_id, normal_sample_id, update=False):
    tantalus_api = dbclients.tantalus.TantalusApi()
    
    print(len(library_jira_id) , len(library_id) , len(sample_id) , len(normal_library_id) , len(normal_sample_id)) 
    if not (len(library_jira_id) == len(library_id) == len(sample_id) == len(normal_library_id) == len(normal_sample_id)):
        raise ValueError('library_jira_id, library_id, sample_id normal_library_id, normal_sample_id, must be of the same length')

    args = {
        'group_id': group_id,
        'inputs': [],
    }

    for j, l, s, nl, ns in zip(library_jira_id, library_id, sample_id, normal_library_id, normal_sample_id):
        args['inputs'].append({
            'library_jira_id': j,
            'library_id': l,
            'sample_id': s,
            'normal_library_id': nl,
            'normal_sample_id': ns,
        })

    SnvGenotypingAnalysis.create_from_args(tantalus_api, jira_id, version, args, update=update)


@analysis.command()
@click.argument('version')
@click.argument('info_table')
@click.option('--update', is_flag=True)
def create_multiple_analyses(version, info_table, update=False):
    tantalus_api = dbclients.tantalus.TantalusApi()

    info = pd.read_csv(info_table)

    for (jira_id, group_id), df in info.groupby(['jira_id', 'group_id']):
        args = {
            'group_id': group_id,
            'inputs': [],
        }

        for idx, row in df.iterrows():
            args['inputs'].append({
                'library_jira_id': row['library_jira_id'],
                'library_id': row['library_id'],
                'sample_id': row['sample_id'],
                'normal_library_id': row['normal_library_id'],
                'normal_sample_id': row['normal_sample_id'],
            })

        try:
            SnvGenotypingAnalysis.create_from_args(tantalus_api, jira_id, version, args, update=update)
        except KeyboardInterrupt:
            raise
        except:
            logging.exception(f'create analysis failed for {jira_id}')


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    analysis()

