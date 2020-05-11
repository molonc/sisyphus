import json
import click
import logging
import datetime
import pandas as pd

import dbclients.tantalus


class Analysis:
    """
    A class representing an Analysis model in Tantalus.
    """
    def __init__(self, tantalus_api, analysis):
        """
        Create an Analysis object in Tantalus.
        """
        self.tantalus_api = tantalus_api
        self.analysis = analysis

    analysis_classes = {}

    @classmethod
    def register_analysis(cls, analysis):
        cls.analysis_classes[analysis.analysis_type_] = analysis

    @classmethod
    def get_by_id(cls, tantalus_api, id):
        tantalus_analysis = tantalus_api.get('analysis', id=id)
        analysis_class = cls.analysis_classes[tantalus_analysis['analysis_type']]
        return analysis_class(tantalus_api, tantalus_analysis)

    @classmethod
    def create_from_args(cls, tantalus_api, jira, version, args, update=False):
        tantalus_analysis = cls.get_or_create_analysis(tantalus_api, jira, version, args, update=update)
        analysis_class = cls.analysis_classes[tantalus_analysis['analysis_type']]
        return analysis_class(tantalus_api, tantalus_analysis)

    @property
    def analysis_type(self):
        return self.analysis['analysis_type']

    @property
    def name(self):
        return self.analysis['name']

    @property
    def jira(self):
        return self.analysis['jira_ticket']

    @property
    def version(self):
        return self.analysis['version']

    @property
    def args(self):
        return self.analysis['args']

    @property
    def status(self):
        return self.analysis['status']

    @classmethod
    def search_input_datasets(cls, tantalus_api, jira, version, args):
        """
        Get the list of input datasets required to run this analysis.
        """

        return []

    @classmethod
    def search_input_results(cls, tantalus_api, jira, version, args):
        """
        Get the list of input results required to run this analysis.
        """
        return []

    @classmethod
    def generate_unique_name(cls, tantalus_api, jira, version, args, input_datasets, input_results):
        raise NotImplementedError()

    @classmethod
    def get_or_create_analysis(cls, tantalus_api, jira, version, args, update=False):
        """
        Get the analysis by querying Tantalus. Create the analysis
        if it doesn't exist. Set the input dataset ids.
        """

        analysis_type = cls.analysis_type_

        input_datasets = cls.search_input_datasets(tantalus_api, jira, version, args)
        input_results = cls.search_input_results(tantalus_api, jira, version, args)

        name = cls.generate_unique_name(
            tantalus_api,
            jira,
            version,
            args,
            input_datasets,
            input_results,
        )

        fields = {
            'name': name,
            'analysis_type': analysis_type,
            'jira_ticket': jira,
            'args': args,
            'input_datasets': input_datasets,
            'input_results': input_results,
            'version': version,
        }

        keys = [
            'name',
            'jira_ticket',
        ]

        analysis, updated = tantalus_api.create('analysis', fields, keys, get_existing=True, do_update=update)

        if updated:
            if analysis['status'] == 'running':
                logging.error(f'updated running analysis {analysis["id"]}')

            else:
                logging.info(f'updated existing analysis {analysis["id"]} with status {analysis["status"]}')

            logging.error(f'resetting analysis {analysis["id"]} to status error')
            analysis = tantalus_api.update('analysis', id=analysis['id'], status='error')

        elif analysis['status'].lower() == 'unknown':
            analysis = tantalus_api.update('analysis', id=analysis['id'], status='ready')
            logging.info(f'created analysis {analysis["id"]} with status {analysis["status"]}')

        else:
            logging.info(f'existing analysis {analysis["id"]} is identical')

        return analysis

    def get_config(self, args):
        """ 
        Get configuration string for scp docker command
        """

        reference_genome_map = {
            'HG19': 'grch37',
            'MM10': 'mm10',
        }

        config = {
            'aligner': args["aligner"].lower().replace("_", "-"),
            'reference': reference_genome_map[args["ref_genome"]],
        }

        config_string = json.dumps(config)

        # Remove all whitespace
        config_string = ''.join(config_string.split()) 
        return r"{}".format(config_string)

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        raise NotImplementedError()

    def set_ready_status(self):
        """
        Set run status of analysis to running.
        """
        self.update_status('ready')
        self.update_last_updated()

    def set_run_status(self):
        """
        Set run status of analysis to running.
        """
        self.update_status('running')
        self.update_last_updated()

    def set_archive_status(self):
        """
        Set run status of analysis to archiving.
        """
        self.update_status('archiving')
        self.update_last_updated()

    def set_complete_status(self):
        """
        Set run status of analysis to complete.
        """
        self.update_status('complete')
        self.update_last_updated()

    def set_error_status(self):
        """
        Set run status to error.
        """
        self.update_status('error')
        self.update_last_updated()

    def update_status(self, status):
        """
        Update the run status of the analysis in Tantalus.
        """
        self.analysis = self.tantalus_api.update('analysis', id=self.get_id(), status=status)

    def update_last_updated(self, last_updated=None):
        """
        Update the last updated field of the analysis in Tantalus.
        """
        if last_updated is None:
            last_updated = datetime.datetime.now().isoformat()
        self.analysis = self.tantalus_api.update('analysis', id=self.get_id(), last_updated=last_updated)

    def get_id(self):
        return self.analysis['id']

    def create_output_datasets(self, storages, update=False):
        """
        Create the set of output sequence datasets produced by this analysis.
        """
        return []

    def create_output_results(self, storages, update=False, skip_missing=False):
        """
        Create the set of output results produced by this analysis.
        """
        return []

    def get_input_samples(self):
        """
        Get the primary keys for the samples associated with
        the input datasets.
        """
        input_samples = set()

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequencedataset', id=dataset_id)
            input_samples.add(dataset['sample']['id'])

        for dataset_id in self.analysis['input_results']:
            dataset = self.tantalus_api.get('resultsdataset', id=dataset_id)
            for sample in dataset["samples"]:
                input_samples.add(sample['id'])

        return list(input_samples)

    def get_input_libraries(self):
        """
        Get the primary keys for the libraries associated with
        the input datasets.
        """
        input_libraries = set()

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.tantalus_api.get('sequencedataset', id=dataset_id)
            input_libraries.add(dataset['library']['id'])

        for dataset_id in self.analysis['input_results']:
            dataset = self.tantalus_api.get('resultsdataset', id=dataset_id)
            for library in dataset["libraries"]:
                input_libraries.add(library['id'])

        return list(input_libraries)

    def get_results_filenames(self):
        """
        Get the filenames of results from a list of templates.
        """
        raise NotImplementedError

    @classmethod
    def create_cli(cls, arguments, options=()):
        """
        Simple cli for creating analyses.
        """
        tantalus_api = dbclients.tantalus.TantalusApi()

        def create_single_analysis(jira_id, version, update=False, **args):
            cls.create_from_args(tantalus_api, jira_id, version, args, update=update)

        for arg_name in reversed(['jira_id', 'version'] + arguments):
            create_single_analysis = click.argument(arg_name)(create_single_analysis)

        for opt_name, default in reversed(options):
            create_single_analysis = click.option('--' + opt_name, default=default)(create_single_analysis)

        create_single_analysis = click.option('--update', is_flag=True)(create_single_analysis)

        def create_multiple_analyses(version, info_table, update=False):
            info = pd.read_csv(info_table)

            for idx, row in info.iterrows():
                jira_id = row['jira_id']

                args = {}
                for arg_name in arguments:
                    args[arg_name] = row[arg_name]
                for opt_name, default in options:
                    args[opt_name] = row.get(opt_name, default)

                try:
                    cls.create_from_args(tantalus_api, jira_id, version, args, update=update)
                except KeyboardInterrupt:
                    raise
                except:
                    logging.exception(f'create analysis failed for {jira_id}')

        def analysis():
            pass

        analysis = click.group()(analysis)
        create_single_analysis = analysis.command()(create_single_analysis)

        create_multiple_analyses = click.argument('info_table')(create_multiple_analyses)
        create_multiple_analyses = click.argument('version')(create_multiple_analyses)
        create_multiple_analyses = click.option('--update', is_flag=True)(create_multiple_analyses)
        create_multiple_analyses = analysis.command()(create_multiple_analyses)

        analysis()
