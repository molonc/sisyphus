import logging
import datetime

import dbclients.tantalus


class Analysis:
    """
    A class representing an Analysis model in Tantalus.
    """
    def __init__(self, analysis_type, jira, version, args, storages, run_options, update=False):
        """
        Create an Analysis object in Tantalus.
        """
        self.tantalus_api = dbclients.tantalus.TantalusApi()

        if storages is None:
            raise Exception("no storages specified for Analysis")
        self.storages = storages

        self.run_options = run_options

        self.analysis = self.get_or_create_analysis(analysis_type, jira, version, args, update=update)

    @property
    def analysis_type(self):
        return self.analysis['analysis_type']

    @property
    def name(self):
        return self.analysis['name']

    @property
    def jira(self):
        return self.analysis['jira']

    @property
    def version(self):
        return self.analysis['version']

    @property
    def args(self):
        return self.analysis['args']

    @property
    def status(self):
        return self.analysis['status']

    def generate_unique_name(self, analysis_type, jira, version, args, input_datasets, input_results):
        raise NotImplementedError()

    def get_or_create_analysis(self, analysis_type, jira, version, args, update=False):
        """
        Get the analysis by querying Tantalus. Create the analysis
        if it doesn't exist. Set the input dataset ids.
        """

        input_datasets = self.search_input_datasets(analysis_type, jira, version, args)
        input_results = self.search_input_results(analysis_type, jira, version, args)

        name = self.generate_unique_name(
            analysis_type,
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

        keys = ['name']

        analysis = self.tantalus_api.create(
            'analysis', fields, keys, get_existing=True, do_update=update)

        return analysis

    def get_input_datasets(self):
        """ Get input dataset ids
        """
        return self.analysis['input_datasets']

    def get_input_results(self):
        """ Get input results ids
        """
        return self.analysis['input_results']

    def add_inputs_yaml(self, inputs_yaml, update=False):
        """
        Add the inputs yaml to the logs field of the analysis.
        """

        logging.info('Adding inputs yaml file {} to {}'.format(inputs_yaml, self.name))

        file_resource, file_instance = self.tantalus_api.add_file(
            storage_name=self.storages['local_results'],
            filepath=inputs_yaml,
            update=update,
        )

        self.tantalus_api.update('analysis', id=self.get_id(), logs=[file_resource['id']])

    def get_dataset(self, dataset_id):
        """
        Get a dataset by id.
        """
        return self.tantalus_api.get('sequence_dataset', id=dataset_id)

    def get_results(self, results_id):
        """
        Get a results by id.
        """
        return self.tantalus_api.get('results', id=results_id)

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

    def search_input_datasets(self, analysis_type, jira, version, args):
        """
        Get the list of input datasets required to run this analysis.
        """

        return []

    def search_input_results(self, analysis_type, jira, version, args):
        """
        Get the list of input results required to run this analysis.
        """
        return []

    def create_output_datasets(self, update=False):
        """
        Create the set of output sequence datasets produced by this analysis.
        """
        return []

    def create_output_results(self, update=False, skip_missing=False):
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
            dataset = self.get_dataset(dataset_id)
            input_samples.add(dataset['sample']['id'])
        return list(input_samples)

    def get_input_libraries(self):
        """
        Get the primary keys for the libraries associated with
        the input datasets.
        """
        input_libraries = set()
        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)
            input_libraries.add(dataset['library']['id'])
        return list(input_libraries)

    def get_results_filenames(self):
        """
        Get the filenames of results from a list of templates.
        """
        raise NotImplementedError


