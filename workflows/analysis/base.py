import logging
import datetime

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
    def register_analysis(cls, analysis)
        analysis_classes[analysis.analysis_type_] = analysis

    @classmethod
    def get_by_id(cls, tantalus_api, id):
        tantalus_analysis = tantalus_api.get('analysis', id=id)
        analysis_class = analysis_classes[tantalus_analysis['analysis_type']]
        analysis_class(tantalus_api, analysis)

    @classmethod
    def create_from_args(cls, tantalus_api, jira, version, args, update=False):
        analysis_type = cls.analysis_type_
        analysis = cls.get_or_create_analysis(analysis_type, jira, version, args, update=update)
        analysis_class = analysis_classes[tantalus_analysis['analysis_type']]
        analysis_class(tantalus_api, analysis)

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

        input_datasets = cls.search_input_datasets(tantalus_api, analysis_type, jira, version, args)
        input_results = cls.search_input_results(tantalus_api, analysis_type, jira, version, args)

        name = cls.generate_unique_name(
            tantalus_api,
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

        analysis = tantalus_api.create(
            'analysis', fields, keys, get_existing=True, do_update=update)

        return analysis

    def generate_inputs_yaml(self, storages, inputs_yaml_filename):
        raise NotImplementedError()

    def add_inputs_yaml(self, storages, inputs_yaml, update=False):
        """
        Add the inputs yaml to the logs field of the analysis.
        """

        logging.info('Adding inputs yaml file {} to {}'.format(inputs_yaml, self.name))

        file_resource, file_instance = self.tantalus_api.add_file(
            storage_name=storages['local_results'],
            filepath=inputs_yaml,
            update=update,
        )

        self.tantalus_api.update('analysis', id=self.get_id(), logs=[file_resource['id']])

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
        return list(input_libraries)

    def get_results_filenames(self):
        """
        Get the filenames of results from a list of templates.
        """
        raise NotImplementedError


