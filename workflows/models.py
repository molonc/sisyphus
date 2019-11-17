import logging
import datetime
import json
import os
import re
import collections
import gzip
import yaml
import hashlib
import subprocess
import pandas as pd
import numpy as np
from datamanagement.utils import dlp
import dbclients.tantalus
import dbclients.colossus
from dbclients.basicclient import NotFoundError
from datamanagement.utils.utils import make_dirs
from datamanagement.transfer_files import transfer_dataset

from workflows.generate_inputs import generate_sample_info
from workflows import launch_pipeline
import workflows.launchsc
import datamanagement.templates as templates
from datamanagement.utils.utils import get_datasets_lanes_hash, get_lanes_hash
from workflows.utils import tantalus_utils, file_utils

log = logging.getLogger('sisyphus')

tantalus_api = dbclients.tantalus.TantalusApi()
colossus_api = dbclients.colossus.ColossusApi()


class AnalysisInfo:
    """
    A class representing an analysis information object in Colossus,
    containing settings for the analysis run.
    """
    def __init__(self, jira):
        self.status = 'idle'
        self.analysis_info = colossus_api.get('analysis_information', analysis_jira_ticket=jira)
        self.analysis_run = self.analysis_info['analysis_run']['id']

    def set_run_status(self):
        self.update('running')

    def set_archive_status(self):
        self.update('archiving')

    def set_error_status(self):
        self.update('error')

    def set_finish_status(self, analysis_type=None):
        if analysis_type is not None:
            self.update(f'{analysis_type}_complete')
        else:
            self.update('complete')

    def update(self, status):
        data = {
            'run_status': status,
            'last_updated': datetime.datetime.now().isoformat(),
        }
        colossus_api.update('analysis_run', id=self.analysis_run, **data)

    def update_results_path(self, path_type, path):
        data = {
            path_type: path,
            'last_updated': datetime.datetime.now().isoformat(),
        }

        colossus_api.update('analysis_run', id=self.analysis_run, **data)


class TenXAnalysisInfo(AnalysisInfo):
    """
    A class representing TenX analysis information object in Colossus,
    containing settings for the analysis run.
    """
    def __init__(self, jira, version, tenx_library_id):
        self.status = 'idle'
        self.analysis = self.get_or_create_analysis(jira, version, tenx_library_id)

    def get_library_id(self, tenx_library_id):
        """
        Given library id, return library pk

        Args:
            tenx_library_id (str)

        Return:
            int
        """

        library = colossus_api.get("tenxlibrary", name=tenx_library_id)

        return library["id"]

    # TODO: Move this to automated tenx script after
    def get_or_create_analysis(self, jira_ticket, version, tenx_library_id):

        try:
            analysis = colossus_api.get('analysis', input_type="TENX", jira_ticket=jira_ticket)

        except NotFoundError:
            library_id = self.get_library_id(tenx_library_id)

            data = {
                "jira_ticket": str(jira_ticket),
                "input_type": "TENX",
                "version": "v1.0.0", # hack
                "run_status": "idle",
                "submission_date": str(datetime.date.today()),
                "dlp_library": None,
                "pbal_library": None,
                "tenx_library": library_id,
                "tenxsequencing_set": [],
                "pbalsequencing_set": [],
                "dlpsequencing_set": [],
            }

            analysis = colossus_api.create('analysis', **data)
            log.info("Created analysis for {} with data {}".format(tenx_library_id, data))

        return analysis

    def set_finish_status(self):
        self.update('complete')

    def update(self, status):
        data = {
            'run_status': status,
        }
        colossus_api.update('analysis', id=self.analysis['id'], **data)


class Analysis(object):
    """
    A class representing an Analysis model in Tantalus.
    """
    def __init__(self, analysis_type, jira, version, args, storages, run_options, update=False):
        """
        Create an Analysis object in Tantalus.
        """
        if storages is None:
            raise Exception("no storages specified for Analysis")

        self.jira = jira
        self.analysis_type = analysis_type
        self.analysis = self.get_or_create_analysis(jira, version, args, update=update)
        self.storages = storages
        self.run_options = run_options

    @property
    def name(self):
        return self.analysis['name']

    @property
    def args(self):
        return self.analysis['args']

    @property
    def status(self):
        return self.analysis['status']

    @property
    def version(self):
        return self.analysis['version']

    def generate_unique_name(self, jira, version, args, input_datasets, input_results):
        raise NotImplementedError()

    def get_or_create_analysis(self, jira, version, args, update=False):
        """
        Get the analysis by querying Tantalus. Create the analysis
        if it doesn't exist. Set the input dataset ids.
        """

        input_datasets = self.search_input_datasets(args)
        input_results = self.search_input_results(args)

        name = self.generate_unique_name(jira, version, args, input_datasets, input_results)

        log.info('Searching for existing analysis {}'.format(name))

        try:
            analysis = tantalus_api.get('analysis', name=name, jira_ticket=jira)
        except NotFoundError:
            analysis = None

        if analysis is not None:
            log.info('Found existing analysis {}'.format(name))

            updated = False

            fields_to_check = {
                'args': (args, lambda a, b: a != b),
                'version': (version, lambda a, b: a != b),
                'input_datasets': (input_datasets, lambda a, b: set(a) != set(b)),
                'input_results': (input_results, lambda a, b: set(a) != set(b)),
            }

            for field_name, (new_data, f_check) in fields_to_check.items():
                if f_check(analysis[field_name], new_data):
                    if update:
                        tantalus_api.update('analysis', id=analysis['id'], **{field_name: new_data})
                        updated = True
                        log.info('{} for analysis {} changed, previously {}, now {}'.format(
                            field_name, name, analysis[field_name], new_data))
                    else:
                        log.warning('{} for analysis {} have changed, previously {}, now {}'.format(
                            field_name, name, analysis[field_name], new_data))

            if updated:
                analysis = tantalus_api.get('analysis', name=name, jira_ticket=jira)

        else:
            log.info('Creating analysis {}'.format(name))

            data = {
                'name': name,
                'jira_ticket': jira,
                'args': args,
                'status': 'idle',
                'input_datasets': input_datasets,
                'input_results': input_results,
                'version': version,
                'analysis_type': self.analysis_type,
            }

            # TODO: created timestamp for analysis
            analysis = tantalus_api.create('analysis', **data)

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

        log.info('Adding inputs yaml file {} to {}'.format(inputs_yaml, self.name))

        file_resource, file_instance = tantalus_api.add_file(
            storage_name=self.storages['local_results'],
            filepath=inputs_yaml,
            update=update,
        )

        tantalus_api.update('analysis', id=self.get_id(), logs=[file_resource['id']])

    def get_dataset(self, dataset_id):
        """
        Get a dataset by id.
        """
        return tantalus_api.get('sequence_dataset', id=dataset_id)

    def get_results(self, results_id):
        """
        Get a results by id.
        """
        return tantalus_api.get('results', id=results_id)

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
        self.analysis = tantalus_api.update('analysis', id=self.get_id(), status=status)

    def update_last_updated(self, last_updated=None):
        """
        Update the last updated field of the analysis in Tantalus.
        """
        if last_updated is None:
            last_updated = datetime.datetime.now().isoformat()
        self.analysis = tantalus_api.update('analysis', id=self.get_id(), last_updated=last_updated)

    def get_id(self):
        return self.analysis['id']

    @staticmethod
    def search_input_datasets(args):
        """
        Get the list of input datasets required to run this analysis.
        """

        return []

    @staticmethod
    def search_input_results(args):
        """
        Get the list of input results required to run this analysis.
        """
        return []

    def create_output_datasets(self, update=False):
        """
        Create the set of output sequence datasets produced by this analysis.
        """
        return []

    def create_output_results(self, update=False, skip_missing=False, analysis_type=None):
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


class DLPAnalysisMixin(object):
    """
    Common functionality for DLP analyses (align, hmmcopy)
    """
    def generate_unique_name(self, jira, version, args, input_datasets, input_results):
        lanes_hashed = get_datasets_lanes_hash(tantalus_api, input_datasets)

        name = templates.SC_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=self.analysis_type,
            aligner=args['aligner'],
            ref_genome=args['ref_genome'],
            library_id=args['library_id'],
            lanes_hashed=lanes_hashed,
        )

        return name


class AlignAnalysis(DLPAnalysisMixin, Analysis):
    """
    Align analysis on Tantalus 
    """
    def __init__(self, jira, version, args, storages, run_options, **kwargs):
        super(AlignAnalysis, self).__init__('align', jira, version, args, storages, run_options, **kwargs)
        self.bams_dir = os.path.join(jira, "results", "bams")
        self.results_dir = os.path.join(jira, "results", self.analysis_type)

    @staticmethod
    def search_input_datasets(args):
        """
        Query Tantalus for paired-end fastq datasets given library id and sample id.

        Returns:
            dataset_ids: list of ids for paired end fastq datasets
        """

        filter_lanes = []
        if args['gsc_lanes'] is not None:
            filter_lanes += args['gsc_lanes']
        if args['brc_flowcell_ids'] is not None:
            # Each BRC flowcell has 4 lanes
            filter_lanes += ['{}_{}'.format(args['brc_flowcell_ids'], i + 1) for i in range(4)]

        datasets = tantalus_api.list(
            'sequence_dataset',
            library__library_id=args['library_id'],
            dataset_type='FQ',
        )

        if not datasets:
            raise Exception('no sequence datasets matching library_id {}'.format(args['library_id']))

        dataset_ids = set()

        for dataset in datasets:
            if len(dataset['sequence_lanes']) != 1:
                raise Exception('sequence dataset {} has {} lanes'.format(
                    dataset['id'],
                    len(dataset['sequence_lanes']),
                ))

            lane_id = '{}_{}'.format(
                dataset['sequence_lanes'][0]['flowcell_id'],
                dataset['sequence_lanes'][0]['lane_number'],
            )

            if filter_lanes and (lane_id not in filter_lanes):
                continue

            dataset_ids.add(dataset['id'])

        return list(dataset_ids)

    def check_inputs_yaml(self, inputs_yaml_filename):
        inputs_dict = file_utils.load_yaml(inputs_yaml_filename)

        lanes = list(self.get_lanes().keys())
        input_lanes = list(inputs_dict.values())[0]['fastqs'].keys()

        num_cells = len(inputs_dict)
        fastq_1_set = set()
        fastq_2_set = set()

        for cell_id, cell_info in inputs_dict.items():
            fastq_1 = list(cell_info["fastqs"].values())[0]["fastq_1"]
            fastq_2 = list(cell_info["fastqs"].values())[0]["fastq_2"]

            fastq_1_set.add(fastq_1)
            fastq_2_set.add(fastq_2)

        if not (num_cells == len(fastq_1_set) == len(fastq_2_set)):
            raise Exception(
                "number of cells is {} but found {} unique fastq_1 and {} unique fastq_2 in inputs yaml".format(
                    num_cells,
                    len(fastq_1_set),
                    len(fastq_2_set),
                ))

        if set(lanes) != set(input_lanes):
            raise Exception('lanes in input datasets: {}\nlanes in inputs yaml: {}'.format(lanes, input_lanes))

    def _generate_cell_metadata(self, storage_name):
        """ Generates per cell metadata

        Args:
            storage_name: Which tantalus storage to look at
        """
        log.info('Generating cell metadata')

        sample_info = generate_sample_info(self.args["library_id"], test_run=self.run_options.get("is_test_run", False))

        if sample_info['index_sequence'].duplicated().any():
            raise Exception('Duplicate index sequences in sample info.')

        if sample_info['cell_id'].duplicated().any():
            raise Exception('Duplicate cell ids in sample info.')

        lanes = self.get_lanes()

        # Sort by index_sequence, lane id, read end
        fastq_filepaths = dict()

        # Lane info
        lane_info = dict()

        tantalus_index_sequences = set()
        colossus_index_sequences = set()

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)

            if len(dataset['sequence_lanes']) != 1:
                raise ValueError('unexpected lane count {} for dataset {}'.format(
                    len(dataset['sequence_lanes']),
                    dataset_id,
                ))

            lane_id = tantalus_utils.get_flowcell_lane(dataset['sequence_lanes'][0])

            lane_info[lane_id] = {
                'sequencing_centre': dataset['sequence_lanes'][0]['sequencing_centre'],
                'sequencing_instrument': dataset['sequence_lanes'][0]['sequencing_instrument'],
                'read_type': dataset['sequence_lanes'][0]['read_type'],
            }

            file_instances = tantalus_api.get_dataset_file_instances(dataset['id'], 'sequencedataset', storage_name)

            for file_instance in file_instances:
                file_resource = file_instance['file_resource']
                read_end = file_resource['sequencefileinfo']['read_end']
                index_sequence = file_resource['sequencefileinfo']['index_sequence']
                tantalus_index_sequences.add(index_sequence)
                fastq_filepaths[(index_sequence, lane_id, read_end)] = str(file_instance['filepath'])

        input_info = {}

        for idx, row in sample_info.iterrows():
            index_sequence = row['index_sequence']

            if self.run_options.get("is_test_run", False) and (index_sequence not in tantalus_index_sequences):
                # Skip index sequences that are not found in the Tantalus dataset, since
                # we need to refer to the original library in Colossus for metadata, but
                # we don't want to iterate through all the cells present in that library
                continue

            colossus_index_sequences.add(index_sequence)

            lane_fastqs = collections.defaultdict(dict)
            for lane_id, lane in lanes.items():
                lane_fastqs[lane_id]['fastq_1'] = fastq_filepaths[(index_sequence, lane_id, 1)]
                lane_fastqs[lane_id]['fastq_2'] = fastq_filepaths[(index_sequence, lane_id, 2)]
                lane_fastqs[lane_id]['sequencing_center'] = lane_info[lane_id]['sequencing_centre']
                lane_fastqs[lane_id]['sequencing_instrument'] = lane_info[lane_id]['sequencing_instrument']

            if len(lane_fastqs) == 0:
                raise Exception('No fastqs for cell_id {}, index_sequence {}'.format(
                    row['cell_id'], row['index_sequence']))

            sample_id = row['sample_id']
            if self.run_options.get("is_test_run", False):
                assert 'TEST' in sample_id

            input_info[str(row['cell_id'])] = {
                'bam': None,
                'fastqs': dict(lane_fastqs),
                'pick_met': str(row['pick_met']),
                'condition': str(row['condition']),
                'primer_i5': str(row['primer_i5']),
                'index_i5': str(row['index_i5']),
                'primer_i7': str(row['primer_i7']),
                'index_i7': str(row['index_i7']),
                'img_col': int(row['img_col']),
                'column': int(row['column']),
                'row': int(row['row']),
                'sample_type': 'null' if (row['sample_type'] == 'X') else str(row['sample_type']),
            }

        if colossus_index_sequences != tantalus_index_sequences:
            raise Exception("index sequences in Colossus and Tantalus do not match")

        return input_info

    def generate_inputs_yaml(self, inputs_yaml_filename):
        """ Generates a YAML file of input information

        Args:
            inputs_yaml_filename: the directory to which the YAML file should be saved
            storage_name: Which tantalus storage to look at
        """

        input_info = self._generate_cell_metadata(self.storages['working_inputs'])

        with open(inputs_yaml_filename, 'w') as inputs_yaml:
            yaml.safe_dump(input_info, inputs_yaml, default_flow_style=False)

        self.check_inputs_yaml(inputs_yaml_filename)

    def get_lanes(self):
        """
        Get the lanes for each input dataset for the analysis.
        """
        lanes = dict()
        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)
            for lane in dataset['sequence_lanes']:
                lane_id = tantalus_utils.get_flowcell_lane(lane)
                lanes[lane_id] = lane
        return lanes

    def create_output_datasets(self, tag_name=None, update=False):
        """ Create BAM datasets in tantalus.
        """
        storage_client = tantalus_api.get_storage_client(self.storages["working_results"])
        metadata_yaml_path = os.path.join(self.bams_dir, "metadata.yaml")
        metadata_yaml = yaml.safe_load(storage_client.open_file(metadata_yaml_path))

        cell_sublibraries = colossus_api.get_sublibraries_by_cell_id(self.args['library_id'])

        sequence_lanes = []

        for lane_id, lane in self.get_lanes().items():

            if self.run_options.get("is_test_run", False):
                assert 'TEST' in lane["flowcell_id"]

            sequence_lanes.append(
                dict(
                    flowcell_id=lane["flowcell_id"],
                    lane_number=lane["lane_number"],
                    sequencing_centre=lane["sequencing_centre"],
                    read_type=lane["read_type"],
                ))

        bam_cell_ids = metadata_yaml["meta"]["cell_ids"]
        bam_template = metadata_yaml["meta"]["bams"]["template"]
        output_file_info = []
        for cell_id in bam_cell_ids:
            bam_filename = bam_template.format(cell_id=cell_id)
            bam_filepath = os.path.join(
                storage_client.prefix,
                self.bams_dir,
                bam_filename,
            )
            bai_filepath = os.path.join(
                storage_client.prefix,
                self.bams_dir,
                f'{bam_filename}.bai',
            )

            for filepath in (bam_filepath, bai_filepath):
                file_info = dict(
                    analysis_id=self.analysis['id'],
                    dataset_type='BAM',
                    sample_id=cell_sublibraries[cell_id]['sample_id']['sample_id'],
                    library_id=self.args['library_id'],
                    library_type='SC_WGS',
                    index_format='D',
                    sequence_lanes=sequence_lanes,
                    ref_genome=self.args['ref_genome'],
                    aligner_name=self.args['aligner'],
                    index_sequence=cell_sublibraries[cell_id]['index_sequence'],
                    filepath=filepath,
                )
                output_file_info.append(file_info)

        log.info('creating sequence dataset models for output bams')

        output_datasets = dlp.create_sequence_dataset_models(
            file_info=output_file_info,
            storage_name=self.storages["working_results"],
            tag_name=tag_name, # TODO: tag?
            tantalus_api=tantalus_api,
            analysis_id=self.get_id(),
            update=update,
        )

        log.info("created sequence datasets {}".format(output_datasets))

    def create_output_results(self, update=False, skip_missing=False, analysis_type=None):
        """
        Create the set of output results produced by this analysis.
        """
        tantalus_results = Results(
            self,
            self.storages['working_results'],
            self.results_dir,
            update=update,
            skip_missing=skip_missing,
            analysis_type=analysis_type,
        )

        return [tantalus_results.get_id()]

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
        if self.run_options["skip_pipeline"]:
            return launch_pipeline.run_pipeline2()

        else:
            return launch_pipeline.run_pipeline(
                analysis_type="alignment",
                args=self.args,
                version=self.version,
                run_options=self.run_options,
                scpipeline_dir=scpipeline_dir,
                tmp_dir=tmp_dir,
                inputs_yaml=inputs_yaml,
                context_config_file=context_config_file,
                docker_env_file=docker_env_file,
                docker_server=docker_server,
                output_dirs={
                    'bams_dir': self.bams_dir,
                    'output_dir': self.results_dir,
                },
                max_jobs='400',
                dirs=dirs,
            )


class HmmcopyAnalysis(DLPAnalysisMixin, Analysis):
    def __init__(self, jira, version, args, storages, run_options, **kwargs):
        super(HmmcopyAnalysis, self).__init__('hmmcopy', jira, version, args, storages, run_options, **kwargs)
        self.results_dir = os.path.join(jira, "results", self.analysis_type)

    def search_input_datasets(self, args):
        datasets = tantalus_api.list(
            'sequence_dataset',
            analysis__jira_ticket=self.jira,
            library__library_id=args['library_id'],
            dataset_type='BAM',
        )

        return [dataset["id"] for dataset in datasets]

    def generate_inputs_yaml(self, inputs_yaml_filename):
        storage_client = tantalus_api.get_storage_client(self.storages["working_results"])
        input_info = yaml.safe_load(
            storage_client.open_file(os.path.join(
                self.jira,
                "results",
                "align",
                "input.yaml",
            )))

        hmmcopy_input_info = dict()
        for cell, cell_info in input_info.items():
            hmmcopy_input_info[cell] = dict()
            for key in cell_info:
                if key != "fastqs":
                    hmmcopy_input_info[cell][key] = cell_info[key]

            hmmcopy_input_info[cell]["bam"] = os.path.join(
                storage_client.prefix,
                self.jira,
                "results",
                "bams",
                f"{cell}.bam",
            )

        with open(inputs_yaml_filename, 'w') as inputs_yaml:
            yaml.safe_dump(hmmcopy_input_info, inputs_yaml, default_flow_style=False)

    def create_output_results(self, update=False, skip_missing=False, analysis_type=None):
        """
        Create the set of output results produced by this analysis.
        """
        tantalus_results = Results(
            self,
            self.storages['working_results'],
            self.results_dir,
            update=update,
            skip_missing=skip_missing,
            analysis_type=analysis_type,
        )

        return [tantalus_results.get_id()]

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
        if self.run_options["skip_pipeline"]:
            return launch_pipeline.run_pipeline2()

        else:
            return launch_pipeline.run_pipeline(
                analysis_type="hmmcopy",
                args=self.args,
                version=self.version,
                run_options=self.run_options,
                scpipeline_dir=scpipeline_dir,
                tmp_dir=tmp_dir,
                inputs_yaml=inputs_yaml,
                context_config_file=context_config_file,
                docker_env_file=docker_env_file,
                docker_server=docker_server,
                output_dirs={
                    'output_dir': self.results_dir,
                },
                max_jobs='400',
                dirs=dirs,
            )


class AnnotationAnalysis(Analysis):
    def __init__(self, jira, version, args, storages, run_options, **kwargs):
        super(AnnotationAnalysis, self).__init__('annotation', jira, version, args, storages, run_options, **kwargs)
        self.results_dir = os.path.join(jira, "results", self.analysis_type)

    def generate_unique_name(self, jira, version, args, input_datasets, input_results):
        """
        Find align analysis name and replace analysis type with annotation in order 
        to have consistent hashed lanes in name.
        """
        try:
            align_analysis = tantalus_api.get(
                "analysis",
                jira_ticket=jira,
                analysis_type__name="align",
            )
        except:
            raise Exception("An align analysis needs to exist in order for annotations to run.")

        analysis_name = align_analysis["name"]
        name = analysis_name.replace("align", "annotation")

        return name

    def search_input_results(self, args):
        try:
            align_results_dataset = tantalus_api.get(
                'resultsdataset',
                name=f"{self.jira}_align",
                analysis__jira_ticket=self.jira,
            )
        except:
            raise Exception("an align results dataset is expected before annotations run")

        try:
            hmmcopy_results_dataset = tantalus_api.get(
                'resultsdataset',
                name=f"{self.jira}_hmmcopy",
                analysis__jira_ticket=self.jira,
            )
        except:
            raise Exception("a hmmcopy results dataset is expected before annotations run")

        return [dataset["id"] for dataset in [align_results_dataset, hmmcopy_results_dataset]]

    def generate_inputs_yaml(self, inputs_yaml_filename):
        storage_client = tantalus_api.get_storage_client(self.storages["working_results"])
        storage_prefix = storage_client.prefix

        alignment_prefix = "align"
        alignment_input_info = dict(
            alignment_metrics=f"{self.args['library_id']}_alignment_metrics.csv.gz",
            gc_metrics=f"{self.args['library_id']}_gc_metrics.csv.gz",
        )

        hmmcopy_prefix = "hmmcopy"
        hmmcopy_input_info = dict(
            hmmcopy_metrics=f"{self.args['library_id']}_hmmcopy_metrics.csv.gz",
            hmmcopy_reads=f"{self.args['library_id']}_reads.csv.gz",
            segs_pdf_tar=f"{self.args['library_id']}_segs.tar.gz",
        )

        for key in alignment_input_info:
            alignment_input_info[key] = os.path.join(
                storage_prefix,
                self.jira,
                "results",
                alignment_prefix,
                alignment_input_info[key],
            )

        for key in hmmcopy_input_info:
            hmmcopy_input_info[key] = os.path.join(
                storage_prefix,
                self.jira,
                "results",
                hmmcopy_prefix,
                hmmcopy_input_info[key],
            )

        input_info = {**alignment_input_info, **hmmcopy_input_info}
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
        if self.run_options["skip_pipeline"]:
            return launch_pipeline.run_pipeline2()

        else:
            return launch_pipeline.run_pipeline(
                analysis_type="annotation",
                args=self.args,
                version=self.version,
                run_options=self.run_options,
                scpipeline_dir=scpipeline_dir,
                tmp_dir=tmp_dir,
                inputs_yaml=inputs_yaml,
                context_config_file=context_config_file,
                docker_env_file=docker_env_file,
                docker_server=docker_server,
                output_dirs={
                    'output_dir': self.results_dir,
                },
                max_jobs='400',
                dirs=dirs,
            )

    def create_output_results(self, update=False, skip_missing=False, analysis_type=None):
        """
        Create the set of output results produced by this analysis.
        """
        tantalus_results = Results(
            self,
            self.storages['working_results'],
            self.results_dir,
            update=update,
            skip_missing=skip_missing,
            analysis_type=analysis_type,
        )

        return [tantalus_results.get_id()]


class SplitWGSBamAnalysis(Analysis):
    def __init__(self, jira, version, args, storages, run_options, **kwargs):
        super(SplitWGSBamAnalysis, self).__init__('split_wgs_bam', jira, version, args, storages, run_options, **kwargs)
        self.run_options = run_options
        self.bams_dir = os.path.join(jira, "results", self.analysis_type)

        # TODO: Hard coded for now but should be read out of the metadata.yaml files in the future
        self.split_size = 10000000

    def search_input_datasets(self, args):
        dataset = tantalus_api.get(
            "sequencedataset",
            sample__sample_id=args["sample_id"],
            library__library_id=args["library_id"],
            aligner__name__startswith=args["aligner"],
            reference_genome__name=args["ref_genome"],
            region_split_length=None,
            dataset_type="BAM",
        )

        return [dataset["id"]]

    def generate_unique_name(self, jira, version, args, input_datasets, input_results):
        lanes_hashed = get_datasets_lanes_hash(tantalus_api, input_datasets)

        name = templates.SC_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type=self.analysis_type,
            aligner=args['aligner'],
            ref_genome=args['ref_genome'],
            library_id=args['library_id'],
            lanes_hashed=lanes_hashed,
        )

        return name

    def generate_inputs_yaml(self, inputs_yaml_filename):
        assert len(self.analysis['input_datasets']) == 1

        dataset_id = self.analysis['input_datasets'][0]
        file_instances = tantalus_api.get_dataset_file_instances(
            dataset_id, 'sequencedataset', self.storages['working_inputs'])

        input_info = {'normal': {}}
        for file_instance in file_instances:
            if file_instance['file_resource']['filename'].endswith('.bam'):
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
        ):
        storage_client = tantalus_api.get_storage_client(self.storages["working_inputs"])
        bams_path = os.path.join(storage_client.prefix, self.bams_dir)

        if self.run_options["skip_pipeline"]:
            return workflows.launchsc.run_pipeline2()

        else:
            return workflows.launchsc.run_pipeline(
                analysis_type='split_wgs_bam',
                version=self.version,
                run_options=self.run_options,
                scpipeline_dir=scpipeline_dir,
                tmp_dir=tmp_dir,
                inputs_yaml=inputs_yaml,
                context_config_file=context_config_file,
                docker_env_file=docker_env_file,
                docker_server=docker_server,
                output_dirs={
                    'out_dir': bams_path,
                },
                max_jobs='400',
                dirs=dirs,
            )

    def create_output_datasets(self, update=False):
        """
        Create the set of output sequence datasets produced by this analysis.
        """
        assert len(self.analysis['input_datasets']) == 1
        input_dataset = self.get_dataset(self.analysis['input_datasets'][0])

        storage_client = tantalus_api.get_storage_client(self.storages["working_inputs"])
        metadata_yaml_path = os.path.join(self.bams_dir, "metadata.yaml")
        metadata_yaml = yaml.safe_load(storage_client.open_file(metadata_yaml_path))

        name = templates.WGS_SPLIT_BAM_NAME_TEMPLATE.format(
            dataset_type="BAM",
            sample_id=input_dataset["sample"]["sample_id"],
            library_type=input_dataset["library"]["library_type"],
            library_id=input_dataset["library"]["library_id"],
            lanes_hash=get_lanes_hash(input_dataset["sequence_lanes"]),
            aligner=input_dataset['aligner'],
            reference_genome=input_dataset['reference_genome'],
            split_length=self.split_size,
        )

        file_resources = []
        for filename in metadata_yaml["filenames"] + ['metadata.yaml']:
            filepath = os.path.join(
                storage_client.prefix, self.bams_dir, filename)
            file_resource, file_instance = tantalus_api.add_file(
                self.storages["working_inputs"], filepath, update=update)
            file_resources.append(file_resource["id"])

        output_dataset = tantalus_api.get_or_create(
            "sequencedataset",
            name=name,
            dataset_type="BAM",
            sample=input_dataset["sample"]["id"],
            library=input_dataset["library"]["id"],
            sequence_lanes=[a["id"] for a in input_dataset["sequence_lanes"]],
            file_resources=file_resources,
            aligner=input_dataset["aligner"],
            reference_genome=input_dataset["reference_genome"],
            region_split_length=self.split_size,
            analysis=self.analysis['id'],
        )

        log.info("Created sequence dataset {}".format(name))

        return [output_dataset]


class SplitTumourAnalysis(DLPAnalysisMixin, Analysis):
    def __init__(self, jira, version, args, run_options, **kwargs):
        super(SplitTumourAnalysis, self).__init__('splittumour', jira, version, args, **kwargs)
        self.run_options = run_options
        self.bams_dir = os.path.join(jira, "results", self.analysis_type)

        # TODO: Hard coded for now but should be read out of the metadata.yaml files in the future
        self.split_size = 10000000

    def search_input_datasets(self, args):
        dataset = tantalus_api.get(
            'sequence_dataset',
            analysis__jira_ticket=self.jira,
            library__library_id=args['library_id'],
            sample__sample_id=args['sample_id'],
            dataset_type='BAM',
        )

        return [dataset["id"]]

    def search_input_results(self, args):
        results = tantalus_api.get(
            'resultsdataset',
            analysis__jira_ticket=self.jira,
            libraries__library_id=args['library_id'],
            results_type='annotation',
        )

        return [results["id"]]

    def get_passed_cell_ids(self):
        assert len(self.analysis['input_results']) == 1

        # Find the metrics file in the annotation results
        library_id = self.args['library_id']
        results_id = self.analysis['input_results'][0]
        file_instances = tantalus_api.get_dataset_file_instances(
            results_id, 'resultsdataset', self.storages['working_inputs'],
            filters={'filename__endswith': f'{library_id}_metrics.csv.gz'})
        assert len(file_instances) == 1
        file_instance = file_instances[0]

        storage_client = tantalus_api.get_storage_client(file_instance['storage']['name'])
        f = storage_client.open_file(file_instance['filepath'])
        alignment_metrics = pd.read_csv(f)

        # Filter cells marked as contaminated
        alignment_metrics = alignment_metrics[~alignment_metrics["is_contaminated"]]
        alignment_metrics = alignment_metrics[alignment_metrics['experimental_condition'] != 'NTC']

        return set(alignment_metrics['cell_id'].values)

    def generate_inputs_yaml(self, inputs_yaml_filename):
        assert len(self.analysis['input_datasets']) == 1

        dataset_id = self.analysis['input_datasets'][0]
        file_instances = tantalus_api.get_dataset_file_instances(
            dataset_id, 'sequencedataset', self.storages['working_inputs'])

        cell_ids = self.get_passed_cell_ids()

        index_sequence_sublibraries = colossus_api.get_sublibraries_by_index_sequence(self.args['library_id'])

        input_info = {'cell_bams': {}}
        for file_instance in file_instances:
            file_resource = file_instance['file_resource']

            if not file_resource['filename'].endswith('.bam'):
                continue

            index_sequence = file_resource['sequencefileinfo']['index_sequence']
            cell_id = index_sequence_sublibraries[index_sequence]['cell_id']

            if not cell_id in cell_ids:
                continue

            input_info['cell_bams'][cell_id]['bam'] = str(file_instance['filepath'])

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
        storage_client = tantalus_api.get_storage_client(self.storages["working_inputs"])
        bams_path = os.path.join(storage_client.prefix, self.bams_dir)

        if self.run_options["skip_pipeline"]:
            return workflows.launchsc.run_pipeline2()

        else:
            return workflows.launchsc.run_pipeline(
                analysis_type='merge_cell_bams',
                version=self.version,
                run_options=self.run_options,
                scpipeline_dir=scpipeline_dir,
                tmp_dir=tmp_dir,
                inputs_yaml=inputs_yaml,
                context_config_file=context_config_file,
                docker_env_file=docker_env_file,
                docker_server=docker_server,
                output_dirs={
                    'out_dir': bams_path,
                },
                max_jobs='400',
                dirs=dirs,
            )

    def create_output_datasets(self, update=False):
        """
        Create the set of output sequence datasets produced by this analysis.
        """
        assert len(self.analysis['input_datasets']) == 1
        input_dataset = self.get_dataset(self.analysis['input_datasets'][0])

        storage_client = tantalus_api.get_storage_client(self.storages["working_results"])
        metadata_yaml_path = os.path.join(self.bams_dir, "metadata.yaml")
        metadata_yaml = yaml.safe_load(storage_client.open_file(metadata_yaml_path))

        name = templates.WGS_SPLIT_BAM_NAME_TEMPLATE.format(
            dataset_type="BAM",
            sample_id=input_dataset["sample"]["sample_id"],
            library_type=input_dataset["library"]["library_type"],
            library_id=input_dataset["library"]["library_id"],
            lanes_str=get_lanes_hash(input_dataset["sequence_lanes"]),
            split_length=self.split_size,
        )

        file_resources = []
        for filename in metadata_yaml["filenames"] + ['metadata.yaml']:
            filepath = os.path.join(
                storage_client.prefix, self.bams_dir, filename)
            file_resource, file_instance = tantalus_api.add_file(
                self.storages["working_results"], filepath, update=update)
            file_resources.append(file_resource["id"])

        output_dataset = tantalus_api.get_or_create(
            "sequencedataset",
            name=name,
            dataset_type="BAM",
            sample=input_dataset["sample"]["id"],
            library=input_dataset["library"]["id"],
            sequence_lanes=[a["id"] for a in input_dataset["sequence_lanes"]],
            file_resources=file_resources,
            aligner=input_dataset["aligner"],
            reference_genome=input_dataset["reference_genome"],
            region_split_length=self.split_size,
            analysis=self.analysis['id'],
        )

        log.info("Created sequence dataset {}".format(name))

        return [output_dataset]


class VariantCallingAnalysis(DLPAnalysisMixin, Analysis):
    def __init__(self, jira, version, args, run_options, **kwargs):
        super(SplitTumourAnalysis, self).__init__('splittumour', jira, version, args, **kwargs)
        self.run_options = run_options

    def search_input_datasets(self, args):
        raise

    def generate_inputs_yaml(self, inputs_yaml_filename):
        raise

    def run_pipeline(self):
        raise


class PseudoBulkAnalysis(Analysis):
    """
    A class representing an pseudobulk analysis in Tantalus.
    """
    def __init__(self, jira, version, args, run_options, **kwargs):
        super(PseudoBulkAnalysis, self).__init__('pseudobulk', jira, version, args, **kwargs)
        self.run_options = run_options

    def generate_unique_name(self, jira, version, args, input_datasets, input_results):
        return '{}_{}'.format(jira, self.analysis_type)

    @staticmethod
    def search_input_datasets(args):
        """
        Query Tantalus for bams that match the associated
        pseudobulk analysis.
        """

        tag_name = args['inputs_tag_name']

        datasets = tantalus_api.list('sequence_dataset', tags__name=tag_name)

        dataset_ids = [dataset['id'] for dataset in datasets]

        if len(dataset_ids) == 0:
            raise Exception('no datasets found with tag {}'.format(tag_name))

        return dataset_ids

    def generate_inputs_yaml(self, inputs_yaml_filename, metric_paths):
        """ Generates a YAML file of input information
        Args:
            inputs_yaml_filename: the directory to which the YAML file should be saved
            storage_name: Which tantalus storage to look at
        """

        make_dirs(os.path.dirname(inputs_yaml_filename))

        input_info = {}

        assert len(self.analysis['input_datasets']) > 0

        # Type of dataset for normal
        normal_library_type = None

        for dataset_id in self.analysis['input_datasets']:
            storage_name = self.storages['working_inputs']
            dataset = self.get_dataset(dataset_id)

            library_id = dataset['library']['library_id']
            sample_id = dataset['sample']['sample_id']
            library_type = dataset['library']['library_type']

            is_normal = (sample_id == self.args['matched_normal_sample']
                         and library_id == self.args['matched_normal_library'])

            if is_normal:
                file_resources = list(
                    tantalus_api.get_dataset_file_resources(
                        dataset_id,
                        'sequencedataset',
                        filters={'filename__endswith': '.bam'},
                    ))
                single_file_resource_id = file_resources[0]["id"]

                file_instances = list(tantalus_api.list("file_instance", file_resource=single_file_resource_id))
                storage_name = file_instances[0]["storage"]["name"]

            dataset_class = ('tumour', 'normal')[is_normal]

            if not is_normal and not self.run_options["no_contamination_check"]:
                # Read metric file
                metric_path = metric_paths[library_id]
                with gzip.open(metric_path) as f:
                    alignment_metric = pd.read_csv(f)
                    alignment_metric = alignment_metric[alignment_metric["is_contaminated"] == False]

            if dataset_class == 'normal':
                assert normal_library_type is None
                normal_library_type = library_type

            if dataset_class not in input_info:
                input_info[dataset_class] = {}

            if sample_id not in input_info[dataset_class]:
                input_info[dataset_class][sample_id] = {}

            input_info[dataset_class][sample_id][library_id] = {}

            file_instances = tantalus_api.get_dataset_file_instances(
                dataset_id,
                'sequencedataset',
                storage_name,
                filters={'filename__endswith': '.bam'},
            )

            if library_type == 'WGS':
                if not is_normal:
                    raise ValueError('WGS only supported for normal')

                file_instances = list(file_instances)
                if len(file_instances) != 1:
                    raise ValueError('expected 1 file got {}'.format(len(file_instances)))

                file_instance = file_instances[0]
                filepath = str(file_instance['filepath'])
                input_info[dataset_class][sample_id][library_id] = {'bam': filepath}

            elif library_type == 'SC_WGS':
                sample_info = generate_sample_info(library_id, test_run=self.run_options.get("is_test_run", False))

                cell_ids = sample_info.set_index('index_sequence')['cell_id'].to_dict()

                for file_instance in file_instances:
                    index_sequence = str(file_instance['file_resource']['sequencefileinfo']['index_sequence'])
                    cell_id = str(cell_ids[index_sequence])
                    filepath = str(file_instance['filepath'])

                    if not is_normal and not self.run_options["no_contamination_check"]:
                        # If cell is contaminated, exclude from run
                        if cell_id not in alignment_metric["cell_id"].values:
                            log.info("Skipping contaminated cell {}".format(cell_id))
                            continue

                    if cell_id not in input_info[dataset_class][sample_id][library_id]:
                        input_info[dataset_class][sample_id][library_id][cell_id] = {}

                    input_info[dataset_class][sample_id][library_id][cell_id] = {'bam': filepath}

            else:
                raise ValueError('unknown library type {}'.format(library_type))

        if 'normal' not in input_info or len(input_info['normal']) == 0:
            raise ValueError('unable to find normal {}, {}'.format(
                self.args['matched_normal_sample'],
                self.args['matched_normal_library'],
            ))

        if 'tumour' not in input_info or len(input_info['tumour']) == 0:
            raise ValueError('no tumour cells found')

        # Fix up input key names dependent on library type
        if normal_library_type == 'SC_WGS':
            normal_sample_ids = list(input_info['normal'].keys())
            assert len(normal_sample_ids) == 1
            input_info['normal_cells'] = input_info.pop('normal')
        elif normal_library_type == 'WGS':
            input_info['normal_wgs'] = input_info.pop('normal')
        else:
            raise Exception('normal library type {}'.format(normal_library_type))

        input_info['tumour_cells'] = input_info.pop('tumour')

        with open(inputs_yaml_filename, 'w') as inputs_yaml:
            yaml.safe_dump(input_info, inputs_yaml, default_flow_style=False)

    def get_results_filenames(self):
        """ Get list of results produced by pseudobulk pipeline.
        """
        results_prefix = os.path.join(self.run_options["job_subdir"], "results")

        destruct_prefix = os.path.join(results_prefix, "destruct")
        haps_prefix = os.path.join(results_prefix, "haps")
        lumpy_prefix = os.path.join(results_prefix, "lumpy")
        variants_prefix = os.path.join(results_prefix, "variants")

        filenames = []

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)

            library_id = dataset['library']['library_id']
            sample_id = dataset['sample']['sample_id']

            if sample_id == self.args['matched_normal_sample'] and library_id == self.args['matched_normal_library']:
                continue

            destruct_filenames = [
                "metadata.yaml",
                "{sample_id}_{library_id}_cell_counts_destruct.csv.gz",
                "{sample_id}_{library_id}_cell_counts_destruct.csv.gz.yaml",
                "{sample_id}_{library_id}_destruct_library.csv.gz",
                "{sample_id}_{library_id}_destruct_library.csv.gz.yaml",
                "{sample_id}_{library_id}_destruct.csv.gz",
                "{sample_id}_{library_id}_destruct.csv.gz.yaml",
            ]
            destruct_filenames = [os.path.join(destruct_prefix, filename) for filename in destruct_filenames]

            haps_filenames = [
                "haplotypes.tsv",
                "metadata.yaml",
                "{sample_id}_{library_id}_allele_counts.csv",
                "{sample_id}_{library_id}_allele_counts.csv.yaml",
            ]
            haps_filenames = [os.path.join(haps_prefix, filename) for filename in haps_filenames]

            lumpy_filenames = [
                "metadata.yaml",
                "{sample_id}_{library_id}_lumpy_breakpoints_evidence.csv.gz",
                "{sample_id}_{library_id}_lumpy_breakpoints.bed",
                "{sample_id}_{library_id}_lumpy_breakpoints.csv.gz",
            ]

            lumpy_filenames = [os.path.join(lumpy_prefix, filename) for filename in lumpy_filenames]

            variants_filenames = [
                "metadata.yaml",
                "{sample_id}_{library_id}_museq.vcf.gz",
                "{sample_id}_{library_id}_snv_annotations.h5",
                "{sample_id}_{library_id}_snv_counts.h5",
                os.path.join(
                    "variant_calling_rawdata",
                    "{sample_id}_{library_id}_variant_calling",
                    "snv",
                    "cosmic_status.h5",
                ),
                os.path.join(
                    "variant_calling_rawdata",
                    "{sample_id}_{library_id}_variant_calling",
                    "snv",
                    "dbsnp_status.h5",
                ),
                os.path.join(
                    "variant_calling_rawdata",
                    "{sample_id}_{library_id}_variant_calling",
                    "snv",
                    "mappability.h5",
                ),
                os.path.join(
                    "variant_calling_rawdata",
                    "{sample_id}_{library_id}_variant_calling",
                    "snv",
                    "snpeff.h5",
                ),
                os.path.join(
                    "variant_calling_rawdata",
                    "{sample_id}_{library_id}_variant_calling",
                    "snv",
                    "tri_nucleotide_context.h5",
                ),
            ]

            for snv_caller in ('museq', 'strelka_snv', 'strelka_indel'):
                variants_filenames.append('{}_{}_{}.vcf.gz'.format(sample_id, library_id, snv_caller))
                variants_filenames.append('{}_{}_{}.vcf.gz.csi'.format(sample_id, library_id, snv_caller))
                variants_filenames.append('{}_{}_{}.vcf.gz.tbi'.format(sample_id, library_id, snv_caller))

            variants_filenames = [os.path.join(variants_prefix, filename) for filename in variants_filenames]

            for files in [destruct_filenames, haps_filenames, lumpy_filenames, variants_filenames]:
                filenames += [f.format(sample_id=sample_id, library_id=library_id) for f in files]

        return filenames

    def run_pipeline(
            self,
            results_dir,
            pipeline_dir,
            scpipeline_dir,
            tmp_dir,
            inputs_yaml,
            config,
            destruct_output,
            lumpy_output,
            haps_output,
            variants_output,
    ):
        dirs = [
            pipeline_dir,
            config['docker_path'],
            config['docker_sock_path'],
            config['refdata_path'],
        ]

        # Pass all server storages to docker
        for storage_name in self.storages.values():
            storage = tantalus_api.get('storage', name=storage_name)
            if storage['storage_type'] == 'server':
                dirs.append(storage['storage_directory'])

        run_cmd = [
            'single_cell',
            'multi_sample_pseudo_bulk',
            '--input_yaml',
            inputs_yaml,
            '--tmpdir',
            tmp_dir,
            '--maxjobs',
            '1000',
            '--nocleanup',
            '--sentinel_only',
            '--loglevel',
            'DEBUG',
            '--pipelinedir',
            scpipeline_dir,
            '--context_config',
            config['context_config_file']['sisyphus'],
            '--destruct_output',
            destruct_output,
            '--lumpy_output',
            lumpy_output,
            '--haps_output',
            haps_output,
            '--variants_output',
            variants_output,
        ]

        if self.run_options['local_run']:
            run_cmd += ["--submit", "local"]

        else:
            run_cmd += [
                '--submit',
                'azurebatch',
                '--storage',
                'azureblob',
            ]

        # Append docker command to the beginning
        docker_cmd = [
            'docker',
            'run',
            '-w',
            '$PWD',
            '-v',
            '$PWD:$PWD',
            '-v',
            '/var/run/docker.sock:/var/run/docker.sock',
            '-v',
            '/usr/bin/docker:/usr/bin/docker',
            '--rm',
            '--env-file',
            config['docker_env_file'],
        ]

        for d in dirs:
            docker_cmd.extend([
                '-v',
                '{d}:{d}'.format(d=d),
            ])

        docker_cmd.append('{}/scp/single_cell_pipeline:{}'.format(config["docker_server"], self.version))

        run_cmd = docker_cmd + run_cmd

        if self.run_options['sc_config'] is not None:
            run_cmd += ['--config_file', self.run_options['sc_config']]
        if self.run_options['interactive']:
            run_cmd += ['--interactive']

        run_cmd_string = r' '.join(run_cmd)
        log.debug(run_cmd_string)
        subprocess.check_call(run_cmd_string, shell=True)


class TenXAnalysis(Analysis):
    """
    A class representing an TenX analysis in Tantalus.
    """
    def __init__(self, jira, version, args, run_options, **kwargs):
        super(TenXAnalysis, self).__init__('tenx', jira, "v1.0.0", args, **kwargs)
        self.run_options = run_options

    def generate_unique_name(self, jira, version, args, input_datasets, input_results):
        return '{}_{}_{}'.format(args['library_id'], jira, self.analysis_type)

    def get_lane_ids(self):
        """
        Get the lanes for each input dataset for the analysis.
        """
        lane_ids = []
        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)
            for lane in dataset['sequence_lanes']:
                lane_ids.append(lane["id"])
        return lane_ids

    @staticmethod
    def search_input_datasets(args):

        # Double check this
        datasets = tantalus_api.list(
            "sequence_dataset",
            library__library_id=args["library_id"],
            dataset_type="FQ",
        )

        dataset_ids = [dataset["id"] for dataset in datasets]

        # Check if each datasets file resource has a file instance in rnaseq
        for dataset_id in dataset_ids:
            file_instances = tantalus_api.get_dataset_file_instances(dataset_id, "sequencedataset", "scrna_fastq")

        return dataset_ids

    def create_output_datasets(self, tag_name=None, update=False):

        library_id = self.args["library_id"]
        ref_genome = self.args["ref_genome"]

        dna_library = tantalus_api.get("dna_library", library_id=library_id)

        tenx_library = colossus_api.get("tenxlibrary", name=library_id)
        sample_id = tenx_library["sample"]["sample_id"]
        sample = tantalus_api.get("sample", sample_id=sample_id)

        storage_name = "scrna_bams"
        storage_client = tantalus_api.get_storage(storage_name)

        sequence_lanes = self.get_lane_ids()

        lanes_hashed = get_datasets_lanes_hash(tantalus_api, self.analysis["input_datasets"])

        bam_filepath = os.path.join(storage_client["prefix"], library_id, "bams.tar.gz")
        file_resource, file_instance = tantalus_api.add_file(storage_name, bam_filepath, update=True)

        name = "BAM-{}-SC_RNASEQ-lanes_{}-{}".format(library_id, lanes_hashed, ref_genome)

        sequence_dataset = tantalus_api.get_or_create(
            "sequence_dataset",
            name=name,
            dataset_type="BAM",
            sample=sample["id"],
            library=dna_library["id"],
            sequence_lanes=sequence_lanes,
            file_resources=[file_resource["id"]],
            reference_genome=self.args["ref_genome"],
            aligner=None,
            analysis=self.analysis['id'],
        )

        log.info("Created sequence dataset {}".format(name))

    def get_results_filenames(self):

        results_prefix = "scrnadata"

        # Double check if there are more files
        filenames = [
            os.path.join("cellrangerv3", "{library_id}.tar.gz"),
            os.path.join("rdatav3", "{library_id}.rdata"),
            os.path.join("rdatarawv3", "{library_id}.rdata"),
            os.path.join("reports", "{library_id}.tar.gz"),
            os.path.join("bams", "{library_id}", "bams.tar.gz"),
        ]

        return [os.path.join(results_prefix, filename.format(**self.args)) for filename in filenames]

    def run_pipeline(self, version, data_dir, runs_dir, reference_dir, results_dir, library_id, reference_genome):

        for directory in [data_dir, runs_dir, reference_dir, results_dir]:
            directory_path = os.path.join(os.environ["HEADNODE_AUTOMATION_DIR"], "workflows", directory)
            if not os.path.exists(directory_path):
                log.info("creating dir {}".format(directory_path))
                os.makedirs(directory_path)

        reference_genome_map = {
            "HG38": "GRCh38",
            "MM10": "mm10",
        }

        reference_genome = reference_genome_map[reference_genome]

        docker_cmd = [
            'docker',
            'run',
            '--mount type=bind,source={},target=/reference '.format(reference_dir),
            '--mount type=bind,source={},target=/results '.format(results_dir),
            '--mount type=bind,source={},target=/data '.format(data_dir),
            '--mount type=bind,source="{}",target=/runs '.format(runs_dir),
            '-w="/runs"',
            '-t',
            'nceglia/scrna-pipeline:devvm run_vm',
            '--sampleid',
            library_id,
            '--build',
            reference_genome,
        ]

        run_cmd_string = r' '.join(docker_cmd)
        log.debug(run_cmd_string)

        subprocess.check_call(run_cmd_string, shell=True)

    def create_output_results(self, update=False, skip_missing=False):
        """
        Create the set of output results produced by this analysis.
        """
        tantalus_results = TenXResults(
            self,
            self.storages['working_results'],
            update=update,
            skip_missing=skip_missing,
        )

        return [tantalus_results.get_id()]


class Results:
    """
    A class representing a Results model in Tantalus.
    """
    def __init__(
            self,
            tantalus_analysis,
            storage_name,
            results_dir,
            update=False,
            skip_missing=False,
            analysis_type=None):
        """
        Create a Results object in Tantalus.
        """

        self.tantalus_analysis = tantalus_analysis
        self.storage_name = storage_name
        self.results_dir = results_dir
        self.name = '{}_{}'.format(self.tantalus_analysis.jira, analysis_type)
        self.analysis = self.tantalus_analysis.get_id()
        self.analysis_type = analysis_type
        self.samples = self.tantalus_analysis.get_input_samples()
        self.libraries = self.tantalus_analysis.get_input_libraries()
        self.pipeline_version = self.tantalus_analysis.version
        self.last_updated = datetime.datetime.now().isoformat()

        self.results = self.get_or_create_results(
            update=update,
            skip_missing=skip_missing,
            analysis_type=self.analysis_type,
        )

    def get_or_create_results(self, update=False, skip_missing=False, analysis_type=None):
        log.info('Searching for existing results {}'.format(self.name))
        storage_client = tantalus_api.get_storage_client(self.storage_name)

        try:
            results = tantalus_api.get(
                'results',
                name=self.name,
                results_type=self.analysis_type,
                analysis=self.analysis,
            )
        except NotFoundError:
            results = None

        # Load the metadata.yaml file, assumed to exist in the root of the results directory
        metadata_filename = os.path.join(self.results_dir, "metadata.yaml")
        metadata = yaml.safe_load(storage_client.open_file(metadata_filename))

        # Add all files to tantalus including the metadata.yaml file
        file_resource_ids = set()
        for filename in metadata["filenames"] + ['metadata.yaml']:
            filename = os.path.join(self.results_dir, filename)
            filepath = os.path.join(storage_client.prefix, filename)

            if not storage_client.exists(filename) and skip_missing:
                logging.warning('skipping missing file: {}'.format(filename))
                continue

            file_resource, _ = tantalus_api.add_file(
                storage_name=self.storage_name,
                filepath=filepath,
                update=update,
            )

            file_resource_ids.add(file_resource["id"])

        data = {
            'name': self.name,
            'results_type': self.analysis_type,
            'results_version': metadata["meta"]["version"],
            'analysis': self.analysis,
            'file_resources': list(file_resource_ids),
            'samples': self.samples,
            'libraries': self.libraries,
        }

        if results is not None:
            log.info('Found existing results {}'.format(self.name))
            if update:
                log.info("Updating {} ".format(self.name))
                tantalus_api.update('results', results["id"], **data)

            results = tantalus_api.get("results", name=self.name)

        else:
            log.info('Creating results {}'.format(self.name))

            # TODO: created timestamp for results
            results = tantalus_api.create('results', **data)

        return results

    def update_results(self, field):
        field_value = vars(self)[field]
        if self.results[field] != field_value:
            tantalus_api.update('results', id=self.get_id(), **{field: field_value})

    def get_id(self):
        return self.results['id']


class TenXResults(Results):
    """
    A class representing a Results model in Tantalus.
    """
    def __init__(self, tantalus_analysis, storages, update=False, skip_missing=False):
        """
        Create a TenX Results object in Tantalus.
        """

        self.tantalus_analysis = tantalus_analysis
        self.storages = storages
        self.name = '{}_{}'.format(self.tantalus_analysis.jira, self.tantalus_analysis.analysis_type)
        self.analysis = self.tantalus_analysis.get_id()
        self.analysis_type = self.tantalus_analysis.analysis_type
        self.samples = self.tantalus_analysis.get_input_samples()
        self.libraries = self.tantalus_analysis.get_input_libraries()
        self.pipeline_version = self.tantalus_analysis.version
        self.last_updated = datetime.datetime.now().isoformat()

        self.results = self.get_or_create_results(update=update)

    def get_file_resources(self, update=False, skip_missing=False, analysis_type=None):
        """
        Create file resources for each results file and return their ids.
        """
        file_resource_ids = set()

        results_filepaths = self.tantalus_analysis.get_results_filenames()

        for storage in self.storages:
            storage_client = tantalus_api.get_storage_client(storage)
            for result_filepath in results_filepaths:
                if result_filepath.startswith(storage_client.prefix):
                    result_filename = result_filepath.strip(storage_client.prefix + '/')

                    if not storage_client.exists(result_filename) and skip_missing:
                        logging.warning('skipping missing file: {}'.format(result_filename))
                        continue

                    file_resource, file_instance = tantalus_api.add_file(
                        storage_name=storage,
                        filepath=result_filepath,
                        update=update,
                    )

                    file_resource_ids.add(file_resource["id"])

        return list(file_resource_ids)
