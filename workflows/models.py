import logging
import datetime
import json
import os
import re
import collections
import yaml

from datamanagement.utils import dlp
import dbclients.tantalus
from dbclients.basicclient import NotFoundError

import generate_inputs
import datamanagement.templates as templates
from utils import colossus_utils, tantalus_utils, file_utils

from azure.storage.blob import BlockBlobService

AZURE_STORAGE_ACCOUNT= os.environ['AZURE_STORAGE_ACCOUNT']
AZURE_STORAGE_KEY=os.environ['AZURE_STORAGE_KEY']

log = logging.getLogger('sisyphus')

tantalus_api = dbclients.tantalus.TantalusApi()


class AnalysisInfo:
    """
    A class representing an analysis information object in Colossus,
    containing settings for the analysis run.
    """
    def __init__(self, jira, log_file, args):
        self.jira = jira
        self.status = 'idle'

        self.aligner_choices = {
            'A':    'bwa-aln',
            'M':    'bwa-mem',
        }

        self.smoothing_choices = {
            'M':    'modal',
            'L':    'loess',
        }

        self.analysis_info = colossus_utils.get_analysis_info(jira)

        self.aligner = self.get_aligner()
        self.smoothing = self.get_smoothing()
        self.reference_genome = self.get_reference_genome()
        self.pipeline_version = self.get_pipeline_version()

        self.id = self.analysis_info['id']
        self.analysis_run = self.analysis_info['analysis_run']['id']
        self.sequencing_ids = self.analysis_info['sequencings']
        self.log_file = log_file

        # Set the chip ID (= DLP library ID) from the sequencings associated with the analysis object from Colossus
        self.chip_id = self.get_chip_id()
        self.paths_to_archives = self.get_paths_to_archives()

    def get_reference_genome(self):
        reference_genome = self.analysis_info['reference_genome']['reference_genome']
        if reference_genome not in ('grch37', 'mm10'):
            raise Exception('Unrecognized reference genome {}'.format(reference_genome))
        return reference_genome

    def get_pipeline_version(self):
        version_str = self.analysis_info['version']['version']
        if not version_str.startswith('Single Cell Pipeline '):
            raise Exception('Unrecognized version string {}'.format(version_str))
        return version_str.replace('Single Cell Pipeline v', '').replace('_', '.')

    def get_aligner(self):
        if 'aligner' in self.analysis_info:
            return self.aligner_choices[self.analysis_info['aligner']]
        return None

    def get_smoothing(self):
        if 'smoothing' in self.analysis_info:
            return self.smoothing_choices[self.analysis_info['smoothing']]
        return None

    def get_chip_id(self):
        chip_ids = set()
        for seq in self.sequencing_ids:
            chip_ids.add(colossus_utils.get_chip_id_from_sequencing(seq))
        return chip_ids.pop()

    def set_run_status(self, analysis_type):
        self.update('running')

    def set_archive_status(self):
        self.update('archiving')

    def set_error_status(self):
        self.update('error')

    def set_finish_status(self):
        self.update('complete')

    def update(self, status):
        data = {
            'run_status' :  status,
            'last_updated': datetime.datetime.now().isoformat(),
        }

        colossus_utils.update_analysis_run(self.analysis_run, data)

    def update_results_path(self, path_type, path):
        data = {
            path_type:      path,
            'last_updated': datetime.datetime.now().isoformat(),
        }

        colossus_utils.update_analysis_run(self.analysis_run, data)

    def get_paths_to_archives(self):
        """
        For each sequencing associated with the analysis information object,
        we need to find the flowcell id and corresponding path to archive for each flowcell.
        When BCL2FASTQ is run on BCL files, we get 4 lanes for each flowcell id.

        Set paths_to_archives for the analysis information object.
        """
        paths_to_archives = {}

        for sequencing_id in self.sequencing_ids:
            sequencing = colossus_utils.get_sequencing(sequencing_id)
            sequencing_center = sequencing['dlpsequencingdetail']['sequencing_center']

            for lane in sequencing['dlplane_set']:
                if 'brc' not in sequencing_center.lower():
                    continue

                if not lane['path_to_archive'].strip():
                    raise Exception('no path to archive specified for sequencing {}'.format(sequencing_id))

                paths_to_archives[lane['flow_cell_id']] = lane['path_to_archive']

        return paths_to_archives


class Analysis(object):
    """
    A class representing an Analysis model in Tantalus.
    """
    def __init__(self, analysis_type, args, update=False):
        """
        Create an Analysis object in Tantalus.
        """

        self.args = args
        self.analysis_type = analysis_type
        self.jira = self.args['jira']
        self.name = '{}_{}'.format(self.jira, analysis_type)
        self.status = 'idle'
        # TODO: do we need this? the tantalus field should autoupdate
        self.last_updated = datetime.datetime.now().isoformat()
        self.analysis = self.get_or_create_analysis(update=update)
        self.bams = []

        self.update_analysis('status')
        self.update_analysis('last_updated')

    def get_or_create_analysis(self, update=False):
        """
        Get the analysis by querying Tantalus. Create the analysis
        if it doesn't exist. Set the input dataset ids.
        """

        log.info('Searching for existing analysis {}'.format(self.name))

        try:
            analysis = tantalus_api.get('analysis', name=self.name, jira_ticket=self.jira)
        except NotFoundError:
            analysis = None

        self.input_datasets = self.search_input_datasets()

        if analysis is not None:
            log.info('Found existing analysis {}'.format(self.name))

            updated = False

            if analysis['args'] != self.args:
                if update:
                    tantalus_api.update('analysis', id=analysis['id'], args=self.args)
                    updated = True
                    log.info('Args for analysis {} changed, previously {}, now {}'.format(
                        self.name, analysis['args'], self.args))
                else:
                    log.warning('Args for analysis {} have changed, previously {}, now {}'.format(
                        self.name, analysis['args'], self.args))

            if set(analysis['input_datasets']) != set(self.input_datasets):
                if update:
                    tantalus_api.update('analysis', id=analysis['id'], input_datasets=input_datasets)
                    updated = True
                    log.info('Input datasets for analysis {} changed, previously {}, now {}'.format(
                        self.name, analysis['input_datasets'], self.input_datasets))
                else:
                    log.warning('Input datasets for analysis {} have changed, previously {}, now {}'.format(
                        self.name, analysis['input_datasets'], self.input_datasets))

            if updated:
                analysis = tantalus_api.get('analysis', name=self.name, jira_ticket=self.jira)

        else:
            log.info('Creating analysis {}'.format(self.name))

            data = {
                'name':             self.name,
                'jira_ticket':      self.jira,
                'args':             self.args,
                'status':           self.status,
                'input_datasets':   list(input_datasets),
            }

            # TODO: created timestamp for analysis
            analysis = tantalus_api.create('analysis', **data)

        return analysis

    def get_input_file_instances(self, storage_name):
        """ Get file instances for input datasets.

        Args:
            storage_name: name of storage for which we want file instances

        Returns:
            input_file_instances: list of nested dictionaries for file instances
        """

        input_file_instances = []
        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)
            input_file_instances.extend(tantalus_api.get_sequence_dataset_file_instances(dataset, storage_name))
        return input_file_instances

    def add_inputs_yaml(self, inputs_yaml, inputs_yaml_storage=None):
        """
        Add the inputs yaml to the logs field of the analysis.
        """
        if inputs_yaml_storage is None:
            log.debug('No storage for inputs yaml {} exists yet, not adding to analysis'.format(inputs_yaml))
            return

        log.info('Adding inputs yaml file {} to {}'.format(inputs_yaml, self.name))

        file_resource, file_instance = tantalus_api.add_file(
            storage_name=inputs_yaml_storage,
            filepath=inputs_yaml,
            file_type="YAML",
         )

        tantalus_api.update('analysis', id=self.get_id(), logs=[file_resource['id']])

    def get_dataset(self, dataset_id):
        """
        Get a dataset by id.
        """
        return tantalus_api.get('sequence_dataset', id=dataset_id)

    def set_run_status(self):
        """
        Set run status of analysis to running.
        """
        self.update_status('running')

    def set_archive_status(self):
        """
        Set run status of analysis to archiving.
        """
        self.update_status('archiving')

    def set_complete_status(self):
        """
        Set run status of analysis to complete.
        """
        self.update_status('complete')

    def set_error_status(self):
        """
        Set run status to error.
        """
        self.update_status('error')

    def update_status(self, status):
        """
        Update the run status of the analysis in Tantalus.
        """
        self.status = status
        tantalus_api.update('analysis', id=self.get_id(), status=self.status)

    def update_analysis(self, field):
        """
        Check to see if the field matches the current field that exists.
        """
        field_value = vars(self)[field]
        if self.analysis[field] != field_value:
            tantalus_api.update('analysis', id=self.get_id(), **{field: field_value})

    def get_id(self):
        return self.analysis['id']

    def search_input_datasets(self):
        """
        Get the list of input datasets required to run this analysis.
        """
        raise NotImplementedError

    def _get_blob_dir(self, dir_type):
        if dir_type == 'results':
            template = templates.AZURE_RESULTS_DIR
        elif dir_type == 'tmp':
            template = templates.AZURE_TMP_DIR
        elif dir_type == 'scpipeline':
            template = templates.AZURE_SCPIPELINE_DIR
        else:
            raise Exception('Unrecognized dir type {}'.format(dir_type))

        return template.format(jira=self.args['jira'], tag=self.args['tag'])

    def _get_server_dir(self, dir_type):
        if dir_type == 'results':
            template = templates.SHAHLAB_RESULTS_DIR
        elif dir_type == 'tmp':
            template = templates.SHAHLAB_TMP_DIR
        elif dir_type == 'scpipeline':
            template = templates.SHAHLAB_PIPELINE_DIR
        else:
            raise Exception('Unrecognized dir type {}'.format(dir_type))

        return template.format(jobs_dir=self.args['jobs_dir'], jira=self.args['jira'], tag=self.args['tag'])

    def _get_dir(self, dir_type):
        if self.args['shahlab_run']:
            return self._get_server_dir(dir_type)

        return self._get_blob_dir(dir_type)

    def get_results_dir(self):
        return self._get_dir('results')

    def get_tmp_dir(self):
        return self._get_dir('tmp')

    def get_scpipeine_dir(self):
        return self._get_dir('scpipeline')

    def create_output_datasets(self):
        """
        Create the set of output sequence datasets produced by this analysis.
        """
        raise NotImplementedError

    def create_output_results(self, results_storage, pipeline_dir, pipeline_version):
        """
        Create the set of output results produced by this analysis.
        """
        tantalus_results = Results(
            self,
            results_storage,
            pipeline_dir,
            pipeline_version,
        )

        return tantalus_results


class AlignAnalysis(Analysis):
    """
    A class representing an alignment analysis in Tantalus.
    """
    def __init__(self, args, **kwargs):
        super(AlignAnalysis, self).__init__('align', args, **kwargs)

    def search_input_datasets(self):
        """
        Query Tantalus for paired-end fastq datasets given library id and sample id.

        Returns:
            dataset_ids: list of ids for paired end fastq datasets
        """

        filter_lanes = []
        if self.args['gsc_lanes'] is not None:
            filter_lanes += self.args['gsc_lanes']
        if self.args['brc_flowcell_ids'] is not None:
            # Each BRC flowcell has 4 lanes
            filter_lanes += ['{}_{}'.format(flowcell_id, i+1) for i in range(4)]

        datasets = tantalus_api.list(
            'sequence_dataset',
            library__library_id=self.args['library_id'],
            dataset_type='FQ',
        )

        if not datasets:
            raise Exception('no sequence datasets matching library_id {}'.format(self.args['library_id']))

        dataset_ids = set()

        for dataset in datasets:
            sequencing_centre = tantalus_utils.get_sequencing_centre_from_dataset(dataset)
            sequencing_instrument = tantalus_utils.get_sequencing_instrument_from_dataset(dataset)

            lanes = tantalus_utils.get_lanes_from_dataset(dataset)
            if len(lanes) != 1:
                raise Exception('sequence dataset {} has {} lanes'.format(dataset['id'], len(lanes)))

            lane_id = lanes.pop()  # One lane per fastq
            if filter_lanes and (lane_id not in filter_lanes):
                continue

            if 'gsc' in sequencing_centre.lower():
                # If the FASTQ was sequenced at the GSC, check that the lane id
                # is in the correct format
                # TODO: make sure the regular expression matches [flowcell_id]_[lane_number]
                tantalus_utils.check_gsc_lane_id(lane_id)

            dataset_ids.add(dataset['id'])

        return dataset_ids

    def check_inputs_yaml(self, inputs_yaml_filename):
        lane_ids = self.get_lanes().keys()
        inputs_dict = file_utils.load_yaml(inputs_yaml_filename)
        input_lane_ids = inputs_dict.values()[0]['fastqs'].keys()

        if set(lane_ids) != set(input_lane_ids):
            raise Exception('lanes in input datasets: {}\nlanes in input yaml: {}'.format(
                lane_ids, input_lane_ids
            ))

        self.bams = [cell_info['bam'] for _, cell_info in inputs_dict.items()]

    def _generate_cell_metadata(self, storage_name):
        """ Generates per cell metadata

        Args:
            storage_name: Which tantalus storage to look at
        """

        sample_info = generate_inputs.generate_sample_info(self.args['library_id'])

        if sample_info['index_sequence'].duplicated().any():
            raise Exception('Duplicate index sequences in sample info.')

        if sample_info['cell_id'].duplicated().any():
            raise Exception('Duplicate cell ids in sample info.')

        file_instances = self.get_input_file_instances(storage_name)
        lanes = self.get_lanes()

        # Sort by index_sequence, lane id, read end
        fastq_file_instances = dict()

        for file_instance in file_instances:
            lane_id = tantalus_utils.get_flowcell_lane(file_instance['sequence_dataset']['sequence_lanes'][0])
            read_end = file_instance['file_resource']['sequencefileinfo']['read_end']
            index_sequence = file_instance['file_resource']['sequencefileinfo']['index_sequence']
            fastq_file_instances[(index_sequence, lane_id, read_end)] = file_instance

        input_info = {}
        for idx, row in sample_info.iterrows():
            lane_fastqs = collections.defaultdict(dict)
            sequence_lanes = []
            for lane_id, lane in lanes.iteritems():
                sequencing_centre = fastq_file_instances[(index_sequence, lane_id, 1)]['sequence_dataset']['sequence_lanes'][0]['sequencing_centre']
                sequencing_instrument = fastq_file_instances[(index_sequence, lane_id, 1)]['sequence_dataset']['sequence_lanes'][0]['sequencing_instrument']
                lane_fastqs[lane_id]['fastq_1'] = str(fastq_file_instances[(index_sequence, lane_id, 1)]['filepath'])
                lane_fastqs[lane_id]['fastq_2'] = str(fastq_file_instances[(index_sequence, lane_id, 2)]['filepath'])
                lane_fastqs[lane_id]['sequencing_center'] = str(sequencing_centre)
                lane_fastqs[lane_id]['sequencing_instrument'] = str(sequencing_instrument)
                sequence_lanes.append(dict(
                        flowcell_id=str(lane['flowcell_id']),
                        lane_number=str(lane['lane_number'])))


            if len(lane_fastqs) == 0:
                raise Exception('No fastqs for cell_id {}, index_sequence {}'.format(
                    row['cell_id'], row['index_sequence']))

            bam_filename = templates.SC_WGS_BAM_TEMPLATE.format(
                library_id=self.args['library_id'],
                ref_genome=self.args['ref_genome'],
                aligner_name=self.args['aligner'],
                number_lanes=len(sequence_lanes),
                cell_id=row['cell_id'],
            )

            bam_filepath = str(tantalus_api.get_filepath(storage_name, bam_filename))

            input_info[str(row['cell_id'])] = {
                'fastqs':       dict(lane_fastqs),
                'bam':          bam_filepath,
                'pick_met':     str(row['pick_met']),
                'condition':    str(row['condition']),
                'primer_i5':    str(row['primer_i5']),
                'index_i5':     str(row['index_i5']),
                'primer_i7':    str(row['primer_i7']),
                'index_i7':     str(row['index_i7']),
                'img_col':      int(row['img_col']),
                'column':       int(row['column']),
                'row':          int(row['row']),
                'sample_type':  'null' if (row['sample_type'] == 'X') else str(row['sample_type']),
                'index_sequence': str(row['primer_i7']) + '-' + str(row['primer_i5']),
                'sequence_lanes': sequence_lanes,
                'sample_id':    str(row['sample_id']),
            }

        return input_info

    def generate_inputs_yaml(self, inputs_yaml_filename, storage_name):
        """ Generates a YAML file of input information

        Args:
            inputs_yaml_filename: the directory to which the YAML file should be saved
            storage_name: Which tantalus storage to look at
        """
        input_info = self._generate_cell_metadata(storage_name)

        with open(inputs_yaml_filename, 'w') as inputs_yaml:
            yaml.dump(input_info, inputs_yaml, default_flow_style=False)

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

    def get_output_bams(self):
        """
        Query Tantalus for bams that match the lane_ids
        of the input fastqs
        """
        if not self.bams:
            raise Exception('no output bams found, regenerate or provide an existing inputs yaml')
        return self.bams

    def create_output_datasets(self, storage_name, tag_name=None):
        """
        """
        cell_metadata = self._generate_cell_metadata(storage_name)

        output_file_info = []
        for cell_id, metadata in cell_metadata.iteritems():
            log.info('getting bam metadata for cell {}'.format(cell_id))

            bam_filepath = metadata['bam']

            file_types = {'BAM': bam_filepath, 'BAI': bam_filepath + '.bai'}

            for file_type, filepath in file_types.iteritems():
                file_info = dict(
                    analysis_id=self.analysis['id'],
                    dataset_type='BAM',
                    sample_id=metadata['sample_id'],
                    library_id=self.args['library_id'],
                    library_type='SC_WGS',
                    index_format='D',
                    sequence_lanes=metadata['sequence_lanes'],
                    ref_genome=self.args['ref_genome'],
                    aligner_name=self.args['aligner'],
                    file_type=file_type,
                    index_sequence=metadata['index_sequence'],
                    compression='UNCOMPRESSED',
                    filepath=filepath,
                )

                output_file_info.append(file_info)

        log.info('creating sequence dataset models for output bams')
        self.output_datasets = dlp.create_sequence_dataset_models(
            file_info=output_file_info,
            storage_name=storage_name,
            tag_name=tag_name,  # TODO: tag?
            tantalus_api=tantalus_api,
            analysis_id=self.get_id(),
        )

        log.info("created sequence datasets {}".format(self.output_datasets))


class HmmcopyAnalysis(Analysis):
    """
    A class representing an hmmcopy analysis in Tantalus.
    """
    def __init__(self, align_analysis, args, **kwargs):
        self.align_analysis = align_analysis
        super(HmmcopyAnalysis, self).__init__('hmmcopy', args, **kwargs)

    def search_input_datasets(self):
        """
        Query Tantalus for bams that match the associated
        alignment analysis.
        """

        return align_analysis.output_datasets


class PseudoBulkAnalysis(Analysis):
    """
    A class representing an pseudobulk analysis in Tantalus.
    """
    def __init__(self, args, **kwargs):
        super(PseudoBulkAnalysis, self).__init__('pseudobulk', args, **kwargs)

    def search_input_datasets(self):
        """
        Query Tantalus for bams that match the associated
        pseudobulk analysis.
        """

        datasets = tantalus_api.list(
            'sequence_dataset',
            tags__name=self.jira)
        dataset_ids = [dataset['id'] for dataset in datasets]

        return dataset_ids

    def generate_inputs_yaml(self, inputs_yaml_filename, storage_name):
        """ Generates a YAML file of input information

        Args:
            inputs_yaml_filename: the directory to which the YAML file should be saved
            storage_name: Which tantalus storage to look at
        """

        input_info = {'normal': {}, 'tumour': {}}

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)

            library_id = dataset['library']['library_id']
            sample_id = dataset['sample']['sample_id']

            if sample_id == self.args['matched_normal_sample']:
                for file_instance in tantalus_api.get_sequence_dataset_file_instances(dataset, storage_name):
                    if not file_instance['file_resource']['file_type'] == 'BAM':
                        continue

                    filepath = str(file_instance['filepath'])

                    assert 'bam' not in input_info['normal']
                    input_info['normal'] = {'bam': filepath}

            else:
                sample_info = generate_inputs.generate_sample_info(library_id)
                cell_ids = sample_info.set_index('index_sequence')['cell_id'].to_dict()

                for file_instance in tantalus_api.get_sequence_dataset_file_instances(dataset, storage_name):
                    if not file_instance['file_resource']['file_type'] == 'BAM':
                        continue

                    index_sequence = str(file_instance['file_resource']['sequencefileinfo']['index_sequence'])
                    cell_id = str(cell_ids[index_sequence])
                    filepath = str(file_instance['filepath'])

                    if sample_id not in input_info['tumour']:
                        input_info['tumour'][sample_id] = {}

                    if cell_id not in input_info['tumour'][sample_id]:
                        input_info['tumour'][sample_id][cell_id] = {}

                    input_info['tumour'][sample_id][cell_id] = {'bam': filepath}

        with open(inputs_yaml_filename, 'w') as inputs_yaml:
            yaml.safe_dump(input_info, inputs_yaml, default_flow_style=False)

    def create_output_results(self, storage_name):
        """
        """
        pass


class FileResource:
    """
    A class representing a FileResource model in Tantalus.
    Currently only supports the creation of a new FileResource during
    initialization.
    """
    def __init__(self, source_file, storage_name, file_type=None, compression=None):
        """
        Create a file resource object in Tantalus.
        """
        self.source_file = source_file

        if file_type is None:
            self.file_type = self.get_file_type(self.source_file)
        else:
            self.file_type = file_type

        self.file_types = (
            ('.h5', 'H5'),
            ('.yaml', 'YAML'),
            ('.pdf', 'PDF'),
            ('.seg', 'SEG'),
        )

        self.storage = tantalus_api.get('storage', name=storage_name)
        self.file_resource, self.file_instance = tantalus_api.add_file(
            storage_name, self.source_file, self.file_type, compression=compression)

    def get_file_type(self, filename):
        """
        Get the file type of a file given its filename.
        """

        for extension, file_type in self.file_types:
            if filename.endswith(extension):
                return file_type
        raise Exception('File type of {} not recognized. Add the file type to Tantalus.'.format(filename))

    def get_id(self):
        return self.file_resource['id']


class Results:
    """
    A class representing a Results model in Tantalus.
    """
    def __init__(
            self,
            tantalus_analysis,
            storage_name,
            pipeline_dir,
            pipeline_version,
            update=False,
        ):
        """
        Create a Results object in Tantalus.
        """

        self.tantalus_analysis = tantalus_analysis
        self.name = '{}_{}'.format(self.tantalus_analysis.jira, self.tantalus_analysis.analysis_type)
        self.storage_name = storage_name
        self.analysis = self.tantalus_analysis.get_id()
        self.analysis_type = tantalus_analysis.analysis_type
        self.pipeline_dir = pipeline_dir
        self.pipeline_version = pipeline_version
        self.last_updated = datetime.datetime.now().isoformat()

        self.result = self.get_or_create_results(update=update)

        self.update_results('last_updated') # TODO: not needed?

    def get_or_create_results(self, update=False):
        log.info('Searching for existing results {}'.format(self.name))

        try:
            results = tantalus_api.get(
                'results',
                name=self.name,
                results_type=self.analysis_type,
                analysis=self.analysis,
            )
        except NotFoundError:
            results = None

        self.file_resources = self.get_file_resources()

        if results is not None:

            updated = False

            log.info('Found existing results {}'.format(self.name))

            if set(results['file_resources']) != set(self.file_resources):
                if update:
                    tantalus_api.update('results', id=results['id'], args=self.args)
                    updated=True
                    log.info('File resources for analysis {} have changed, previously {}, now {}'.format(
                        self.name, results['file_resources'], self.file_resources))
                else:
                    log.warning('File resources for analysis {} have changed, previously {}, now {}'.format(
                        self.name, results['file_resources'], self.file_resources))

            if updated:
                results = tantalus_api.get(
                    'results', 
                    name=self.name, 
                    results_type=self.analysis_type, 
                    analysis=self.analysis
                )
        else:
            log.info('Creating results {}'.format(self.name))

            data = {
                'name':             self.name,
                'results_type':     self.analysis_type,
                'results_version':  self.pipeline_version,
                'analysis':         self.analysis,
                'file_resources':   list(self.file_resources),
            }

            # TODO: created timestamp for results
            results = tantalus_api.create('results', **data)

        return results

    def update_results(self, field):
        field_value = vars(self)[field]
        if self.results[field] != field_value:
            tantalus_api.update('results', id=self.get_id(), **{field: field_value})

    def get_analysis_results_dir(self):
        if self.analysis_type == 'align':
            template = templates.ALIGNMENT_RESULTS
        elif self.analysis_type == 'hmmcopy':
            template = templates.HMMCOPY_RESULTS
        else:
            raise Exception('unrecognized analysis type {}'.format(self.analysis_type))

        return template.format(results_dir=self.tantalus_analysis.get_results_dir())

    def _get_server_info_yaml(self):
        """
        Get the path to the info.yaml for the corresponding analysis
        """
        analysis_dir = self.get_analysis_results_dir()
        info_yaml = os.path.join(analysis_dir, 'info.yaml')
        if not os.path.exists(info_yaml):
            raise Exception('no info.yaml found in {}'.format(analysis_dir))

        return info_yaml

    def _get_blob_info_yaml(self):
        """
        Download the info.yaml from blob to the pipeline directory
        for the analysis, then return the path
        """
        blob_name = os.path.join(
            # TODO: get prefix from storage
            os.path.relpath(self.get_analysis_results_dir(), 'singlecelldata/results'),  
            'info.yaml',
        )

        blob_service = BlockBlobService(account_name=AZURE_STORAGE_ACCOUNT, account_key=AZURE_STORAGE_KEY)

        if not blob_service.exists('results', blob_name=blob_name):
            raise Exception('{} not found in results container'.format(blob_name))




        info_yaml = os.path.join(self.pipeline_dir, 'info.yaml')
        log.info('downloading info.yaml to {}'.format(info_yaml))
        blob_service.get_blob_to_path('results', blob_name, info_yaml)

        return info_yaml

    def get_results_info(self):
        """
        Return a dictionary
        """

        if self.storage_name == 'singlecellblob_results':
            info_yaml = self._get_blob_info_yaml()
        else:
            info_yaml = self._get_server_info_yaml()

        if self.analysis_type == "align":
            analysis_type = "alignment"
        else:
            analysis_type = self.analysis_type

        return file_utils.load_yaml(info_yaml)[analysis_type]['results'].values()

    def get_file_resources(self):
        """
        Create file resources for each results file and return their ids.
        """
        file_resource_ids = set()
        results_info = self.get_results_info()
        for result in results_info:
            file_resource, file_instance = tantalus_api.add_file(
                self.storage_name,
                result["filename"],
                result["type"],
                compression=None,
            )

            file_resource_ids.add(file_resource["id"])

        return file_resource_ids

    def get_id(self):
        return self.results['id']
