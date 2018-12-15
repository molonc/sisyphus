import logging
import datetime
import json
import os
import re
import collections
import yaml

from datamanagement.utils import dlp
import dbclients.tantalus
import dbclients.colossus
from dbclients.basicclient import NotFoundError
from datamanagement.utils.utils import make_dirs

import generate_inputs
import datamanagement.templates as templates
from utils import tantalus_utils, file_utils

log = logging.getLogger('sisyphus')

tantalus_api = dbclients.tantalus.TantalusApi()
colossus_api = dbclients.colossus.ColossusApi()


class AnalysisInfo:
    """
    A class representing an analysis information object in Colossus,
    containing settings for the analysis run.
    """
    def __init__(self, jira, log_file, args, update=False):
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

        self.analysis_info = colossus_api.get('analysis_information', analysis_jira_ticket=jira)
        self.args = args

        self.aligner = self.get_aligner()
        self.smoothing = self.get_smoothing()
        self.reference_genome = self.get_reference_genome()
        self.pipeline_version = self.get_pipeline_version(update=update)

        self.id = self.analysis_info['id']
        self.analysis_run = self.analysis_info['analysis_run']['id']
        self.sequencing_ids = self.analysis_info['sequencings']
        self.log_file = log_file

        # Set the chip ID (= DLP library ID) from the sequencings associated with the analysis object from Colossus
        self.chip_id = self.get_chip_id()

    def get_reference_genome(self):
        reference_genome = self.analysis_info['reference_genome']['reference_genome']
        if reference_genome not in ('grch37', 'mm10'):
            raise Exception('Unrecognized reference genome {}'.format(reference_genome))
        return reference_genome

    def get_pipeline_version(self, update=False):
        version_str = self.analysis_info['version']
        if version_str.startswith('Single Cell Pipeline'):
            version_str = version_str.replace('Single Cell Pipeline', '').replace('_', '.')

        if version_str != self.args['version']:
            log.warning('Version for Analysis Information {} changed, previously {} now {}'.format(
                self.analysis_info['id'], version_str, self.args['version']))

            if update:
                colossus_api.update(
                    'analysis_information', 
                    id=self.analysis_info['id'], 
                    version=self.args['version'])
                analysis_info = colossus_api.get('analysis_information', id=self.analysis_info['id'])

        return self.args['version']

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
        for sequencing_id in self.sequencing_ids:
            chip_ids.add(colossus_api.get('sequencing', id=sequencing_id)['library'])
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
        colossus_api.update('analysis_run', id=self.analysis_run, **data)

    def update_results_path(self, path_type, path):
        data = {
            path_type:      path,
            'last_updated': datetime.datetime.now().isoformat(),
        }

        colossus_api.update('analysis_run', id=self.analysis_run, **data)



class Analysis(object):
    """
    A class representing an Analysis model in Tantalus.
    """
    def __init__(self, analysis_type, args, storages, update=False):
        """
        Create an Analysis object in Tantalus.
        """
        if storages is None:
            raise Exception("no storages specified for Analysis")

        self.analysis_type = analysis_type

        self.analysis = self.get_or_create_analysis(args, update=update)

        self.storages = storages

        # TODO: remove self.bams
        self.bams = []

    @property
    def name(self):
        return self.analysis['name']

    @property
    def args(self):
        return self.analysis['args']

    @property
    def jira(self):
        return self.args['jira']

    @property
    def status(self):
        return self.analysis['status']

    @property
    def version(self):
        return self.analysis['version']
    

    def get_or_create_analysis(self, args, update=False):
        """
        Get the analysis by querying Tantalus. Create the analysis
        if it doesn't exist. Set the input dataset ids.
        """

        jira = args['jira']
        name = '{}_{}'.format(jira, self.analysis_type)
        version = args['version']

        log.info('Searching for existing analysis {}'.format(name))

        try:
            analysis = tantalus_api.get('analysis', name=name, jira_ticket=jira)
        except NotFoundError:
            analysis = None

        input_datasets = self.search_input_datasets(args)
        input_results = self.search_input_results(args)

        if analysis is not None:
            log.info('Found existing analysis {}'.format(name))

            updated = False

            fields_to_check = {
                'args': (args, lambda a, b: a != b),
                'version': (version, lambda a, b: a != b),
                'input_datasets': (input_datasets, lambda a, b: set(a) != set(b)),
                'input_results': (input_results, lambda a, b: set(a) != set(b)),
            }

            for field_name, (new_data, f_check) in fields_to_check.iteritems():
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
                'name':             name,
                'jira_ticket':      jira,
                'args':             args,
                'status':           'idle',
                'input_datasets':   input_datasets,
                'input_results':    input_results,
                'version':          version,
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

    def add_inputs_yaml(self, inputs_yaml, update=False):
        """
        Add the inputs yaml to the logs field of the analysis.
        """

        log.info('Adding inputs yaml file {} to {}'.format(inputs_yaml, self.name))

        file_resource, file_instance = tantalus_api.add_file(
            storage_name=self.storages['local_results'],
            filepath=inputs_yaml,
            file_type="YAML",
            fields={"compression": "UNCOMPRESSED"},
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

    def create_output_datasets(self):
        """
        Create the set of output sequence datasets produced by this analysis.
        """
        raise NotImplementedError

    def create_output_results(self, pipeline_dir, update=False):
        """
        Create the set of output results produced by this analysis.
        """
        tantalus_results = Results(
            self,
            self.storages['working_results'],
            pipeline_dir,
            update=update,
        )

        return tantalus_results

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


class AlignAnalysis(Analysis):
    """
    A class representing an alignment analysis in Tantalus.
    """
    def __init__(self, args, **kwargs):
        super(AlignAnalysis, self).__init__('align', args, **kwargs)

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
            filter_lanes += ['{}_{}'.format(flowcell_id, i+1) for i in range(4)]

        datasets = tantalus_api.list(
            'sequence_dataset',
            library__library_id=args['library_id'],
            dataset_type='FQ',
        )

        if args['integrationtest']:
            # Skip checking of lanes and just return the dataset ids at this point,
            # since we're only considering a subset of lanes for testing
            return [dataset["id"] for dataset in datasets]

        if not datasets:
            raise Exception('no sequence datasets matching library_id {}'.format(args['library_id']))

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

        return list(dataset_ids)

    def check_inputs_yaml(self, inputs_yaml_filename):
        inputs_dict = file_utils.load_yaml(inputs_yaml_filename)

        lanes = self.get_lanes().keys()
        input_lanes = inputs_dict.values()[0]['fastqs'].keys()

        num_cells = len(inputs_dict)
        fastq_1_set = set()
        fastq_2_set = set()

        for cell_id, cell_info in inputs_dict.iteritems():
            fastq_1 = cell_info["fastqs"].values()[0]["fastq_1"]
            fastq_2 = cell_info["fastqs"].values()[0]["fastq_2"]

            fastq_1_set.add(fastq_1)
            fastq_2_set.add(fastq_2)

        if not (num_cells == len(fastq_1_set) == len(fastq_2_set)):
            raise Exception("number of cells is {} but found {} unique fastq_1 and {} unique fastq_2 in inputs yaml".format(
                num_cells, len(fastq_1_set), len(fastq_2_set)))

        if set(lanes) != set(input_lanes):
            raise Exception('lanes in input datasets: {}\nlanes in inputs yaml: {}'.format(
                lanes, input_lanes
            ))

        self.bams = [cell_info['bam'] for _, cell_info in inputs_dict.items()]

    def _generate_cell_metadata(self, storage_name):
        """ Generates per cell metadata

        Args:
            storage_name: Which tantalus storage to look at
        """
        library_id = args["library_id"]
        if args["integrationtest"]:
            library_id = library_id.strip("TEST")

        sample_info = generate_inputs.generate_sample_info(library_id)

        if sample_info['index_sequence'].duplicated().any():
            raise Exception('Duplicate index sequences in sample info.')

        if sample_info['cell_id'].duplicated().any():
            raise Exception('Duplicate cell ids in sample info.')

        file_instances = self.get_input_file_instances(storage_name)
        lanes = self.get_lanes()

        # Sort by index_sequence, lane id, read end
        fastq_file_instances = dict()

        tantalus_index_sequences = set()
        colossus_index_sequences = set()

        for file_instance in file_instances:
            lane_id = tantalus_utils.get_flowcell_lane(file_instance['sequence_dataset']['sequence_lanes'][0])
            read_end = file_instance['file_resource']['sequencefileinfo']['read_end']
            index_sequence = file_instance['file_resource']['sequencefileinfo']['index_sequence']
            tantalus_index_sequences.add(index_sequence)
            fastq_file_instances[(index_sequence, lane_id, read_end)] = file_instance

        input_info = {}

        for idx, row in sample_info.iterrows():
            index_sequence = row['index_sequence']

            if args["integrationtest"] and (index_sequence not in tantalus_index_sequences):
                # Skip index sequences that are not found in the Tantalus dataset, since
                # we need to refer to the original library in Colossus for metadata, but
                # we don't want to iterate through all the cells present in that library
                continue

            colossus_index_sequences.add(index_sequence)
            
            lane_fastqs = collections.defaultdict(dict)
            for lane_id, lane in lanes.iteritems():
                sequencing_centre = fastq_file_instances[(index_sequence, lane_id, 1)]['sequence_dataset']['sequence_lanes'][0]['sequencing_centre']
                sequencing_instrument = fastq_file_instances[(index_sequence, lane_id, 1)]['sequence_dataset']['sequence_lanes'][0]['sequencing_instrument']
                lane_fastqs[lane_id]['fastq_1'] = str(fastq_file_instances[(index_sequence, lane_id, 1)]['filepath'])
                lane_fastqs[lane_id]['fastq_2'] = str(fastq_file_instances[(index_sequence, lane_id, 2)]['filepath'])
                lane_fastqs[lane_id]['sequencing_center'] = str(sequencing_centre)
                lane_fastqs[lane_id]['sequencing_instrument'] = str(sequencing_instrument)


            if len(lane_fastqs) == 0:
                raise Exception('No fastqs for cell_id {}, index_sequence {}'.format(
                    row['cell_id'], row['index_sequence']))

            bam_filename = templates.SC_WGS_BAM_TEMPLATE.format(
                library_id=args['library_id'],
                ref_genome=args['ref_genome'],
                aligner_name=args['aligner'],
                number_lanes=len(lanes),
                cell_id=row['cell_id'],
            )

            bam_filepath = str(tantalus_api.get_filepath(storage_name, bam_filename))

            sample_id = row['sample_id']
            if args['integrationtest']:
               sample_id += "TEST"

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
                'sample_id':    str(sample_id),
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
            yaml.dump(input_info, inputs_yaml, default_flow_style=False)

    def get_lanes(self):
        """
        Get the lanes for each input dataset for the analysis.
        """
        lanes = dict()
        for dataset_id in self.analysis['input_datasets']:
            print("dataset_id in get_lanes: {}".format(dataset_id))
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

    def create_output_datasets(self, tag_name=None, update=False):
        """
        """
        cell_metadata = self._generate_cell_metadata(storages['working_inputs'])
        sequence_lanes = []

        for lane_id, lane in self.get_lanes().iteritems():

            if args["integrationtest"]:
                lane["flowcell_id"] += "TEST"

            sequence_lanes.append(dict(
                flowcell_id=lane["flowcell_id"],
                lane_number=lane["lane_number"]))

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
                    library_id=args['library_id'],
                    library_type='SC_WGS',
                    index_format='D',
                    sequence_lanes=sequence_lanes,
                    ref_genome=args['ref_genome'],
                    aligner_name=args['aligner'],
                    file_type=file_type,
                    index_sequence=metadata['index_sequence'],
                    compression='UNCOMPRESSED',
                    filepath=filepath,
                )

                output_file_info.append(file_info)

        log.info('creating sequence dataset models for output bams')

        output_datasets = dlp.create_sequence_dataset_models(
            file_info=output_file_info,
            storage_name=storage_name,
            tag_name=tag_name,  # TODO: tag?
            tantalus_api=tantalus_api,
            analysis_id=self.get_id(),
            update=update,
        )

        log.info("created sequence datasets {}".format(output_datasets))

    def get_output_datasets(self):
        """
        Query Tantalus for bams that match the associated analysis
        by filtering based on the analysis id.
        """

        datasets = tantalus_api.list('sequence_dataset', analysis=self.get_id(), dataset_type='BAM')
        return [dataset['id'] for dataset in datasets] 


class HmmcopyAnalysis(Analysis):
    """
    A class representing an hmmcopy analysis in Tantalus.
    """
    def __init__(self, align_analysis, args, **kwargs):
        self.align_analysis = align_analysis
        super(HmmcopyAnalysis, self).__init__('hmmcopy', args, **kwargs)

    @staticmethod
    def search_input_datasets(args):
        """
        Get the input BAM datasets for this analysis.
        """
        return self.align_analysis.get_output_datasets()


class PseudoBulkAnalysis(Analysis):
    """
    A class representing an pseudobulk analysis in Tantalus.
    """
    def __init__(self, args, **kwargs):
        super(PseudoBulkAnalysis, self).__init__('pseudobulk', args, **kwargs)

    @staticmethod
    def search_input_datasets(args):
        """
        Query Tantalus for bams that match the associated
        pseudobulk analysis.
        """

        jira = args['jira']

        datasets = tantalus_api.list(
            'sequence_dataset',
            tags__name=jira)

        dataset_ids = [dataset['id'] for dataset in datasets]

        return dataset_ids

    def generate_inputs_yaml(self, inputs_yaml_filename, storage_name):
        """ Generates a YAML file of input information

        Args:
            inputs_yaml_filename: the directory to which the YAML file should be saved
            storage_name: Which tantalus storage to look at
        """
        make_dirs(os.path.dirname(inputs_yaml_filename))

        input_info = {'normal': {}, 'tumour': {}}

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)

            library_id = dataset['library']['library_id']
            sample_id = dataset['sample']['sample_id']

            if sample_id == args['matched_normal_sample']:
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

    def create_output_results(self):
        """
        """
        pass


class CNCloneAnalysis(Analysis):
    """
    A class representing an copy number clone analysis in Tantalus.
    """
    def __init__(self, args, **kwargs):
        super(CNCloneAnalysis, self).__init__('cnclone', args, **kwargs)

    @staticmethod
    def search_input_results(args):
        """
        Query Tantalus for hmmcopy inputs that match the associated
        cnclone analysis.
        """

        jira = args['jira']

        input_results = tantalus_api.list(
            'results',
            tags__name=jira)

        for results in input_results:
            if results["results_type"] != "hmmcopy":
                raise ValueError("Expected hmmcopy results, got {}".format(results["results_type"]))

        results_ids = [results['id'] for results in input_results]

        return results_ids

    def generate_inputs_yaml(self, inputs_yaml_filename, storage_name):
        """ Generates a YAML file of input information

        Args:
            inputs_yaml_filename: the directory to which the YAML file should be saved
            storage_name: Which tantalus storage to look at
        """

        input_info = {
            "samples": args["samples"],
            "hmmcopy": [],
        }

        for results_id in self.analysis['input_results']:
            results = self.get_results(results_id)

            # TODO: check samples

            for file_instance in tantalus_api.get_sequence_dataset_file_instances(dataset, storage_name):
                if file_instance["file_resource"]["filename"].endswith("_hmmcopy.h5"):
                    input_info["hmmcopy"].append(file_instance["filepath"])

        with open(inputs_yaml_filename, 'w') as inputs_yaml:
            yaml.safe_dump(input_info, inputs_yaml, default_flow_style=False)


class Results:
    """
    A class representing a Results model in Tantalus.
    """
    def __init__(
            self,
            tantalus_analysis,
            storage_name,
            pipeline_dir,
            update=False,
        ):
        """
        Create a Results object in Tantalus.
        """

        self.tantalus_analysis = tantalus_analysis
        self.storage_name = storage_name
        self.name = '{}_{}'.format(self.tantalus_analysis.jira, self.tantalus_analysis.analysis_type)
        self.analysis = self.tantalus_analysis.get_id()
        self.analysis_type = self.tantalus_analysis.analysis_type
        self.samples = self.tantalus_analysis.get_input_samples()
        self.pipeline_version = self.tantalus_analysis.version
        self.last_updated = datetime.datetime.now().isoformat()

        self.results = self.get_or_create_results(update=update)

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

        self.file_resources = self.get_file_resources(update=update)

        if results is not None:

            updated = False

            log.info('Found existing results {}'.format(self.name))

            if set(results['file_resources']) != set(self.file_resources):
                if update:
                    tantalus_api.update('results', id=results['id'], file_resources=self.file_resources)
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
                    analysis=self.analysis,
                )
        else:
            log.info('Creating results {}'.format(self.name))

            data = {
                'name':             self.name,
                'results_type':     self.analysis_type,
                'results_version':  self.pipeline_version,
                'analysis':         self.analysis,
                'file_resources':   self.file_resources,
                'samples':          self.samples,
            }

            # TODO: created timestamp for results
            results = tantalus_api.create('results', **data)

        return results

    def update_results(self, field):
        field_value = vars(self)[field]
        if self.results[field] != field_value:
            tantalus_api.update('results', id=self.get_id(), **{field: field_value})


    def get_file_resources(self, update=False):
        """
        Create file resources for each results file and return their ids.
        """
        file_resource_ids = set()

        storage_client = tantalus_api.get_storage_client(self.storages["working_results"])

        prefix = os.path.join(
            self.args["job_subdir"], 
            "results", 
            "results")  # Exclude metrics files

        for result_filepath in storage_client.list(prefix=prefix):  # Exclude metrics files
            if result_filepath.endswith(".gz"):
                compression = "GZIP"
            else:
                compression = "UNCOMPRESSED"

            # TODO: determine file type from filepath
            file_type = result_filepath.split(".")[1].upper()

            file_resource, file_instance = tantalus_api.add_file(
                self.storages["working_results"],
                result_filepath,
                file_type,
                {'compression': compression},
                update=update,
            )

            file_resource_ids.add(file_resource["id"])
            
        return list(file_resource_ids)

    def get_id(self):
        return self.results['id']
