import logging
import datetime
import json
import os
import re
import collections
import yaml
import hashlib
import subprocess

from datamanagement.utils import dlp
import dbclients.tantalus
import dbclients.colossus
from dbclients.basicclient import NotFoundError
from datamanagement.utils.utils import make_dirs
from datamanagement.transfer_files import transfer_dataset

import generate_inputs
import launch_pipeline
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
    def __init__(self, jira, log_file, version, update=False):
        self.jira = jira
        self.status = 'idle'

        self.aligner_choices = {
            'A':    'BWA_ALN_0_5_7',
            'M':    'BWA_MEM_0_7_6A',
        }

        self.reference_genome_choices = {
            'grch37': 'HG19',
            'mm10': 'MM10',
        }

        self.smoothing_choices = {
            'M':    'modal',
            'L':    'loess',
        }

        self.analysis_info = colossus_api.get('analysis_information', analysis_jira_ticket=jira)
        self.version = version

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
        if reference_genome not in self.reference_genome_choices:
            raise Exception('Unrecognized reference genome {}'.format(reference_genome))
        return self.reference_genome_choices[reference_genome]

    def get_pipeline_version(self, update=False):
        version_str = self.analysis_info['version']
        if version_str.startswith('Single Cell Pipeline'):
            version_str = version_str.replace('Single Cell Pipeline', '').replace('_', '.')

        if version_str != self.version:
            log.warning('Version for Analysis Information {} changed, previously {} now {}'.format(
                self.analysis_info['id'], version_str, self.version))

            if update:
                colossus_api.update(
                    'analysis_information', 
                    id=self.analysis_info['id'], 
                    version=self.version)
                analysis_info = colossus_api.get('analysis_information', id=self.analysis_info['id'])

        return self.version

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
    def __init__(self, analysis_type, jira, version, args, storages, update=False):
        """
        Create an Analysis object in Tantalus.
        """
        if storages is None:
            raise Exception("no storages specified for Analysis")

        self.analysis_type = analysis_type

        self.analysis = self.get_or_create_analysis(jira, version, args, analysis_type, update=update)

        self.storages = storages

    @property
    def name(self):
        return self.analysis['name']

    @property
    def args(self):
        return self.analysis['args']

    @property
    def jira(self):
        return self.analysis['jira_ticket']

    @property
    def status(self):
        return self.analysis['status']

    @property
    def version(self):
        return self.analysis['version']

    def get_or_create_analysis(self, jira, version, args, analysis_type, update=False):
        """
        Get the analysis by querying Tantalus. Create the analysis
        if it doesn't exist. Set the input dataset ids.
        """

        input_datasets = self.search_input_datasets(args)
        input_results = self.search_input_results(args)

        lanes = set()

        for input_dataset in input_datasets:
            dataset = tantalus_api.get('sequence_dataset', id=input_dataset)
            for sequence_lane in dataset['sequence_lanes']:
                lane = "{}_{}".format(sequence_lane['flowcell_id'], sequence_lane['lane_number'])
                lanes.add(lane)

        lanes = ", ".join(sorted(lanes))
        lanes = hashlib.md5(lanes)
        lanes_hashed = "{}".format(lanes.hexdigest()[:8])

        # MAYBE: Add this to templates?
        name = "sc_{}_{}_{}_{}_{}".format(
            analysis_type, 
            args['aligner'], 
            args['ref_genome'], 
            args['library_id'],
            lanes_hashed,
        )

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
                'name':             name,
                'jira_ticket':      jira,
                'args':             args,
                'status':           'idle',
                'input_datasets':   input_datasets,
                'input_results':    input_results,
                'version':          version,
                'analysis_type':    self.analysis_type,
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

    def create_output_results(self, update=False):
        """
        Create the set of output results produced by this analysis.
        """
        tantalus_results = Results(
            self,
            self.storages['working_results'],
            update=update,
        )

        return [tantalus_results.get_id()]

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


class AlignAnalysis(Analysis):
    """
    A class representing an alignment analysis in Tantalus.
    """
    def __init__(self, jira, version, args, run_options, **kwargs):
        super(AlignAnalysis, self).__init__('align', jira, version, args, **kwargs)
        self.run_options = run_options

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
            filter_lanes += ['{}_{}'.format(args['brc_flowcell_ids'], i+1) for i in range(4)]

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
                    dataset['id'], len(dataset['sequence_lanes'])))

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
            raise Exception("number of cells is {} but found {} unique fastq_1 and {} unique fastq_2 in inputs yaml".format(
                num_cells, len(fastq_1_set), len(fastq_2_set)))

        if set(lanes) != set(input_lanes):
            raise Exception('lanes in input datasets: {}\nlanes in inputs yaml: {}'.format(
                lanes, input_lanes
            ))

    def _generate_cell_metadata(self, storage_name):
        """ Generates per cell metadata

        Args:
            storage_name: Which tantalus storage to look at
        """
        log.info('Generating cell metadata')
        reference_genome_choices = {
            'grch37': 'HG19',
            'mm10': 'MM10',
        }

        inverted_ref_genome_map = dict([[v,k] for k,v in reference_genome_choices.items()])

        sample_info = generate_inputs.generate_sample_info(
            self.args["library_id"], test_run=self.run_options.get("is_test_run", False))

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
                    len(dataset['sequence_lanes']), dataset_id))

            lane_id = tantalus_utils.get_flowcell_lane(dataset['sequence_lanes'][0])

            lane_info[lane_id] = {
                'sequencing_centre': dataset['sequence_lanes'][0]['sequencing_centre'],
                'sequencing_instrument': dataset['sequence_lanes'][0]['sequencing_instrument'],
                'read_type': dataset['sequence_lanes'][0]['read_type'],
            }

            file_instances = tantalus_api.get_dataset_file_instances(
                dataset['id'], 'sequencedataset', storage_name)

            for file_instance in file_instances:
                read_end = file_instance['file_resource']['sequencefileinfo']['read_end']
                index_sequence = file_instance['file_resource']['sequencefileinfo']['index_sequence']
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
                lane_fastqs[lane_id]['read_type'] = lane_info[lane_id]['read_type']

            if len(lane_fastqs) == 0:
                raise Exception('No fastqs for cell_id {}, index_sequence {}'.format(
                    row['cell_id'], row['index_sequence']))

            bam_filename = templates.SC_WGS_BAM_TEMPLATE.format(
                library_id=self.args['library_id'],
                ref_genome=inverted_ref_genome_map[self.args['ref_genome']],
                aligner_name=self.args['aligner'],
                number_lanes=len(lanes),
                cell_id=row['cell_id'],
            )

            bam_filepath = str(tantalus_api.get_filepath(storage_name, bam_filename))

            sample_id = row['sample_id']
            if self.run_options.get("is_test_run", False):
               assert 'TEST' in sample_id

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
        """
        """
        cell_metadata = self._generate_cell_metadata(self.storages['working_inputs'])
        sequence_lanes = []

        for lane_id, lane in self.get_lanes().items():

            if self.run_options.get("is_test_run", False):
                assert 'TEST' in lane["flowcell_id"]

            sequence_lanes.append(dict(
                flowcell_id=lane["flowcell_id"],
                lane_number=lane["lane_number"],
                sequencing_centre=lane["sequencing_centre"],
                read_type=lane["read_type"],
            ))

        output_file_info = []
        for cell_id, metadata in cell_metadata.items():
            log.info('getting bam metadata for cell {}'.format(cell_id))

            bam_filepath = metadata['bam']

            for filepath in (bam_filepath, bam_filepath + '.bai'):
                file_info = dict(
                    analysis_id=self.analysis['id'],
                    dataset_type='BAM',
                    sample_id=metadata['sample_id'],
                    library_id=self.args['library_id'],
                    library_type='SC_WGS',
                    index_format='D',
                    sequence_lanes=sequence_lanes,
                    ref_genome=self.args['ref_genome'],
                    aligner_name=self.args['aligner'],
                    index_sequence=metadata['index_sequence'],
                    filepath=filepath,
                )
                
                output_file_info.append(file_info)

        log.info('creating sequence dataset models for output bams')

        output_datasets = dlp.create_sequence_dataset_models(
            file_info=output_file_info,
            storage_name=self.storages["working_inputs"],
            tag_name=tag_name,  # TODO: tag?
            tantalus_api=tantalus_api,
            analysis_id=self.get_id(),
            update=update,
        )

        log.info("created sequence datasets {}".format(output_datasets))

        return output_datasets

    def get_results_filenames(self):
        results_prefix = os.path.join(
            self.run_options["job_subdir"],
            "results",
            "results",
            "alignment")

        filenames = [
            os.path.join("plots", "{library_id}_plot_metrics.pdf"),
            "{library_id}_alignment_metrics.h5",
            "info.yaml"
        ]

        return [os.path.join(results_prefix, filename.format(**self.args)) for filename in filenames]

    def run_pipeline(self):
        if self.run_options["skip_pipeline"]:
            return launch_pipeline.run_pipeline2
        else:
            return launch_pipeline.run_pipeline


class HmmcopyAnalysis(Analysis):
    """
    A class representing an hmmcopy analysis in Tantalus.
    """
    def __init__(self, jira, version, args, run_options, **kwargs):
        super(HmmcopyAnalysis, self).__init__('hmmcopy', jira, version, args, **kwargs)
        self.run_options = run_options

    @staticmethod
    def search_input_datasets(args):
        """
        Get the input BAM datasets for this analysis.
        """
        
        filter_lane_flowcells = []
        dataset_ids = set()

        if args['gsc_lanes'] is not None:
            for lane in args['gsc_lanes']:
                flowcell_id = (args['gsc_lanes'].split('_'))[0]
                lane_number = (args['gsc_lanes'].split('_'))[1]
                sequence_lane = tantalus_api.get(
                    'sequencing_lane',
                    flowcell_id=flowcell_id,
                    lane_number=lane_number
                )


                filter_lane_flowcells.extend(flowcell_id)

        if args['brc_flowcell_ids'] is not None:
            for flowcell_id in args['brc_flowcell_ids']:
                sequence_lane = tantalus_api.get(
                    'sequencing_lane',
                    flowcell_id=flowcell_id
                )

                filter_lane_flowcells.extend(flowcell_id)

        if not filter_lane_flowcells:
            datasets = tantalus_api.list(
            'sequence_dataset', 
            library__library_id=args['library_id'], 
            reference_genome=args['ref_genome'],
            dataset_type='BAM',
            )           

            if not datasets:
                raise Exception('no sequence datasets matching library_id {}'.format(args['library_id']))

            for dataset in datasets:
                dataset_ids.add(dataset['id'])
            
            return list(dataset_ids)

        for flowcell_id in filter_lane_flowcells:
            datasets = tantalus_api.list(
                'sequence_dataset', 
                library__library_id=args['library_id'], 
                reference_genome=args['ref_genome'],
                dataset_type='BAM',
                sequence_lane__flowcell_id=flowcell_id
            )   

            for dataset in list(datasets):
                dataset_ids.add(dataset['id'])

        return list(dataset_ids)
      
    def get_results_filenames(self):
        results_prefix = os.path.join(
            self.run_options["job_subdir"],
            "results",
            "results",
            "hmmcopy_autoploidy")

        filenames = [
            os.path.join("plots", "bias", "{library_id}_bias.tar.gz"),
            os.path.join("plots", "segments", "{library_id}_segs.tar.gz"),
            os.path.join("plots", "{library_id}_heatmap_by_ec_filtered.pdf"),
            os.path.join("plots", "{library_id}_heatmap_by_ec.pdf"),
            os.path.join("plots", "{library_id}_kernel_density.pdf"),
            os.path.join("plots", "{library_id}_metrics.pdf"),
            "{library_id}_hmmcopy.h5",
            "{library_id}_igv_segments.seg",
            "info.yaml"
        ]

        return [os.path.join(results_prefix, filename.format(**self.args)) for filename in filenames]

    def run_pipeline(self):
        if self.run_options["skip_pipeline"]:
            return launch_pipeline.run_pipeline2
        else:
            return launch_pipeline.run_pipeline

    def generate_inputs_yaml(self, inputs_yaml_filename):
        log.info("inputs.yaml should already exists from align analysis.")
        pass

    def create_output_datasets(self, tag_name=None, update=False):
        log.info("No outputs need to be created for hmmcopy analysis.")
        pass


class PseudoBulkAnalysis(Analysis):
    """
    A class representing an pseudobulk analysis in Tantalus.
    """
    def __init__(self, jira, version, args, run_options, **kwargs):
        super(PseudoBulkAnalysis, self).__init__('pseudobulk', jira, version, args, **kwargs)
        self.run_options = run_options

    @staticmethod
    def search_input_datasets(args):
        """
        Query Tantalus for bams that match the associated
        pseudobulk analysis.
        """

        tag_name = args['inputs_tag_name']

        datasets = tantalus_api.list(
            'sequence_dataset',
            tags__name=tag_name)

        dataset_ids = [dataset['id'] for dataset in datasets]

        if len(dataset_ids) == 0:
            raise Exception('no datasets found with tag {}'.format(tag_name))

        return dataset_ids

    def generate_inputs_yaml(self, inputs_yaml_filename):
        """ Generates a YAML file of input information

        Args:
            inputs_yaml_filename: the directory to which the YAML file should be saved
            storage_name: Which tantalus storage to look at
        """
        storage_name = self.storages['working_inputs']

        make_dirs(os.path.dirname(inputs_yaml_filename))

        input_info = {}

        assert len(self.analysis['input_datasets']) > 0

        # Type of dataset for normal
        normal_library_type = None

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)

            library_id = dataset['library']['library_id']
            sample_id = dataset['sample']['sample_id']
            library_type = dataset['library']['library_type']

            # WORKAROUND: the single cell pipeline doesnt take
            # both sample and library specific cell info so use a
            # a concatenation of sample and library in the
            # inputs yaml
            sample_library_id = sample_id + '_' + library_id

            is_normal = (
                sample_id == self.args['matched_normal_sample'] and
                library_id == self.args['matched_normal_library'])

            dataset_class = ('tumour', 'normal')[is_normal]

            if dataset_class == 'normal':
                assert normal_library_type is None
                normal_library_type = library_type

            if dataset_class not in input_info:
                input_info[dataset_class] = {}

            if sample_library_id not in input_info[dataset_class]:
                input_info[dataset_class][sample_library_id] = {}

            file_instances = tantalus_api.get_dataset_file_instances(
                dataset_id, 'sequencedataset', storage_name,
                filters={'filename__endswith': '.bam'})

            if library_type == 'WGS':
                if not is_normal:
                    raise ValueError('WGS only supported for normal')

                file_instances = list(file_instances)
                if len(file_instances) != 1:
                    raise ValueError('expected 1 file got {}'.format(len(file_instances)))

                file_instance = file_instances[0]
                filepath = str(file_instance['filepath'])
                input_info[dataset_class][sample_library_id] = {'bam': filepath}

            elif library_type == 'SC_WGS':
                sample_info = generate_inputs.generate_sample_info(
                    library_id, test_run=self.run_options.get("is_test_run", False))

                cell_ids = sample_info.set_index('index_sequence')['cell_id'].to_dict()

                for file_instance in file_instances:
                    index_sequence = str(file_instance['file_resource']['sequencefileinfo']['index_sequence'])
                    cell_id = str(cell_ids[index_sequence])
                    filepath = str(file_instance['filepath'])

                    if cell_id not in input_info[dataset_class][sample_library_id]:
                        input_info[dataset_class][sample_library_id][cell_id] = {}

                    input_info[dataset_class][sample_library_id][cell_id] = {'bam': filepath}
            
            else:
                raise ValueError('unknown library type {}'.format(library_type))

        if 'normal' not in input_info or len(input_info['normal']) == 0:
            raise ValueError('unable to find normal {}, {}'.format(
                self.args['matched_normal_sample'], self.args['matched_normal_library']))

        if 'tumour' not in input_info or len(input_info['tumour']) == 0:
            raise ValueError('no tumour cells found')

        # Fix up input key names dependent on library type
        if normal_library_type == 'SC_WGS':
            normal_sample_ids = list(input_info['normal'].keys())
            assert len(normal_sample_ids) == 1
            normal_info = input_info.pop('normal')
            input_info['normal_cells'] = normal_info[normal_sample_ids[0]]
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
        results_prefix = os.path.join(
            self.run_options["job_subdir"],
            "results")

        filenames = []

        filenames.append('haplotypes.tsv')

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)

            library_id = dataset['library']['library_id']
            sample_id = dataset['sample']['sample_id']

            if sample_id == self.args['matched_normal_sample'] and library_id == self.args['matched_normal_library']:
                continue

            filenames.append('{}_{}_allele_counts.csv'.format(sample_id, library_id))
            filenames.append('{}_{}_snv_annotations.h5'.format(sample_id, library_id))
            filenames.append('{}_{}_snv_counts.h5'.format(sample_id, library_id))
            filenames.append('{}_{}_destruct.h5'.format(sample_id, library_id))

            for snv_caller in ('museq', 'strelka_snv', 'strelka_indel'):
                filenames.append('{}_{}_{}.vcf.gz'.format(sample_id, library_id, snv_caller))
                filenames.append('{}_{}_{}.vcf.gz.csi'.format(sample_id, library_id, snv_caller))
                filenames.append('{}_{}_{}.vcf.gz.tbi'.format(sample_id, library_id, snv_caller))

        return [os.path.join(results_prefix, filename.format(**self.args)) for filename in filenames]

    def run_pipeline(self, results_dir, pipeline_dir, scpipeline_dir, tmp_dir, inputs_yaml, config):
        dirs = [
            pipeline_dir, 
            config['docker_path'],
            config['docker_sock_path'],
        ]

        # Pass all server storages to docker
        for storage_name in self.storages.values():
            storage = tantalus_api.get('storage', name=storage_name)
            if storage['storage_type'] == 'server':
                dirs.append(storage['storage_directory'])

        run_cmd = [
            'single_cell',
            'multi_sample_pseudo_bulk',
            '--input_yaml', inputs_yaml,
            '--out_dir', results_dir,
            '--tmpdir', tmp_dir,
            '--maxjobs', '1000',
            '--nocleanup',
            '--sentinal_only',
            '--loglevel', 'DEBUG',
            '--pipelinedir', scpipeline_dir,
            '--context_config', config['context_config_file'],
        ]

        if self.run_options['local_run']:
            run_cmd += ["--submit", "local"]

        else:
            run_cmd += [
                '--submit', 'azurebatch',
                '--storage', 'azureblob',
            ]

        # Append docker command to the beginning
        docker_cmd = [
            'docker', 'run', '-w', '$PWD',
            '-v', '$PWD:$PWD',
            '-v', '/var/run/docker.sock:/var/run/docker.sock',
            '-v', '/usr/bin/docker:/usr/bin/docker',
            '--rm',
            '--env-file', config['docker_env_file'],
        ]

        for d in dirs:
            docker_cmd.extend([
                '-v', '{d}:{d}'.format(d=d),
            ])

        docker_cmd.append(
            'shahlab.azurecr.io/scp/single_cell_pipeline:{}'.format(self.version)
        )

        run_cmd = docker_cmd + run_cmd

        if self.run_options['sc_config'] is not None:
            run_cmd += ['--config_file', self.run_options['sc_config']]
        if self.run_options['interactive']:
            run_cmd += ['--interactive']

        run_cmd += ['--call_variants', '--call_haps']

        run_cmd += ['--config_override', '\'{"bigdisk":true}\'']

        run_cmd_string = r' '.join(run_cmd)
        log.debug(run_cmd_string)
        subprocess.check_call(run_cmd_string, shell=True)


class Results:
    """
    A class representing a Results model in Tantalus.
    """
    def __init__(
            self,
            tantalus_analysis,
            storage_name,
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
        self.libraries = self.tantalus_analysis.get_input_libraries()
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
                'libraries':        self.libraries,
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

        storage_client = tantalus_api.get_storage_client(self.storage_name)
        results_filenames = self.tantalus_analysis.get_results_filenames()

        for result_filename in results_filenames:  # Exclude metrics files
            result_filepath = os.path.join(storage_client.prefix, result_filename)
            file_resource, file_instance = tantalus_api.add_file(
                storage_name=self.storage_name,
                filepath=result_filepath,
                update=update,
            )

            file_resource_ids.add(file_resource["id"])
            
        return list(file_resource_ids)

    def get_id(self):
        return self.results['id']
