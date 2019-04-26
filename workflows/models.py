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
from datamanagement.utils.utils import get_analysis_lanes_hash
from utils import tantalus_utils, file_utils

log = logging.getLogger('sisyphus')

tantalus_api = dbclients.tantalus.TantalusApi()
colossus_api = dbclients.colossus.ColossusApi()


class AnalysisInfo:
    """
    A class representing an analysis information object in Colossus,
    containing settings for the analysis run.
    """
    def __init__(self, jira, analysis_type):
        self.status = 'idle'
        self.analysis_type = analysis_type
        self.analysis_info = colossus_api.get('analysis_information', analysis_jira_ticket=jira)
        self.analysis_run = self.analysis_info['analysis_run']['id']

    def set_run_status(self):
        self.update('running')

    def set_archive_status(self):
        self.update('archiving')

    def set_error_status(self):
        self.update('error')

    def set_finish_status(self):
        self.update('{}_complete'.format(self.analysis_type))

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


class TenXAnalysisInfo(AnalysisInfo):
    """
    A class representing an analysis information object in Colossus,
    containing settings for the analysis run.
    """
    def __init__(self, jira, version, tenx_library_id):
        self.status = 'idle'
        self.analysis_info = self.get_or_create_analysis(jira, version, tenx_library_id)
        # Replace with this once automation script is complete
        # self.analysis_info = colossus_api.get("analysis", jira_ticket=jira)

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
            analysis_info = colossus_api.get('analysis', input_type="TENX", jira_ticket=jira_ticket)

        except NotFoundError:
            library_id = self.get_library_id(tenx_library_id)


            data = {
                "jira_ticket":          jira_ticket, 
                "input_type":           "TENX",
                "version":              version, 
                "run_status":           "idle",
                "submission_date":      str(datetime.date.today()),
                "tenx_library":         library_id,
                "tenxsequencing_set":   [], 
                "pbalsequencing_set":   [],
                "dlpsequencing_set":    [],
            }

            analysis = colossus_api.create('analysis', **data)
            log.info("Created analysis for {} with data {}".format(tenx_library_id, data))

        return analysis

    def set_finish_status(self):
        self.update('complete')

    def update(self, status):
        data = {
            'run_status' :  status,
        }
        colossus_api.update('analysis', id=self.analysis, **data)


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
        self.analysis = self.get_or_create_analysis(jira, version, args, update=update)
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


class AlignHmmcopyMixin(object):
    """
    Common functionality for both Align and Hmmcopy analyses.
    """

    def generate_unique_name(self, jira, version, args, input_datasets, input_results):

        lanes = set()

        for input_dataset in input_datasets:
            dataset = tantalus_api.get('sequence_dataset', id=input_dataset)
            for sequence_lane in dataset['sequence_lanes']:
                lane = "{}_{}".format(sequence_lane['flowcell_id'], sequence_lane['lane_number'])
                lanes.add(lane)

        lanes = ", ".join(sorted(lanes))
        lanes = hashlib.md5(lanes.encode('utf-8'))
        lanes_hashed = "{}".format(lanes.hexdigest()[:8])

        # MAYBE: Add this to templates?
        name = "sc_{}_{}_{}_{}_{}".format(
            self.analysis_type, 
            args['aligner'], 
            args['ref_genome'], 
            args['library_id'],
            lanes_hashed,
        )

        return name


class AlignAnalysis(AlignHmmcopyMixin, Analysis):
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

        reference_genome_map = {
            'HG19': 'grch37',
            'MM10': 'mm10',
        }

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
                file_resource = tantalus_api.get("file_resource", id=file_instance["file_resource"]["id"])
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
                lane_fastqs[lane_id]['read_type'] = lane_info[lane_id]['read_type']

            if len(lane_fastqs) == 0:
                raise Exception('No fastqs for cell_id {}, index_sequence {}'.format(
                    row['cell_id'], row['index_sequence']))

            bam_filename = templates.SC_WGS_BAM_TEMPLATE.format(
                library_id=self.args['library_id'],
                ref_genome=reference_genome_map[self.args['ref_genome']],
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
            "{library_id}_alignment_metrics.csv.gz",
            "{library_id}_alignment_metrics.csv.gz.yaml",
            "{library_id}_gc_metrics.csv.gz",
            "{library_id}_gc_metrics.csv.gz.yaml",
            "info.yaml"
        ]

        return [os.path.join(results_prefix, filename.format(**self.args)) for filename in filenames]

    def run_pipeline(self):
        if self.run_options["skip_pipeline"]:
            return launch_pipeline.run_pipeline2
        else:
            return launch_pipeline.run_pipeline


class HmmcopyAnalysis(AlignHmmcopyMixin, Analysis):
    """
    A class representing an hmmcopy analysis in Tantalus.
    """
    def __init__(self, jira, version, args, run_options, **kwargs):
        super(HmmcopyAnalysis, self).__init__('hmmcopy', jira, version, args, **kwargs)
        self.run_options = run_options

    def _check_lane_exists(self, flowcell_id, lane_number, sequencing_centre):
        try:
            sequence_lane = tantalus_api.get(
                'sequencing_lane',
                flowcell_id=flowcell_id,
                lane_number=lane_number,
                sequencing_centre=sequencing_centre,
            )
        except NotFoundError:
            raise Exception("{}_{} is not a valid lane from the {}".format(
                flowcell_id, lane_number, sequencing_centre))

    @staticmethod
    def search_input_datasets(args):
        library_datasets = list(tantalus_api.list("sequence_dataset",
            library__library_id=args['library_id'],
            dataset_type='BAM',
            aligner__name=args['aligner'],
            reference_genome__name=args['ref_genome']),
        )

        # Set of lanes requiring analysis
        lanes = set()

        # If no lanes were specified, find all lanes
        # for all datasets for the given library
        if not args["gsc_lanes"] and not args["brc_flowcell_ids"]:
            for dataset in library_datasets:
                for sequence_lane in dataset["sequence_lanes"]:
                    lane = "{}_{}".format(
                        sequence_lane["flowcell_id"],
                        sequence_lane["lane_number"],
                    )
                    lanes.add(lane)

        # Find bam flowcell lane ids for the specified
        # brc flowcells and gsc flowcell lanes
        else:
            if args['gsc_lanes'] is not None:
                for lane in args['gsc_lanes']:
                    flowcell_id = (lane.split('_'))[0]
                    lane_number = (lane.split('_'))[1]
                    self._check_lane_exists(flowcell_id, lane_number, "GSC")
                    lanes.add(lane)

            if args['brc_flowcell_ids'] is not None:
                for flowcell_id in args['brc_flowcell_ids']:
                    for lane_number in range(1, 5):
                        self._check_lane_exists(flowcell_id, lane_number, "BRC")
                        lanes.add("{}_{}".format(flowcell_id, lane_number))

        # Generate a list of datasets with the exact set of lanes specified
        input_datasets = list()
        for dataset in library_datasets:
            dataset_lanes = set()
            for lane in dataset["sequence_lanes"]:
                dataset_lanes.add("{}_{}".format(
                    lane["flowcell_id"],
                    lane["lane_number"]))

            if dataset_lanes == lanes:
                input_datasets.append(dataset)

        HmmcopyAnalysis.check_input_datsets(args, input_datasets)

        input_dataset_ids = [d['id'] for d in input_datasets]

        return input_dataset_ids

    @staticmethod
    def check_input_datsets(args, input_datasets):
        '''
        Check if all samples for the library have exactly one input dataset
        '''
        library_samples = set()
        sublibraries = colossus_api.list("sublibraries", library__pool_id=args['library_id'])

        for sublibrary in sublibraries:
            sample_id = sublibrary["sample_id"]["sample_id"]
            library_samples.add(sample_id)

        for sample_id in library_samples:
            input_dataset_samples = [dataset["sample"]["sample_id"] for dataset in input_datasets]

            if sample_id not in input_dataset_samples:
                raise Exception("No input dataset for library sample {}".format(sample_id))

            log.info("Sample {} has a dataset in the input datasets".format(sample_id))

        log.info("Every sample in the library {} has an input dataset".format(args['library_id']))

        # Check if one dataset per sample
        dataset_samples = collections.defaultdict(list)
        for dataset in input_datasets:
            sample_id = dataset["sample"]["sample_id"]
            dataset_samples[sample_id].append(dataset["id"])

        for sample in dataset_samples:
            if len(dataset_samples[sample]) != 1:
                raise Exception("Sample {} has more than one input dataset".format(sample))
            
            log.info("Sample {} has exactly one input dataset".format(sample))

        log.info("Each sample has only one input dataset")
      
    def get_results_filenames(self):
        results_prefix = os.path.join(
            self.run_options["job_subdir"],
            "results",
            "results",
            "hmmcopy_autoploidy")

        filenames = [
            os.path.join("plots", "bias", "{}_bias.tar.gz".format(self.args["library_id"])),
            os.path.join("plots", "segments", "{}_segs.tar.gz".format(self.args["library_id"])),
            os.path.join("plots", "{}_heatmap_by_ec_filtered.pdf".format(self.args["library_id"])),
            os.path.join("plots", "{}_heatmap_by_ec.pdf".format(self.args["library_id"])),
            os.path.join("plots", "{}_kernel_density.pdf".format(self.args["library_id"])),
            os.path.join("plots", "{}_metrics.pdf".format(self.args["library_id"])),
            "info.yaml"
        ]

        for i in range(0,7):
            filenames.append("{}_multiplier{}_igv_segments.seg".format(self.args["library_id"], i)),
            filenames.append("{}_multiplier{}_metrics.csv.gz".format(self.args["library_id"], i)),
            filenames.append("{}_multiplier{}_metrics.csv.gz.yaml".format(self.args["library_id"], i)),
            filenames.append("{}_multiplier{}_params.csv.gz".format(self.args["library_id"], i)),
            filenames.append("{}_multiplier{}_params.csv.gz.yaml".format(self.args["library_id"], i)),
            filenames.append("{}_multiplier{}_reads.csv.gz".format(self.args["library_id"], i)),
            filenames.append("{}_multiplier{}_reads.csv.gz.yaml".format(self.args["library_id"], i)),
            filenames.append("{}_multiplier{}_segments.csv.gz".format(self.args["library_id"], i)),
            filenames.append("{}_multiplier{}_segments.csv.gz.yaml".format(self.args["library_id"], i)),

        return [os.path.join(results_prefix, filename) for filename in filenames]

    def run_pipeline(self):
        if self.run_options["skip_pipeline"]:
            return launch_pipeline.run_pipeline2
        else:
            return launch_pipeline.run_pipeline


    def generate_inputs_yaml(self, inputs_yaml_filename):

        if os.path.isfile(inputs_yaml_filename):
            log.info("inputs.yaml already exists from align analysis.")
            return

        log.info('Generating cell metadata')

        sample_info = generate_inputs.generate_sample_info(
            self.args["library_id"], test_run=self.run_options.get("is_test_run", False))

        if sample_info['index_sequence'].duplicated().any():
            raise Exception('Duplicate index sequences in sample info.')

        if sample_info['cell_id'].duplicated().any():
            raise Exception('Duplicate cell ids in sample info.')

        # Sort bam files by index_sequence
        bam_file_instances = dict()

        tantalus_index_sequences = set()
        colossus_index_sequences = set()

        for dataset_id in self.analysis['input_datasets']:
            dataset = self.get_dataset(dataset_id)

            file_instances = tantalus_api.get_dataset_file_instances(
                dataset['id'], 'sequencedataset', self.storages['working_inputs'],
                filters={'filename__endswith': '.bam'})

            for file_instance in file_instances:
                index_sequence = file_instance['file_resource']['sequencefileinfo']['index_sequence']
                tantalus_index_sequences.add(index_sequence)
                bam_file_instances[index_sequence] = file_instance

        input_info = {}

        for idx, row in sample_info.iterrows():
            index_sequence = row['index_sequence']

            if self.run_options.get("is_test_run", False) and (index_sequence not in tantalus_index_sequences):
                # Skip index sequences that are not found in the Tantalus dataset, since
                # we need to refer to the original library in Colossus for metadata, but
                # we don't want to iterate through all the cells present in that library
                continue

            colossus_index_sequences.add(index_sequence)

            bam_filepath = bam_file_instances[index_sequence]['filepath']

            sample_id = row['sample_id']
            if self.run_options.get("is_test_run", False):
               assert 'TEST' in sample_id

            input_info[str(row['cell_id'])] = {
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
                'fastqs':       {}, # HACK: required for hmmcopy analysis by pipeline
            }

        if colossus_index_sequences != tantalus_index_sequences:
            raise Exception("index sequences in Colossus and Tantalus do not match")

        with open(inputs_yaml_filename, 'w') as inputs_yaml:
            yaml.dump(input_info, inputs_yaml, default_flow_style=False)

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

    def generate_unique_name(self, jira, version, args, input_datasets, input_results):
        return '{}_{}'.format(jira, self.analysis_type)

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
            #filenames.append('{}_{}_destruct.h5'.format(sample_id, library_id))

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
            '--loglevel', 'DEBUG',
            '--pipelinedir', scpipeline_dir,
            '--context_config', config['context_config_file']['sisyphus'],
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


class TenXAnalysis(Analysis):
    """
    A class representing an alignment analysis in Tantalus.
    """ 
    def __init__(self, jira, version, args, run_options, **kwargs):
        super(TenXAnalysis, self).__init__('tenx', jira, version, args, **kwargs)
        self.run_options = run_options

    def generate_unique_name(self, jira, version, args, input_datasets, input_results):
        return '{}_{}'.format(jira, self.analysis_type)

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
        datasets = tantalus_api.list("sequence_dataset", 
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
        storage_client = tantalus_api.get_storage("storage", name=storage_name)

        sequence_lanes = self.get_lane_ids()

        lanes_hashed = get_analysis_lanes_hash(tantalus_api, self.analysis)

        tenx_library = colossus_api.get("tenxlibrary", name=library_id)
        sample = tenx_library["sample"]

        bam_filepath = os.path.join(storage_client["prefix"], library_id, "bams.tar.gz")


        name = "BAM-{}-SC_RNASEQ-lanes_{}-{}".format(
            library_id,
            lanes_hashed,
            ref_genome
        )

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
            "HG38":     "GRCh38",
            "MM10":     "mm10",
        }

        reference_genome = reference_genome_map[reference_genome]
        
        docker_cmd = [
            'docker', 'run', 
            '-e', '"R_HOME=/usr/local/lib/R/"',
            '-e', '"LD_LIBRARY_PATH=/usr/local/lib/R/lib/"',
            '-e', '"PYTHONPATH=$HEADNODE_AUTOMATION_PYTHON:/codebase/SCRNApipeline/"',
            '--mount type=bind,source="$PWD"/{},target=/reference '.format(reference_dir),
            '--mount type=bind,source="$PWD"/{},target=/results '.format(results_dir),
            '--mount type=bind,source="$PWD"/{},target=/data '.format(data_dir),
            '--mount type=bind,source="$PWD/{}",target=/runs '.format(runs_dir), 
            '-w="/{}"'.format(runs_dir), 
            '-t', 'nceglia/scrna-pipeline:{} run_vm'.format(version),
            '--sampleid', library_id,
            '--build', reference_genome,
        ]


        run_cmd_string = r' '.join(docker_cmd)
        log.debug(run_cmd_string)

        subprocess.check_call(run_cmd_string, shell=True)


    def create_output_results(self, update=False):
        """
        Create the set of output results produced by this analysis.
        """
        tantalus_results = TenXResults(
            self,
            self.storages['working_results'], 
            update=update,
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


class TenXResults(Results):
    """
    A class representing a Results model in Tantalus.
    """
    def __init__(
            self,
            tantalus_analysis,
            storages, 
            update=False,
        ):
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

    def get_file_resources(self, update=False):
        """
        Create file resources for each results file and return their ids.
        """
        file_resource_ids = set()

        results_filepaths = self.tantalus_analysis.get_results_filenames()

        for storage in self.storages:
            storage_client = tantalus_api.get_storage_client(storage)
            for result_filepath in results_filepaths:
                if result_filepath.startswith(storage_client.prefix):
                    file_resource, file_instance = tantalus_api.add_file(
                        storage_name=storage,
                        filepath=result_filepath,
                        update=update,
                    )

                    file_resource_ids.add(file_resource["id"])
            
        return list(file_resource_ids)
