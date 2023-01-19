import os
import re
import logging
import datetime
import yaml
import subprocess
import dbclients.tantalus
import dbclients.colossus
from dbclients.basicclient import NotFoundError

import datamanagement.templates as templates
from datamanagement.utils.utils import get_datasets_lanes_hash

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
    def __init__(self, jira, version, tenx_library_id, library_pk=None):
        self.status = 'idle'
        self.library_pk = int(library_pk)
        self.analysis = self.get_or_create_analysis(jira, version, tenx_library_id)

    def get_library_id(self, tenx_library_id):
        """
        Given library id, return library pk

        Args:
            tenx_library_id (str)

        Return:
            int
        """

        library = colossus_api.get("tenxlibrary", id=self.library_pk)

        return library["id"]

    def get_or_create_analysis(self, jira_ticket, version, tenx_library_id):

        try:
            analysis = colossus_api.get('tenxanalysis', jira_ticket=jira_ticket)

        except NotFoundError:
            library_id = self.get_library_id(tenx_library_id)

            data = {
                "jira_ticket": str(jira_ticket),
                "version": version,
                "run_status": "idle",
                "submission_date": str(datetime.date.today()),
                "tenx_library": library_id,
                "tenxsequencing_set": [],
            }

            analysis = colossus_api.create('tenxanalysis', fields=data, keys=["jira_ticket"])
            log.info("Created analysis for {} with data {}".format(tenx_library_id, data))

        return analysis

    def set_run_status(self):
        self.update('running')

    def set_error_status(self):
        self.update('error')

    def set_finish_status(self):
        self.update('complete')

    def update(self, status):
        data = {
            'run_status': status,
        }
        colossus_api.update('tenxanalysis', id=self.analysis['id'], **data)


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

        self.jira = jira
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

        try:
            analysis = tantalus_api.get("analysis", name=name)
            return analysis
        except:
            analysis = None
        fields = {
            'name': name,
            'analysis_type': self.analysis_type,
            'jira_ticket': jira,
            'args': args,
            'status': 'ready',
            'input_datasets': input_datasets,
            'input_results': input_results,
            'version': version,
        }

        keys = ['name', 'jira_ticket']

        analysis, _ = tantalus_api.create('analysis', fields, keys, get_existing=True, do_update=update)

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

        for dataset_id in self.analysis['input_results']:
            dataset = self.get_results(dataset_id)
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
            dataset = self.get_dataset(dataset_id)
            input_libraries.add(dataset['library']['id'])

        for dataset_id in self.analysis['input_results']:
            dataset = self.get_results(dataset_id)
            for library in dataset["libraries"]:
                input_libraries.add(library['id'])

        return list(input_libraries)

    def get_results_filenames(self):
        """
        Get the filenames of results from a list of templates.
        """
        raise NotImplementedError

    def upload_tenx_result(
        self,
        cellranger_filepath,
        rdata_filepath,
        rdataraw_filepath,
        report_filepath,
        bam_filepath,
        ):
        """
        Upload local TenX results to Azure blob storage.
        """
        raise NotImplementedError


class TenXAnalysis(Analysis):
    """
    A class representing an TenX analysis in Tantalus.
    """
    def __init__(self, jira, version, args, **kwargs):
        super(TenXAnalysis, self).__init__('tenx', jira, "v1.0.0", args, kwargs["storages"])

        # Tantalus storage name for hg38 TenX analysis
        self.hg38_cellranger_storage_name = "scrna_cellrangerv3"
        self.hg38_rdata_storage_name = "scrna_rdatav3"
        self.hg38_rdataraw_storage_name = "scrna_rdatarawv3"
        self.hg38_report_storage_name = "scrna_reports"
        self.hg38_bam_storage_name = "scrna_bams"

        # Tantalus storage name for mm10 TenX analysis
        self.mm10_cellranger_storage_name = "scrna_cellrangermousev3"
        self.mm10_rdata_storage_name = "scrna_rdatamousev3"
        self.mm10_rdataraw_storage_name = "scrna_rdatarawmousev3"
        self.mm10_report_storage_name = "scrna_mousereports"
        self.mm10_bam_storage_name = "scrna_mousebams"

        # human reference genome blob name
        self.hg38_cellranger_blobname = "hg38_{library_id}_cellranger.tar.gz".format(**self.args)
        self.hg38_rdata_blobname = "hg38_{library_id}_rdata.rdata".format(**self.args)
        self.hg38_rdataraw_blobname = "hg38_{library_id}_rdataraw.rdata".format(**self.args)
        self.hg38_report_blobname = "hg38_{library_id}_report.tar.gz".format(**self.args)
        self.hg38_bam_blobname = os.path.join("{library_id}", "hg38_bams.tar.gz").format(**self.args)

        # mouse reference genome blob names
        self.mm10_cellranger_blobname = "mm10_{library_id}_cellranger.tar.gz".format(**self.args)
        self.mm10_rdata_blobname = "mm10_{library_id}_rdata.rdata".format(**self.args)
        self.mm10_rdataraw_blobname = "mm10_{library_id}_rdataraw.rdata".format(**self.args)
        self.mm10_report_blobname = "mm10_{library_id}_report.tar.gz".format(**self.args)
        self.mm10_bam_blobname = os.path.join("{library_id}", "mm10_bams.tar.gz").format(**self.args)

    def generate_unique_name(self, jira, version, args, input_datasets, input_results):
        lanes_hashed = get_datasets_lanes_hash(tantalus_api, input_datasets)

        name = templates.TENX_ANALYSIS_NAME_TEMPLATE.format(
            analysis_type="tenx",
            ref_genome=args['ref_genome'],
            library_id=args['library_id'],
            lanes_hashed=lanes_hashed,
        )

        return name

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
        datasets = list(tantalus_api.list(
            "sequence_dataset",
            library__library_id=args["library_id"],
            dataset_type="FQ",
        ))

        if not(datasets):
            detail = '\n'.join([f"{str(k)}: {str(args[k])}" for k in args])
            raise ValueError(f"No matching input dataset with the following args:\n{detail}")

        dataset_ids = [dataset["id"] for dataset in datasets]

        # Check if each datasets file resource has a file instance in rnaseq
        for dataset_id in dataset_ids:
            file_instances = tantalus_api.get_dataset_file_instances(dataset_id, "sequencedataset", "scrna_fastq")

        return dataset_ids

    def create_output_datasets(self, tag_name=None, update=False):

        library_id = self.args["library_id"]
        ref_genome = self.args["ref_genome"]

        if (ref_genome not in ["MM10", "HG38"]):
            raise ValueError(f"Unknown reference genome {self.args['ref_genome']}. Expected one of 'HG38' or 'MM10.")

        dna_library = tantalus_api.get("dna_library", library_id=library_id)

        tenx_library = colossus_api.get("tenxlibrary", name=library_id)

        sample_id = tenx_library["sample"]["sample_id"]
        sample = tantalus_api.get("sample", sample_id=sample_id)

        storage_name = self.hg38_bam_storage_name if ref_genome == "HG38" else self.mm10_bam_storage_name
        storage_client = tantalus_api.get_storage(storage_name)

        sequence_lanes = self.get_lane_ids()

        lanes_hashed = get_datasets_lanes_hash(tantalus_api, self.analysis["input_datasets"])

        bam_basename = self.hg38_bam_blobname if ref_genome == "HG38" else self.mm10_bam_blobname
        bam_filepath = os.path.join(storage_client["prefix"], bam_basename)
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
        ref_genome = self.args['ref_genome']

        # list of pipeline output filenames
        # For HG38
        if(ref_genome == 'HG38'):
            filenames = [
                os.path.join("cellrangerv3", self.hg38_cellranger_blobname),
                os.path.join("rdatav3", self.hg38_rdata_blobname),
                os.path.join("rdatarawv3", self.hg38_rdataraw_blobname),
                os.path.join("reports", self.hg38_report_blobname),
                os.path.join("bams", self.hg38_bam_blobname),
            ]
        # For MM10
        elif(ref_genome == 'MM10'):
            filenames = [
                os.path.join("cellrangermousev3", self.mm10_cellranger_blobname),
                os.path.join("rdatamousev3", self.mm10_rdata_blobname),
                os.path.join("rdatarawmousev3", self.mm10_rdataraw_blobname),
                os.path.join("mousereports", self.mm10_report_blobname),
                os.path.join("mousebams", self.mm10_bam_blobname),
            ]
        else:
            raise ValueError(f"Unknown reference genome {self.args['ref_genome']}. Expected one of 'HG38' or 'MM10.")

        return [os.path.join(results_prefix, filename) for filename in filenames]

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
            '--rm',
            '-t',
            'nceglia/scrna-pipeline:vm run_vm',
            # '-t', 'nceglia/scrna-pipeline:{} run_vm'.format(version),
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
            self.storages['working_results'][self.args["ref_genome"]],
            update=update,
            skip_missing=skip_missing,
        )

        return [tantalus_results.get_id()]

    def upload_tenx_result(
        self,
        cellranger_filepath,
        rdata_filepath,
        rdataraw_filepath,
        report_filepath,
        bam_filepath,
        update=False,
        ):
        ref_genome = self.args['ref_genome']
        if (ref_genome not in ["MM10", "HG38"]):
            raise ValueError(f"Unknown reference genome {self.args['ref_genome']}. Expected one of 'HG38' or 'MM10.")

        cellranger_storage_name = self.hg38_cellranger_storage_name if ref_genome == 'HG38'else self.mm10_cellranger_storage_name
        rdata_storage_name = self.hg38_rdata_storage_name if ref_genome == 'HG38'else self.mm10_rdata_storage_name
        rdataraw_storage_name = self.hg38_rdataraw_storage_name if ref_genome == 'HG38'else self.mm10_rdataraw_storage_name
        report_storage_name = self.hg38_report_storage_name if ref_genome == 'HG38'else self.mm10_report_storage_name
        bam_storage_name = self.hg38_bam_storage_name if ref_genome == 'HG38'else self.mm10_bam_storage_name

        cellranger_blobname = self.hg38_cellranger_blobname if ref_genome == 'HG38'else self.mm10_cellranger_blobname
        rdata_blobname = self.hg38_rdata_blobname if ref_genome == 'HG38' else self.mm10_rdata_blobname
        rdataraw_blobname = self.hg38_rdataraw_blobname if ref_genome == 'HG38'else self.mm10_rdataraw_blobname
        report_blobname = self.hg38_report_blobname if ref_genome == 'HG38'else self.mm10_report_blobname
        bam_blobname = self.hg38_bam_blobname if ref_genome == 'HG38'else self.mm10_bam_blobname

        cellranger_storage_client = tantalus_api.get_storage_client(cellranger_storage_name)
        rdata_storage_client = tantalus_api.get_storage_client(rdata_storage_name)
        rdataraw_storage_client = tantalus_api.get_storage_client(rdataraw_storage_name)
        report_storage_client = tantalus_api.get_storage_client(report_storage_name)
        bam_storage_client = tantalus_api.get_storage_client(bam_storage_name)

        log.info(f"Uploading {ref_genome} results to Azure")
        print(cellranger_filepath)
        print("see above")
        self.upload_blob(cellranger_storage_client, cellranger_blobname, cellranger_filepath, update=update)
        self.upload_blob(rdata_storage_client, rdata_blobname, rdata_filepath, update=update)
        self.upload_blob(rdataraw_storage_client, rdataraw_blobname, rdataraw_filepath, update=update)
        self.upload_blob(report_storage_client, report_blobname, report_filepath, update=update)
        self.upload_blob(bam_storage_client, bam_blobname, bam_filepath, update=update)

    def upload_blob(self, storage_client, blobname, filepath, update=False):
        log.info(f"Checking if {filepath} exists locally")
        if not(os.path.exists(filepath)):
            raise ValueError(f"{filepath} does not exist locally!")

        log.info(f"Checking if {blobname} exists on Azure")
        if(storage_client.exists(blobname)):
            if(storage_client.get_size(blobname) == os.path.getsize(filepath)):
                log.info(f"{blobname} already exists and is the same size. Skipping...")

                return
            else:
                if not(update):
                    message = f"{blobname} has different size from {filepath}. Please specify --update option to overwrite."
                    log.error(message)
                    raise ValueError(message)

        log.info(f"Uploading {filepath} to {blobname}")
        storage_client.create(
            blobname,
            filepath,
            update=update,
        )



class TenXResults:
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

    def get_or_create_results(self, update=False, skip_missing=False):
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

        self.file_resources = self.get_file_resources(update=update, skip_missing=skip_missing)

        if results is not None:

            updated = False

            log.info('Found existing results {}'.format(self.name))

            if set(results['file_resources']) != set(self.file_resources):
                if update:
                    tantalus_api.update('results', id=results['id'], file_resources=self.file_resources)
                    updated = True
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
                'name': self.name,
                'results_type': self.analysis_type,
                'results_version': self.pipeline_version,
                'analysis': self.analysis,
                'file_resources': self.file_resources,
                'samples': self.samples,
                'libraries': self.libraries,
            }

            # TODO: created timestamp for results
            results, _ = tantalus_api.create(
                'results',
                fields=data,
                keys=["name", "results_type"],
            )

        return results

    def update_results(self, field):
        field_value = vars(self)[field]
        if self.results[field] != field_value:
            tantalus_api.update('results', id=self.get_id(), **{field: field_value})

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

    def get_id(self):
        return self.results['id']
