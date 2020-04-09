#!/usr/bin/env python
import os
import click
import logging
import subprocess
from datetime import datetime, timedelta
from dateutil import parser

from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

import workflows.analysis.dlp.utils
from workflows.analysis.dlp import (
    merge_cell_bams,
    split_wgs_bam,
    breakpoint_calling,
    variant_calling,
    haplotype_calling,
    haplotype_counting,
)

from workflows.utils import saltant_utils, file_utils, tantalus_utils
from workflows.utils.jira_utils import update_jira_dlp, add_attachment, comment_jira

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False

tantalus_api = TantalusApi()
colossus_api = ColossusApi()

# load config file
config = file_utils.load_json(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'config',
        'normal_config.json',
    ))

sample_normal_mapping = file_utils.load_json(
    os.path.join(
        os.path.dirname(os.path.realpath(__file__)),
        'config',
        'sample_mapping.json',
    ))


def attach_qc_report(jira, library_id, storages):
    """ 
    Adds qc report to library jira ticket

    Arguments:
        jira {str} -- id of jira ticket e.g SC-1234
        library_id {str} -- library name
        storages {dict} -- dictionary of storages names for results and inputs
    """

    storage_client = tantalus_api.get_storage_client(storages["remote_results"])
    results_dataset = tantalus_api.get(
        "resultsdataset",
        name="{}_annotation_{}".format(
            jira,
            library_id,
        ),
    )

    qc_filename = "{}_QC_report.html".format(library_id)
    jira_qc_filename = "{}_{}_QC_report.html".format(library_id, jira)

    qc_report = list(
        tantalus_api.get_dataset_file_resources(
            results_dataset["id"],
            "resultsdataset",
            {"filename__endswith": qc_filename},
        ))

    blobname = qc_report[0]["filename"]
    local_dir = os.path.join("qc_reports", jira)
    local_path = os.path.join(local_dir, jira_qc_filename)
    if not os.path.exists(local_dir):
        os.makedirs(local_dir)

    # Download blob
    blob = storage_client.blob_service.get_blob_to_path(
        container_name="results",
        blob_name=blobname,
        file_path=local_path,
    )

    # Get library ticket
    analysis = colossus_api.get("analysis_information", analysis_jira_ticket=jira)
    library_ticket = analysis["library"]["jira_ticket"]

    log.info("Adding report to parent ticket of {}".format(jira))
    add_attachment(library_ticket, local_path, jira_qc_filename)


def load_ticket(jira):
    """
    Loads data into montage
    
    Arguments:
        jira {str} -- jira id
    
    Raises:
        Exception: Ticket failed to load
    """
    log.info(f"Loading {jira} into Montage")
    try:
        # TODO: add directory in config
        subprocess.call([
            'ssh',
            '-t',
            'loader',
            f"bash /home/uu/montageloader2_flora/load_ticket.sh {jira}",
        ])
    except Exception as e:
        raise Exception(f"failed to load ticket: {e}")

    log.info(f"Successfully loaded {jira} into Montage")


def get_normal_dataset(normal_sample_id, normal_library_id):
    # get normal dataset
    normal_dataset = tantalus_api.get(
        "sequencedataset",
        library__library_id=normal_library_id,
        sample__sample_id=normal_sample_id,
    )
    if not normal_dataset:
        raise Exception(f"no normal dataset found for sample {normal_sample_id} and library {normal_library_id}")

    return normal_dataset


def run_haplotype_calling(jira, args):
    """
    Run haplotype calling if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            sample_id
            library_id 
            normal_sample_id
            normal_library_id
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """
    # get analysis
    infer_haps_analysis = tantalus_api.get(
        "analysis",
        analysis_type="infer_haps",
        input_datasets__sample__sample_id=args['normal_sample_id'],
        input_datasets__library__library_id=args['normal_library_id'],
    )
    if not infer_haps_analysis:
        # create split wgs bam analysis
        infer_haps_analysis = haplotype_calling.HaplotypeCallingAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )

    # check status
    if infer_haps_analysis['status'] in ('complete', 'running'):
        log.info("infer_haps_analysis has status {} for sample {} and library {}".format(
            infer_haps_analysis['status'],
            args['normal_sample_id'],
            args['normal_library_id'],
        ))
        return infer_haps_analysis['status'] == 'complete'

    saltant_utils.run_analysis(
        infer_haps_analysis['id'],
        'infer_haps',
        jira,
        config["scp_version"],
        args['normal_library_id'],
        args['aligner'],
        config,
    )

    return False


def run_split_wgs_bam(jira, args):
    """
    Run split wgs bams if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            sample_id
            library_id 
            normal_sample_id
            normal_library_id
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """

    # get analysis
    split_wgs_bam_analysis = tantalus_api.get(
        "analysis",
        analysis_type="split_wgs_bam",
        input_datasets__sample__sample_id=args['normal_sample_id'],
        input_datasets__library__library_id=args['normal_library_id'],
    )
    if not split_wgs_bam_analysis:
        # create split wgs bam analysis
        split_wgs_bam_analysis = split_wgs_bam.SplitWGSBamAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )

    # check status
    if split_wgs_bam_analysis['status'] in ('complete', 'running'):
        log.info("split wgs bams has status {} for sample {} and library {}".format(
            split_wgs_bam_analysis['status'],
            args['normal_sample_id'],
            args['normal_library_id'],
        ))
        return split_wgs_bam_analysis['status'] == 'complete'

    saltant_utils.run_analysis(
        split_wgs_bam_analysis['id'],
        'split_wgs_bam',
        jira,
        config["scp_version"],
        args['normal_library_id'],
        args['aligner'],
        config,
    )

    return False


def run_merge_cell_bams(jira, args, is_normal=False):
    """
    Run merge cells bams if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            sample_id
            library_id 
            normal_sample_id
            normal_library_id
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """
    # define sample and library depending on is_normal bool
    sample_id = args['normal_sample_id'] if is_normal else args['sample_id']
    library_id = args['normal_library_id'] if is_normal else args['library_id']

    # get analysis
    merge_cell_bams_analysis = tantalus_api.get(
        "analysis",
        jira_ticket=jira,
        analysis_type="merge_cell_bams",
        input_datasets__sample__sample_id=sample_id,
        input_datasets__library__library_id=library_id,
    )
    if not merge_cell_bams_analysis:
        # create split wgs bam analysis
        merge_cell_bams_analysis = merge_cell_bams.MergeCellBamsAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )

    # check status
    if merge_cell_bams_analysis['status'] in ('complete', 'running'):
        log.info("merge_cell_bams has status {} for sample {} and library {}".format(
            merge_cell_bams_analysis['status'],
            sample_id,
            library_id,
        ))
        return merge_cell_bams_analysis['status'] == 'complete'

    saltant_utils.run_analysis(
        merge_cell_bams_analysis['id'],
        'merge_cell_bams',
        jira,
        config["scp_version"],
        args['library_id'],
        args['aligner'],
        config,
    )

    return False


def run_breakpoint_calling(jira, args):
    """
    Run breakpoint calling if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            sample_id
            library_id 
            normal_sample_id
            normal_library_id
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """

    # get analysis
    breakpoint_calling_analysis = tantalus_api.get(
        "analysis",
        jira_ticket=jira,
        analysis_type="breakpoint_calling",
        input_datasets__sample__sample_id=args['sample_id'],
        input_datasets__library__library_id=args['library_id'],
    )
    if not breakpoint_calling_analysis:
        # create breakpoint calling analysis
        breakpoint_calling_analysis = breakpoint_calling.BreakpointCallingAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )

    # check status
    if breakpoint_calling_analysis['status'] in ('complete', 'running'):
        log.info("breakpoint_calling has status {} for sample {} and library {}".format(
            breakpoint_calling_analysis['status'],
            args['sample_id'],
            args['library_id'],
        ))
        return breakpoint_calling_analysis['status'] == 'complete'

    saltant_utils.run_analysis(
        breakpoint_calling_analysis['id'],
        'breakpoint_calling',
        jira,
        config["scp_version"],
        args['library_id'],
        args['aligner'],
        config,
    )

    return False


def run_haplotype_counting(jira, args):
    """
    Run haplotype counting if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            sample_id
            library_id 
            normal_sample_id
            normal_library_id
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """

    # get analysis
    count_haps_analysis = tantalus_api.get(
        "analysis",
        jira_ticket=jira,
        analysis_type="count_haps",
        input_datasets__sample__sample_id=args['sample_id'],
        input_datasets__library__library_id=args['library_id'],
    )

    if not count_haps_analysis:
        # create breakpoint calling analysis
        count_haps_analysis = haplotype_counting.HaplotypeCountingAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )

    # check status
    if count_haps_analysis['status'] in ('complete', 'running'):
        log.info("count_haps has status {} for sample {} and library {}".format(
            count_haps_analysis['status'],
            args['sample_id'],
            args['library_id'],
        ))
        return count_haps_analysis['status'] == 'complete'

    saltant_utils.run_analysis(
        count_haps_analysis['id'],
        'count_haps',
        jira,
        config["scp_version"],
        args['library_id'],
        args['aligner'],
        config,
    )

    return False


def run_variant_calling(jira, args):
    """
    Run variant calling if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            sample_id
            library_id 
            normal_sample_id
            normal_library_id
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """

    # get analysis
    variant_calling_analysis = tantalus_api.get(
        "analysis",
        jira_ticket=jira,
        analysis_type="variant_calling",
        input_datasets__sample__sample_id=args['sample_id'],
        input_datasets__library__library_id=args['library_id'],
    )

    if not variant_calling_analysis:
        # create variant calling analysis
        variant_calling_analysis = variant_calling.VariantCallingAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )

    # check status
    if variant_calling_analysis['status'] in ('complete', 'running'):
        log.info("count_haps has status {} for sample {} and library {}".format(
            variant_calling_analysis['status'],
            args['sample_id'],
            args['library_id'],
        ))
        return variant_calling_analysis['status'] == 'complete'

    saltant_utils.run_analysis(
        variant_calling_analysis['id'],
        'variant_calling',
        jira,
        config["scp_version"],
        args['library_id'],
        args['aligner'],
        config,
    )

    return False


def run_pseudobulk(jira, library_id):
    """
    Checks if conditions are satisfied for each step of pseudobulk and triggers runs
    
    Arguments:
        jira {str} -- jira id
        library_id {str} -- library name
    """

    # get hmmcopy analysis to get samples of library
    analysis = tantalus_api.get(
        "analysis",
        jira_ticket=jira,
        analysis_type__name="hmmcopy",
        input_datasets__library__library_id=library_id,
    )

    if not analysis:
        return

    # get library on colossus to get primary sample
    library = colossus_api.get("library", pool_id=library_id)
    primary_sample_id = library["sample"]["sample_id"]

    # iterate through input datasets and run pseudobulk
    for input_dataset in analysis['input_datasets']:
        dataset = tantalus_api.get("sequencedataset", id=input_dataset)

        # TODO: need to check if the sample is worth running
        # maybe check if sample is primary sample on colossus?
        # or check exp condition is not gDNA, htert etc.
        sample_id = dataset['sample']['sample_id']
        library_id = dataset['library']['library_id']

        if primary_sample_id != sample_id:
            continue

        # init args for analysis
        args = {
            'sample_id': sample_id,
            'library_id': library_id,
            'aligner': analysis['args']['aligner'],
            'ref_genome': analysis['args']['ref_genome'],
        }

        # get normal sample and library for dataset sample by searching in
        # mapping for a key partially matching with sample_id
        normal_info = [
            sample_normal_mapping[sample_prefix] for sample_prefix in sample_normal_mapping
            if sample_id.startswith(sample_prefix)
        ]

        if normal_info:
            normal_info = normal_info[0]
            print(normal_info)
            normal_sample_id = normal_info['normal_sample_id']
            normal_library_id = normal_info['normal_library_id']
            args['normal_sample_id'] = normal_sample_id
            args['normal_library_id'] = normal_library_id
        else:
            raise Exception(f"no normal match found for sample {sample_id}")

        # get normal dataset
        normal_dataset = workflows.analysis.dlp.utils.get_most_recent_dataset(
            tantalus_api,
            sample__sample_id=normal_sample_id,
            library__library_id=normal_library_id,
            aligner__name__startswith=args["aligner"],
            reference_genome__name=args["ref_genome"],
            region_split_length=None,
            dataset_type="BAM",
        )

        if not normal_dataset:
            raise Exception(f"no normal dataset found for sample {normal_sample_id} and library {normal_library_id}")

        # track pseudobulk analyses
        statuses = {}

        # TODO: need to create jira ticket for infer haps
        statuses['infer_haps'] = run_haplotype_calling(jira, args)

        # check if dataset is linked to WGS library
        if normal_dataset["library"]["library_type"] == "WGS":
            # TODO: create ticket for split wgs bams

            # split wgs bam
            statuses['split_wgs_bams'] = run_split_wgs_bam(jira, args)

            # check if split wgs bams completed
            if statuses['split_wgs_bams']:
                # run variant calling
                statuses['variant_calling'] = run_variant_calling(jira, args)

        else:
            # merge tumour bam dataset
            statuses['merge_cell_bams'] = run_merge_cell_bams(jira, args)
            # merge normal bam dataset
            statuses['merge_normal_cell_bams'] = run_merge_cell_bams(jira, args, is_normal=True)

            # check if tumour and normal bams merge cell bams completed
            if statuses['merge_normal_cell_bams'] and statuses['merge_cell_bams']:
                # run variant calling
                statuses['variant_calling'] = run_variant_calling(jira, args)

        statuses['breakpoint_calling'] = run_breakpoint_calling(jira, args)

        # check if haplotype calling complete
        if statuses['infer_haps']:
            # run haplotype counting
            statuses['count_haps'] = run_haplotype_counting(jira, args)


@click.command()
@click.option("--aligner", type=click.Choice(['A', 'M']))
def main(aligner):
    """
    Gets all qc (align, hmmcopy, annotation) analyses set to ready 
    and checks if requirements have been satisfied before triggering
    run on saltant.

    Kwargs:
        aligner (str): name of aligner 
    """

    # map of type of analyses required before particular analysis can run
    # note: keep this order to avoid checking requirements more than once
    required_analyses_map = {
        'annotation': [
            'hmmcopy',
            'align',
        ],
        'hmmcopy': ['align'],
        'align': [],
    }

    # get colossus analysis information objects with status not complete
    analyses = colossus_api.list(
        "analysis_information",
        analysis_run__run_status_ne="complete",
        aligner=aligner if aligner else config["default_aligner"],
    )

    for analysis in analyses:
        # get library id
        library_id = analysis["library"]["pool_id"]
        log.info(f"{library_id}")

        # skip analysis if marked as complete
        status = analysis["analysis_run"]["run_status"]

        # skip analyses older than this year
        # parse off ending time range
        last_updated_date = parser.parse(analysis["analysis_run"]["last_updated"][:-6])
        if last_updated_date < datetime(2020, 1, 1):
            continue

        jira_ticket = analysis["analysis_jira_ticket"]
        log.info(f"checking ticket {jira_ticket} library {library_id}")
        for analysis_type in required_analyses_map:
            log.info(f"checking requirements for {analysis_type}")
            # check if analysis exists on tantalus
            try:
                tantalus_analysis = tantalus_api.get(
                    'analysis',
                    jira_ticket=jira_ticket,
                    analysis_type__name=analysis_type,
                )
            except:
                tantalus_analysis = None

            if tantalus_analysis is not None:
                # check if running or complete
                status = tantalus_analysis["status"]
                if status in ('running', 'complete'):
                    log.info(f"skipping {analysis_type} for {jira_ticket} since status is {status}")

                    # update run status on colossus
                    if analysis_type == "annotation" and status == "complete":
                        analysis_run_id = analysis["analysis_run"]["id"]
                        analysis_run = colossus_api.get("analysis_run", id=analysis_run_id)
                        colossus_api.update("analysis_run", id=analysis_run_id, run_status="complete")

                    continue

                log.info(f"running {analysis_type} in library {library_id} with ticket {jira_ticket}")
                # otherwise run analysis
                saltant_utils.run_analysis(
                    tantalus_analysis['id'],
                    analysis_type,
                    jira_ticket,
                    config["scp_version"],
                    library_id,
                    aligner if aligner else config["default_aligner"],
                    config,
                )
            else:
                # set boolean determining trigger of run
                is_ready_to_create = True
                # check if required completed analyses exist
                for required_analysis_type in required_analyses_map[analysis_type]:
                    try:
                        required_analysis = tantalus_api.get(
                            'analysis',
                            jira_ticket=jira_ticket,
                            analysis_type__name=required_analysis_type,
                            status="complete",
                        )
                    except:
                        log.error(
                            f"a completed {required_analysis_type} analysis is required to run before {analysis_type} runs for {jira_ticket}"
                        )
                        # set boolean as false since analysis cannot be created yet
                        is_ready_to_create = False
                        break

                # create analysis and trigger on saltant if analysis creation has met requirements
                if is_ready_to_create:
                    log.info(f"creating {analysis_type} analysis for ticket {jira_ticket}")

                    try:
                        tantalus_utils.create_qc_analyses_from_library(
                            library_id,
                            jira_ticket,
                            config["scp_version"],
                            analysis_type,
                        )
                    except Exception as e:
                        log.error(f"failed to create {analysis_type} analysis for ticket {jira_ticket}")
                        continue
                    tantalus_analysis = tantalus_api.get(
                        'analysis',
                        jira_ticket=jira_ticket,
                        analysis_type__name=analysis_type,
                    )

                    log.info(f"running {analysis_type} in library {library_id} with ticket {jira_ticket}")
                    saltant_utils.run_analysis(
                        tantalus_analysis['id'],
                        analysis_type,
                        jira_ticket,
                        config["scp_version"],
                        library_id,
                        aligner if aligner else config["default_aligner"],
                        config,
                    )

    # get completed analyses that need montage loading
    analyses = colossus_api.list(
        "analysis_information",
        montage_status="Pending",
        analysis_run__run_status="complete",
    )

    for analysis in analyses:
        # get library id
        library_id = analysis["library"]["pool_id"]

        # skip analyses older than this year
        # parse off ending time range
        last_updated_date = parser.parse(analysis["analysis_run"]["last_updated"][:-6])
        if last_updated_date < datetime(2020, 1, 1):
            continue

        jira_ticket = analysis["analysis_jira_ticket"]
        update_jira_dlp(jira_ticket, "M")
        # upload qc report to jira ticket
        attach_qc_report(jira_ticket, library_id, config["storages"])

        # load analysis into montage
        load_ticket(jira_ticket)


if __name__ == "__main__":
    main()
