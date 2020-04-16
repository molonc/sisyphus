#!/usr/bin/env python
import os
import click
import logging

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

from workflows.utils import saltant_utils, file_utils
from workflows.utils.jira_utils import create_ticket

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

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


def run_haplotype_calling(jira, args):
    """
    Run haplotype calling if not ran yet
    
    Arguments:
        jira {str} -- jira id
        args {dict} -- analysis arguments 
            sample_id
            library_id 
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """
    # get analysis
    try:
        analysis = tantalus_api.get(
            "analysis",
            analysis_type="infer_haps",
            input_datasets__sample__sample_id=args['sample_id'],
            input_datasets__library__library_id=args['library_id'],
        )
    except:
        analysis = None

    if not analysis:
        jira_ticket = create_ticket(
            "SC",
            f"Hapolotype calling for {args['sample_id']}_{args['library_id']}",
        )
        # create split wgs bam analysis
        analysis = haplotype_calling.HaplotypeCallingAnalysis.create_from_args(
            tantalus_api,
            jira_ticket,
            config["scp_version"],
            args,
        )

        analysis = analysis.analysis
        log.info(f"created haplotype calling analysis {analysis['id']} under ticket {jira_ticket}")

    # check status
    if analysis['status'] in ('complete', 'running'):
        log.info("infer_haps has status {} for sample {} and library {}".format(
            analysis['status'],
            args['sample_id'],
            args['library_id'],
        ))
        return analysis['status'] == 'complete'

    log.info(f"running infer haps analysis {analysis['id']}")
    saltant_utils.run_analysis(
        analysis['id'],
        'infer_haps',
        jira,
        config["scp_version"],
        args['library_id'],
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
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """

    # get analysis
    try:
        analysis = tantalus_api.get(
            "analysis",
            analysis_type="split_wgs_bam",
            input_datasets__sample__sample_id=args['sample_id'],
            input_datasets__library__library_id=args['library_id'],
        )
    except:
        analysis = None

    if not analysis:
        jira_ticket = create_ticket(
            "SC",
            f"Split WGS bams for {args['sample_id']}_{args['library_id']}",
        )
        # create split wgs bam analysis
        analysis = split_wgs_bam.SplitWGSBamAnalysis.create_from_args(
            tantalus_api,
            jira_ticket,
            config["scp_version"],
            args,
        )
        analysis = analysis.analysis
        log.info(f"created split wgs bams analysis {analysis['id']} under ticket {jira_ticket}")

    # check status
    if analysis['status'] in ('complete', 'running'):
        log.info("split_wgs_bams has status {} for sample {} and library {}".format(
            analysis['status'],
            args['sample_id'],
            args['library_id'],
        ))
        return analysis['status'] == 'complete'

    log.info(f"running split wgs bams analysis {analysis['id']}")
    saltant_utils.run_analysis(
        analysis['id'],
        'split_wgs_bam',
        jira,
        config["scp_version"],
        args['library_id'],
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
            aligner
            ref_genome

    Returns:
        Boolean -- Analysis complete
    """
    # define sample and library depending on is_normal bool
    sample_id = args['normal_sample_id'] if is_normal else args['sample_id']
    library_id = args['normal_library_id'] if is_normal else args['library_id']

    # get analysis
    try:
        analysis = tantalus_api.get(
            "analysis",
            jira_ticket=jira,
            analysis_type="merge_cell_bams",
            input_datasets__sample__sample_id=sample_id,
            input_datasets__library__library_id=library_id,
        )
    except:
        analysis = None

    if not analysis:
        # create split wgs bam analysis
        analysis = merge_cell_bams.MergeCellBamsAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )
        analysis = analysis.analysis
        log.info(f"created merge cell bams analysis {analysis['id']} under ticket {jira}")

    # check status
    if analysis['status'] in ('complete', 'running'):
        log.info("merge_cell_bams has status {} for sample {} and library {}".format(
            analysis['status'],
            sample_id,
            library_id,
        ))
        return analysis['status'] == 'complete'

    log.info(f"running merge cell bams analysis {analysis['id']}")
    saltant_utils.run_analysis(
        analysis['id'],
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
    try:
        analysis = tantalus_api.get(
            "analysis",
            jira_ticket=jira,
            analysis_type="breakpoint_calling",
            input_datasets__sample__sample_id=args['sample_id'],
            input_datasets__library__library_id=args['library_id'],
        )
    except:
        analysis = None

    if not analysis:
        # create breakpoint calling analysis
        analysis = breakpoint_calling.BreakpointCallingAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )
        analysis = analysis.analysis
        log.info(f"created breakpoint calling analysis {analysis['id']} under ticket {jira}")

    # check status
    if analysis['status'] in ('complete', 'running'):
        log.info("breakpoint_calling has status {} for sample {} and library {}".format(
            analysis['status'],
            args['sample_id'],
            args['library_id'],
        ))
        return analysis['status'] == 'complete'

    log.info(f"running breakpoint calling analysis {analysis['id']}")
    saltant_utils.run_analysis(
        analysis['id'],
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
    try:
        analysis = tantalus_api.get(
            "analysis",
            jira_ticket=jira,
            analysis_type="count_haps",
            input_datasets__sample__sample_id=args['sample_id'],
            input_datasets__library__library_id=args['library_id'],
        )
    except:
        analysis = None

    if not analysis:
        # get infer haps ticket
        infer_haps_jira_id = tantalus_api.get(
            "analysis",
            analysis_type="infer_haps",
            input_datasets__sample__sample_id=args['normal_sample_id'],
            input_datasets__library__library_id=args['normal_library_id'],
        )["jira_ticket"]

        # create breakpoint calling analysis
        analysis = haplotype_counting.HaplotypeCountingAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            {
                "infer_haps_jira_id": infer_haps_jira_id,
                **args,
            },
        )
        analysis = analysis.analysis
        log.info(f"created count haps analysis {analysis['id']} under ticket {jira}")

    # check status
    if analysis['status'] in ('complete', 'running'):
        log.info("count_haps has status {} for sample {} and library {}".format(
            analysis['status'],
            args['sample_id'],
            args['library_id'],
        ))
        return analysis['status'] == 'complete'

    log.info(f"running count haps analysis {analysis['id']}")
    saltant_utils.run_analysis(
        analysis['id'],
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
    try:
        analysis = tantalus_api.get(
            "analysis",
            jira_ticket=jira,
            analysis_type="variant_calling",
            input_datasets__sample__sample_id=args['sample_id'],
            input_datasets__library__library_id=args['library_id'],
        )
    except:
        analysis = None

    if not analysis:
        # create variant calling analysis
        analysis = variant_calling.VariantCallingAnalysis.create_from_args(
            tantalus_api,
            jira,
            config["scp_version"],
            args,
        )
        analysis = analysis.analysis
        log.info(f"created count haps analysis {analysis['id']} under ticket {jira}")

    # check status
    if analysis['status'] in ('complete', 'running'):
        log.info("variant_calling has status {} for sample {} and library {}".format(
            analysis['status'],
            args['sample_id'],
            args['library_id'],
        ))
        return analysis['status'] == 'complete'

    log.info(f"running variant calling analysis {analysis['id']}")
    saltant_utils.run_analysis(
        analysis['id'],
        'variant_calling',
        jira,
        config["scp_version"],
        args['library_id'],
        args['aligner'],
        config,
    )

    return False


def run(jira, library_id):
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
            normal_sample_id = normal_info['normal_sample_id']
            normal_library_id = normal_info['normal_library_id']
            args['normal_sample_id'] = normal_sample_id
            args['normal_library_id'] = normal_library_id
            log.info(
                f"found normal info with sample {normal_info['normal_sample_id']} and library {normal_info['normal_library_id']}"
            )
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
        statuses = {
            "infer_haps": False,
            "split_wgs_bams": False,
            "merge_normal_cell_bams": False,
            "merge_cell_bams": False,
            "variant_calling": False,
            "breakpoint_calling": False,
            "count_haps": False,
        }

        statuses['infer_haps'] = run_haplotype_calling(
            jira,
            args={
                'sample_id': args['normal_sample_id'],
                'library_id': args['normal_library_id'],
                'aligner': analysis['args']['aligner'],
                'ref_genome': analysis['args']['ref_genome'],
            },
        )
        # check if dataset is linked to WGS library
        if normal_dataset["library"]["library_type"] == "WGS":
            # run split wgs bam
            statuses['split_wgs_bams'] = run_split_wgs_bam(
                jira,
                args={
                    'sample_id': args['normal_sample_id'],
                    'library_id': args['normal_library_id'],
                    'aligner': analysis['args']['aligner'],
                    'ref_genome': analysis['args']['ref_genome'],
                },
            )
        else:
            # merge tumour cell bams for normal
            statuses['merge_normal_cell_bams'] = run_merge_cell_bams(
                jira,
                args={
                    'sample_id': args['normal_sample_id'],
                    'library_id': args['normal_library_id'],
                    'aligner': analysis['args']['aligner'],
                    'ref_genome': analysis['args']['ref_genome'],
                },
                is_normal=True,
            )

        # merge tumour bam dataset
        statuses['merge_cell_bams'] = run_merge_cell_bams(
            jira,
            args={
                'sample_id': args['sample_id'],
                'library_id': args['library_id'],
                'aligner': analysis['args']['aligner'],
                'ref_genome': analysis['args']['ref_genome'],
            },
        )

        # check if tumour and normal bams merge cell bams completed
        if (statuses['merge_normal_cell_bams'] or statuses['split_wgs_bams']) and statuses['merge_cell_bams']:
            # run variant calling
            statuses['variant_calling'] = run_variant_calling(jira, args)

        statuses['breakpoint_calling'] = run_breakpoint_calling(jira, args)

        # check if haplotype calling complete
        if statuses['infer_haps']:
            # run haplotype counting
            statuses['count_haps'] = run_haplotype_counting(jira, args)