import argparse
import os

def get_args():
    '''
    Acquire arguments for running Sisyphus and the Single Cell Pipeline.
    '''
    parser = argparse.ArgumentParser()

    # REQUIRED ARGUMENTS #
    parser.add_argument(
        'jira',
        help='The JIRA ticket id, e.g. SC-1300.'
    )

    parser.add_argument(
        '--container_version',
        help="The docker container version",
        default=None,
    )

    # RUN ON SHAHLAB #
    parser.add_argument(
        '--shahlab_run',
        default=False,
        action='store_true',
        help='Run pipeline on shahlab15, rather than on Azure.'
    )

    parser.add_argument(
        '--no_transfer',
        default=False,
        action='store_true',
        help='Do not start transfer of files from shahlab to singlecellblob',
    )

    parser.add_argument(
        '--bams_tag',
        default=None,
        help='Optionally tag the bams produced by the single cell pipeline'
    )


    # CONFIGURATION FILES #
    parser.add_argument(
        '--config',
        default=os.path.join('config', 'normal_config.json'),
        help='Path to the user-specific config file.'
    )

    parser.add_argument(
        '--sc_config',
        help='Path to the config file for the single cell pipeline'
    )

    parser.add_argument(
        '--inputs_yaml',
        default=None,
        help='Path to an existing input yaml file.'
    )

    # SELECT A SUBSET OF DATA TO ANALYZE #
    parser.add_argument(
        '--lanes',
        default=None,
        nargs='*',
        help='GSC and/or BRC lane IDs to run. Overrides the database.'
    )

    parser.add_argument(
        '--gsc_lanes',
        default=None,
        nargs='*',
        help='GSC lanes in the form of [flowcell_id]_[lane_number]'
    )

    parser.add_argument(
        '--brc_flowcell_ids',
        default=None,
        nargs='*',
        help='BRC flowcell ids'
    )

    parser.add_argument(
        '--index_sequences',
        nargs='+',
        default=None,
        help='Specify a subset of index sequences for testing'
    )


    # EASE OF RUNNING #
    parser.add_argument(
        '--clean',
        action='store_true',
        help='Remove existing working directory for the ticket, if present.'
    )

    parser.add_argument(
        '--tag',
        default='',
        help='Appends the tag to the end of the pipeline directory.'
    )

    parser.add_argument(
        '--interactive',
        default=False,
        action='store_true',
        help='Run the single cell pipeline in interactive mode.'
    )

    parser.add_argument(
        '--sisyphus_interactive',
        default=False,
        action='store_true',
        help='Runs Sisyphus in interactive mode.'
    )

    parser.add_argument(
        '--no_data_import',
        default=False,
        action='store_true',
        help='Skips import of data from BRC and GSC.'
    )

    parser.add_argument(
        '--no_bcl2fastq',
        default=False,
        action='store_true',
        help='Skip running of bcl2fastq on BRC lanes.'
    )


    # SINGLE CELL PIPELINE OPTIONS #
    parser.add_argument(
        '--rerun',
        default=False,
        action='store_true',
        help='Rerun the single cell pipeline, regardless of existing results.'
    )

    parser.add_argument(
        '--no_align',
        default=False,
        action='store_true',
        help='Skip alignment step of single cell pipeline.'
    )

    parser.add_argument(
        '--no_hmmcopy',
        default=False,
        action='store_true',
        help='Skip hmmcopy step of single cell pipeline.'
    )

    parser.add_argument(
        '--no_classifier',
        default=False,
        action='store_true',
        help='Run hmmcopy without the classifier.'
    )

    parser.add_argument(
        '--alignment_metrics',
        default=None,
        help='Path to alignment metrics h5 file.'
    )

    parser.add_argument(
        '--jobs',
        default=1000,
        type=int,
        help='Specifies the number of jobs to submit to the queue on shahlab15.'
    )

    args = parser.parse_args()

    return dict(vars(args))
