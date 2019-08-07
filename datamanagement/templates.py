import os
import re

LANE_ID_RE = re.compile("^.*_\\d$")
JIRA_ID_RE = re.compile('^SC-[0-9]+$')

ALIGNMENT_METRICS = os.path.join(
	'{results_dir}',
	'results',
	'alignment',
	'{library_id}_alignment_metrics.csv.gz'
)

SC_WGS_FQ_TEMPLATE = os.path.join(
    "single_cell_indexing",
    "fastq",
    "{primary_sample_id}",
    "{dlp_library_id}",
    "{flowcell_id}_{lane_number}",
    "{cell_sample_id}_{dlp_library_id}_{index_sequence}_{read_end}.fastq{extension}",
)

SC_WGS_BAM_TEMPLATE = os.path.join(
	'single_cell_indexing',
	'bam',
	'{library_id}',
	'{ref_genome}',
	'{aligner_name}',
	'numlanes_{number_lanes}',
	'{cell_id}.bam'
)

WGS_BAM_NAME_TEMPLATE = os.path.join(
    "{sample_id}",
    "bam",
    "{library_type}",
    "{library_id}",
    "lanes_{lanes_str}",
    "{sample_id}_{library_id}_{lanes_str}.bam",
)

MERGE_BAM_PATH_TEMPLATE = {
    "WGS": "{data_path}/{library_name}_{num_lanes}_lane{lane_pluralize}_dupsFlagged.bam",
    "EXOME": "{data_path}/{library_name}_{num_lanes}_lane{lane_pluralize}_dupsFlagged.bam",
}

LANE_BAM_PATH_TEMPLATE = {
    "WGS": "{data_path}/{flowcell_id}_{lane_number}.bam",
    "RNASEQ": "{data_path}/{flowcell_id}_{lane_number}_withJunctionsOnGenome_dupsFlagged.bam",
}

MULTIPLEXED_LANE_BAM_PATH_TEMPLATE = {
    "FFPE_WGS": "{data_path}/{flowcell_id}_{lane_number}_{adapter_index_sequence}.bam",
    "WGS": "{data_path}/{flowcell_id}_{lane_number}_{adapter_index_sequence}.bam",
    "RNASEQ": "{data_path}/{flowcell_id}_{lane_number}_{adapter_index_sequence}_withJunctionsOnGenome_dupsFlagged.bam",
}

SC_WGS_BAM_NAME_TEMPLATE = "-".join([
    "{dataset_type}",
    "{sample_id}",
    "{library_type}",
    "{library_id}",
    "lanes_{lanes_hash}",
    "{aligner}",
    "{reference_genome}",
])

SC_WGS_FQ_NAME_TEMPLATE = "-".join([
    "{dataset_type}",
    "{sample_id}",
    "{library_type}",
    "{library_id}",
    "{lane}",
])

SC_ANALYSIS_NAME_TEMPLATE = "_".join([
    "sc",
    "{analysis_type}",
    "{aligner}",
    "{ref_genome}",
    "{library_id}",
    "{lanes_hashed}"   
])

GSC_SCRNA_FASTQ_PATH_TEMPLATE = os.path.join(
    "/home/aldente/private/Projects/Sam_Aparicio/",
    "{gsc_library_name}",
    "AnalyzedData",
    "{gsc_run_directory}",
    "Solexa",
    "Data",
    "current",
    "BaseCalls"
)

TENX_FASTQ_NAME_TEMPLATE = "_".join([
    "{library_id}",
    "{sample_id}",
    "{fastq}",
])

TENX_FASTQ_BLOB_TEMPLATE = os.path.join(
    "{library_id}",
    "{fastq_name}"
)

TENX_SCRNA_DATASET_TEMPLATE = "-".join([
    "{dataset_type}",
    "{sample_id}",
    "{library_type}",
    "{library_id}",
    "lanes_{lanes_hash}",
])
