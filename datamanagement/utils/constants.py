"""Contains some useful constants for the scripts."""


# Logging stuff
LOGGING_FORMAT = "%(asctime)s - %(levelname)s - %(lineno)d - %(message)s"

REF_GENOME_REGEX_MAP = {
    'HG18': [r'hg[-_ ]?18',                
             r'ncbi[-_ ]?build[-_ ]?36.1',  
            ],
    'HG19': [r'hg[-_ ]?19',                
             r'grc[-_ ]?h[-_ ]?37',        
             r'ncbi[-_ ]?build[-_ ]?37'
            ],}

# Useful Shahlab-specific variables
SHAHLAB_TANTALUS_SERVER_NAME = 'shahlab'
SHAHLAB_HOSTNAME = 'node0515'
SHAHLAB_SPEC_TO_BAM_BINARY_PATH = r'/gsc/software/linux-x86_64-centos6/spec-1.3.2/spec2bam'

HUMAN_REFERENCE_GENOMES_MAP = {
    'HG18': r'/shahlab/pipelines/reference/gsc_hg18.fa',
    'HG19': r'/shahlab/pipelines/reference/gsc_hg19a.fa',}

STORAGE_PREFIX_MAP = {
    'shahlab': r'/shahlab/archive',
    'gsc': r'/projects/analysis',
    'singlecell_blob': r'singlecell/data',
    'rocks': r'/share/lustre/archive'
}

STORAGE_PREFIX_REGEX_MAP = {
    'shahlab': r'^/shahlab/archive/(.+)',
    'gsc': r'^/projects/analysis/(.+)',
    'singlecell_blob': r'^singlecell/data/(.+)',
    'rocks': r'^/share/lustre/archive/(.+)'
}

PROTOCOL_ID_MAP = {
    12: "WGS",
    23: "WGS",
    73: "WGS",
    136: "WGS",
    140: "WGS",
    123: "WGS",
    179: "WGS",
    96: "EXOME",
    80: "RNASEQ",
    137: "RNASEQ",
}

SOLEXA_RUN_TYPE_MAP = {
    "Paired": "P",
    "Single": "S",
}

SEQUENCING_CENTRE_MAP = {
    "BCCAGSC": "GSC",
    "UBCBRC": "BRC"
}

DEFAULT_NATIVESPEC = "-q shahlab.q -V -cwd -o tmp -e tmp -l mem_token={mem}G,mem_free={mem}G,h_vmem={mem}G"
DEFAULT_NATIVECRAM = "-q shahlab.q -V -cwd -o tmp -e tmp -l mem_token={mem}G,mem_free={mem}G,h_vmem={mem}G"