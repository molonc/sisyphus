[![https://www.singularity-hub.org/static/img/hosted-singularity--hub-%23e32929.svg](https://www.singularity-hub.org/static/img/hosted-singularity--hub-%23e32929.svg)](https://singularity-hub.org/collections/1334)
![Python version](https://img.shields.io/badge/python-2-blue.svg)
[![Code style: black](https://img.shields.io/badge/code%20style-black-000000.svg)](https://github.com/ambv/black)

# shahlab-automation-scratch

The main purpose of this repository is to have [Singularity
Hub](https://www.singularity-hub.org/) robots detect a [Singularity
container](https://www.sylabs.io/) build recipe in this repository and
build it on Singularity Hub. You can also run these tasks directly,
should you wish.

The code in here is ripped from
[Tantalus](https://github.com/shahcompbio/tantalus) and is written by a
bunch of different people. See Tantalus' repo if you care about
authorship.

## Setup

Make sure the shell you're running this container on has the following
environment variables defined:

+ COLOSSUS_API_URL
+ GSC_API_USERNAME
+ GSC_API_PASSWORD
+ TANTALUS_API_USERNAME
+ TANTALUS_API_PASSWORD

Additionally, the [dlp_bam_import.py](automate_me/dlp_bam_import.py)
script needs the following environment variables defined:

+ AZURE_STORAGE_ACCOUNT
+ AZURE_STORAGE_KEY

The variable names should be self-explanatory.

## Examples

Each of the tasks take in their arguments in the form of a single JSON
dump. Here's a few examples for how you might run them:

### [query_gsc_for_wgs_bams](automate_me/query_gsc_for_wgs_bams.py)

```
python automate_me/query_gsc_for_wgs_bams.py '{"tag_name": "gsc_wgs_bam_test2", "libraries": ["A06679"], "skip_file_import": false, "skip_older_than": "2018-06-01", "storage_name": "shahlab"}'
```

where the JSON dump is

```json
{
  "tag_name": "gsc_wgs_bam_test",
  "libraries": ["A06679"],
  "skip_file_import": false,
  "skip_older_than": "2018-06-01",
  "storage_name": "shahlab"
}
```

### [query_gsc_for_dlp_fastqs](automate_me/query_gsc_for_dlp_fastqs.py)

```
python automate_me/query_gsc_for_dlp_fastqs.py '{"dlp_library_id": "A96225C", "gsc_library_id": "PX0884", "storage_name": "shahlab"}'
```

where the JSON dump is

```json
{
  "dlp_library_id": "A96225C",
  "gsc_library_id": "PX0884",
  "storage_name": "shahlab"
}
```

### [dlp_bcl_fastq_import](automate_me/dlp_bcl_fastq_import.py)

```
python automate_me/dlp_bcl_fastq_import.py '{"output_dir": "/shahlab/archive/single_cell_indexing/NextSeq/fastq/160705_NS500668_0105_AHGTTWBGXY", "flowcell_id": "AHGTTWBGXY", "storage_name": "shahlab", "storage_directory": "/shahlab/archive"}'
```

where the JSON dump is

```json
{
  "output_dir": "/shahlab/archive/single_cell_indexing/NextSeq/fastq/160705_NS500668_0105_AHGTTWBGXY",
  "flowcell_id": "AHGTTWBGXY",
  "storage_name": "shahlab",
  "storage_directory": "/shahlab/archive"
}
```

### [dlp_bam_import](automate_me/dlp_bam_import.py)

```
python automate_me/dlp_bam_import.py '{"bam_filenames": ["/shahlab/archive/single_cell_indexing/bam/A96174B/grch37/bwa-aln/numlanes_2/SA532X5XB00478-A96174B-R55-C10.bam", "/shahlab/archive/single_cell_indexing/bam/A96174B/grch37/bwa-aln/numlanes_2/SA532X5XB00478-A96174B-R55-C12.bam"], "storage_name": "shahlab", "storage_type": "server", "storage_directory": "/shahlab/archive"}'
```

where the JSON dump is

```json
{
  "bam_filenames": ["/shahlab/archive/single_cell_indexing/bam/A96174B/grch37/bwa-aln/numlanes_2/SA532X5XB00478-A96174B-R55-C10.bam",
                    "/shahlab/archive/single_cell_indexing/bam/A96174B/grch37/bwa-aln/numlanes_2/SA532X5XB00478-A96174B-R55-C12.bam"
                   ],
  "storage_name": "shahlab",
  "storage_type": "server"
  "storage_directory": "/shahlab/archive"
}
```

### [transfer_files](automate_me/transfer_files.py)

```
python automate_me/transfer_files.py '{"tag_name": "my-fantastic-tag", "from_storage": "my-fantastic-storage-name", "to_storage": "my-other-fantastic-storage-name"}'
```

where the JSON dump is

```json
{
  "tag_name": "my-fantastic-tag",
  "from_storage": "my-fantastic-storage-name",
  "to_storage": "my-other-fantastic-storage-name"
}
```
