# Sisyphus (aka workflow_automation)


## Creating an environment on shahlab15 for single cell pipeline runs

We currently launch our single cell pipeline runs from shahlab15 so they can run on the shahlab cluster.


### Install Miniconda
* Run `curl -O https://repo.continuum.io/miniconda/Miniconda2-latest-Linux-x86_64.sh`. This downloads the script to install Miniconda
* Run `bash Miniconda2-latest-Linux-x86_64.sh`. This runs the Miniconda installation script. You need to accept the licence terms and choose the location where it will be installed (a good choice is /shahlab/<your user name>)


### Set up conda environment
* Create the environment:
```
conda create --name scpipeline python=2.7
```
* Activate the environment:
```
source activate scpipeline
```
* Ensure that you have the correct channels:
```
conda config --add channels https://conda.anaconda.org/dranew
conda config --add channels https://conda.anaconda.org/aroth85
conda config --add channels 'bioconda'
conda config --add channels 'r'
conda config --add channels 'conda-forge'
```
* Clone the [single_cell_pipeline repo](https://svn.bcgsc.ca/bitbucket/projects/SC/repos/single_cell_pipeline/browse) into your home directory and install:
```
cd single_cell_pipeline/
python setup.py install
```
* Clone the [pypeliner repo](https://bitbucket.org/dranew/pypeliner) and install:
```
cd pypeliner/
python setup.py install
```
* Clone the [tantalus_client repo](https://svn.bcgsc.ca/bitbucket/projects/SC/repos/tantalus_client/browse) and install:
```
cd tantalus_client/
python setup.py install
```
* Install required packages:
```
conda install --file single_cell_pipeline/INSTALL/conda_packages.txt
conda install dill scipy pyyaml networkx
pip install azure-storage azure-batch futures azure-mgmt coreapi openapi_codec
```
* Get Tantalus and Colossus access (ask Andrew for this) and set environment variables (put this in your .bashrc):
```
export TANTALUS_API_USER=<your Tantalus username>
export TANTALUS_API_PASSWORD=<your Tantalus password>
export COLOSSUS_API_USER=<your Colossus username>
export COLOSSUS_API_PASSWORD=<your Colossus password>
```

### Other stuff
* Make sure you have a file called `normal_config.json` in workflow_automation/ that follows `config_template.json`
* Setup passwordless login from shahlab15 to thost


### Configure workflow_automation
* Open `config_template.json`
* Change the fields `user`, `email` and `jobs_dir` to those specific to the user
* Save the file as `normal_config.json` in the same directory

* Setup passwordless login from shahlab15 to thost, beast and the Azure headnode:
```
cat ~/.ssh/id_rsa.pub | ssh <your headnode username>@52.235.40.186 'cat >> ~/.ssh/authorized_keys'
cat ~/.ssh/id_rsa.pub | ssh thost 'cat >> ~/.ssh/authorized_keys'
cat ~/.ssh/id_rsa.pub | ssh beast 'cat >> ~/.ssh/authorized_keys'
```

Make sure these entries are in `~/.ssh/config`:
```
Host thost
Hostname 10.9.208.161
User <your username>

Host beast
Hostname 10.9.4.26
User <your username>
```

* Set up passwordless login for BRC data (password is Next&3141Seq):
```
cat ~/.ssh/id_rsa.pub | ssh patientdata@brclogin1.brc.ubc.ca 'cat >> ~/.ssh/authorized_keys'
```

---

## Using saltant to run tasks

Ask for an account to [shahlab jobs](https://shahlabjobs.ca/), and refer to [the wiki page](https://www.bcgsc.ca/wiki/display/MO/Shahlab+Jobs) for more information.

### Set up a celery worker on thost

* Log onto thost
* Clone the [saltant repo](https://github.com/mwiens91/saltant) and the  [automation_tasks repo](https://svn.bcgsc.ca/bitbucket/projects/SC/repos/automation_tasks/browse)
* Create a new conda environment:
```
conda create --name thost-worker python=2.7
```
* Activate the conda environment:
```
source activate thost-worker
```
* Install requirements:
```
pip install -r saltant/requirements/requirements-worker-python2.txt
pip install -r automation_tasks/requirements.txt
```
* Copy `.env.example` to `.env`:
```
cd saltant/
cp .env.example .env
```
* Create a new auth token from the [admin page](https://shahlabjobs.ca/admin/authtoken/token/)
* Create logs and results directories for the worker on genesis, e.g.:
```
mkdir -p /shahlab/sochan/thost-worker/logs/
mkdir -p /shahlab/sochan/thost-worker/results/
```
* Update the following variables in `.env`:
```
IM_A_CELERY_WORKER=False
WORKER_LOGS_DIRECTORY={path to logs directory for the worker}
WORKER_RESULTS_DIRECTORY={path to results directory for the worker}
API_AUTH_TOKEN={your newly generated auth token}
CELERY_BROKER_URL='pyamqp://shahlab:75iXI!NlARec@40.86.226.171/shahlab_vhost'
RABBITMQ_USES_SSL=True
DJANGO_BASE_URL='https://www.shahlabjobs.ca'
```
* Get the Python path from your environment:
```
which python
```
* Add the following to your `~/.bash_profile` (use absolute paths):
```
export SHAHLAB_AUTOMATION_PYTHON={path to python}
export SHAHLAB_AUTOMATION_DIR={path to cloned automation_tasks repo}
```
* Add a task queue to the [task queue list](https://shahlabjobs.ca/api/taskqueues) and give it an appropriate name, e.g. `sochan-thost-worker`
* Inside a `screen`, launch a celery worker on thost with the same name, replacing `sochan_worker` with whatever you wish:
```
source activate thost-worker
cd saltant
celery worker -A saltant -Q {name of your newly created task queue} --concurrency=10 -n sochan-worker@%h
```
* Update `normal_config.json` with the name of your task queue
```
"thost_task_queue":"{name of your newly created task queue}"
```
* Follow [these instructions](https://saltant.readthedocs.io/en/latest/using/celery-workers.html#integrating-papertrail-without-root) to set up Papertrail to view your logs. Here's an example config file:
```
files:
  - /shahlab/sochan/thost-worker/logs/**/*
hostname: "sochan-thost-worker"
destination:
  host: logs4.papertrailapp.com
  port: 24662
  protocol: tls
pid_file: /shahlab/sochan/thost-worker/papertrail.pid
```

### Configure saltant on shahlab

Make sure you have these variables defined in your `scpipeline` environment:
```
SALTANT_API_TOKEN=
```
---


## Running the pipeline

There is detailed documentation of the single cell pipeline in the readme on its [BitBucket page](https://svn.bcgsc.ca/bitbucket/projects/SC/repos/single_cell_pipeline/browse).

When we run the pipeline from workflow_automation, we run the alignment and HMMCopy steps.
In alignment, the fastq files from the sequencer are aligned with a reference genome to generate bam files for each cell.
During the HMMCopy step, a hidden Markov model is used on the bam files to infer the copy numbers of each bin.
Statistics and metrics files are also generated for both steps.

The single cell pipeline can be configured through the Analysis Information page in Colossus.
They are specified with the `--config_override` flag when running from the command line.
The default settings are as follows:
```
--config_override '{"cluster": "azure", "aligner": "bwa-mem", "reference": "grch37", "smoothing_function": "modal", "version":"0.1.5"}'
```

The default version is the most recent of the pipeline. All versions can be seen [here](https://svn.bcgsc.ca/bitbucket/plugins/servlet/view-tags?repo=single_cell_pipeline&projKey=SC).

The options are as follows:
* clusters: {azure, shahlab}
* reference: {grch37, mm10}
* aligner: {bwa-mem, bwa-aln}
* smoothing_function: {loess, modal}

---

## BRC Runs

Sometimes, samples are sequenced at the BRC ([Biomedical Research Centre](http://brc.ubc.ca/next-generation-sequencing-at-the-brc/)) instead of the usual GSC ([Genome Sciences Centre](http://www.bcgsc.ca/)). This gives .bcl files instead of .fastq.gz files, which we need for the pipeline. We need to add an extra step to the pipeline for this conversion using [Illumina's `bcl2fastq` tool](https://support.illumina.com/content/dam/illumina-support/documents/documentation/software_documentation/bcl2fastq/bcl2fastq2_guide_15051736_v2.pdf).

In Colossus, on the Sequencing Details page for the particular sequencing, the following values need to be set:
* Sequencing center: UBCBRC
* Reverse Compliment Override may need to be set
* Flow Cell ID should not include the lane number. For example, if the flow cell ABCD1234 was associated with the sequencing instance, the Flow Cell ID field should only include ABCD1234; not ABCD1234_1, ABCD1234_2, ABCD1234_3 and ABCD1234_4
* Path to Archive also needs to be set for each flow cell ID. This should match the Temp path to raw data on BRC server given on the JIRA ticket.

The Path to Archive must match one of the following:
* `http://bigwigs.brc.ubc.ca/sequencing/<6 digits>_<run ID>_<4 digits>_<flow cell ID>/` (no longer used)
* `/share/lustre/archive/single_cell_indexing/NextSeq/bcl/<6 digits>_<run ID>_<4 digits>_<flow cell ID>/`
* `patientdata@brclogin1.brc.ubc.ca/brcwork/patientdata/<6 digits>_<run ID>_<4 digits>_<flow cell ID>/`

Note the use of trailing slashes.

`bcl2fastq` is run on `shahlab15`. Before this is done, the .bcl files are transferred to somewhere accessible to shahlab15. This is determined by the `nextseq_archive` field set in the user's `normal_config.json`.

Next, the sample sheet for the sequencing is downloaded from Colossus. Make sure the correct Reverse Compliment Override is set on the Sequencing Details page on Colossus before this is done.

Finally, `bcl2fastq` is run. It generates four lanes for the flowcell. For example, for the flowcell ID ABCD1234, the following lanes are generated: ABCD1234_1, ABCD1234_2, ABCD1234_3 and ABCD1234_4. The paths to the .fastq files are added to Tantalus at http://tantalus.bcgsc.ca/brcfastqimports/.

The single cell pipeline can now be run as with GSC fastqs.

---

## Debugging

Common issues:
* Incorrect values in `normal_config.json`
* Missing or incorrectly installed conda packages
* Incorrect or missing .fastq file paths in the input yaml file
* Missing bams due to errors during alignment
