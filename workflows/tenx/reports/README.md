# Description

`report_generation` is a module that supports the feature of generating quality check reports for single cell pipeline. See JIRA ticket for more information: [https://shahcompbio.atlassian.net/browse/SCP-427](https://shahcompbio.atlassian.net/browse/SCP-427)

# Dependency Requirements

The module requires `R` and `Python` to run. Check that you have these two installed, then proceed to install R-specific libraries in an `R` session:
```bash
install.packages("tidyverse")
install.packages("optparse")
install.packages("stringr")
if (!requireNamespace("BiocManager", quietly = TRUE))
    install.packages("BiocManager", repos="https://cran.rstudio.com/")
BiocManager::install("SingleCellExperiment")
BiocManager::install("scater")
```

# Usage

We would be calling `qc.R` to generate data and pngs for a specific library. After this step, we would call `generate.py` to generate the html report using the pngs and the data.

## Inputs

The script takes four arguments preceded by flags `-l`, `-i` and `-o`:

- `library_id`: the library id of a single cell sample. Example: `SCRNA10X_SA_CHIP0077_004`
- `input_dir`: the path to the local input directory that contains `.rdata` objects fetched from Azure Storage Blob. Example on numbers: `/projects/molonc/klei/scrna/qc_script/rdata_inputs`
- `output_dir`: the path to the local output directory that will store the generated qc reports. Example on numbers: `/projects/molonc/klei/scrna/qc_script/results`

## Outputs 

The outputs are `png` data plots, `csv` file containing library info and a summary `html` as requested in the JIRA ticket. 

The output file hierarchy is as follows:
```
--results/
	--libraries/
		--SCRNA10X_SA_CHIP0077_004
			--.png
			--.csv
			--.html
```

## Calling the function from a script

If you wish to call this function from a script (for example from a python script, which is most likely the case), include a function that works similarly as this in your script:
```python
Rpath = "/path/to/script/qc.R"
library_id = "library_id"
input_dir = "/path/to/input"
output_dir = "/path/to/output"

def rscript():
	import subprocess

	commands = ["Rscript", Rpath]
	args = ["-l", library_id, "-i", input_dir,  "-o", output_dir]
	subprocess.call(commands + args)


def generate_html():
	import html_generation.generation as generate

	generate.generate_html(library_id, output_dir)
```
