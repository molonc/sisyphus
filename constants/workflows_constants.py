DOCKER_IMAGES = {
	'align': 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_alignment',
	'hmmcopy': 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_hmmcopy',
	'annotation': 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_annotation',
	'breakpoint_calling': 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_breakpoint:v0.8.0',
}

# valid projects were obtained from loader VM (bccrc-pr-loader-vm)
# using get_projects() in elasticsearch.py (it's under /home/spectrum/alhena-loader/alhena/elasticsearch.py)
ALHENA_VALID_PROJECTS = {
	'DLP',
	'Caldas',
	'SPECTRUM',
	'IMAXT',
	'NYGC',
	'TracerX-Peace',
	'HGSC-Multisite',
	'Crick',
	'Brugge',
}