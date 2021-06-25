DOCKER_IMAGES = {
	'align': 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_alignment',
	'hmmcopy': 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_hmmcopy',
	'annotation': 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_annotation',
	'breakpoint_calling': 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_breakpoint:v0.8.0',
}

# valid projects were obtained from loader VM (bccrc-pr-loader-vm)
# using get_projects() in elasticsearch.py (it's under /home/spectrum/alhena-loader/alhena/elasticsearch.py)
ALHENA_VALID_PROJECTS = {
	'DLP': 'DLP',
	'Collab-Caldas': 'Caldas',
	'SPECTRUM': 'SPECTRUM',
	'IMAXT': 'IMAXT',
	'NYGC': 'NYGC',
	'Collab-TRACERx/PEACE': 'TracerX-Peace',
	'HGSC-Multisite': 'HGSC-Multisite',
	'Collab-Crick': 'Crick',
	'Collab-Brugge': 'Brugge',
	'Structure-Variation': 'Structure-Variation',
	'Xeno-Organoids': 'Xeno-Organoids',
}