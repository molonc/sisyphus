ALIGNMENT_IMAGE = 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_alignment'
HMMCOPY_IMAGE = 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_hmmcopy'
ANNOTATION_IMAGE = 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_annotation'
BREAKPOINT_IMAGE = 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_breakpoint'
VARIANT_IMAGE = 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_variant'
HAPLOTYPES_IMAGE = 'scdnaprod.azurecr.io/singlecellpipeline/single_cell_pipeline_haplotypes'

DOCKER_IMAGES = {
	'align': ALIGNMENT_IMAGE,
	'merge_cell_bams': ALIGNMENT_IMAGE,
	'split_wgs_bam': ALIGNMENT_IMAGE,
	'hmmcopy': HMMCOPY_IMAGE,
	'annotation': ANNOTATION_IMAGE,
	'breakpoint_calling': BREAKPOINT_IMAGE,
	'variant_calling': VARIANT_IMAGE,
	'snv_genotyping': VARIANT_IMAGE,
	'infer_haps': HAPLOTYPES_IMAGE,
	'count_haps': HAPLOTYPES_IMAGE,
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