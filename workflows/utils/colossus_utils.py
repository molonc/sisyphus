import os
from dbclients.colossus import ColossusApi

colossus_api = ColossusApi()


def get_ref_genome(library_info, is_tenx=False):
	"""
	Get reference genome from taxonomy id

	Args:
		library_info (dict): Library from colossus

	Return:
		reference_genome (str)
	"""

	if is_tenx:
		taxonomy_id_map = {
			'9606':      'HG38',
			'10090':     'MM10',
		}

	else:
		taxonomy_id_map = {
			'9606':      'HG19',
			'10090':     'MM10',
		}

	taxonomy_id = library_info['sample']['taxonomy_id']
	reference_genome = taxonomy_id_map[taxonomy_id]

	return reference_genome
