import pprint
from datamanagement.utils.gsc import get_sequencing_instrument, GSCAPI

pp = pprint.PrettyPrinter(indent=4)

gsc_api = GSCAPI()
# pp.pprint(gsc_api.query("merge?library=IX7551"))
pp.pprint(gsc_api.query("concat_fastq?parent_library=IX7551"))
