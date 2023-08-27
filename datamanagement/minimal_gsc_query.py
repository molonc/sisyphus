import pprint
from utils.gsc import get_sequencing_instrument, GSCAPI

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(gsc_api.query("Merge?external_identifier=SA1040"))
gsc_api = GSCAPI()
pp.pprint(gsc_api.query("parent_library?library=PX2985"))
pp.pprint(gsc_api.query("library?deliverable_format=BAM"))


gsc_api.query("merge?collaborator_id=81")
bams = gsc_api.query("library?deliverable_format=BAM")
collaborator_id
collab81 = gsc_api.query("library?collaborator_id=81")
metadata = gsc_api.query("Metadata/library?collaborator_id=81")
bams[0]['collaborator_id']

wgs = []
for seq_i in collab81:
    if seq_i['deliverable_format'] == 'BAM':
        wgs += [seq_i]


whole = []
for seq_i in collab81:
    if seq_i['library_strategy'] == 'WGS':
        whole += [seq_i]

whole[0]

external_identifier

SA1040 = []

for seq_i in whole:
    if seq_i['external_identifier'] == 'SA501':
        SA1040 += [seq_i]