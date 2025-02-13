import pprint
from utils.gsc import get_sequencing_instrument, GSCAPI

pp = pprint.PrettyPrinter(indent=4)
pp.pprint(gsc_api.query("Merge?external_identifier=SEQ1143_A143881A"))
gsc_api = GSCAPI()
pp.pprint(gsc_api.query("=library=PX3405"))
pp.pprint(gsc_api.query("library?deliverable_format=BAM"))


gsc_api.query("merge?collaborator_id=81")
bams = gsc_api.query("library?deliverable_format=BAM")
collaborator_id
collab81 = gsc_api.query("library?collaborator_id=81")
metadata = gsc_api.query("Metadata/library=PX3411")
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
gsc_api.query(f"concat_fastq?library=PX3410")
gsc_api.query(f"concat_fastq?name=SEQ1143_A143820B-GGTTGAAATTAACCCA")
gsc_api.query(f"concat_fastq?external_identifier=SEQ1143_A1143_A143821A")
gsc_api.query(f"concat_fastq?external_identifier=SEQ1143_*")
external_identifier

SA1040 = []

pp.pprint(collab81[980249])

for seq_i in whole:
    if seq_i['external_identifier'] == 'SA501':
        SA1040 += [seq_i]