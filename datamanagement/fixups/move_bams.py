import os
from dbclients.tantalus import TantalusApi
from workflows.utils.tantalus_utils import get_flowcell_lane

tantalus_api = TantalusApi()


SC_WGS_BAM_DIR_TEMPLATE = os.path.join(
    'single_cell_indexing',
    'bam',
    '{library_id}',
    '{ref_genome}',
    '{aligner_name}',
    'numlanes_{number_lanes}',
    '{jira_ticket}',
)

reference_genome_map = {
    'HG19': 'grch37',
    'MM10': 'mm10',
}

analyses_list = []
from_storage_name = "singlecellresults"
to_storage_name = "singlecellblob"
from_storage_client = tantalus_api.get_storage_client(from_storage_name)
to_storage_client = tantalus_api.get_storage_client(to_storage_name)
to_storage_id = tantalus_api.get('storages', name=to_storage_name)['id']

# Get all completed align analyses ran with specific version
# the bams associated to these analyses are in the wrong storage account
for version in ('v0.5.2', 'v0.5.3'):
    analyses = tantalus_api.list('analysis', analysis_type__name="align", status="complete", version=version)
    analyses_list += [a for a in analyses]

for analysis in analyses_list:
    jira_ticket = analysis["jira_ticket"]
    print(f"moving bams for {jira_ticket}")

    # get all bam datasets associated with the jira ticket
    bam_datasets = tantalus_api.list(
        "sequencedataset",
        dataset_type="BAM",
        analysis__jira_ticket=jira_ticket,
    )

    for dataset in bam_datasets:
        # Get number of lanes from dataset for use with filepath
        lanes = set()
        for sequence_lane in dataset['sequence_lanes']:
            lane = "{}_{}".format(sequence_lane['flowcell_id'], sequence_lane['lane_number'])
            lanes.add(lane)
        number_lanes = len(lanes)

        file_instances = tantalus_api.get_dataset_file_instances(
            dataset["id"],
            "sequencedataset",
            from_storage_name,
        )

        for file_instance in file_instances:
            blobname = file_instance["file_resource"]["filename"]

            # get url of source blob
            blob_url = from_storage_client.get_url(blobname)

            bam_filename = blobname.split("/bams/")[1]
            new_blobname = os.path.join(
                SC_WGS_BAM_DIR_TEMPLATE.format(
                    library_id=dataset["library"]["library_id"],
                    ref_genome=reference_genome_map[dataset["reference_genome"]],
                    aligner_name=dataset["aligner"],
                    number_lanes=number_lanes,
                    jira_ticket=jira_ticket,
                ),
                bam_filename,
            )

            # copy blob to desired storage account with new blobname
            blob_filepath = f"{to_storage_client.prefix}/{new_blobname}"
            print(f"copying {from_storage_client.prefix}/{blobname} to {blob_filepath}")
            to_storage_client.blob_service.copy_blob(
                container_name="data",
                blob_name=new_blobname,
                copy_source=blob_url,
            )

            tantalus_api.update(
                'file_resource',
                id=file_instance['file_resource']['id'],
                filename=new_blobname)

            tantalus_api.update(
                'file_resource',
                id=file_instance['id'],
                storage=to_storage_id)
