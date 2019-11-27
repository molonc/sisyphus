import os
from dbclients.tantalus import TantalusApi
from workflows.utils.tantalus_utils import get_flowcell_lane

tantalus_api = TantalusApi()


def get_lane_number(analysis):
    lanes = dict()
    for dataset_id in analysis['input_datasets']:
        dataset = tantalus_api.get("sequencedataset", id=dataset_id)
        for lane in dataset['sequence_lanes']:
            lane_id = get_flowcell_lane(lane)
            lanes[lane_id] = lane
    return len(lanes)


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

    # Get number of lanes that was analyzed for filepath
    lane_number = get_lane_number(analysis)

    for dataset in bam_datasets:
        file_instances = tantalus_api.get_dataset_file_instances(
            dataset["id"],
            "sequencedataset",
            from_storage_name,
        )

        update_file_resources_ids = []
        # for each bam dataset, get the filenames in the dataset and create the new blob name
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
                    number_lanes=lane_number,
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

            # TODO: collect file resource ids,
            # update bam dataset with new file resources
            # and delete bams from results storage account

            file_resource, _ = tantalus_api.add_file(to_storage_name, blob_filepath)
            update_file_resources_ids += file_resource["id"]

            # delete blob from storage and file instance from tantalus
            from_storage_client.delete(file_instance["filepath"])
            tantalus_api.delete("file_instance", id=file_instance["id"])

        # update bam dataset with newly tracked file resources
        tantalus_api.update("sequencedataset", id=dataset["id"], file_resources=update_file_resources_ids)
