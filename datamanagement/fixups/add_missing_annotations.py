import os
import click
import logging

from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError
from datamanagement.utils.constants import LOGGING_FORMAT

logging.basicConfig(format=LOGGING_FORMAT, level=logging.INFO)
logging.getLogger("azure.storage.common.storageclient").setLevel(
    logging.WARNING)

tantalus_api = TantalusApi()

storage_client = tantalus_api.get_storage_client("singlecellresults")

@click.command()
@click.option('--jira_ticket')
def add_missing_annotations_files(jira_ticket=None):
    """
    There exists QC runs that have align and hmmcopy but no annotations results on Tantalus.
    These analyses were ran with version v0.2.25 of the single cell pipeline. Thus, filter for all align objects
    ran with v0.2.25 and see if the ticket has annotation blobs on Azure. If so, check if there exists an
    annotations result dataset for that ticket and create it and an annotations analysis if not.

    If result dataset already exist, iterate through annotation blobs and add to Tantalus if it has not been tracked.
    """

    if jira_ticket is None:
        analyses = list(tantalus_api.list('analysis', version="v0.2.25", analysis_type__name="align"))

    else:
        analyses = list(tantalus_api.list('analysis', version="v0.2.25", analysis_type__name="align", jira_ticket=jira_ticket))


    for analysis in analyses:
        jira_ticket = analysis['jira_ticket']
        version = analysis["version"]

        # Get and collect annotation blobs on azure
        blobs = list(storage_client.list(jira_ticket))
        annotation_blobs = [blob for blob in blobs if "annotation/" in blob]

        # Check if analysis ticket has an annotation results dataset if annotations blobs exist
        if len(annotation_blobs) != 0:
            try:
                annotation_results = tantalus_api.get("resultsdataset", results_type="annotation", name=f"{jira_ticket}_annotation")
                annotation_filenames = tantalus_api.get_dataset_file_resources(annotation_results["id"], "resultsdataset")
                annotation_filenames = [f["filename"] for f in annotation_filenames]
            
            except NotFoundError:
                logging.info(f"Cannot find annotation results for {jira_ticket}")
                annotation_results = None


        else:
            logging.info(f"annotations dont exist for {jira_ticket}")
            continue

        
        # Create annotation analysis object as well as results and add the files to tantalus
        if annotation_results is None:
            logging.info(f"no {jira_ticket}_annotation results exists yet, adding now")
            # Create annotation analysis
            hmmcopy_analysis =  tantalus_api.get('analysis', version=version, analysis_type__name="hmmcopy", jira_ticket=jira_ticket)
            analysis_name = analysis["name"].replace('align', 'annotation')
            annotation_analysis = tantalus_api.get_or_create(
                "analysis", 
                name=analysis_name,
                jira_ticket=jira_ticket,
                version=version,
                input_datasets=hmmcopy_analysis["input_datasets"],
                input_results=[],
                args=analysis["args"],
                status="complete",
                analysis_type="annotation",
            )

            # Create annotation result
            align_results = tantalus_api.get("resultsdataset", results_type="align", name=f"{jira_ticket}_align")
            file_resource_ids = []
            for blobname in annotation_blobs:
                file_resource, file_instance = tantalus_api.add_file('singlecellresults', os.path.join(storage_client.prefix, blobname))
                file_resource_ids.append(file_resource["id"])

            
            annotation_results = tantalus_api.get_or_create(
                "resultsdataset",
                name=f"{jira_ticket}_annotation",
                samples=[s["id"] for s in align_results["samples"]],
                libraries=[l["id"] for l in align_results["libraries"]],
                file_resources=file_resource_ids, 
                results_type="annotation",
                results_version=version,
                analysis=annotation_analysis["id"]
            )
        
        # Check if all annotations blobs on azure are on tantalus
        else:
            for blobname in annotation_blobs:
                if blobname in annotation_filenames:
                    continue
                
                logging.info(
                    f"{blobname} on azure but not tantalus, adding to {jira_ticket}_annotation now")
                file_resource, file_instance = tantalus_api.add_file(
                    'singlecellresults',  os.path.join(storage_client.prefix, blobname))
                tantalus_api.update('resultsdataset', id=annotation_results['id'], file_resources=list(
                    annotation_results['file_resources']) + [file_resource['id']])


if __name__ == '__main__':
    add_missing_annotations_files()

