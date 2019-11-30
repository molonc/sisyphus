import os
import click
import sys
import logging
from dbclients.tantalus import TantalusApi
from datamanagement.utils.constants import LOGGING_FORMAT


@click.command()
@click.option('--jira_ticket')
@click.option('--dry_run', is_flag=True, default=False)
def fix_bams(jira_ticket=None, dry_run=False):

    tantalus_api = TantalusApi()

    analyses_list = []
    storage_name = "singlecellresults"

    if jira_ticket is not None:
        analyses_list.append(tantalus_api.get('analysis', jira_ticket=jira_ticket, analysis_type__name="align", status="complete"))
    
    else:
        # Get all completed align analyses ran with specific version
        # the bams associated to these analyses are in the wrong storage account
        for version in ('v0.5.2', 'v0.5.3'):
            analyses = tantalus_api.list('analysis', analysis_type__name="align", status="complete", version=version)
            analyses_list += [a for a in analyses]

    for analysis in analyses_list:
        jira_ticket = analysis["jira_ticket"]

        filename = f'{jira_ticket}/results/bams/metadata.yaml'

        logging.info(f'adding file {filename}')
        if not dry_run:
            file_instance, file_resource = tantalus_api.add_file(storage_name, filename)

        # get all bam datasets associated with the jira ticket
        bam_datasets = tantalus_api.list(
            "sequencedataset",
            dataset_type="BAM",
            analysis__jira_ticket=jira_ticket,
        )

        for dataset in bam_datasets:
            dataset_id = dataset['id']

            logging.info(f'adding file to dataset {dataset_id}')
            if not dry_run:
                file_resource_ids = dataset['file_resources']
                file_resource_ids = file_resource_ids.append(file_resource['id'])
                tantalus_api.update('sequencedataset', id=dataset['id'], file_resources=file_resource_ids)


if __name__ == "__main__":
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
    fix_bams()

