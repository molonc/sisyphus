import sys
import yaml
import io
import logging
import click
import itertools

import dbclients.tantalus
import dbclients.basicclient
from datamanagement.utils.constants import LOGGING_FORMAT


@click.command()
@click.option('--jira_ticket')
def fix_results(jira_ticket=None, update=False):
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    logger = logging.getLogger("azure.storage")
    logger.setLevel(logging.ERROR)

    tantalus_api = dbclients.tantalus.TantalusApi()

    if jira_ticket is not None:
        results_iter = tantalus_api.list('results', analysis__jira_ticket=jira_ticket)

    else:
        results_iter = itertools.chain(
            tantalus_api.list('results', results_type='align'),
            tantalus_api.list('results', results_type='hmmcopy'),
            tantalus_api.list('results', results_type='annotation'))

    for results in results_iter:
        try:
            assert len(results['libraries']) <= 1

            if len(results['libraries']) == 0:
                analysis = tantalus_api.get('analysis', id=results['analysis'])
                input_datasets = analysis['input_datasets']

                libraries = set()
                for dataset_id in input_datasets:
                    dataset = tantalus_api.get('sequencedataset', id=dataset_id)
                    library = dataset['library']['id']
                    libraries.add(library)

                if len(libraries) != 1:
                    raise ValueError(f'found {len(libraries)} libraries')

                library = libraries.pop()

                logging.info(f'adding library {library} to {results["id"]}')

                results = tantalus_api.update('resultsdataset', id=results['id'], libraries=[library])

        except (ValueError, AssertionError, KeyError, dbclients.basicclient.FieldMismatchError):
            logging.exception(f'failed for {results["results_type"]}, {results["results_version"]}')


if __name__ == '__main__':
    fix_results()
