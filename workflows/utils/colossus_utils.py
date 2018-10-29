import requests
import os
import logging

COLOSSUS_API_USER = os.environ['COLOSSUS_API_USER']
COLOSSUS_API_PASSWORD = os.environ['COLOSSUS_API_PASSWORD']
COLOSSUS_API_URL = os.environ.get('COLOSSUS_API_URL', 'http://colossus.bcgsc.ca/api/')

log = logging.getLogger('sisyphus')


def make_colossus_query(table, params, auth=None):
    """ Query colossus with pagination"""

    params = dict(params)
    params['limit'] = 100

    url = COLOSSUS_API_URL + table

    data = []

    while url is not None:
        r = requests.get(url, params=params, auth=auth)

        log.debug("Querying table: {} with params: {} at url {}".format(table, params, url))

        if r.status_code != 200:
            raise Exception("Error getting {}, {}: {}".format(r.url, r.status_code, r.reason))

        if len(r.json()) == 0:
            raise Exception('No results for {}'.format(params))

        data.extend(r.json()['results'])

        if 'next' in r.json():
            url = r.json()["next"]
        else:
            url = None

        params = None

    return data


def get_analysis_info(jira):
    return make_colossus_query('analysis_information', {'analysis_jira_ticket': jira})[0]


def query_colossus_library(chip_id):
    """Query the lims for information about a given library"""
    return make_colossus_query('library', {'pool_id': chip_id})[0]


def query_colossus_for_sublibraries(chip_id):
    """
    Gets the sublibraries for a chip_id
    :param chip_id: relevant chip_id
    :return: sublibraries
    """

    return make_colossus_query('sublibraries', {'library__pool_id': chip_id})


def get_sequencing(sequencing_id):
    url = COLOSSUS_API_URL + 'sequencing/' + str(sequencing_id)

    r = requests.get(url)

    if r.status_code != 200:
        raise Exception('Could not get sequencing at ' + url)

    return r.json()


def get_chip_id_from_sequencing(sequencing_id):
    return get_sequencing(sequencing_id)['library']


def get_sequencing_loc(sequencing):
    if sequencing['dlpsequencingdetail']['sequencing_center'].upper() == 'UBCBRC':
        return 'BRC'
    elif sequencing['dlpsequencingdetail']['sequencing_center'].upper() == "BCCAGSC":
        return 'GSC'

    raise Exception("Sequencing center not recognized")


def get_path_to_archive(lane):
    # Make sure that the path to archive is specified for BRC lanes
    if not lane['path_to_archive'] or str(lane['path_to_archive']) == '':
        raise Exception('Path to archive for flowcell ID {} not specified'.format(lane['flow_cell_id']))
    return str(lane['path_to_archive'])


def update_analysis_run(analysis_run, data):
    url = COLOSSUS_API_URL + 'analysis_run/' + str(analysis_run) + '/'
    
    json_obj = requests.get(url).json()
    for field_name, field_value in data.items():
        json_obj[field_name] = field_value

    r = requests.put(url, data=json_obj, auth=(COLOSSUS_API_USER, COLOSSUS_API_PASSWORD))

    if r.status_code != 200:
        log.error('Could not PUT at {}, status code {}'.format(r.url, r.status_code))
        log.error('Trying to PUT {}'.format(data))
        raise Exception('PUT error at {} with data {}'.format(r.url, data))


def get_samplesheet(destination, chip_id, lane_id):
    colossus_url = COLOSSUS_API_URL.replace('/api/', '')
    sheet_url = '{colossus_url}/dlp/sequencing/samplesheet/query_download/{chip_id}/{lane_id}'
    sheet_url = sheet_url.format(
        colossus_url = colossus_url,
        chip_id = chip_id,
        lane_id = lane_id
    )

    log.debug("Getting the sample sheet from {}".format(sheet_url))
    utils.sync_call("Downloading the sample sheet", ["wget", "-O", dest, sheet_url])
