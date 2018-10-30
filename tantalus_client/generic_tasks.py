import os
import time
import requests
import coreapi
from openapi_codec import OpenAPICodec
from coreapi.codecs import JSONCodec
from django.core.serializers.json import DjangoJSONEncoder

TANTALUS_API_USER = os.environ['TANTALUS_API_USER']
TANTALUS_API_PASSWORD = os.environ['TANTALUS_API_PASSWORD']
TANTALUS_API_URL = os.environ.get('TANTALUS_API_URL', 'http://tantalus.bcgsc.ca/api/')

TANTALUS_DOCUMENT_URL = TANTALUS_API_URL + 'swagger/?format=openapi'

auth = coreapi.auth.BasicAuthentication(
    username = TANTALUS_API_USER,
    password = TANTALUS_API_PASSWORD
)
decoders = [OpenAPICodec(), JSONCodec()]

CLIENT = coreapi.Client(auth = auth, decoders = decoders)
SCHEMA = CLIENT.get(TANTALUS_DOCUMENT_URL, format = 'openapi')

pagination_param_names = ('limit', 'offset')


class NotFoundError(Exception):
    pass


def tantalus_create(table_name, **fields):
    for field_name, field_value in fields.iteritems():
        fields[field_name] = eval(DjangoJSONEncoder().encode(field_value))
    print(fields)

    return CLIENT.action(SCHEMA, [table_name, 'create'], params=fields)


def tantalus_get(table_name, **fields):
    list_results = tantalus_list(table_name, **fields)

    try:
        result = next(list_results)
    except StopIteration:
        raise NotFoundError('no object for {}, {}'.format(
            table_name, fields))

    try:
        next(list_results)
        raise Exception('more than 1 object for {}, {}'.format(
            table_name, fields))
    except StopIteration:
        pass

    return result


def tantalus_list(table_name, **fields):
    get_params = {}

    for field in SCHEMA[table_name]['list'].fields:
        if field.name in pagination_param_names:
            continue
        if field.name in fields:
            get_params[field.name] = fields[field.name]

    get_params['limit'] = 100
    get_params['offset'] = 0

    while True:
        list_results = CLIENT.action(SCHEMA, [table_name, 'list'], params=get_params)

        for result in list_results['results']:
            for field_name, field_value in fields.iteritems():
                # Currently no support for checking related model fields
                if '__' in field_name:
                    continue

                if field_name not in result:
                    raise Exception('field {} not in {}'.format(
                        field_name, table_name))

                try:
                    result_field = result[field_name]['id']
                except TypeError:
                    result_field = result[field_name]

                # Fields to exclude. Note that * to many
                # relationships are problems because filter for
                # exactly one related row will work even if there
                # are many related rows.
                # TODO(mwiens91): find a more elegant way of
                # achieving this effect
                exclude_fields = (
                    'created',  # datetimes have different formats
                    'sequence_lanes',   # in list is nested, but not in create
                    'file_resources',   # *->many are issues
                    'tags',   # *->many are issues
                )

                if result_field != field_value and field_name not in exclude_fields:
                    raise Exception('field {} mismatches, set to {} not {}'.format(
                        field_name, result_field, field_value))

            yield result

        if list_results.get('next') is None:
            break

        # Set up for the next page
        get_params['offset'] += get_params['limit']


def tantalus_get_or_create(table_name, **fields):
    """ Check if a resource exists in and if so return it.
        If it does not exist, create the resource and return it. """
    try:
        return tantalus_get(table_name, **fields)
    except NotFoundError:
        pass

    for field_name, field_value in fields.iteritems():
        fields[field_name] = eval(DjangoJSONEncoder().encode(field_value))

    return CLIENT.action(SCHEMA, [table_name, 'create'], params=fields)


def get_or_create(table_name, **fields):
    ''' Check if a resource exists in Tantalus and return it.
    If it does not exist, create the resource and return it. '''

    get_params = {}

    named_tasks = ['sequence_dataset_tag', 'file_transfer', 'analysis', 'results', 'file_instance', 'file_resource']
    skip_fields = ['last_updated', 'status', 'args', 'created', 'storage', 'input_datasets']

    table_fields = SCHEMA[table_name]['list'].fields
    for field in table_fields:
        if field.name in ('limit', 'offset'):
            continue
        if field.name in fields:
            get_params[field.name] = fields[field.name]
        if table_name not in named_tasks and field.name == 'name':
            # Ignore tasks that aren't named
            get_params['name'] = ''
        if field.name in skip_fields:
            get_params[field.name] = ''

    list_results = CLIENT.action(SCHEMA, [table_name, 'list'], params=get_params)

    if list_results['count'] > 1 and table_name != 'import_dlp_bam':
        raise ValueError('more than 1 object for {}, {}'.format(
            table_name, fields))

    elif list_results['count'] == 1:
        result = list_results['results'][0]
        print result

        for field_name, field_value in fields.iteritems():
            if field_name not in result:
                raise ValueError('field {} not in {}'.format(
                    field_name, table_name))

            if field_name == 'name' and result[field_name] == '':
                # Ignore tasks that aren't named
                continue
            if result[field_name] != field_value:
                if isinstance(result[field_name], list) and isinstance(field_value, list) and set(result[field_name]) == set(field_value):
                    # Handle if results are lists
                    continue
                if field_name in skip_fields:
                    continue

                raise ValueError('field {} already set to {} not {}'.format(
                    field_name, result[field_name], field_value))

    else:
        result = CLIENT.action(SCHEMA, [table_name, 'create'], params=fields)

    return result


def make_tantalus_query(table, params):
    """ Query Tantalus with pagination """

    params = dict(params)
    params['limit'] = 100
    params['offset'] = 0

    data = []
    next = 'next'

    while next is not None:
        r = CLIENT.action(SCHEMA, [table, 'list'], params = params)

        if r['count'] == 0:
            print data
            raise Exception('No results for {}'.format(params))

        data.extend(r['results'])

        if 'next' in r:
            next = r['next']
        else:
            next = None

        params['offset'] += params['limit']

    return data


def query_SimpleTask_for_status(table, params):
    """ Queries the status of a simple task """
    g = CLIENT.action(SCHEMA, table, params = params)
    return g['results'][0]


def wait_for_finish(table, task_ids):
    """ Waits for tasks to finish """

    for task in task_ids:
        status = query_SimpleTask_for_status([table, 'list'], {'id': task})

        while not status["finished"]:
            time.sleep(10)
            status = query_SimpleTask_for_status([table, 'list'], {'id': task})

        if status["finished"] and not status["success"]:
            raise Exception("Task {} failed".format(task))


def tantalus_update(table_name, id, **fields):
    # TODO: handle nested fields
    # TOOD: use coreapi
    url = TANTALUS_API_URL + table_name + '/' + str(id) + '/'

    r = requests.patch(
            url,
            data=fields,
            auth=(TANTALUS_API_USER, TANTALUS_API_PASSWORD),
        )

    if r.status_code != 200:
        raise Exception('PATCH at {} failed with error {}. {}'.format(r.url, r.status_code, r.reason))

    return r.json()
