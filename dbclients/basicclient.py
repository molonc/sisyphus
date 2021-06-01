""" Contains a Basic API class to make requests to a REST API.
"""

from __future__ import absolute_import
from __future__ import division
import coreapi
import json
from coreapi.codecs import JSONCodec, TextCodec
from datamanagement.utils.django_json_encoder import DjangoJSONEncoder
from openapi_codec import OpenAPICodec
import requests
import pandas as pd
import time
import logging
import traceback
import random
from common_utils.utils import build_url

log = logging.getLogger('sisyphus')


class NotFoundError(Exception):
    pass


class ExistsError(Exception):
    pass


class FieldMismatchError(Exception):
    pass


class BasicAPIClient(object):
    """ Basic API class. """

    # Parameters used for pagination. Change this in subclasses.
    pagination_param_names = ()

    def __init__(self, base_url, username=None, password=None):
        """ Set up authentication using basic authentication.
        """

        # Create session and give it with auth
        self.session = requests.Session()
        if username is not None and password is not None:
            self.session.auth = (username, password)

        # Tell Tantalus we're sending JSON
        self.session.headers.update({"content-type": "application/json"})

        # Record the base URL
        self.base_url = base_url

        # Record the base API URL
        api_path = 'api'
        self.base_api_url = build_url(base_url, api_path)

        openapi_document_path = self.join_urls('api', 'swagger')
        query = {'format': 'openapi'}
        self.document_url = build_url(base_url, openapi_document_path, query)

        auth = None
        if username is not None and password is not None:
            auth = coreapi.auth.BasicAuthentication(
                username=username, password=password
            )

        decoders = [OpenAPICodec(), JSONCodec(), TextCodec()]

        self.coreapi_client = coreapi.Client(auth=auth, decoders=decoders)
        retries = 5
        for retry in range(retries):
            try:
                if retry != 0:
                    wait_time = random.randint(10,60)
                    log.info("Waiting {} seconds before connecting to {}".format(wait_time, api_url))
                    time.sleep(wait_time)
                    
                self.coreapi_schema = self.coreapi_client.get(
                    self.document_url, format="openapi"
                )
                break
            except Exception:
                log.error("Connecting to {} failed. Retrying.".format(api_url))

                if retry < retries - 1:
                    traceback.print_exc()
                else:
                    log.error("Failed all retry attempts")
                    raise

    def get(self, table_name, **fields):
        """ Check if a resource exists and if so return it. """

        list_results = self.list(table_name, **fields)

        try:
            result = next(list_results)
        except StopIteration:
            raise NotFoundError("no object for {}, {}".format(table_name, fields))

        try:
            next(list_results)
            raise Exception("more than 1 object for {}, {}".format(table_name, fields))
        except StopIteration:
            pass

        return result

    def get2(self, table_name, filters):
        """ Check if a resource exists and if so return it. """

        list_results = self.filter(table_name, filters)

        try:
            result = next(list_results)
        except StopIteration:
            raise NotFoundError("no object for {}, {}".format(table_name, filters))

        try:
            next(list_results)
            raise Exception("more than 1 object for {}, {}".format(table_name, filters))
        except StopIteration:
            pass

        return result

    def get_list_pagination_initial_params(self, params):
        """ Get initial pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["page"] = 1

    def get_list_pagination_next_page_params(self, params):
        """ Get next page pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["page"] += 1

    def filter(self, table_name, filters):
        """ List resources in from endpoint with given filter fields.

        Args:
            table_name (str): the name of the table to query
            filters (dict): the name and value to filter by
        """
        list_field_names = set()
        for field in self.coreapi_schema[table_name]["list"].fields:
            list_field_names.add(field.name)

        get_params = {}
        for field_name in filters:
            if field_name in self.pagination_param_names:
                raise Exception(f'pagination param {field_name} not permitted in filters')
            if field_name not in list_field_names:
                raise Exception(f'unsupported filter field {field_name}')
            get_params[field_name] = filters[field_name]

        # Add in pagination params
        self.get_list_pagination_initial_params(get_params)

        while True:
            list_results = self.coreapi_client.action(
                self.coreapi_schema, [table_name, "list"], params=get_params)

            for result in list_results["results"]:
                yield result

            if list_results.get("next") is None:
                break

            # Set up for the next page
            self.get_list_pagination_next_page_params(get_params)

    def list(self, table_name, **fields):
        """ List resources in from endpoint with given filter fields. """

        get_params = {}

        for field in self.coreapi_schema[table_name]["list"].fields:
            if field.name in self.pagination_param_names:
                continue
            if field.name in fields:
                get_params[field.name] = fields[field.name]

        filter_fields = set(get_params.keys())

        # Since we are not accepting related fields for checking
        # they must be implemented as a filter for this endpoint
        for field_name in fields:
            if "__" in field_name and field_name not in filter_fields:
                raise ValueError("field {} not accepted for {}".format(
                    field_name, table_name))

        # Add in pagination params
        self.get_list_pagination_initial_params(get_params)

        while True:
            list_results = self.coreapi_client.action(
                self.coreapi_schema, [table_name, "list"], params=get_params
            )

            for result in list_results["results"]:

                filtered = False
                for field_name, field_value in fields.items():
                    # Currently no support for checking related model fields
                    if "__" in field_name:
                        continue

                    if field_name not in result:
                        raise Exception(
                            "field {} not in {}".format(field_name, table_name)
                        )

                    result_field = result[field_name]

                    # Response has nested foreign key relationship
                    try:
                        result_field = result[field_name]["id"]
                    except (TypeError, KeyError):
                        pass

                    # Response has nested many to many
                    many = False
                    try:
                        result_field = [a["id"] for a in result[field_name]]
                        many = True
                    except TypeError:
                        pass

                    # Response is non nested many to many
                    try:
                        result_field = [a+0 for a in result[field_name]]
                        many = True
                    except TypeError:
                        pass

                    # Response is a timestamp
                    try:
                        if result[field_name] and isinstance(result[field_name], str):
                            result_field = pd.Timestamp(result[field_name])
                            field_value = pd.Timestamp(field_value)
                    except (ValueError, TypeError):
                        pass

                    if many:
                        result_field = set(result_field)
                        field_value = set(field_value)

                    if result_field != field_value:
                        if result_field in filter_fields:
                            raise FieldMismatchError(
                                "field {} mismatches for model {}, set to {} not {}".format(
                                    field_name, result["id"], result_field, field_value
                                )
                            )
                        else:
                            filtered = True
                            continue

                if not filtered:
                    yield result

            if list_results.get("next") is None:
                break

            # Set up for the next page
            self.get_list_pagination_next_page_params(get_params)

    def create(self, table_name, fields, keys, get_existing=False, do_update=False):
        """ Create the resource and return it.
        
        Args:
            table_name (str): name of the table
            fields (dict): field names and values for new record
            keys (list): fields to act as primary keys

        Kwargs:
            get_existing (bool): get existing if possible
            update (bool): update existing if necessary

        Returns:
            obj (dict), is_updated (bool): created object and updated boolean

        First try to create the new record.  On failure if requested,
        subset the fields to those with filters and attempt to get the
        single existing record.  If the existing record is diffrent update
        if requested.
        """

        filters = {a: fields[a] for a in keys}

        try:
            result = self.get2(table_name, filters)
        except NotFoundError:
            result = None

        # Existing result not expected, raise
        if result is not None and not get_existing:
            raise ExistsError(f'existing record in {table_name} with id {result["id"]}')

        # No existing record found, attempt create
        if result is None:
            return self.coreapi_client.action(
                self.coreapi_schema, [table_name, "create"], params=fields), False

        # Record exists, check equality
        is_equal = True
        for field_name, field_value in fields.items():
            if field_name not in result:
                raise Exception(
                    "field {} not in {}".format(field_name, table_name)
                )

            result_field = result[field_name]

            # Response has nested foreign key relationship
            try:
                result_field = result[field_name]["id"]
            except (TypeError, KeyError):
                pass

            # Response has nested many to many
            many = False
            try:
                result_field = [a["id"] for a in result[field_name]]
                many = True
            except TypeError:
                pass

            # Response is non nested many to many
            try:
                result_field = [a+0 for a in result[field_name]]
                many = True
            except TypeError:
                pass

            # Response is a timestamp
            try:
                if result[field_name] and isinstance(result[field_name], str):
                    result_field = pd.Timestamp(result[field_name])
                    field_value = pd.Timestamp(field_value)
            except (ValueError, TypeError):
                pass

            if many:
                result_field = set(result_field)
                field_value = set(field_value)

            if result_field != field_value:
                is_equal = False

                if not do_update:
                    raise FieldMismatchError(
                        "field {} mismatches for {} model {}, set to {} not {}".format(
                            field_name, table_name, result["id"], result_field, field_value
                        )
                    )

        # If not equal, update
        if not is_equal:
            assert do_update
            result = self.update(table_name, id=result['id'], **fields)

        return result, not is_equal

    def get_or_create(self, table_name, **fields):
        """ Check if a resource exists in and if so return it.
        If it does not exist, create the resource and return it. """

        try:
            return self.get(table_name, **fields)
        except NotFoundError:
            pass

        return self.coreapi_client.action(
            self.coreapi_schema, [table_name, "create"], params=fields
        )

    @staticmethod
    def join_urls(*pieces):
        """Join pieces of an URL together safely."""
        return '/'.join(s.strip('/') for s in pieces) + '/'

    def update(self, table_name, id=None, **fields):
        """ Create the resource and return it. """

        if id is None:
            raise ValueError('must specify id of existing model')

        path = self.join_urls('api', table_name, str(id))
        endpoint_url = build_url(self.base_url, path)

        payload = json.dumps(fields, cls=DjangoJSONEncoder)

        r = self.session.patch(
            endpoint_url,
            data=payload)

        if not r.ok:
            raise Exception('failed with error: "{}", reason: "{}", data: "{}"'.format(
                r.reason, r.text, payload))

        return self.get(table_name, id=id)

    def delete(self, table_name, id=None):
        if id is None:
            raise ValueError('must specify id of existing model')

        path = self.join_urls('api', table_name, str(id))
        endpoint_url = build_url(self.base_url, path)

        r = self.session.delete(
            endpoint_url)

        if not r.ok:
            raise Exception('failed with error: "{}", reason: "{}"'.format(
                r.reason, r.text))

