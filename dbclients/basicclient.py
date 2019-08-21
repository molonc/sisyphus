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

log = logging.getLogger('sisyphus')


class NotFoundError(Exception):
    pass


class FieldMismatchError(Exception):
    pass


class BasicAPIClient(object):
    """ Basic API class. """

    # Parameters used for pagination. Change this in subclasses.
    pagination_param_names = ()

    def __init__(self, api_url, username=None, password=None):
        """ Set up authentication using basic authentication.
        """

        # Create session and give it with auth
        self.session = requests.Session()
        if username is not None and password is not None:
            self.session.auth = (username, password)

        # Tell Tantalus we're sending JSON
        self.session.headers.update({"content-type": "application/json"})

        # Record the base API URL
        self.base_api_url = api_url

        self.document_url = self.base_api_url + "swagger/?format=openapi"

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

    def get_list_pagination_initial_params(self, params):
        """Get initial pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        # Implement specific methods here
        pass

    def get_list_pagination_next_page_params(self, params):
        """Get next page pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        # Implement specific methods here
        pass

    def list(self, table_name, **fields):
        """ List resources in from endpoint with given filter fields. """

        get_params = {}

        for field in self.coreapi_schema[table_name]["list"].fields:
            if field.name in self.pagination_param_names:
                continue
            if field.name in fields:
                get_params[field.name] = fields[field.name]

        # Since we are not accepting related fields for checking
        # they must be implemented as a filter for this endpoint
        for field_name in fields:
            if "__" in field_name and field_name not in get_params:
                raise ValueError("field {} not accepted for {}".format(
                    field_name, table_name))

        # Add in pagination params
        self.get_list_pagination_initial_params(get_params)

        while True:
            list_results = self.coreapi_client.action(
                self.coreapi_schema, [table_name, "list"], params=get_params
            )

            for result in list_results["results"]:
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
                        raise FieldMismatchError(
                            "field {} mismatches for model {}, set to {} not {}".format(
                                field_name, result["id"], result_field, field_value
                            )
                        )

                yield result

            if list_results.get("next") is None:
                break

            # Set up for the next page
            self.get_list_pagination_next_page_params(get_params)

    def create(self, table_name, **fields):
        """ Create the resource and return it. """

        return self.coreapi_client.action(
            self.coreapi_schema, [table_name, "create"], params=fields
        )

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

        endpoint_url = self.join_urls(self.base_api_url, table_name, str(id))

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

        endpoint_url = self.join_urls(self.base_api_url, table_name, str(id))

        r = self.session.delete(
            endpoint_url)

        if not r.ok:
            raise Exception('failed with error: "{}", reason: "{}"'.format(
                r.reason, r.text))

