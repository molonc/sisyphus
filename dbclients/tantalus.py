"""Contains a Tantalus API class to make requests to Tantalus.

This class makes no attempt at being all-encompassing. It covers a
subset of Tantalus API features to meet the needs of the automation
scripts.
"""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import json
import os
from django.core.serializers.json import DjangoJSONEncoder
from dbclients.basicclient import BasicAPIClient


TANTALUS_API_URL = "http://tantalus.bcgsc.ca/api/"


class TantalusApi(BasicAPIClient):
    """Tantalus API class."""

    # Parameters used for pagination
    pagination_param_names = ("limit", "offset")

    def __init__(self):
        """Set up authentication using basic authentication.

        Expects to find valid environment variables
        TANTALUS_API_USERNAME and TANTALUS_API_PASSWORD. Also looks for
        an optional TANTALUS_API_URL.
        """

        super(TantalusApi, self).__init__(
            os.environ.get("TANTALUS_API_URL", TANTALUS_API_URL),
            username=os.environ.get("TANTALUS_API_USERNAME"),
            password=os.environ.get("TANTALUS_API_PASSWORD"),
        )

    def get_list_pagination_initial_params(self, params):
        """Get initial pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["limit"] = 100
        params["offset"] = 0

    def get_list_pagination_next_page_params(self, params):
        """Get next page pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["offset"] += params["limit"]

    @staticmethod
    def join_urls(*pieces):
        """Join pieces of an URL together safely."""
        return "/".join(s.strip("/") for s in pieces) + "/"

    def sequence_dataset_add(self, model_dictionaries, tag_name=None):
        """POST to the sequence_dataset_add endpoint.

        Args:
            model_dictionaries: A list of dictionaries containing
                information about a model to create.
            tag_name: An optional string (or None) containing the name
                of the tag to associate with the model instances
                represented in the model_dictionaries.

        Raises:
            RuntimeError: The request returned with a non-2xx status
                code.
        """
        endpoint_url = self.join_urls(self.base_api_url, "/sequence_dataset_add/")

        payload = json.dumps(
            {"model_dictionaries": model_dictionaries, "tag": tag_name},
            cls=DjangoJSONEncoder,
        )

        r = self.session.post(endpoint_url, data=payload)

        try:
            # Ensure that the request was successful
            assert 200 <= r.status_code < 300
        except AssertionError:
            msg = (
                "Request to {url} failed with status {status_code}:\n"
                "The reponse from the request was as follows:\n\n"
                "{content}"
            ).format(url=endpoint_url, status_code=r.status_code, content=r.text)

            raise RuntimeError(msg)
