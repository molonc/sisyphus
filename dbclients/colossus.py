from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import os
from dbclients.basicclient import BasicAPIClient


COLOSSUS_API_URL = os.environ.get("COLOSSUS_API_URL", "http://colossus.bcgsc.ca/api/")


class ColossusApi(BasicAPIClient):
    """ Colossus API class. """

    # Parameters used for pagination
    pagination_param_names = ("page",)

    def __init__(self):
        """ Set up authentication using basic authentication.

        Expects to find valid environment variables
        COLOSSUS_API_USERNAME and COLOSSUS_API_PASSWORD. Also looks for
        an optional COLOSSUS_API_URL.
        """

        super(ColossusApi, self).__init__(
            os.environ.get("COLOSSUS_API_URL", COLOSSUS_API_URL),
            username=os.environ.get("COLOSSUS_API_USERNAME"),
            password=os.environ.get("COLOSSUS_API_PASSWORD"),
        )

    def get_list_pagination_initial_params(self, params):
        """Get initial pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["page"] = 1

    def get_list_pagination_next_page_params(self, params):
        """Get next page pagination parameters specific to this API.

        For example, offset and limit for offset/limit pagination.

        Args:
            params: A dict which is changed in place.
        """
        params["page"] += 1

    def get_colossus_sublibraries_from_library_id(self, library_id, brief=False):
        """ Gets the sublibrary information from a library id.
        """
        endpoint = "sublibraries"
        if brief:
            endpoint += "_brief"

        return list(self.list(endpoint, library__pool_id=library_id))

    def query_libraries_by_library_id(self, library_id):
        """ Gets a library by its library_id.
        """

        return self.get("library", pool_id=library_id)


_default_client = ColossusApi()
get_colossus_sublibraries_from_library_id = (
    _default_client.get_colossus_sublibraries_from_library_id
)
query_libraries_by_library_id = _default_client.query_libraries_by_library_id
