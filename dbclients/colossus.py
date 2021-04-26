from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import os
from dbclients.basicclient import BasicAPIClient

from constants.dbclients_constants import DEFAULT_COLOSSUS_API_URL

COLOSSUS_API_URL = os.environ.get("COLOSSUS_API_URL", DEFAULT_COLOSSUS_API_URL)


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

    def get_sublibraries_by_field(self, library_id, field_name):
        """ Get sublibraries in a dictionary keyed by a given field.
        """
        sublibrary_list = self.list("sublibraries", library__pool_id=library_id)

        sublibrary_dict = dict()
        for sublibrary in sublibrary_list:
            if 'index_sequence' not in sublibrary:
                sublibrary["index_sequence"] = f"{sublibrary['primer_i7']}-{sublibrary['primer_i5']}"

            field_value = sublibrary[field_name]

            if field_value in sublibrary_dict:
                raise Exception(f"multiple sublibraries for {field_name} {field_value}")

            sublibrary_dict[field_value] = sublibrary

        return sublibrary_dict

    def get_sublibraries_by_cell_id(self, library_id):
        """ Get sublibraries in a dictionary keyed by cell_id.
        """
        return self.get_sublibraries_by_field(library_id, 'cell_id')

    def get_sublibraries_by_index_sequence(self, library_id):
        """ Get sublibraries in a dictionary keyed by index_sequence.
        """
        return self.get_sublibraries_by_field(library_id, 'index_sequence')


_default_client = ColossusApi()
get_colossus_sublibraries_from_library_id = (
    _default_client.get_colossus_sublibraries_from_library_id
)
query_libraries_by_library_id = _default_client.query_libraries_by_library_id
