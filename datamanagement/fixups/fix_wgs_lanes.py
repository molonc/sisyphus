from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import sys
import logging

from datamanagement.utils.gsc import GSCAPI
from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stdout, level=logging.INFO)

    gsc_api = GSCAPI()

    tantalus_api = TantalusApi()

    # List of relevant libraries from GSC lanes
    lanes = list(tantalus_api.list(
        'sequencing_lane',
        sequencing_centre='GSC'))

    libraries = set()
    for lane in lanes:
        library = tantalus_api.get('dna_library', id=lane['dna_library'])
        if library['library_type'] == 'WGS':
            libraries.add(library['library_id'])

    lane_fixes = []

    for library_id in libraries:
        infos = gsc_api.query("library?name={}".format(library_id))

        if len(infos) == 0:
            logging.warning('unable to find {}'.format(library_id))

        elif len(infos) > 1:
            raise Exception('found {} libraries for {}'.format(len(infos), library_id))

        merge_infos = gsc_api.query("merge?library={}".format(library_id))

        for merge_info in merge_infos:

            for merge_xref in merge_info["merge_xrefs"]:
                if merge_xref["object_type"] == "metadata.run":
                    # Get the incorrect lane number and flowcell id
                    aligned_libcore = gsc_api.query("aligned_libcore/{}/info".format(merge_xref["object_id"]))
                    libcore = aligned_libcore["libcore"]
                    run = libcore["run"]
                    flowcell_info = gsc_api.query("flowcell/{}".format(run["flowcell_id"]))
                    incorrect_flowcell_id = flowcell_info["lims_flowcell_code"]
                    incorrect_lane_number = run["lane_number"]

                    # Get the correct lane number and flowcell id
                    run = gsc_api.query("run/{}".format(merge_xref["object_id"]))
                    flowcell_info = gsc_api.query("flowcell/{}".format(run["flowcell_id"]))
                    correct_flowcell_id = flowcell_info["lims_flowcell_code"]
                    correct_lane_number = run["lane_number"]

                    lane_fixes.append(dict(
                        library_id=library_id,
                        incorrect_flowcell_id=incorrect_flowcell_id,
                        incorrect_lane_number=incorrect_lane_number,
                        correct_flowcell_id=correct_flowcell_id,
                        correct_lane_number=correct_lane_number,
                    ))

                elif not merge_xref["object_type"] == "metadata.aligned_libcore":
                    logging.warning('unknown object type {} for library {}'.format(
                        library_id, merge_xref["object_type"]))

    lane_fixes = pd.DataFrame(lane_fixes).drop_duplicates()

    for idx, row in lane_fixes.iterrows():
        try:
            incorrect_lane = tantalus_api.get(
                'sequencing_lane',
                flowcell_id=row['incorrect_flowcell_id'],
                lane_number=str(row['incorrect_lane_number']),
                dna_library__library_id=row['library_id'],
            )

        except NotFoundError:
            continue

        assert incorrect_lane['sequencing_library_id'] == row['library_id']

        try:
            correct_lane = tantalus_api.get(
                'sequencing_lane',
                flowcell_id=row['correct_flowcell_id'],
                lane_number=str(row['correct_lane_number']),
                dna_library=incorrect_lane['dna_library'],
            )

        except NotFoundError:
            correct_lane = None

        if correct_lane is None:
            correct_lane = tantalus_api.create(
                'sequencing_lane',
                flowcell_id=row['correct_flowcell_id'],
                lane_number=str(row['correct_lane_number']),
                dna_library=incorrect_lane['dna_library'],
                sequencing_centre="GSC",
                sequencing_instrument=incorrect_lane['sequencing_instrument'],
                sequencing_library_id=incorrect_lane['sequencing_library_id'],
                read_type=incorrect_lane['read_type'],
            )
            pass

        else:
            for field in ('sequencing_centre', 'sequencing_instrument', 'sequencing_library_id'):
                if correct_lane[field] != incorrect_lane[field]:
                    logging.warning('updating {} from {} to {}'.format(
                        field, correct_lane[field], incorrect_lane[field]))
                    correct_lane = tantalus_api.update(
                        'sequencing_lane',
                        id=correct_lane['id'],
                        **dict(field=incorrect_lane[field])
                    )

        datasets = list(tantalus_api.list(
            'sequence_dataset',
            library__library_id=row['library_id'],
        ))

        for dataset in datasets:
            lane_pks = [l['id'] for l in dataset['sequence_lanes']]
            if incorrect_lane['id'] not in lane_pks:
                continue
            num_lanes = len(lane_pks)
            lane_pks.remove(incorrect_lane['id'])
            lane_pks.append(correct_lane['id'])
            assert num_lanes == len(lane_pks)
            tantalus_api.update(
                'sequence_dataset',
                id=dataset['id'],
                sequence_lanes=lane_pks,
            )

        tantalus_api.delete('sequencing_lane', id=incorrect_lane['id'])
