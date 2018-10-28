from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
from datetime import datetime
import logging
import os
import sys
import time
import pandas as pd
from utils.constants import LOGGING_FORMAT
from utils.filecopy import rsync_file
from utils.gsc import get_sequencing_instrument, GSCAPI
from utils.runtime_args import parse_runtime_args
from utils.tantalus import TantalusApi
from utils.utils import get_lanes_str

# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stdout, level=logging.INFO)


def convert_time(a):
    try:
        return datetime.strptime(a, "%Y-%m-%dT%H:%M:%S")
    except Exception:
        pass

    try:
        return datetime.strptime(a, "%Y-%m-%dT%H:%M:%S.%f")
    except Exception:
        pass

    raise RuntimeError("Unable to parse %s" % a)


def add_compression_suffix(path, compression):
    # GSC paths for non-lane SpEC-compressed BAM files. Differ from BAM
    # paths above only in that they have `.spec` attached on the end
    if compression == "spec":
        return path + ".spec"
    else:
        raise ValueError("unsupported compression {}".format(compression))


merge_bam_path_template = {
    "WGS": "{data_path}/{library_name}_{num_lanes}_lane{lane_pluralize}_dupsFlagged.bam",
    "EXOME": "{data_path}/{library_name}_{num_lanes}_lane{lane_pluralize}_dupsFlagged.bam",
}


def get_merge_bam_path(
    library_type, data_path, library_name, num_lanes, compression=None
):
    lane_pluralize = "s" if num_lanes > 1 else ""
    bam_path = merge_bam_path_template[library_type].format(
        data_path=data_path,
        library_name=library_name,
        num_lanes=num_lanes,
        lane_pluralize=lane_pluralize,
    )
    if compression is not None:
        bam_path = add_compression_suffix(bam_path, compression)
    return bam_path


lane_bam_path_templates = {
    "WGS": "{data_path}/{flowcell_id}_{lane_number}.bam",
    "RNASEQ": "{data_path}/{flowcell_id}_{lane_number}_withJunctionsOnGenome_dupsFlagged.bam",
}

multiplexed_lane_bam_path_templates = {
    "WGS": "{data_path}/{flowcell_id}_{lane_number}_{adapter_index_sequence}.bam",
    "RNASEQ": "{data_path}/{flowcell_id}_{lane_number}_{adapter_index_sequence}_withJunctionsOnGenome_dupsFlagged.bam",
}


def get_lane_bam_path(
    library_type,
    data_path,
    flowcell_id,
    lane_number,
    adapter_index_sequence=None,
    compression=None,
):
    if adapter_index_sequence is not None:
        bam_path = multiplexed_lane_bam_path_templates[library_type].format(
            data_path=data_path,
            flowcell_id=flowcell_id,
            lane_number=lane_number,
            adapter_index_sequence=adapter_index_sequence,
        )
    else:
        bam_path = lane_bam_path_templates[library_type].format(
            data_path=data_path, flowcell_id=flowcell_id, lane_number=lane_number
        )
    if compression is not None:
        bam_path = add_compression_suffix(bam_path, compression)
    return bam_path


protocol_id_map = {
    12: "WGS",
    73: "WGS",
    136: "WGS",
    140: "WGS",
    123: "WGS",
    179: "WGS",
    96: "EXOME",
    80: "RNASEQ",
    137: "RNASEQ",
}


solexa_run_type_map = {"Paired": "P"}


tantalus_bam_filename_template = os.path.join(
    "{sample_id}",
    "bam",
    "{library_type}",
    "{library_id}",
    "lanes_{lanes_str}",
    "{sample_id}_{library_id}_{lanes_str}.bam",
)


def get_tantalus_bam_filename(sample, library, lane_infos):
    lanes_str = get_lanes_str(lane_infos)

    bam_path = tantalus_bam_filename_template.format(
        sample_id=sample["sample_id"],
        library_type=library["library_type"],
        library_id=library["library_id"],
        lanes_str=lanes_str,
    )

    return bam_path


def add_gsc_wgs_bam_dataset(
    bam_path, storage, sample, library, lane_infos, is_spec=False
):
    # TODO(mwiens91): this isn't used anywhere
    library_name = library["library_id"]

    bai_path = bam_path + ".bai"

    tantalus_bam_filename = get_tantalus_bam_filename(sample, library, lane_infos)
    tantalus_bai_filename = tantalus_bam_filename + ".bai"

    tantalus_bam_path = os.path.join(
        storage["storage_directory"], tantalus_bam_filename
    )
    tantalus_bai_path = os.path.join(
        storage["storage_directory"], tantalus_bai_filename
    )

    json_list = []

    rsync_file(bam_path, tantalus_bam_path)
    rsync_file(bai_path, tantalus_bai_path)

    # Save BAM file info xor save BAM SpEC file info
    bam_file = dict(
        size=os.path.getsize(bam_path),
        created=pd.Timestamp(
            time.ctime(os.path.getmtime(bam_path)), tz="Canada/Pacific"
        ),
        file_type="BAM",
        compression="SPEC" if is_spec else "UNCOMPRESSED",
        filename=tantalus_bam_filename,
        sequencefileinfo={},
    )

    bam_instance = dict(
        storage={"name": storage["name"]}, file_resource=bam_file, model="FileInstance"
    )
    json_list.append(bam_instance)

    # BAI files are only found with uncompressed BAMs (and even then not
    # always)
    if not is_spec and os.path.exists(bai_path):
        bai_file = dict(
            size=os.path.getsize(bai_path),
            created=pd.Timestamp(
                time.ctime(os.path.getmtime(bai_path)), tz="Canada/Pacific"
            ),
            file_type="BAI",
            compression="UNCOMPRESSED",
            filename=tantalus_bai_filename,
            sequencefileinfo={},
        )

        bai_instance = dict(
            storage={"name": storage["name"]},
            file_resource=bai_file,
            model="FileInstance",
        )
        json_list.append(bai_instance)

    else:
        bai_file = None

    dataset_name = "BAM-{}-{}-{} (lanes {})".format(
        sample["sample_id"],
        library["library_type"],
        library["library_id"],
        get_lanes_str(lane_infos),
    )

    # If the bam file is compressed, store the file under the BamFile's
    # bam_spec_file column. Otherwise, use the bam_file column.
    bam_dataset = dict(
        name=dataset_name,
        dataset_type="BAM",
        sample=sample,
        library=library,
        sequence_lanes=[],
        file_resources=[bam_file, bai_file],
        model="SequenceDataset",
    )

    json_list.append(bam_dataset)

    reference_genomes = set()
    aligners = set()

    for lane_info in lane_infos:
        lane = dict(
            flowcell_id=lane_info["flowcell_id"],
            lane_number=lane_info["lane_number"],
            sequencing_centre="GSC",
            sequencing_instrument=lane_info["sequencing_instrument"],
            read_type=lane_info["read_type"],
            dna_library=library,
        )
        bam_dataset["sequence_lanes"].append(lane)

        reference_genomes.add(lane_info["reference_genome"])
        aligners.add(lane_info["aligner"])

    if len(reference_genomes) > 1:
        bam_dataset["reference_genome"] = "UNUSABLE"
    elif len(reference_genomes) == 1:
        bam_dataset["reference_genome"] = list(reference_genomes)[0]
        bam_dataset["aligner"] = ", ".join(aligners)

    return json_list


def add_gsc_bam_lanes(sample, library, lane_infos):
    # TODO(mwiens91): sample arg isn't used anywhere
    json_list = []

    for lane_info in lane_infos:
        lane = dict(
            flowcell_id=lane_info["flowcell_id"],
            lane_number=lane_info["lane_number"],
            sequencing_centre="GSC",
            sequencing_instrument=lane_info["sequencing_instrument"],
            read_type=lane_info["read_type"],
            dna_library=library,
            model="SequenceLane",
        )

        json_list.append(lane)

    return json_list


def import_gsc_library(
    libraries,
    storage,
    tantalus_api,
    skip_file_import=False,
    skip_older_than=None,
    tag_name=None,
):
    """
    Copy GSC libraries to a storage and return metadata json.
    """
    # TODO(mwiens91): tantalus_api and tag_name not used

    json_list = []

    gsc_api = GSCAPI()

    for library_name in libraries:
        library_infos = gsc_api.query("library?name={}".format(library_name))

        logging.info("importing %s", library_name)

        for library_info in library_infos:
            protocol_info = gsc_api.query(
                "protocol/{}".format(library_info["protocol_id"])
            )

            if library_info["protocol_id"] not in protocol_id_map:
                logging.warning(
                    "warning, protocol %s:%s not supported",
                    library_info["protocol_id"],
                    protocol_info["extended_name"],
                )
                continue

            library_type = protocol_id_map[library_info["protocol_id"]]

            logging.info("found %s", library_type)

            sample_id = library_info["external_identifier"]

            sample = dict(sample_id=sample_id)

            library_name = library_info["name"]

            library = dict(
                library_id=library_name, library_type=library_type, index_format="N"
            )

            merge_infos = gsc_api.query("merge?library={}".format(library_name))

            # Keep track of lanes that are in merged BAMs so that we
            # can exclude them from the lane specific BAMs we add to
            # the database
            merged_lanes = set()

            for merge_info in merge_infos:
                data_path = merge_info["data_path"]
                num_lanes = len(merge_info["merge_xrefs"])

                if merge_info["complete"] is None:
                    logging.info("skipping merge with no completed date")
                    continue

                completed_date = convert_time(merge_info["complete"])

                logging.info("merge completed on %s", completed_date)

                if skip_older_than is not None and completed_date < skip_older_than:
                    logging.info("skipping old merge")
                    continue

                lane_infos = []

                for merge_xref in merge_info["merge_xrefs"]:
                    libcore_id = merge_xref["object_id"]

                    libcore = gsc_api.query(
                        "aligned_libcore/{}/info".format(libcore_id)
                    )
                    flowcell_id = libcore["libcore"]["run"]["flowcell_id"]
                    lane_number = libcore["libcore"]["run"]["lane_number"]
                    sequencing_instrument = get_sequencing_instrument(
                        libcore["libcore"]["run"]["machine"]
                    )
                    solexa_run_type = libcore["libcore"]["run"]["solexarun_type"]
                    reference_genome = libcore["lims_genome_reference"]["path"]
                    aligner = libcore["analysis_software"]["name"]
                    flowcell_info = gsc_api.query("flowcell/{}".format(flowcell_id))
                    flowcell_id = flowcell_info["lims_flowcell_code"]
                    adapter_index_sequence = libcore["libcore"]["primer"][
                        "adapter_index_sequence"
                    ]

                    merged_lanes.add((flowcell_id, lane_number, adapter_index_sequence))

                    lane_info = dict(
                        flowcell_id=flowcell_id,
                        lane_number=lane_number,
                        adapter_index_sequence=adapter_index_sequence,
                        sequencing_instrument=sequencing_instrument,
                        read_type=solexa_run_type_map[solexa_run_type],
                        reference_genome=reference_genome,
                        aligner=aligner,
                    )

                    lane_infos.append(lane_info)

                if skip_file_import:
                    json_list += add_gsc_bam_lanes(sample, library, lane_infos)

                else:
                    if data_path is None:
                        raise Exception(
                            "no data path for merge info {}".format(merge_info["id"])
                        )

                    bam_path = get_merge_bam_path(
                        library_type=library_type,
                        data_path=data_path,
                        library_name=library_name,
                        num_lanes=num_lanes,
                    )

                    bam_spec_path = get_merge_bam_path(
                        library_type=library_type,
                        data_path=data_path,
                        library_name=library_name,
                        num_lanes=num_lanes,
                        compression="spec",
                    )

                    # Test for BAM path first, then BAM SpEC path if
                    # no BAM available
                    if os.path.exists(bam_path):
                        json_list += add_gsc_wgs_bam_dataset(
                            bam_path, storage, sample, library, lane_infos
                        )
                    elif os.path.exists(bam_spec_path):
                        json_list += add_gsc_wgs_bam_dataset(
                            bam_spec_path,
                            storage,
                            sample,
                            library,
                            lane_infos,
                            is_spec=True,
                        )
                    else:
                        raise Exception("missing merged bam file {}".format(bam_path))

            libcores = gsc_api.query(
                "aligned_libcore/info?library={}".format(library_name)
            )

            for libcore in libcores:
                created_date = convert_time(libcore["created"])

                logging.info(
                    "libcore {} created {}".format(libcore["id"], created_date)
                )

                if skip_older_than is not None and created_date < skip_older_than:
                    logging.info("skipping old lane")
                    continue

                lims_run_validation = libcore["libcore"]["run"]["lims_run_validation"]
                if lims_run_validation == "Rejected":
                    logging.info("skipping rejected lane")
                    continue

                flowcell_id = libcore["libcore"]["run"]["flowcell_id"]
                lane_number = libcore["libcore"]["run"]["lane_number"]
                sequencing_instrument = get_sequencing_instrument(
                    libcore["libcore"]["run"]["machine"]
                )
                solexa_run_type = libcore["libcore"]["run"]["solexarun_type"]
                reference_genome = libcore["lims_genome_reference"]["path"]
                aligner = libcore["analysis_software"]["name"]
                adapter_index_sequence = libcore["libcore"]["primer"][
                    "adapter_index_sequence"
                ]
                data_path = libcore["data_path"]

                if not skip_file_import and data_path is None:
                    logging.error("data path is None")

                flowcell_info = gsc_api.query("flowcell/{}".format(flowcell_id))
                flowcell_id = flowcell_info["lims_flowcell_code"]

                # Skip lanes that are part of merged BAMs
                if (flowcell_id, lane_number, adapter_index_sequence) in merged_lanes:
                    continue

                lane_infos = [
                    dict(
                        flowcell_id=flowcell_id,
                        lane_number=lane_number,
                        adapter_index_sequence=adapter_index_sequence,
                        sequencing_instrument=sequencing_instrument,
                        read_type=solexa_run_type_map[solexa_run_type],
                        reference_genome=reference_genome,
                        aligner=aligner,
                    )
                ]

                if skip_file_import:
                    json_list += add_gsc_bam_lanes(sample, library, lane_infos)

                else:
                    bam_path = get_lane_bam_path(
                        library_type=library_type,
                        data_path=data_path,
                        flowcell_id=flowcell_id,
                        lane_number=lane_number,
                        adapter_index_sequence=adapter_index_sequence,
                    )

                    bam_spec_path = get_lane_bam_path(
                        library_type=library_type,
                        data_path=data_path,
                        flowcell_id=flowcell_id,
                        lane_number=lane_number,
                        adapter_index_sequence=adapter_index_sequence,
                        compression="spec",
                    )

                    # Test for BAM path first, then BAM SpEC path if
                    # no BAM available
                    if os.path.exists(bam_path):
                        json_list += add_gsc_wgs_bam_dataset(
                            bam_path, storage, sample, library, lane_infos
                        )
                    elif os.path.exists(bam_spec_path):
                        json_list += add_gsc_wgs_bam_dataset(
                            bam_spec_path,
                            storage,
                            sample,
                            library,
                            lane_infos,
                            is_spec=True,
                        )
                    else:
                        raise Exception("missing lane bam file {}".format(bam_path))

    return json_list


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Not a valid date: '{0}'.".format(s))


if __name__ == "__main__":
    # Parse the incoming arguments
    args = parse_runtime_args()

    # Convert the date to the format we want
    if "skip_older_than" in args:
        args["skip_older_than"] = valid_date(args["skip_older_than"])

    # Connect to the Tantalus API (this requires appropriate environment
    # variables defined)
    tantalus_api = TantalusApi()

    storage = tantalus_api.get("storage_server", name=args["storage_name"])

    # Query the GSC many times
    json_to_post = import_gsc_library(
        args["libraries"],
        storage,
        tantalus_api,
        skip_file_import=args.get("skip_file_import"),
        skip_older_than=args.get("skip_older_than"),
    )

    # Get the tag name if it was passed in
    try:
        tag_name = args["tag_name"]
    except KeyError:
        tag_name = None

    # Post data to Tantalus
    tantalus_api.sequence_dataset_add(
        model_dictionaries=json_to_post, tag_name=tag_name
    )
