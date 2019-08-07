from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import os
import sys
import time
import click
import socket
import logging
import subprocess
from datetime import datetime

from datamanagement.utils.gsc import get_sequencing_instrument, GSCAPI
from dbclients.tantalus import TantalusApi
from datamanagement.utils.utils import (
        get_lanes_hash, make_dirs, 
        convert_time, 
        valid_date, 
        add_compression_suffix,
        connect_to_client
    )
from datamanagement.spec_to_bam import create_bam
from datamanagement.cram_to_bam import create_bam as HelperCram
from datamanagement.bam_import import import_bam

from datamanagement.templates import (
        WGS_BAM_NAME_TEMPLATE, 
        MERGE_BAM_PATH_TEMPLATE, 
        LANE_BAM_PATH_TEMPLATE, 
        MULTIPLEXED_LANE_BAM_PATH_TEMPLATE,
)
from datamanagement.utils.constants import (
        LOGGING_FORMAT,
        SOLEXA_RUN_TYPE_MAP,
        PROTOCOL_ID_MAP
)

logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
gsc_api = GSCAPI()


def rsync_file(from_path, to_path, sftp=None, remote_host=None):
    """
    Rsyncs file and performs checks to ensure rsync was successful

    Args:
        from_path:      (string) source path of the file
        to_path:        (string) destination path of the file
        sftp:           (sftp object) sftp client if rsync is performed remotely
        remote_host:    (string) name of the remote host if rsync is remote
    """
    # Prepend remote host shortcut to the source path
    if remote_host:
        transfer_from_path = remote_host + from_path
    else:
        transfer_from_path = from_path

    # Create the rsync command
    subprocess_cmd = [
        "rsync",
        "-avPL",
        "--chmod=D555",
        "--chmod=F444",
        transfer_from_path,
        to_path,
    ]

    # Copy the file if it doesn't exist
    if not os.path.isfile(to_path):
        make_dirs(os.path.dirname(to_path))
        logging.info("Copying file from {} to {}".format(from_path, to_path))
        subprocess.check_call(subprocess_cmd)
    
    # If the file exists and we are using sftp, check size
    elif os.path.isfile(to_path) and sftp:
        remote_file = sftp.stat(from_path)
        if remote_file.st_size != os.path.getsize(to_path):
            logging.info("The size of {} on the GSC does not match {} -- copying new file".format(
                from_path, 
                to_path,
            ))
            subprocess.check_call(subprocess_cmd)
        else:
            logging.info("The file already exists at {} -- skipping import".format(to_path))
    
    # If the file exists and we are not using sftp, check size
    elif os.path.isfile(to_path) and not sftp:
        if os.path.getsize(from_path) != os.path.getsize(to_path):
            logging.info("The size of {} on the GSC does not match {} -- copying new file".format(
                from_path,
                to_path,
            ))
            subprocess.check_call(subprocess_cmd)
        else:
            logging.info("The file already exists at {} -- skipping import".format(to_path))

    # Check the rsync was successful
    if sftp:
        try:
            remote_file = sftp.stat(from_path)
            if remote_file.st_size != os.path.getsize(to_path):
                raise Exception("copy failed for {} to {}".format(from_path, to_path))
        except IOError:
            raise Exception("missing source file {}".format(from_path))
    else:
        if os.path.getsize(to_path) != os.path.getsize(from_path):
            raise Exception("copy failed for {} to {}".format(from_path, to_path))


def get_merge_bam_path(library_type, data_path, library_name, num_lanes, compression=None):
    """
    Constructs the filename for the source bam based on the bam metadata and joins with 
    the source datapath to create full path to source bam

    Args:
        library_type:   (string) WGS or EXOME
        data_path:      (string) path to original bam 
        library_name:   (string) internal library ID
        num_lanes:      (string) number of lanes included in bam
        compression:    (String) if bam is spec or uncompressed

    Returns:
        bam_path:   (string) full path to source merge bam
    """
    lane_pluralize = "s" if num_lanes > 1 else ""
    bam_path = MERGE_BAM_PATH_TEMPLATE[library_type].format(
        data_path=data_path,
        library_name=library_name,
        num_lanes=num_lanes,
        lane_pluralize=lane_pluralize,
    )
    if compression is not None:
        bam_path = add_compression_suffix(bam_path, compression)
    return bam_path


def get_lane_bam_path(
        library_type, 
        data_path, 
        flowcell_id, 
        lane_number, 
        adapter_index_sequence=None, 
        compression=None
):
    """
    Constructs the filename for the source bam based on the bam metadata and joins with 
    the source datapath to create full path to source bam

    Args:
        library_type:           (string) WGS or RNASEQ
        data_path:              (string) path to original bam 
        flowcell_id:            (string) flowcell ID for the lane
        lane_number:            (string) number of lanes included in bam
        adapter_index_sequence: (string) index sequence from GSC
        compression:            (string) if bam is spec or uncompressed

    Returns:
        bam_path:   (string) full path to source lane bam
    '"""
    if adapter_index_sequence is not None:
        bam_path = MULTIPLEXED_LANE_BAM_PATH_TEMPLATE[library_type].format(
            data_path=data_path,
            flowcell_id=flowcell_id,
            lane_number=lane_number,
            adapter_index_sequence=adapter_index_sequence,
        )
    else:
        bam_path = LANE_BAM_PATH_TEMPLATE[library_type].format(
            data_path=data_path, flowcell_id=flowcell_id, lane_number=lane_number
        )
    if compression is not None:
        bam_path = add_compression_suffix(bam_path, compression)
    return bam_path


def get_tantalus_bam_filename(sample, library, lane_infos):
    """
    Creates filename for bam that matches current naming conventions
    in Tantalus

    Args:
        sample:     (dict) the sample associated with the bam
        library:    (dict) the library associated with the bam
        lane_infos: (list of dictionaries) contains lane info 
                    associated with the bam
    Returns:
        bam_path:   (string) the filename for the bam following 
                    naming conventions
    """
    lanes_str = get_lanes_hash(lane_infos)
    bam_path = WGS_BAM_NAME_TEMPLATE.format(
        sample_id=sample["sample_id"],
        library_type=library["library_type"],
        library_id=library["library_id"],
        lanes_str=lanes_str,
    )

    return bam_path


def transfer_gsc_bams(bam_detail, bam_paths, storage, sftp=None):
    """
    Transfers the bams and bais from GSC to destination storage. If the source
    bam is a spec, performs spec to bam conversion and creates a bam index
    on the destination storage 

    Args:
        bam_detail: (dict) contains metadata for the bam
        bam_paths:  (dict) contains  the source paths, and 
                    destination paths for the bam
        storage:    (string) the name of destination storage to which the 
                    bam and bai will be transferred
        sftp:       (sftp object) the sftp client connected to remote host 
                    if the script is not run on thost
    """
    if sftp:
        remote_host = "thost:"
    else:
        remote_host = None

    # If the input is a spec, run spec2bam
    if bam_paths["source_bam_path"].endswith(".spec"):
        create_bam(
            spec_path=bam_paths["source_bam_path"],
            raw_reference_genome=bam_detail["reference_genome"],
            output_bam_path=bam_paths["tantalus_bam_path"],
            to_storage=storage,
            library_id=bam_detail["library"]["library_id"],
            sftp_client=sftp
        )
    elif bam_paths["source_bam_path"].endswith(".cram"):
        HelperCram.create_bam(
            cram_path=bam_paths["source_bam_path"],
            raw_reference_genome=bam_detail["reference_genome"],
            output_bam_path=bam_paths["tantalus_bam_path"],
            to_storage=storage,
            library_id=bam_detail["library"]["library_id"],
            sftp_client=sftp
        )
    # Otherwise, rsync to destination server
    else:
        # Transfer the bam
        rsync_file(
            from_path=bam_paths["source_bam_path"],
            to_path=bam_paths["tantalus_bam_path"],
            sftp=sftp,
            remote_host=remote_host
        )
        # Transfer the bam index if it exists
        if bam_paths["source_bai_path"]:
            # Transfer the bai
            rsync_file(
                from_path=bam_paths["source_bai_path"],
                to_path=bam_paths["tantalus_bai_path"],
                sftp=sftp,
                remote_host=remote_host
            )
        # Create a new bai if the source bai does not exist
        else:
            logging.info("Creating bam index at {}".format(bam_paths["tantalus_bai_path"]))
            cmd = [ 'samtools',
                'index',
                bam_paths["tantalus_bam_path"],
            ]
            subprocess.check_call(cmd)

            logging.info("Successfully created bam index at {}".format(bam_paths["tantalus_bai_path"]))


def check_sftp_bam(bam_path, spec_path, sftp=None):
    """
    Checks if the bam exists over SFTP. If not, checks for a spec
    If neither exists, returns None

    Args:
        bam_path:   (string) filepath to where the source bam should be
        spec_path:  (string) filepath to where the source spec should be
        sftp:       (sftp object) sftp client connected to the remote host

    Returns:
        returns the filepath to either the source bam or the source spec
    '"""
    try:
        sftp.stat(bam_path)
        return bam_path
    except IOError:
        logging.error("The bam does not exist at {} -- checking for spec instead".format(bam_path))
        
    try:
        sftp.stat(spec_path)
        return spec_path
    except IOError:
        logging.error("The spec does not exist at {} -- skipping import".format(spec_path))
        return None


def rename_bam_paths(bam_detail, storage, sftp=None):
    """
    Creates full path for the source bam, source bai, destination bam, 
    and destination bai

    Args:
        bam_detail: (dict) metadata required to create the bam name and path
        storage:    (dict) the destination storage for the bam and bai
        sftp:       (sftp object) sftp client connected to the remote server
    Returns:
        bam_paths:  (dict) contains the source and destination paths
    """
    # Determine whether the path is to bam or spec
    if bam_detail["data_path"].endswith(".spec"):
        compression = "spec"
    else:
        compression = None

    
    # Get merge bam path
    if bam_detail["info_type"] == "merge":
        bam_path = get_merge_bam_path(
                library_type=bam_detail["library"]["library_type"],
                data_path=bam_detail["data_path"],
                library_name=bam_detail["library"]["library_id"],
                num_lanes=bam_detail["num_lanes"],
        )
        spec_path = get_merge_bam_path(
                library_type=bam_detail["library"]["library_type"],
                data_path=bam_detail["data_path"],
                library_name=bam_detail["library"]["library_id"],
                num_lanes=bam_detail["num_lanes"],
                compression="spec"
        )

    # Get libcore bam path
    elif bam_detail["info_type"] == "libcore":
        bam_path = get_lane_bam_path(
                library_type=bam_detail["library"]["library_type"],
                data_path=bam_detail["data_path"],
                flowcell_id=bam_detail["lane_info"][0]["flowcell_id"],
                lane_number=bam_detail["lane_info"][0]["lane_number"],
                adapter_index_sequence=bam_detail["lane_info"][0]["adapter_index_sequence"]
        )
        spec_path = get_lane_bam_path(
                library_type=bam_detail["library"]["library_type"],
                data_path=bam_detail["data_path"],
                flowcell_id=bam_detail["lane_info"][0]["flowcell_id"],
                lane_number=bam_detail["lane_info"][0]["lane_number"],
                adapter_index_sequence=bam_detail["lane_info"][0]["adapter_index_sequence"],
                compression="spec"
        )

    # Create bai path 
    source_bai_path = bam_path + ".bai"

    # Check the source bam exists, and then transfer
    if sftp:
        source_bam_path = check_sftp_bam(bam_path, spec_path, sftp)
    else:
        if not os.path.exists(bam_path):
            logging.error("The bam does not exist at {} -- checking for spec instead".format(bam_path))
            if os.path.exists(spec_path):
                source_bam_path = spec_path
            else:
                source_bam_path = None

    # Check if the source bai exists, if not then reindex
    if sftp:
        try:
            sftp.stat(source_bai_path)
        except IOError:
            source_bai_path = None
    else:
        if not os.path.exists(source_bai_path):
            source_bai_path = None

    logging.info("Bam file exists at {}".format(source_bam_path))

    # Create the destination bam name and path
    tantalus_bam_filename = get_tantalus_bam_filename(
            bam_detail["sample"], 
            bam_detail["library"], 
            bam_detail["lane_info"]
    )
    tantalus_bai_filename = tantalus_bam_filename + ".bai"
    
    tantalus_bam_filepath = os.path.join(
            storage["prefix"], 
            tantalus_bam_filename
    )
    tantalus_bai_filepath = os.path.join(
            storage["prefix"],
            tantalus_bai_filename
    )

    # Store all paths in a dictionary to return
    bam_paths = {
        'source_bam_path': source_bam_path,
        'source_bai_path': source_bai_path,
        'tantalus_bam_name': tantalus_bam_filename,
        'tantalus_bam_path': tantalus_bam_filepath,
        'tantalus_bai_name': tantalus_bai_filename,
        'tantalus_bai_path': tantalus_bai_filepath,
    }

    return bam_paths


def get_merge_info(details_list, gsc_api, library, sample, skip_older_than):
    """
    Collects merge metadata for the given library

    Args:
        details_list:       (list) list of dictionaries containing metadata
                            about each run
        gsc_api:            (string)
        library:            (dict) contains library_id, library_type, index_format
        sample:             (dict) contains sample_id
        skip_older_than:    (string) skip bams older than this date
    
    Returns:
        merged_lanes:   (set) 
    """
    merge_infos = gsc_api.query("merge?library={}".format(library["library_id"]))
    merged_lanes = set()
    for merge_info in merge_infos:
        data_path = merge_info["data_path"]
        num_lanes = len(merge_info["merge_xrefs"])
        reference_genome = merge_info["lims_genome_reference"]["path"]
        aligner = merge_info["aligner_software"]["name"]
        
        # Check if data exists
        if data_path is None:
            logging.error("Merge data path is None")
            continue
        # Skip incomplete merges
        if merge_info["complete"] is None:
            logging.info("skipping merge with no completed date")
            continue
        completed_date = convert_time(merge_info["complete"])
        # Skip older merges 
        if skip_older_than is not None and completed_date < skip_older_than:
            logging.info("skipping old merge")
            continue
        lane_infos = []
        for merge_xref in merge_info["merge_xrefs"]:
            if merge_xref["object_type"] == "metadata.aligned_libcore":
                aligned_libcore = gsc_api.query("aligned_libcore/{}/info".format(merge_xref["object_id"]))
                libcore = aligned_libcore["libcore"]
                run = libcore["run"]
                primer = libcore["primer"]
            elif merge_xref["object_type"] == "metadata.run":
                run = gsc_api.query("run/{}".format(merge_xref["object_id"]))
                libcores = gsc_api.query("libcore?run_id={}".format(merge_xref["object_id"]))
                assert len(libcores) == 1
                libcore = libcores[0]
                primer = gsc_api.query("primer/{}".format(libcore["primer_id"]))
            else:
                raise Exception('unknown object type {}'.format(merge_xref["object_type"]))
            flowcell_info = gsc_api.query("flowcell/{}".format(run["flowcell_id"]))
            flowcell_id = flowcell_info["lims_flowcell_code"]
            lane_number = run["lane_number"]
            sequencing_instrument = get_sequencing_instrument(run["machine"])
            solexa_run_type = run["solexarun_type"]
            read_type = SOLEXA_RUN_TYPE_MAP[solexa_run_type]
            adapter_index_sequence = primer["adapter_index_sequence"]
            library_id = merge_xref["library_id"]
            lane_library_infos = gsc_api.query("library?id={}".format(library_id))
            lane_library_name = lane_library_infos[0]["name"]
            merged_lanes.add((flowcell_id, lane_number, adapter_index_sequence))
            lane_info = dict(
                flowcell_id=flowcell_id,
                lane_number=lane_number,
                library_id=lane_library_name,
                adapter_index_sequence=adapter_index_sequence,
                sequencing_centre="GSC",
                sequencing_instrument=sequencing_instrument,
                read_type=read_type,
                reference_genome=reference_genome,
                aligner=aligner,
            )
            lane_infos.append(lane_info)
        list_temp = dict(
            library=library,
            sample=sample,
            data_path=data_path,
            lane_info=lane_infos,
            num_lanes=num_lanes,
            read_type=read_type,
            sequencing_instrument=sequencing_instrument,
            reference_genome=reference_genome,
            aligner=aligner,
            info_type="merge",
        )
        details_list.append(list_temp)
    
    return merged_lanes


def get_libcore_info(details_list, libcores, library, sample, skip_older_than, merged_lanes):
    """
    Collects libcore metadata for the given library

    Args:
        details_list:       (list) list of dictionaries containing metadata
                            about each run
        libcores:           (list)
        library:            (dict) contains library_id, library_type, index_format
        sample:             (dict) contains sample_id
        skip_older_than:    (string) skip bams older than this date
        merged_lanes:       (set) list of merged lanes, skip libcore if it is
                            in this set
    """
    for libcore in libcores:
        data_path = libcore["data_path"]
        created_date = convert_time(libcore["created"])
        logging.info(
            "libcore {} created {}".format(libcore["id"], created_date)
        )
        # Check if the data is too old
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
        read_type = SOLEXA_RUN_TYPE_MAP[solexa_run_type]
        reference_genome = libcore["lims_genome_reference"]["path"]
        aligner = libcore["analysis_software"]["name"]
        adapter_index_sequence = libcore["libcore"]["primer"][
            "adapter_index_sequence"
        ]
        data_path = libcore["data_path"]
        # Check the datapath
        if data_path is None:
            logging.error("Libcore data path is None")
            continue
        flowcell_info = gsc_api.query("flowcell/{}".format(flowcell_id))
        flowcell_id = flowcell_info["lims_flowcell_code"]
        lane_library_name = libcore["libcore"]["library"]["name"]
        # Skip lanes that are part of merged BAMs
        if (flowcell_id, lane_number, adapter_index_sequence) in merged_lanes:
            continue
        lane_infos = [
            dict(
                flowcell_id=flowcell_id,
                lane_number=lane_number,
                library_id=lane_library_name,
                adapter_index_sequence=adapter_index_sequence,
                sequencing_centre="GSC",
                sequencing_instrument=sequencing_instrument,
                read_type=read_type,
                reference_genome=reference_genome,
                aligner=aligner,
            )
        ]
        list_temp = dict(
            library=library,
            sample=sample,
            data_path=data_path,
            lane_info=lane_infos,
            read_type=read_type,
            sequencing_instrument=sequencing_instrument,
            reference_genome=reference_genome,
            aligner=aligner,
            info_type="libcore",
        )
        details_list.append(list_temp)


def query_gsc(identifier, id_type):
    """
    Queries the GSC to see if the given library or sample exists in their system

    Args:
        identifier: (string) internal ID of the library/sample to query for
        id_type:    (string) specifies whether identifier represents a library or sample

    Returns:
        library_infos:  (dict) contains GSC library info for the provided identifier
    """
    logging.info("Querying GSC for {} {}".format(id_type, identifier))

    # Check for spaces in ID
    if ' ' in identifier:
        raise ValueError('space in id "{}"'.format(identifier))

    # Query GSC for library info 
    if id_type == 'library':
        library_infos = gsc_api.query("library?name={}".format(identifier))
    elif id_type == 'sample':
        library_infos = gsc_api.query('library?external_identifier={}'.format(identifier))

    return library_infos


def get_gsc_details(library_infos, skip_older_than):
    """
    Extract the required metadata from GSC API

    Args:
        library_infos:      (dict) response containing library info 
                            from the GSC API query
        skip_older_than:    (date) skip bams older than this date

    Returns:
        details_list:   (list of dictionaries) contains sample, library,
                        and sequencing metadata for each bam found in the GSC
    """
    details_list = []

    # Get the details for each library info returned by the query
    for library_info in library_infos:
        logging.info("Collecting data for library {}".format(library_info["name"]))
        protocol_info = gsc_api.query(
            "protocol/{}".format(library_info["protocol_id"])
        )

        # Skip the fetching details if the protocol ID is not supported
        if library_info["protocol_id"] not in PROTOCOL_ID_MAP:
            logging.warning(
                "warning, protocol {}}:{} not supported".format(
                library_info["protocol_id"],
                protocol_info["extended_name"]
            ))
            continue

        sample_id = library_info["external_identifier"]

        # Check for spaces in the sample ID
        if ' ' in sample_id:
            raise ValueError('space in sample_id "{}"'.format(sample_id))

        sample = dict(sample_id=sample_id)

        # Collect library details
        library_type = PROTOCOL_ID_MAP[library_info["protocol_id"]]
        logging.info("found {}".format(library_type))
        library_name = library_info["name"]
        library = dict(
            library_id=library_name, 
            library_type=library_type, 
            index_format="N"
        )

        # Collect merge info
        merged_lanes = get_merge_info(details_list, gsc_api, library, sample, skip_older_than)

        # Collect libcore info
        libcores = gsc_api.query(
            "aligned_libcore/info?library={}".format(library_name)
        )
        get_libcore_info(details_list, libcores, library, sample, skip_older_than, merged_lanes)

    return details_list


@click.command()
@click.argument('ids', nargs=-1)
@click.argument('storage', nargs=1)
@click.option('--id_type', type=click.Choice(['sample', 'library']))
@click.option('--skip_older_than')
@click.option('--tag_name')
@click.option('--update', is_flag=True)
@click.option('--skip_file_import', is_flag=True)
@click.option('--query_only', is_flag=True)
def main(**kwargs):
    """
    Queries the GSC for WGS bams. Transfers bams to specified storage if 
    necessary and uploads metadata to tantalus

    Args:
        ids:                (string) a list of internal IDs to query the GSC for 
        storage:            (string) destination storage to transfer bams to
        id_type:            (string) type of ID specified (either sample or library) 
        skip_older_than:    (string) skip bams older than this date
        tag_name:           (string) tag name to associate the resulting sequence datasets
                            with when importing into tantalus
        update:             (flag) specifies whether metadata in tantalus is
                            to be updated or not
        skip_file_import:   (flag) import only new lanes into tantalus
        query_only:         (flag) only query for the bam paths on the GSC 
    """
    # Check if this script is being run on thost
    # If not, connect to an ssh client to access /projects/files
    if socket.gethostname() != "txshah":
        ssh_client = connect_to_client("10.9.208.161")
        sftp = ssh_client.open_sftp()
    else:
        sftp = None

    # Connect to the Tantalus API
    tantalus_api = TantalusApi()
    storage = tantalus_api.get_storage(kwargs["storage"])

    # Convert the date to the format we want
    if kwargs["skip_older_than"]:
        skip_older_than = valid_date(kwargs["skip_older_than"])

    # Check that an ID type was specified
    if not kwargs["id_type"]:
        raise Exception("Please specify an ID type (sample or library")

    details = []
    for identifier in kwargs["ids"]:
        # Query the GSC to see if the ID exists
        infos = query_gsc(identifier, kwargs["id_type"])

        if not infos:
            logging.info("No results for {} {}. Skipping import".format(kwargs["id_type"], identifier))
        else:
            logging.info("{} {} exists on the GSC".format(kwargs["id_type"], identifier))

        # Get the data from GSC
        details = get_gsc_details(
                infos, 
                skip_older_than=kwargs["skip_older_than"],
        )

        # Import and transfer each file
        for detail in details:
            # Rename the bams according to internal templates
            bam_paths = rename_bam_paths(detail, storage, sftp)

            # If the bam path does not exist at the source, skip
            # the transfer and import
            if not bam_paths["source_bam_path"]:
                break

            # Skip import if we only wanted to query for paths
            if kwargs["query_only"]:
                continue

            if not kwargs["skip_file_import"]:
                # Transfer the bam to the specified storage
                transfer_gsc_bams(detail, bam_paths, storage, sftp)

                # Add the files to Tantalus
                logging.info("Importing {} to Tantalus".format(bam_paths["tantalus_bam_path"]))

                dataset = import_bam(
                    storage_name=storage["name"],
                    library=detail["library"],
                    bam_file_path=bam_paths["tantalus_bam_path"],
                    read_type=detail["read_type"],
                    lane_infos=detail["lane_info"],
                    tag_name=kwargs["tag_name"],
                    update=kwargs["update"]
                )

                logging.info("Successfully added sequence dataset with ID {}".format(dataset["id"]))
            else:
                #Only add lanes, libraries, and samples to tantalus
                for lane in detail["lane_info"]:
                    logging.info("Importing lanes for library {} to tantalus".format(detail["library"]["library_id"]))
                    
                    library_pk = tantalus_api.get_or_create(
                            "dna_library",
                            library_id=detail["library"]["library_id"],
                            library_type=detail["library"]["library_type"],
                            index_format=detail["library"]["index_format"]
                    )["id"]
                    
                    lane = tantalus_api.get_or_create(
                        "sequencing_lane",
                        flowcell_id=lane["flowcell_id"],
                        dna_library=library_pk,
                        read_type=lane["read_type"],
                        lane_number=str(lane["lane_number"]),
                        sequencing_centre="GSC",
                        sequencing_instrument=lane["sequencing_instrument"]
                    )
                    logging.info("Successfully created lane {} in tantalus".format(lane["id"]))



if __name__=='__main__':
    main()