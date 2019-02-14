from __future__ import absolute_import
from __future__ import division
from __future__ import print_function

import logging
import os
import sys
import time
import subprocess
import click
import paramiko
import pwd
import socket
import pandas as pd
from datetime import datetime
from utils.constants import LOGGING_FORMAT
from utils.gsc import get_sequencing_instrument, GSCAPI
from utils.runtime_args import parse_runtime_args
from dbclients.tantalus import TantalusApi
from datamanagement.utils.utils import get_lanes_hash, make_dirs
import datamanagement.templates as templates
from spec_to_bam import create_bam
from bam_import import import_bam
from templates import  (WGS_BAM_NAME_TEMPLATE, 
                        MERGE_BAM_PATH_TEMPLATE, 
                        LANE_BAM_PATH_TEMPLATE, 
                        MULTIPLEXED_LANE_BAM_PATH_TEMPLATE,
                        )

# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

gsc_api = GSCAPI()

protocol_id_map = {
    12: "WGS",
    23: "WGS",
    73: "WGS",
    136: "WGS",
    140: "WGS",
    123: "WGS",
    179: "WGS",
    96: "EXOME",
    80: "RNASEQ",
    137: "RNASEQ",
}

solexa_run_type_map = {
    "Paired": "P",
    "Single": "S",
}

def rsync_file(from_path, to_path, sftp=None):
    make_dirs(os.path.dirname(to_path))

    subprocess_cmd = [
        "rsync",
        "-avPL",
        "--chmod=D555",
        "--chmod=F444",
        from_path,
        to_path,
    ]

    subprocess.check_call(subprocess_cmd)

    if sftp:
        try:
            remote_file = ftp.stat(from_path)
            if remote_file.st_size != os.path.getsize(to_path):
                raise Exception("copy failed for %s to %s", from_path, to_path)
        except IOError:
            raise Exception("missing source file %s", from_path)
    else:
        if os.path.getsize(to_path) != os.path.getsize(from_path):
            raise Exception("copy failed for %s to %s", from_path, to_path)


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


def connect_to_client(hostname):
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()

    username = pwd.getpwuid(os.getuid()).pw_name
    ssh_client.connect(hostname, username=username)

    return ssh_client


def get_merge_bam_path(
    library_type, 
    data_path,
    library_name, 
    num_lanes, 
    compression=None
):
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
    compression=None,
):
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
    lanes_str = get_lanes_hash(lane_infos)
    bam_path = WGS_BAM_NAME_TEMPLATE.format(
        sample_id=sample["sample_id"],
        library_type=library["library_type"],
        library_id=library["library_id"],
        lanes_str=lanes_str,
    )

    return bam_path


def add_gsc_wgs_bam_dataset(
    bam_path, storage, sample, library, lane_infos, sftp=None, is_spec=False
):
    transferred = False

    if is_spec:
        bai_path = bam_path[:-5] + ".bai"
    else:
        bai_path = bam_path + ".bai"

    tantalus_bam_filename = get_tantalus_bam_filename(sample, library, lane_infos)
    tantalus_bai_filename = tantalus_bam_filename + ".bai"

    tantalus_bam_path = os.path.join(
        storage["storage_directory"], tantalus_bam_filename
    )
    tantalus_bai_path = os.path.join(
        storage["storage_directory"], tantalus_bai_filename
    )

    #Check if the script is being run on thost
    current_host = socket.gethostname()
    if current_host != "txshah":
        username = pwd.getpwuid(os.getuid()).pw_name
        transfer_bam_path = "thost:" + bam_path
        transfer_bai_path = "thost:" + bai_path
    else:
        transfer_bam_path = bam_path
        transfer_bai_path = bai_path

    #If this is a spec file, create a bam file in the tantlus_bam_path destination
    if is_spec:
        transferred = create_bam( 
                            bam_path, 
                            lane_infos[0]['reference_genome'], 
                            tantalus_bam_path,
                            storage,
                            library["library_id"],
                            from_gsc=True)            
    #Otherwise, copy the bam and the bam index to the specified tantalus path
    else:

        #If this is not run on thost, an sftp client will need to be used to check file size on /projects/
        if current_host != "txshah":

            if not os.path.isfile(tantalus_bam_path):
                rsync_file(transfer_bam_path, tantalus_bam_path)
                transferred = True 
            else:
                remote_file = sftp.stat(bam_path)
                if remote_file.st_size != os.path.getsize(tantalus_bam_path):
                    logging.info("The size of {} on the GSC does not match {}. Copying new file to {} ".format(
                            bam_path,
                            tantalus_bam_path,
                            storage["name"]
                            ))
                    rsync_file(transfer_bam_path, tantalus_bam_path)
                    transferred = True
                else:
                    logging.info("The bam already exists at {}. Skipping import".format(tantalus_bam_path))

                
            if not os.path.isfile(tantalus_bai_path):
                rsync_file(transfer_bai_path, tantalus_bai_path)
                transferred = True 
            else:
                remote_file = sftp.stat(bai_path)
                if remote_file.st_size != os.path.getsize(tantalus_bai_path):
                    logging.info("The size of {} on the GSC does not match {}. Copying new file to {} ".format(
                            bai_path,
                            tantalus_bai_path,
                            storage["name"]
                            ))
                    rsync_file(transfer_bai_path, tantalus_bai_path)
                    transferred = True
                else:
                    logging.info("The bai already exists at {}. Skipping import".format(tantalus_bai_path))
        else:
            if not os.path.isfile(tantalus_bam_path):
                rsync_file(transfer_bam_path, tantalus_bam_path)
                transferred = True 
            elif os.path.getsize(bam_path) != os.path.getsize(tantalus_bam_path):
                logging.info("The size of {} on the GSC does not match {}. Copying new file to {} ".format(
                        bam_path,
                        tantalus_bam_path,
                        storage["name"]
                        ))
                rsync_file(transfer_bam_path, tantalus_bam_path)
                transferred = True 
                
            else:
                logging.info("The bam already exists at {}. Skipping import".format(tantalus_bam_path))
                
            if not os.path.isfile(tantalus_bai_path):
                rsync_file(transfer_bai_path, tantalus_bai_path)
                transferred = True 
            elif os.path.getsize(bai_path) != os.path.getsize(tantalus_bai_path):
                logging.info("The size of {} on the GSC does not match {}. Copying new file to {} ".format(
                        bai_path,
                        tantalus_bai_path,
                        storage["name"]
                        ))
                rsync_file(transfer_bai_path, tantalus_bai_path)
                transferred = True 
                
            else:
                logging.info("The bam index already exists at {}. Skipping import".format(tantalus_bai_path))
    
    return tantalus_bam_path, transferred
    

def add_gsc_bam_lanes(sample, library, lane_infos):
    detail_list = []

    for lane_info in lane_infos:
        lane = dict(
            flowcell_id=lane_info["flowcell_id"],
            lane_number=lane_info["lane_number"],
            sequencing_centre="GSC",
            sequencing_instrument=lane_info["sequencing_instrument"],
            read_type=lane_info["read_type"],
            dna_library=library,
        )

        detail_list.append(lane)

    return detail_list


def check_sftp_bams(sftp, bam_path, bam_spec_path, storage, sample, library, lane_infos):
    try:
        sftp.stat(bam_path)
        bam_filepath, transferred = add_gsc_wgs_bam_dataset(
            bam_path, storage, sample, library, lane_infos, sftp
        )
        return bam_filepath, transferred
    except IOError:
        pass

    try:
        sftp.stat(bam_spec_path)
        bam_filepath, transferred = add_gsc_wgs_bam_dataset(
            bam_spec_path,
            storage,
            sample,
            library,
            lane_infos,
            sftp,
            is_spec=True,
        )
        return bam_filepath, transferred
    except IOError:
        #raise Exception("missing merged bam file {}".format(bam_path))
        logging.error("Missing merged bam file {}".format(bam_path))
        return None, False


def query_gsc(identifier, id_type):
    logging.info("Querying GSC for {} {}".format(id_type, identifier))

    if ' ' in identifier:
        raise ValueError('space in id "{}"'.format(identifier))

    if id_type == 'library':
        infos = gsc_api.query("library?name={}".format(identifier))
    elif id_type == 'sample':
        infos = gsc_api.query('library?external_identifier={}'.format(identifier))

    return infos


def get_gsc_details(
    library_infos,
    storage,
    skip_file_import=False,
    skip_older_than=None,
):
    """
    Copy GSC libraries to a storage and return metadata json.
    """
    details_list = []

    #If this isn't being run on thost, connect to an ssh client to access /projects/ files
    if socket.gethostname() != "txshah":
        ssh_client = connect_to_client("10.9.208.161")
        sftp = ssh_client.open_sftp()
    else:
        sftp = None

    for library_info in library_infos:
        logging.info("importing %s", library_info["name"])
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

        sample_id = library_info["external_identifier"]

        if ' ' in sample_id:
            raise ValueError('space in sample_id "{}"'.format(sample_id))

        sample = dict(sample_id=sample_id)

        library_type = protocol_id_map[library_info["protocol_id"]]
        logging.info("found %s", library_type)
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
            transferred = False
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

            reference_genome = merge_info["lims_genome_reference"]["path"]
            aligner = merge_info["aligner_software"]["name"]

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
                created_date = convert_time(run["run_datetime"])
                adapter_index_sequence = primer["adapter_index_sequence"]

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
                bam_filepath = None
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
                if sftp:
                    bam_filepath, transferred = check_sftp_bams(
                            sftp, 
                            bam_path,
                            bam_spec_path, 
                            storage, 
                            sample, 
                            library, 
                            lane_infos
                    )

                else:
                    if os.path.exists(bam_path):
                        bam_filepath, transferred = add_gsc_wgs_bam_dataset(
                            bam_path, storage, sample, library, lane_infos, sftp
                        )
                    elif os.path.exists(bam_spec_path):
                        bam_filepath, transferred = add_gsc_wgs_bam_dataset(
                            bam_spec_path,
                            storage,
                            sample,
                            library,
                            lane_infos,
                            sftp,
                            is_spec=True,
                        )
                    else:
                        #raise Exception("missing merged bam file {}".format(bam_path))
                        logging.error("Missing merged bam file {}".format(bam_path))
                        bam_filepath = None
                        transferred = False

            list_temp = dict(
                library_id=library_name,
                storage_name=storage['name'],
                library_type=library_type,
                bam_filepath=bam_filepath,
                read_type=solexa_run_type_map[solexa_run_type],
                sequencing_centre='GSC',
                lane_info=lane_infos, 
                transferred=transferred,
                )

            details_list.append(list_temp)

        libcores = gsc_api.query(
            "aligned_libcore/info?library={}".format(library_name)
        )

        for libcore in libcores:
            transferred = False
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
                bam_filepath = None
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
                if sftp:
                    bam_filepath, transferred = check_sftp_bams(
                            sftp, 
                            bam_path, 
                            bam_spec_path,
                            storage, 
                            sample, 
                            library, 
                            lane_infos
                    )
                else:
                    if os.path.exists(bam_path):
                        bam_filepath, transferred = add_gsc_wgs_bam_dataset(
                            bam_path, storage, sample, library, lane_infos, sftp
                        )
                    elif os.path.exists(bam_spec_path):
                        bam_filepath, transferred = add_gsc_wgs_bam_dataset(
                            bam_spec_path,
                            storage,
                            sample,
                            library,
                            lane_infos,
                            sftp,
                            is_spec=True,
                        )
                    else:
                        #raise Exception("missing merged bam file {}".format(bam_path))
                        logging.error("Missing merged bam file {}".format(bam_path))
                        bam_filepath = None
                        trasnferred = False
    
            list_temp = dict(
                library_id=library_name,
                storage_name=storage['name'],
                library_type=library_type,
                bam_filepath=bam_filepath,
                read_type=solexa_run_type_map[solexa_run_type],
                sequencing_centre='GSC',
                lane_info=lane_infos, 
                transferred=transferred
                )

            details_list.append(list_temp)
    
    if sftp:
    	ssh_client.close()
    return details_list


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Not a valid date: '{0}'.".format(s))


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

    # Convert the date to the format we want
    if kwargs["skip_older_than"]:
        skip_older_than = valid_date(kwargs["skip_older_than"])

    # Connect to the Tantalus API (this requires appropriate environment
    # variables defined)
    tantalus_api = TantalusApi()
    storage = tantalus_api.get("storage_server", name=kwargs["storage"])  

    #Check that an id type was specified
    if not kwargs["id_type"]:
        raise Exception("Please specify an ID type (sample or library)")

    details = []
    for identifier in kwargs["ids"]:
        infos = query_gsc(identifier, kwargs["id_type"])

        if not infos:
            logging.info("No results for {} {}".format(kwargs["id_type"], identifier))
        else:
            logging.info("{} {} exists on the GSC".format(kwargs["id_type"], identifier))
            if kwargs["query_only"]:
                break

            detail = get_gsc_details(
                infos, 
                storage, 
                skip_file_import=kwargs["skip_file_import"],
                skip_older_than=kwargs["skip_older_than"])

            #Add dataset to tantalus
            for instance in detail:
                if not kwargs["skip_file_import"] and instance["transferred"]:
                    logging.info("Importing {} to tantalus".format(instance["bam_filepath"]))

                    dataset = import_bam(
                        storage_name=instance["storage_name"],
                        library_type=instance["library_type"],
                        bam_filename=instance["bam_filepath"],
                        read_type=instance["read_type"],
                        sequencing_centre=instance["sequencing_centre"],
                        index_format="N",
                        lane_info=instance["lane_info"],
                        tag_name=kwargs["tag_name"],
                        update=kwargs["update"]
                    )
                    logging.info("Successfully added sequence dataset with ID {} to tantalus".format(dataset["id"]))
                
                elif kwargs["skip_file_import"]:
                    #Only add lanes, libraries, and samples to tantalus
                    for lane in instance["lane_info"]:
                        logging.info("Importing lanes for library {} to tantalus".format(instance["library_id"]))
                        
                        library_pk = tantalus_api.get_or_create(
                                "dna_library",
                                library_id=instance["library_id"],
                                library_type=instance["library_type"],
                                index_format="N")["id"]
                        
                        lane = tantalus_api.get_or_create(
                            "sequencing_lane",
                            flowcell_id=lane["flowcell_id"],
                            dna_library=library_pk,
                            read_type=lane["read_type"],
                            lane_number=str(lane["lane_number"]),
                            sequencing_centre=instance["sequencing_centre"],
                            sequencing_instrument=lane["sequencing_instrument"])
                        logging.info("Successfully created lane {} in tantalus".format(lane["id"]))


if __name__ == "__main__":
    main()

