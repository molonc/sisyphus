from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import errno
import hashlib
import os
import paramiko
import pwd
import re
from datetime import datetime

from datamanagement.utils.constants import REF_GENOME_REGEX_MAP


def get_lane_str(lane):
    if lane["lane_number"] == "":
        return "{}".format(lane["flowcell_id"])

    # Include lane number
    return "{}_{}".format(lane["flowcell_id"], lane["lane_number"])


def get_analysis_lanes_hash(tantalus_api, analysis):
    """
    Args:
        tantalus_api
        analysis (dict)

    Return:
        lanes_hashed (str)
    """
    lanes = set()

    for input_dataset in analysis["input_dataset"]:
        dataset = tantalus_api.get('sequence_dataset', id=input_dataset)
        for sequence_lane in dataset['sequence_lanes']:
            lane = "{}_{}".format(sequence_lane['flowcell_id'], sequence_lane['lane_number'])
            lanes.add(lane)

    lanes = ", ".join(sorted(lanes))
    lanes = hashlib.md5(lanes.encode('utf-8'))
    lanes_hashed = "{}".format(lanes.hexdigest()[:8])


    return lanes_hashed


def get_lanes_hash(lanes):
    if not lanes:
        raise ValueError("bam with no lanes")

    lanes = ", ".join(sorted([get_lane_str(a) for a in lanes]))
    lanes = hashlib.md5(lanes.encode('utf-8'))
    return "{}".format(lanes.hexdigest()[:8])


def make_dirs(dirname, mode=0o775):
    oldmask = os.umask(0)
    try:
        os.makedirs(dirname, mode)
    except OSError as e:
        if e.errno != errno.EEXIST or not os.path.isdir(dirname):
            raise
    finally:
        os.umask(oldmask)


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


def valid_date(s):
    try:
        return datetime.strptime(s, "%Y-%m-%d")
    except ValueError:
        raise ValueError("Not a valid date: '{0}'.".format(s))


def add_compression_suffix(path, compression):
    if compression == "spec":
        return path + ".spec"
    else:
        raise ValueError("unsupported compression {}".format(compression))


def connect_to_client(hostname, username=None):
    ssh_client = paramiko.SSHClient()
    ssh_client.load_system_host_keys()

    if not username:
        username = pwd.getpwuid(os.getuid()).pw_name
    ssh_client.connect(hostname, username=username)

    return ssh_client


def parse_ref_genome(raw_reference_genome):
    found_match = False
    for ref, regex_list in REF_GENOME_REGEX_MAP.iteritems():
        for regex in regex_list:
            if re.search(regex, raw_reference_genome, flags=re.I):
                # Found a match
                reference_genome = ref
                found_match = True
                break

        if found_match:
            break

    if not found_match:
        raise Exception("Unrecognized reference genome {}".format(raw_reference_genome))

    return reference_genome
