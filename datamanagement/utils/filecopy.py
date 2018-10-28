from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import logging
import os
from subprocess import Popen, PIPE, STDOUT
from utils.utils import make_dirs

# Setup logger
log = logging.getLogger(__name__)


def rsync_file(from_path, to_path):
    make_dirs(os.path.dirname(to_path))

    subprocess_cmd = [
        "rsync",
        "--progress",
        "--chmod=D555",
        "--chmod=F444",
        "--times",
        "--copy-links",
        from_path,
        to_path,
    ]

    log.info(" ".join(subprocess_cmd))

    # The following is a way to use the logging module with subprocess.
    # See
    # https://stackoverflow.com/questions/21953835/run-subprocess-and-print-output-to-logging.
    process = Popen(subprocess_cmd, stdout=PIPE, stderr=STDOUT)

    with process.stdout:
        for line in iter(process.stdout.readline, b""):
            log.info(line)

    if os.path.getsize(to_path) != os.path.getsize(from_path):
        log.error("copy failed for %s to %s", from_path, to_path)
