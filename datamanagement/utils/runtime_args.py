"""Contains a method to get runtime arguments for tasks."""

from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import argparse
import json
import __main__


def parse_runtime_args(name=__main__.__file__):
    """Parse runtime args using argparse.

    Args:
        name (str, optional): The name of the task. Defaults to the
            filename of the main script.

    Returns:
        An dictionary containing the JSON pointed to by the runtime
        arguments.

    Raises:
        RuntimeError: The arguments passed in were absent or ambiguous.
    """
    # Parse args
    parser = argparse.ArgumentParser(prog=name, description="%(prog)s")
    parser.add_argument(
        "json", default="{}", nargs="?", help="arguments for the script in JSON format"
    )
    parser.add_argument(
        "--json-file",
        default=None,
        help="JSON file containing arguments for the script",
    )
    args = parser.parse_args()

    # Validate that acceptable args passed in
    if args.json != "{}" and args.json_file is not None:
        raise RuntimeError("Specify either JSON xor `.json` file")

    # Read from a file if one was specified
    if args.json_file is not None:
        with open(args.json_file) as f:
            return json.load(f)

    # Just parse what was passed in directly
    return json.loads(args.json)
