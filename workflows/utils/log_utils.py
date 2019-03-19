import logging
import logging.handlers
import inspect
import os
import pickle
import requests
import subprocess
from datetime import datetime
import pytz
import re
import shutil
import yaml

log = logging.getLogger('sisyphus')

interactive_mode = True
working_directory = ""
modified = False


def send_logging_email(email, subject):
    smtp_handler = logging.handlers.SMTPHandler(
                        mailhost='localhost',
                        fromaddr=email,
                        toaddrs=email,
                        subject=subject)

    log.addHandler(smtp_handler)
    log.exception(subject)


def setup_sentinel(interactive, pipeline_directory):
    global modified
    if modified:
        raise Exception("Trying to modify singletons again")

    global interactive_mode
    interactive_mode = interactive
    global working_directory
    working_directory = pipeline_directory
    modified = True


def init_pl_dir(pipeline_dir, clean):
    """Create a pipeline directory in the jobs directory
    Args:
        SC-code: the jira ticket associated with this run
        clean: boolean, if true, wipes directory if it already exists
    Returns:
        the locations of the newly created or preexisting jobs directory
    """
    log.debug("Cleaning working directory: " + str(clean))

    if clean and os.path.exists(pipeline_dir):
        shutil.rmtree(pipeline_dir)

    if not os.path.exists(pipeline_dir):
        os.makedirs(pipeline_dir)

    return pipeline_dir


def init_log_files(pl_dir):
    tz = pytz.timezone('Canada/Pacific')
    t = datetime.now(tz)
    starttime = '{}-{}-{:02d}_{}-{}-{}'.format(t.hour, t.minute, t.second, t.month, t.day, t.year)

    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')

    log_dir = os.path.join(pl_dir, 'logs')
    if not os.path.exists(log_dir):
        os.mkdir(log_dir)

    log_file = os.path.join(log_dir, '{}.log'.format(starttime))
    fh = logging.FileHandler(os.path.join(log_dir, '{}.log'.format(starttime)))
    fh.setFormatter(formatter)
    log.addHandler(fh)

    # TODO: WTH
    latest_file = os.path.join(log_dir, 'latest')
    if os.path.exists(latest_file):
        os.remove(latest_file)
    os.symlink(log_file, latest_file)

    return str(log_file)


def spaces_to_underscores(str):
    return '_'.join(str.split(' '))


def sync_call(name, args_list):
    """Execute a shell command with file logging.
       The name is argument will be the filename
    """
    log.debug(name)

    name = spaces_to_underscores(name)

    if not os.path.exists(os.path.join(working_directory, 'logs')):
        os.mkdir(os.path.join(working_directory, 'logs'))

    if os.path.exists(name):
        log.warning("Already a file called {}. Please try to name better".format(name))

    with open(working_directory + '/logs/{}_output.txt'.format(name), 'w') as out:
        with open(working_directory + '/logs/{}_error.txt'.format(name), 'w') as err:
            log.debug("Running '{}' stdout: {} stderr: {}".format(args_list, out.name, err.name))
            subprocess.check_call(args_list, stdout=out, stderr=err)


def sentinel2(*args, **kwargs):
    print args, kwargs

def sentinel(filename, function, *args, **kwargs):
    """ Only executes if it hasn't been executed before.
        If the function returns something, then it should be pickled to a file.
        If an object can't be pickled, this will give an error
        Sentinel file pattern: <filename>_<hashed args>_<calling function>
    params:
        filename: a short description of the function to be executed
        function: the function to be executed
        args: arguments to be passed to the function
        kwargs: keyword arguments to be passed to the function
    returns:
        return value for the given function and arguments
    """

    # Since the file created is based off the filename argument, don't reuse filenames
    sentinels_dir = os.path.join(working_directory, 'sentinels')

    if not os.path.exists(sentinels_dir):
        os.makedirs(sentinels_dir)

    log.debug(filename)

    hash_args = yaml.dump(args) + yaml.dump(kwargs)
    filename = spaces_to_underscores(filename) + '_' + str(hash(hash_args))

    # Append the calling function onto the filename.
    caller_name = inspect.stack()[1][3]
    filename = filename + '_' + caller_name
    filename = os.path.join(sentinels_dir, filename)

    # Handle interactive mode
    if interactive_mode:
        if os.path.isfile(filename):
            # If ever run in python 3, change this
            text = raw_input("File {} already exists, would you like to rerun this step? (Type 'run' to rerun)".format(filename))
            if text == 'run' or text == 'yes' or text == 'y':
                os.remove(filename)

    if not os.path.isfile(filename):
        ret_value = function(*args, **kwargs)
        with open(filename, 'wb') as f:
            pickle.dump(ret_value, f)
    else:
        log.debug("{} is present, skipping task".format(filename))
        with open(filename, 'rb') as f:
            ret_value = pickle.load(f)

    return ret_value
