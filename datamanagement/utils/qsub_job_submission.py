import os
import sys
import logging
import datetime
import pypeliner.helpers
from pypeliner.execqueue.qsub import AsyncQsubJobQueue
from datamanagement.utils.constants import LOGGING_FORMAT

# Set up the root logger
logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)


class SpecToBamJob(object):
    def __init__(self, thread, spec_path, ref, out_path, binary):
        self.ctx = {}
        self.thread = str(thread)
        self.spec_path = spec_path
        self.ref = ref
        self.out_path = out_path
        self.binary = binary
        self.finished = False
        self.name = "spec_decompression"
    
    def __call__(self, **kwargs):
        cmd = [
            self.binary,
            "--thread",
            self.thread,
            "--in",
            self.spec_path, 
            "--ref",
            self.ref,
            "--out",
            self.out_path
        ]
        subprocess.check_call(cmd)
        self.finished = True 


class Bcl2FastqJob(object):
    def __init__(self, thread, run_dir, sample_sheet, out_dir):
        self.ctx = {}
        self.thread = str(thread)
        self.run_dir = run_dir
        self.sample_sheet = sample_sheet
        self.out_dir = out_dir
        self.finished = False
        self.name = "bcl2fastq"
    
    def __call__(self, **kwargs):
        cmd = [
            'bcl2fastq',
            "--processing-threads",
            self.thread,
            "--runfolder-dir",
            self.run_dir, 
            "--sample-sheet",
            self.sample_sheet,
            "--output-dir",
            self.out_dir
        ]
        subprocess.check_call(cmd)
        self.finished = True 


def submit_qsub_job(job, native_spec, **kwargs):
    """
    Creates a queue and submits the job to the cluster. Waits for the job to 
    successfully finish

    Args:
        job:            the job object to run
        native_spec:    native specifications to use for the job
    """
    # Create the pypeliner queue object to submit the job
    queue = AsyncQsubJobQueue(modules=(sys.modules[__name__], ), native_spec=native_spec)
    
    current_time = datetime.datetime.now().strftime('%d-%m-%Y_%S-%M-%H')
    dir_name = kwargs["title"] + "_" + current_time

    # Create the temp directory for all output logs
    temps_dir = os.path.join('tmp', dir_name)
    pypeliner.helpers.makedirs(temps_dir)
    
    logging.info("Submitting job to the cluster")
    queue.send({'mem': 10}, job.name, job, temps_dir)
    job_name = None
    
    # Wait for the job to finish
    while True:
        job_name = queue.wait()
        if job_name is not None:
            break
        os.sleep(10)
    
    result = queue.receive(job_name)
    
    assert result.finished == True