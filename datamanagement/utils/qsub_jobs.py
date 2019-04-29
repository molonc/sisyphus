import subprocess

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

class CramToBamJob(object):
    def __init__(self, thread, cram_path, ref, out_path):
        self.ctx = {}
        self.thread = str(thread)
        self.cram_path = cram_path
        self.ref = ref
        self.out_path = out_path
        self.finished = False
        self.name = "cram decompression"

    def __call__(self, **kwargs):
        cmd = [
            "samtools",
            "view",
            "-b",
            self.cram_path,
            "-T",
            self.ref,
            "-o",
            self.out_path,
            "-@",
            self.thread
        ]

        subprocess.check_call(cmd)
        self.finished = True