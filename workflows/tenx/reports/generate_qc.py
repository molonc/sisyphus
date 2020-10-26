import os
import subprocess

from workflows.tenx.reports.html_generation import generation as generate

Rpath = os.path.join(os.path.dirname(os.path.realpath(__file__)), "qc.R")


def rscript(library_id, input_dir, output_dir):
    # make output dir
    os.makedirs(output_dir, exist_ok=True)

    commands = ["Rscript", Rpath]
    args = ["-l", library_id, "-i", input_dir, "-o", output_dir]
    subprocess.call(commands + args)


def generate_html(library_id, output_dir):
    generate.generate_html(library_id, output_dir)
