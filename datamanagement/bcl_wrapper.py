import re
import click
import socket
import os
@click.command()
@click.argument('dir_in', nargs=1)
@click.argument('dir_out', nargs=1)
@click.option('--no_rev_comp', is_flag=True)
@click.option('--no_bcl2fastq', is_flag=True)

def main(dir_in, dir_out, no_bcl2fastq=False, no_rev_comp=False):
    def get_lanes(Lines):
        for line in Lines:
            if re.search("LaneCount=", line):
                line_out = line
                return int(line_out[line_out.find("LaneCount=")+11:line_out.find("LaneCount=")+12])

    def get_flowcell(Lines):
        for line in Lines:
            if re.search("<Flowcell>", line):
                flowcell = line
                return flowcell[int(flowcell.find("<Flowcell>"))+len("<Flowcell>"):int(flowcell.find("</Flowcell>"))]

    if socket.gethostname() =="scdna-prod-headnode":
        raise Exception("wrong machine") 
    if no_rev_comp:
        script_to_call = "/home/sbeatty/sisyphus/datamanagement/dlp_bcl_fastq_import.py"
    else:
        script_to_call = "/home/sbeatty/sisyphus/datamanagement/dlp_bcl_fastq_import_flip_i7_2022.py"

    if dir_out[-1] != "/":
        dir_out = dir_out + "/"

    if dir_in[-1] == "/":
        dir_in = dir_in[0:-1]

    run_config_file_path = dir_in + "/" + "RunInfo.xml"
    file_in = open(run_config_file_path, "r")
    lines_in = file_in.readlines()

    command = "python " + script_to_call + " singlecellblob " + dir_out + " " + get_flowcell(lines_in) + " " + dir_in #+ " " +  str(get_lanes(lines_in))

    if no_bcl2fastq:
        script_to_call = "/home/sbeatty/sisyphus/datamanagement/dlp_bcl_fastq_import.py"
        command = "python " + script_to_call + " singlecellblob " + dir_out + " " + get_flowcell(lines_in) + " " + dir_in + " --no_bcl2fastq " #+  str(get_lanes(lines_in))

    #command2 = "echo " + command
    os.system(command)

if __name__ == "__main__":
    main()
