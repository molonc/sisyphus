import re
import click
import os
@click.command()
@click.argument('dir_in', nargs=1)
@click.argument('dir_out', nargs=1)
@click.argument('orientation', nargs=1, default="i7")
@click.option('--no_bcl2fastq', is_flag=True)

def main(dir_in, dir_out, orientation, no_bcl2fastq=False):
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

    script_to_call = "/home/prod/sisyphus/datamanagement/dlp_bcl_fastq_import.py"
    if orientation == "i7":
        script_to_call = "/home/prod/sisyphus/datamanagement/dlp_bcl_fastq_import_flip_i7_2022.py"

    #dir_out = "/projects/molonc/archive/A118390A_210610_VH00387_33_AAAKHTGM5_new_transfer_i7"
    if dir_out[-1] != "/":
        dir_out = dir_out + "/"

    #dir_in = "/projects/molonc/archive/sean/210610_VH00387_33_AAAKHTGM5/"
    if dir_in[-1] == "/":
        dir_in = dir_in[0:-1]

    run_config_file_path = dir_in + "/" + "RunInfo.xml"
    file_in = open(run_config_file_path, "r")
    lines_in = file_in.readlines()

    command = "python " + script_to_call + " singlecellblob " + dir_out + " " + get_flowcell(lines_in) + " " + dir_in #+ " " +  str(get_lanes(lines_in))

    if no_bcl2fastq:
        script_to_call = "/home/prod/sisyphus/datamanagement/dlp_bcl_fastq_import.py"
        command = "python " + script_to_call + " singlecellblob " + dir_out + " " + get_flowcell(lines_in) + " " + dir_in + " --no_bcl2fastq " #+  str(get_lanes(lines_in))

    #command2 = "echo " + command
    os.system(command)

if __name__ == "__main__":
    main()
