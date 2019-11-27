from datamanagement.templates import TENX_FASTQ_NAME_TEMPLATE, TENX_SCRNA_DATASET_TEMPLATE
from datamanagement.utils.gsc import get_sequencing_instrument, GSCAPI
from dbclients.tantalus import TantalusApi
from datamanagement.query_gsc_for_wgs_bams import rsync_file
from datamanagement.utils.constants import LOGGING_FORMAT
import click
import socket
import logging

logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)
gsc_api = GSCAPI()
SHAHLAB_BASE_PATH = "/shahlab/archive/"
tantalus_api = TantalusApi()
username = os.environ["SERVER_USER"]

@click.group()
def main():
    pass

def get_sftp():
    if socket.gethostname() != "txshah":
        ssh_client = connect_to_client("10.9.208.161")
        sftp = ssh_client.open_sftp()
    else:
        sftp = None
    return sftp

@main.command("query_gsc_rnqseq_fastq")
@click.argument("library_id", nargs=1)
@click.argument("to_storage", nargs=1)
def query_gsc_rnqseq_fastq(library_id, to_storage):
    sftp = get_sftp()
    if sftp:
        remote_host = "thost:"
    else:
        remote_host = None
    file_resource_ids = []
    concat_fastq_results = gsc_api.query("concat_fastq?library={}&production=true".format(library_id))
    for concat_fastq in concat_fastq_results:
        data_path = concat_fastq["data_path"]
        if "chastity_passed" in data_path:
            file_name = TENX_FASTQ_NAME_TEMPLATE.format(
            library_id=library_id,
            sample_id=concat_fastq["external_identifier"],
            fastq="S1_L00{}_R{}_001.fastq.gz".format(
                    concat_fastq["libcore"]["run"]["lane_number"],
                    concat_fastq["file_type"]["filename_pattern"][1]
                    ),
            )
            file_path = os.path.join(SHAHLAB_BASE_PATH, file_name)
            if not os.path.exists("file_path"):
                rsync_file(
                    from_path=data_path,
                    to_path=file_path,
                    sftp=sftp,
                    remote_host=remote_host
                )
            size_match = size_match(file_path, data_path, "10.9.208.161", username)
            if not size_match:
                rsync_file(
                    from_path=data_path,
                    to_path=file_path,
                    sftp=sftp,
                    remote_host=remote_host
                )
            if os.path.exists("file_path") and size_match:
                logging.info("Adding {} into tantalus".format(file_path))
                file_resource, file_instance = tantalus_api.add_file(to_storage["name"], file_path, update=False)
                logging.info("The file has been added into tantalus, with file_resource_id = {}".format(file_resource["id"]))
                file_resource_ids.append(file_resource["id"])
        else:
            continue


if __name__=="__main__":
    main()

