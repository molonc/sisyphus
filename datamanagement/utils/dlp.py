from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import collections
from utils.utils import get_lanes_str


def fastq_paired_end_check(file_info):
    """ Check for paired ends for a set of fastq files """

    # Check for each read end
    pair_check = collections.defaultdict(set)
    for info in file_info:
        if len(info["sequence_lanes"]) > 1:
            raise Exception("more than 1 lane for fastqs not yet supported")

        fastq_id = (
            info["library_id"],
            info["index_sequence"],
            info["sequence_lanes"][0]["flowcell_id"],
            info["sequence_lanes"][0]["lane_number"],
        )

        if info["read_end"] in pair_check[fastq_id]:
            raise Exception(
                "duplicate fastq file with end {} for {}".format(
                    info["read_end"], fastq_id
                )
            )

        pair_check[fastq_id].add(info["read_end"])

    for fastq_id, pair_info in pair_check.iteritems():
        for read_end in (1, 2):
            if read_end not in pair_info:
                raise Exception(
                    "missing fastq file with end {} for {}".format(read_end, fastq_id)
                )


def create_sequence_dataset_models(
    file_info, storage_name, tag_name, tantalus_api, analysis_id=None
):
    """Create tantalus sequence models for a list of files."""
    # Get storage and tag PKs
    storage_pk = tantalus_api.get("storage", name=storage_name)["id"]

    if tag_name is not None:
        tag_pk = tantalus_api.get("sequence_dataset_tag", name=tag_name)["id"]

    # Sort files by dataset
    dataset_info = collections.defaultdict(list)
    for info in file_info:
        dataset_name = "{}-{}-{}-{} (lanes {})".format(
            info["dataset_type"],
            info["sample_id"],
            info["library_type"],
            info["library_id"],
            get_lanes_str(info["sequence_lanes"]),
        )
        dataset_info[dataset_name].append(info)

    # Create datasets
    for dataset_name, infos in dataset_info.iteritems():
        # Get library PK
        library_id = infos[0]["library_id"]
        library_pk = tantalus_api.get("dna_library", library_id=library_id)["id"]

        # Get sample PK
        sample_id = infos[0]["sample_id"]
        sample_pk = tantalus_api.get("sample", sample_id=sample_id)["id"]

        # Build up sequence dataset attrs; we'll add to this as we
        # proceed throughout the function
        sequence_dataset = dict(
            name=dataset_name,
            dataset_type=infos[0]["dataset_type"],
            sample=sample_pk,
            library=library_pk,
            sequence_lanes=[],
            file_resources=[],
        )

        # Add in the analysis id if it's provided
        if analysis_id is not None:
            sequence_dataset["analysis"] = analysis_id

        # Unique set of lanes keyed by flowcell id, lane number
        # TODO(mwiens91): What's this for? It's not used anywhere
        # :thinking:
        unique_sequence_lanes = {}

        # Add in BAM specific items
        if infos[0]["dataset_type"] == "BAM":
            sequence_dataset["aligner"] = infos[0]["aligner_name"]
            ref_genome = infos[0]["ref_genome"]

            # Stick with one naming scheme
            if ref_genome.lower() == "grch36":
                ref_genome = "HG18"
            elif ref_genome.lower() == "grch37":
                ref_genome = "HG19"

            sequence_dataset["reference_genome"] = ref_genome

        # Add in the tag if we have one
        if tag_name is not None:
            sequence_dataset["tags"] = [tag_pk]

        for info in infos:
            # Check consistency for fields used for dataset
            check_fields = (
                "dataset_type",
                "sample_id",
                "library_id",
                "library_type",
                "index_format",
            )
            for field_name in check_fields:
                if info[field_name] != infos[0][field_name]:
                    raise Exception("error with field {}".format(field_name))

            for sequence_lane in info["sequence_lanes"]:
                sequence_lane = dict(sequence_lane)
                sequence_lane["dna_library"] = library_pk
                sequence_lane["lane_number"] = str(sequence_lane["lane_number"])

                sequence_lane = tantalus_api.get_or_create(
                    "sequencing_lane", **sequence_lane
                )

                sequence_dataset["sequence_lanes"].append(sequence_lane["id"])

            sequence_file_info = dict(index_sequence=info["index_sequence"])

            if "read_end" in info:
                sequence_file_info["read_end"] = info["read_end"]

            file_resource = tantalus_api.get_or_create(
                "file_resource",
                size=info["size"],
                created=info["created"],
                file_type=info["file_type"],
                compression=info["compression"],
                filename=info["filename"],
            )

            sequence_file_info = tantalus_api.get_or_create(
                "sequence_file_info",
                file_resource=file_resource["id"],
                **sequence_file_info
            )

            sequence_dataset["file_resources"].append(file_resource["id"])

            file_instance = dict(storage=storage_pk, file_resource=file_resource["id"])

            if "filename_override" in info:
                file_instance["filename_override"] = info["filename_override"]

            tantalus_api.get_or_create("file_instance", **file_instance)

        tantalus_api.get_or_create("sequence_dataset", **sequence_dataset)
