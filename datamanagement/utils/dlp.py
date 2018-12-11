from __future__ import absolute_import
from __future__ import division
from __future__ import print_function
import collections

from datamanagement.utils.utils import get_lanes_hash, get_lane_str
import datamanagement.templates as templates
from dbclients.basicclient import NotFoundError
import logging

log = logging.getLogger('sisyphus')


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


ref_genome_map = {
    'grch36': 'grch36',
    'hg18': 'grch36',
    'grch37': 'grch37',
    'hg19': 'grch36',
    'mm10': 'mm10',
}


aligner_name_map = {
    'bwa-aln': 'bwa-aln',
    'bwa-mem': 'bwa-mem',
}


def standardize_metadata(file_info):
    """ Standardize the metadata of the files """
    for info in file_info:
        if info["dataset_type"] == 'BAM':
            info['ref_genome'] = ref_genome_map[info['ref_genome'].lower()]
            info['aligner_name'] = aligner_name_map[info['aligner_name'].lower()]


def create_sequence_dataset_models(
    file_info, storage_name, tag_name, tantalus_api, analysis_id=None, update=False
):
    """Create tantalus sequence models for a list of files."""

    standardize_metadata(file_info)

    # Get storage and tag PKs
    storage = tantalus_api.get("storage", name=storage_name)
    storage_pk = storage["id"]


    # Sort files by dataset
    dataset_info = collections.defaultdict(list)
    for info in file_info:
        if info["dataset_type"] == 'BAM':
            dataset_name = templates.SC_WGS_BAM_NAME_TEMPLATE.format(
                dataset_type=info["dataset_type"],
                sample_id=info["sample_id"],
                library_type=info["library_type"],
                library_id=info["library_id"],
                lanes_hash=get_lanes_hash(info["sequence_lanes"]),
                aligner=info["aligner_name"],
                reference_genome=info["ref_genome"],
            )
        elif info["dataset_type"] == 'FQ':
            dataset_name = templates.SC_WGS_FQ_NAME_TEMPLATE.format(
                dataset_type=info["dataset_type"],
                sample_id=info["sample_id"],
                library_type=info["library_type"],
                library_id=info["library_id"],
                lane=get_lane_str(info["sequence_lanes"][0]),
            )
        dataset_info[dataset_name].append(info)

    # Create datasets
    dataset_ids = set()
    for dataset_name, infos in dataset_info.iteritems():
        # Get library PK
        library = tantalus_api.get_or_create(
            "dna_library",
            library_id=infos[0]["library_id"],
            library_type=infos[0]["library_type"],
            index_format=infos[0]["index_format"],
        )
        library_pk = library["id"]

        # Get sample PK
        sample = tantalus_api.get_or_create(
            "sample",
            sample_id=infos[0]["sample_id"],
        )
        sample_pk = sample["id"]

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

        # Add in BAM specific items
        if infos[0]["dataset_type"] == "BAM":
            sequence_dataset["aligner"] = infos[0]["aligner_name"]
            sequence_dataset["reference_genome"] = infos[0]["ref_genome"]

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

            file_resource, file_instance = tantalus_api.add_file(
                storage_name,
                info["filepath"],
                info["file_type"],
                {'compression': info["compression"]},
                update=update,
            )

            sequence_file_info = tantalus_api.get_or_create(
                "sequence_file_info",
                file_resource=file_resource["id"],
                **sequence_file_info
            )

            sequence_dataset["file_resources"].append(file_resource["id"])

        try:
            dataset_id = tantalus_api.get("sequence_dataset", name=sequence_dataset["name"])["id"]
        except NotFoundError:
            dataset_id = None

        if update and dataset_id is not None:
            log.warning("sequence dataset {} has changed, updating".format(sequence_dataset["name"]))
            dataset = tantalus_api.update("sequence_dataset", id=dataset_id, **sequence_dataset)

        else:
            log.info("creating sequence dataset {}".format(sequence_dataset["name"]))
            dataset = tantalus_api.get_or_create("sequence_dataset", **sequence_dataset)

        # Add tag and post dataset to tag endpoint
        if tag_name is not None:
            tag_pk = tantalus_api.get_or_create("tag", name=tag_name)["id"]
            sequence_dataset["tags"] = [tag_pk]
            tantalus_api.tag(tag_name, dataset['id']) 

        dataset_ids.add(dataset['id'])


    return dataset_ids
