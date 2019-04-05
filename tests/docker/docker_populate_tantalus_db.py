import json
import os
import datetime

from dbclients.tantalus import TantalusApi

if __name__ == "__main__":

    tantalusApi = TantalusApi()
    print os.getcwd()
    json_file = open('docker/dummy.json',"r")

    data = json.load(json_file)

    storage = tantalusApi.get_or_create(
        "storage_azure_blob",
        name="singlecellblob",
        storage_account="singlecelldata",
        storage_container="data"
     )


    ids = []
    for i in range(1,81):
        ids.append(i)
        resource = tantalusApi.get_or_create(
            'file_resource',
            file_type=data[i]['file_type'],
            last_updated=data[i]['last_updated'],
            size=data[i]['size'],
            created=data[i]['created'],
            compression=data[i]['compression'],
            filename=data[i]['filename'],
            is_folder=data[i]['is_folder'],
            owner=data[i]['owner']
        )


        fileinfo = tantalusApi.get_or_create(
            'sequence_file_info',
            read_end=data[i]["sequencefileinfo"]["read_end"],
            genome_region=data[i]["sequencefileinfo"]["genome_region"],
            index_sequence=data[i]["sequencefileinfo"]["index_sequence"],
            file_resource=resource['id'],
            owner=data[i]["sequencefileinfo"]["owner"]
        )

        for instance in data[i]["file_instances"]:
            instance = tantalusApi.get_or_create(
                'file_instance',
                storage=instance['storage']['id'],
                is_deleted=instance['is_deleted'],
                owner=instance['owner'],
                file_resource=resource['id']
            )

    tag = tantalusApi.get_or_create(
        'tag',
        name="OV_PseudoBulk_Test"
    )

    sample = tantalusApi.get_or_create(
        'sample',
        sample_id=data[0]['sample']['sample_id'],
        external_sample_id=data[0]['sample']["external_sample_id"]
    )

    library = tantalusApi.get_or_create(
        'dna_library',
        library_type=data[0]["library"]["library_type"],
        library_id=data[0]["library"]["library_id"],
        index_format=data[0]["library"]["index_format"],
        owner=1
    )

    lane = tantalusApi.get_or_create(
        'sequencing_lane',
        flowcell_id=data[0]["sequence_lanes"][0]["flowcell_id"],
        lane_number=data[0]["sequence_lanes"][0]["lane_number"],
        sequencing_centre=data[0]["sequence_lanes"][0]["sequencing_centre"],
        sequencing_instrument=data[0]["sequence_lanes"][0]["sequencing_instrument"],
        read_type=data[0]["sequence_lanes"][0]["read_type"],
        owner=data[0]["sequence_lanes"][0]["owner"],
        dna_library=1,
    )

    dataset= tantalusApi.get_or_create(
        'sequencedataset',
        last_updated=data[0]["last_updated"],
        name=data[0]["name"],
        dataset_type=data[0]["dataset_type"],
        owner=data[0]["owner"],
        sample=1,
        library=1,
        sequence_lanes=[1],
        tags=[1],
        file_resources=ids
    )

