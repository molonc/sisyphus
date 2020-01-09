import sys
import logging

from datamanagement.utils.constants import LOGGING_FORMAT
from dbclients.tantalus import TantalusApi
from dbclients.basicclient import NotFoundError


def fix():
    tantalus_api = TantalusApi()

    datasets = list(tantalus_api.list(
        'sequence_dataset',
        dataset_type='BAM',
        library__library_type__name='WGS',
    ))

    for dataset in datasets:
        bams = {}
        bais = {}
        specs = {}
        for file_resource_id in dataset['file_resources']:
            file_resource = tantalus_api.get('file_resource', id=file_resource_id)
            if file_resource['filename'].endswith('.bam'):
                bams[file_resource_id] = file_resource['filename']
            elif file_resource['filename'].endswith('.spec'):
                specs[file_resource_id] = file_resource['filename']
            elif file_resource['filename'].endswith('.bam.bai'):
                bais[file_resource_id] = file_resource['filename']

        if len(bams) == 0 and len(specs) == 0:
            print(dataset['id'])

        elif len(bams) > 1:
            logging.info(f"fixing {dataset['name']}, {bams}")

            to_remove_bam_id = max(bams.keys())
            to_remove_bai_id = None
            for id_, bai in bais.items():
                if bai.startswith(bams[to_remove_bam_id]):
                    assert to_remove_bai_id is None
                    to_remove_bai_id = id_
                    break
            assert to_remove_bai_id is not None

            logging.info((to_remove_bam_id, bams[to_remove_bam_id], to_remove_bai_id, bais[to_remove_bai_id]))

            new_file_resources = dataset['file_resources']
            new_file_resources.remove(to_remove_bam_id)
            new_file_resources.remove(to_remove_bai_id)

            logging.info(f"updating {dataset['id']} to have files {new_file_resources}")

            tantalus_api.update('sequencedataset', id=dataset['id'], file_resources=new_file_resources)

            assert dataset["name"].endswith(str(dataset["version_number"]))

            similar_datasets = list(tantalus_api.list(
                "sequence_dataset",
                name=dataset["name"],
            ))
            new_version_number = max(d['version_number'] for d in similar_datasets) + 1

            new_dataset_params = dict(
                sample=dataset['sample']['id'],
                library=dataset['library']['id'],
                sequence_lanes=[l['id'] for l in dataset['sequence_lanes']],
                aligner=dataset['aligner'],
                reference_genome=dataset['reference_genome'],
                name=dataset['name'][:-1] + str(new_version_number),
                dataset_type=dataset['dataset_type'],
                version_number=new_version_number,
                file_resources=[to_remove_bam_id, to_remove_bai_id],
            )

            logging.info(new_dataset_params)

            new_dataset, _ = tantalus_api.create(
                'sequencedataset',
                new_dataset_params,
                ['name'],
            )

            logging.info(new_dataset)


if __name__ == '__main__':
    logging.basicConfig(format=LOGGING_FORMAT, stream=sys.stderr, level=logging.INFO)

    fix()
