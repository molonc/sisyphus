import os
import logging

from dbclients.tantalus import TantalusApi

log = logging.getLogger('sisyphus')
log.setLevel(logging.DEBUG)
stream_handler = logging.StreamHandler()
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
stream_handler.setFormatter(formatter)
log.addHandler(stream_handler)
log.propagate = False

tantalus_api = TantalusApi()


def split_qc_analyses():
    # get all qc analyses
    analyses = tantalus_api.list(
        "analysis",
        analysis_type__name="qc",
    )

    for analysis in analyses:
        name = analysis["name"]
        jira_ticket = analysis["jira_ticket"]

        # change this analysis to align
        log.info(f"renaming analysis to {name.replace('sc_qc', 'sc_align')}")
        tantalus_api.update(
            "analysis",
            id=analysis['id'],
            name=name.replace("sc_qc", "sc_align"),
            analysis_type="align",
        )

        # get results
        results_datasets = tantalus_api.list(
            "resultsdataset",
            analysis__jira_ticket=jira_ticket,
        )

        # organize results by analysis type
        results = {result["results_type"]: result for result in results_datasets}

        # get bam datasets for hmmcopy
        bam_datasets = tantalus_api.list(
            "sequence_dataset",
            dataset_type__name="BAM",
            analysis__jira_ticket=jira_ticket,
        )
        hmmcopy_inputs = [d['id'] for d in bam_datasets if not d["region_split_length"]]

        # update args
        args = dict(
            aligner=analysis["args"]["aligner"],
            library_id=analysis["args"]["library_id"],
            ref_genome=analysis["args"]["ref_genome"],
        )

        # create hmmcopy analysis
        hmmcopy_analysis, _ = tantalus_api.create(
            "analysis",
            fields=dict(
                name=name.replace('sc_qc', 'sc_hmmcopy'),
                analysis_type="hmmcopy",
                input_datasets=hmmcopy_inputs,
                args=args,
                version=analysis["args"]["version"],
                jira_ticket=jira_ticket,
                status=analysis["args"]["status"],
            ),
            keys=[
                "jira_ticket",
                "name",
            ],
        )
        log.info(f"created annoations analysis {hmmcopy_analysis['id']}")

        log.info(f"updating analysis of hmmcopy results from {results['hmmcopy']['id']} to {hmmcopy_analysis['id']}")
        # update hmmcopy result with hmmcopy analysis
        tantalus_api.update(
            "resultsdataset",
            id=results["hmmcopy"]["id"],
            analysis=hmmcopy_analysis["id"],
        )

        # create annotation analysis
        annotation_analysis, _ = tantalus_api.create(
            "analysis",
            fields=dict(
                name=name.replace('sc_qc', 'sc_annotation'),
                analysis_type="annotation",
                args=args,
                version=analysis["args"]["version"],
                jira_ticket=jira_ticket,
                status=analysis["args"]["status"],
                input_results=[
                    results["align"]["id"],
                    results["hmmcopy"]["id"],
                ],
            ),
            keys=[
                "jira_ticket",
                "name",
            ],
        )
        log.info(f"created annoations analysis {annotation_analysis['id']}")

        log.info(
            f"updating analysis of annotation results from {results['annotation']['id']} to {annotation_analysis['id']}"
        )
        # update annotation result with annotation analysis
        tantalus_api.update(
            "resultsdataset",
            id=results["annotation"]["id"],
            analysis=annotation_analysis["id"],
        )


if __name__ == "__main__":
    split_qc_analyses()