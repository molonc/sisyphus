from dbclients.tantalus import TantalusApi


def init():
    #Query for all fastq data
    tAPI = TantalusApi()

    print "COLLECTING fastq"
    fastq_datasets = tAPI.list('sequence_dataset',library__library_type="SC_WGS", dataset_type="FQ")

    #for fastq_dataset in fastq_datasets:
        #print (list(tAPI.list('sequence_dataset', library__library_id=fastq_dataset['library']['id'], dataset_type="BAM")))
    print "Collecting BAM"
    bam_datasets = (tAPI.list('sequence_dataset', library__library_type="SC_WGS", dataset_type="BAM"))

    print "Collecting results"
    results = tAPI.list('results', results_type="hmmcopy")

    print "Checking..."
    for fastq_dataset in fastq_datasets:
        print "fastq loop"
        for lane in fastq_dataset['sequence_lanes']:
            print "lane loop"
            if not any((lane in bam_dataset['sequence_lanes']) for bam_dataset in bam_datasets):
                print "ERROR: Lane with no associated BAM file\n"
            else:
                print "LANE ID : " + lane["id"]
            for result in results:
                if not any((lane in input_dataset['sequence_lanes']) for input_dataset in tAPI.list("sequence_dataset", id__in= tAPI.get("analysis", id=result["analysis"])['input_datasets'])):
                    print "ERROR missing lane"
                else:
                    print "PASEED"


if __name__ == '__main__':
    init()


