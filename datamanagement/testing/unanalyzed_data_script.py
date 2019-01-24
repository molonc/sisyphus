from dbclients.tantalus import TantalusApi


def init():
    #Query for all fastq data
    tAPI = TantalusApi()

    fastq_lanes = {}
    unanalyzed_fastq_id=[]
    align_analysis_input = []

    print "Collecting fastq..."
    output_text = open("unanalyzed_data.txt", "w+")
    fastq_datasets = tAPI.list('sequence_dataset',library__library_type="SC_WGS", dataset_type="FQ")

    for fastq_dataset in fastq_datasets:
        if not fastq_dataset['sequence_lanes']:
            output_text.write("ID: " + str(fastq_dataset['id']) + " has no lanes\n")
            unanalyzed_fastq_id.append(fastq_dataset['id'])
        else:
            fastq_lanes[fastq_dataset['id']] = fastq_dataset['sequence_lanes']


    print "Collecting BAM..."
    bam_datasets = (tAPI.list('sequence_dataset', library__library_type="SC_WGS", dataset_type="BAM"))

    for bam_dataset in bam_datasets:
        if bam_dataset['analysis']:
            align_analysis_input.extend(tAPI.get('analysis',id=bam_dataset['analysis'])['input_datasets'])



    print "Verifying..."
    for id in fastq_lanes.keys():
        print id
        for lane in fastq_lanes[id]:
            if not tAPI.list("sequence_dataset",sequence_lanes__flowcell_id__in=lane['flowcell_id'], dataset_type="BAM"):
                output_text.write("ID: " + str(fastq_dataset['id']) + " has no BAM File for associated\n")
                unanalyzed_fastq_id.append(id)

            if not tAPI.list('results', sequence_lanes__flowcell_id=lane['flowcell_id'], results_type="hmmcopy"):
                output_text.write("ID: " + str(fastq_dataset['id']) + "'s related result has no hmmcopy\n")
                unanalyzed_fastq_id.append(id)

        if id not in align_analysis_input:
            output_text.write("ID: " + str(id) + " is not an input dataset for BAM file\n")
            unanalyzed_fastq_id.append(id)

if __name__ == '__main__':
    init()


