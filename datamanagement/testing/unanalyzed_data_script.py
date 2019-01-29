from dbclients.tantalus import TantalusApi


def init():
    #Query for all fastq data
    tAPI = TantalusApi()

    fastq_lanes = {}
    unanalyzed_fastq_id=[]
    align_analysis_input = []

    print "Collecting HMMCopy analyses"
    hmmcopy_lane_inputs = []
    hmmcopy_analyses = tAPI.list('analysis', analysis_type__name="hmmcopy")
    for hmmcopy_analysis in hmmcopy_analyses:
        for dataset_id in hmmcopy_analysis['input_datasets']:
            dataset = tAPI.get('sequence_dataset', id=dataset_id)
            for lane in dataset['sequence_lanes']:
                hmmcopy_lane_inputs.append((lane['flowcell_id'], lane['lane_number']))

    print "Collecting BAM lanes..."
    bam_lanes = []
    bam_datasets = tAPI.list('sequence_dataset', library__library_type="SC_WGS", dataset_type="BAM")
    for bam_dataset in bam_datasets:
        for lane in bam_dataset['sequence_lanes']:
            bam_lanes.append((lane['flowcell_id'], lane['lane_number']))

    print "Checking lanes..."
    for lane in bam_lanes:
        if lane not in hmmcopy_lane_inputs:
            print "Data not run with hmmcopy for flowcell_id {}, lane_number {}".format(
                lane[0], lane[1])

    print "Checking fastq..."
    fastq_datasets = tAPI.list('sequence_dataset', library__library_type="SC_WGS", dataset_type="FQ")
    for fastq_dataset in fastq_datasets:
        for lane in fastq_dataset['sequence_lanes']:
            if (lane['flowcell_id'], lane['lane_number']) not in bam_lanes:
                print "Unaligned data for library_id {}, flowcell_id {}, lane_number {}".format(
                    fastq_dataset['library']['library_id'], lane['flowcell_id'], lane['lane_number'])

    """
    raise
    print "Collecting fastq..."
    output_text = open("unanalyzed_data.txt", "w+")
    fastq_datasets = tAPI.list('sequence_dataset',library__library_type="SC_WGS", dataset_type="FQ")

    for fastq_dataset in fastq_datasets:
        if not fastq_dataset['sequence_lanes']:
            output_text.write("ID: " + str(fastq_dataset['id']) + " has no lanes\n")
            unanalyzed_fastq_id.append(fastq_dataset['id'])
        else:
            fastq_lanes[fastq_dataset['id']] = fastq_dataset['sequence_lanes']





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
    """

if __name__ == '__main__':
    init()


