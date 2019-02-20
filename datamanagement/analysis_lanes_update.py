from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi

tantalus_api = TantalusApi()
colossus_api = ColossusApi()


if __name__ == '__main__':
    colossus_analyses = colossus_api.list('analysis_information')
    tantalus_analyses = tantalus_api.list('analysis', analysis_type__name="align")

    analysis_lane_dict = {}

    #for all analyses in tantalus, analysis type align:
    #find lanes for input datasets
    for analysis in tantalus_analyses:
        lane_set = set()
        for input_dataset in analysis['input_datasets']:
            dataset = tantalus_api.get('sequencedataset',id=input_dataset)
            for lane in dataset['sequence_lanes']:
                lane_set.add(str(lane['flowcell_id'] + "_" + str(lane['lane_number'])))

        analysis_lane_dict[analysis['name']] = lane_set
        print str(analysis['id']) + " " + analysis['name']
        print lane_set

    print analysis_lane_dict.keys()

    for analysis in colossus_analyses:
        key = analysis['analysis_jira_ticket'] + '_align'
        print "Colossus Id" + str(analysis['id'])
        print key
        if key in analysis_lane_dict.keys():
            print list(analysis_lane_dict[key])
            lanes = []

            for lane in analysis_lane_dict[key]:

                if list(colossus_api.list('lane', flow_cell_id=lane)):
                    print "searching for"
                    print lane
                    lanes.append(next(colossus_api.list('lane',flow_cell_id=lane))['id'])
                elif list(colossus_api.list('lane',flow_cell_id=lane[:-2])):
                    print "searching instead for"
                    print lane[:-2]
                    lanes.append(next(colossus_api.list('lane',flow_cell_id=lane[:-2]))['id'])
                else:
                    print "no match"
                print lanes

            colossus_api.update('analysis_information', id=analysis['id'], lanes=lanes)
        else:
            print "NONE"
            colossus_api.update('analysis_information',id=analysis['id'],lanes=[])

