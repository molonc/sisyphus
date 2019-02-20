from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
import logging


tantalus_api = TantalusApi()
colossus_api = ColossusApi()


if __name__ == '__main__':
    print "STARTING"
    colossus_analyses = colossus_api.list('analysis_information')
    tantalus_analyses = tantalus_api.list('analysis', analysis_type__name="align")

    analysis_lane_dict = {}

    for analysis in tantalus_analyses:
        lane_set = set()
        for input_dataset in analysis['input_datasets']:
            dataset = tantalus_api.get('sequencedataset',id=input_dataset)
            for lane in dataset['sequence_lanes']:
                lane_set.add(str(lane['flowcell_id'] + "_" + str(lane['lane_number'])))

        analysis_lane_dict[analysis['name']] = lane_set

    print analysis_lane_dict

    for analysis in colossus_analyses:
        key = analysis['analysis_jira_ticket'] + '_align'
        if key in analysis_lane_dict.keys():
            lanes = []
            print "ID" + str(analysis['id'])
            for lane in analysis_lane_dict[key]:
                try:
                    print "Getting lane: " + lane
                    colossus_api.get('lane', flow_cell_id=lane)
                    lanes.append(colossus_api.get('lane',flow_cell_id=lane)['id'])
                except Exception,e:
                    try:
                        print e
                        print "Creating lane: " + lane
                        for sequencing in analysis['library']['dlpsequencing_set']:
                            print "Sequence ID" + str(sequencing['id'])
                            for flow_lane in sequencing['dlplane_set']:
                                print "Lane ID: " + flow_lane['flow_cell_id']
                                if flow_lane['flow_cell_id'] == lane[:-2]:
                                   print "ID: " + str(sequencing['id'])
                                   colossus_api.get_or_create('lane', flow_cell_id=lane, path_to_archive="", sequencing=sequencing['id'])
                                   break

                        print "New ID: " + str(colossus_api.get('lane', flow_cell_id=lane)['id'])
                        lanes.append(colossus_api.get('lane', flow_cell_id=lane)['id'])

                    except Exception,e:
                        print "Error, not found and failed to create"
                        print e
                        lanes= []
                        break

            print lanes
            colossus_api.update('analysis_information', id=analysis['id'], lanes=lanes)


