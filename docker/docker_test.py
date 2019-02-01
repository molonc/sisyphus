from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi

import time

if __name__ == '__main__':
    print "TANTALUS CREATING..."
    tantalus_api = TantalusApi()
    print "COLOSSUS CREATING..."
    colossus_api = ColossusApi()

    instances = tantalus_api.list("file_instance")
    for instance in instances:
        print instance["filepath"]



