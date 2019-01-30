from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
import time

if __name__ == '__main__':
    print "TANTALUS CREATING..."
    time.sleep(5)
    tantalus_api = TantalusApi()
    time.sleep(5)
    print "COLOSSUS CREATING..."
    time.sleep(5)
    colossus_api = ColossusApi()
    time.sleep(5)


