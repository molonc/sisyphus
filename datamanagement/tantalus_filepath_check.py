import sys
import os
import paramiko
import logging
import json
from dbclients.tantalus import TantalusApi
import argparse
from dbclients.basicclient import NotFoundError
from datamanagement.utils.runtime_args import parse_runtime_args
import pandas as pd
from sets import Set

id_to_test = [1, 3, 4, 6, 9, 10, 14]


if __name__ == '__main__':

    assert len(sys.argv) > 3,  "PLEASE SET A VALID HOSTNAME, USERNAME AND PASSWORD\n eg. to test on rocks, the script should run as:\n python manage.py 10.9.4.27 user123 password123  "
    ssh = paramiko.SSHClient()

    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
    try:
        ssh.connect(str(sys.argv[1]), username=str(sys.argv[2]), password=str(sys.argv[3]))
        sftp = ssh.open_sftp()
    except paramiko.SSHException:
        print("Connection Error, Please check your hostname")

    tantalus_api  = TantalusApi()
    tantalus_filecheck_result = open("tantalus_filecheck_result.txt", "w+")
    blob_storage = tantalus_api.get_storage_client('singlecellblob')
    fail_flag = False


    #Non Azure
    storage_to_test_server = []
    #Azure
    storage_to_test_blob = []

    print "Initializing on hostname: " + sys.argv[1]

    for id in id_to_test:
        storage = tantalus_api.get("storage", id=id)
        if storage['storage_type'] ==  "server":
            storage_to_test_server.append(storage)
        elif storage['storage_type'] == 'blob':
            storage_to_test_blob.append(storage)
        else:
            pass

    count = 0
    print "Collecting File Instances..."

    for storage in storage_to_test_server:
        file_instances = tantalus_api.list('file_instance', storage__name=storage['name'])

        assert \
            file_instances , \
            storage['name'] + " is empty"

        for file_instance in file_instances:
            print "Checking filepath: " + str(file_instance['filepath'])
            try:
                file_stat = sftp.stat(file_instance['filepath'])
                print str(file_stat.st_size)
                print str(tantalus_api.get('file_resource', id=file_instance['file_resource'])['size'])
                if not file_stat.st_size == \
                       tantalus_api.get('file_resource', id=file_instance['file_resource'])['size']:
                    print "Expected size do not match the actual size!"
                    tantalus_filecheck_result.write("ERROR: Size mismatch in: " + file_instance['filepath'] + \
                                                    "\nActual Size: " + str(file_stat.st_size)  + \
                                                    "\nExpected Size: " + str(tantalus_api.get('file_resource', id=file_instance['file_resource'])['size']) + "\n")
                    fail_flag = True

                else:
                    print "Passed"

            except IOError:
                print "Invalid filepath!"
                tantalus_filecheck_result.write("ERROR: " + file_instance['filepath'] + " is not a valid filepath \n")
                fail_flag = True
                continue

    ssh.close()

    for storage in storage_to_test_blob:
        file_instances = tantalus_api.list('file_instance', storage__name=storage['name'])

        assert \
            file_instances, \
            storage['name'] + " is empty"

        for file_instance in file_instances:
            print "Checking filepath: " + str(file_instance['filepath'])
            file_resource = tantalus_api.get('file_resource', id=file_instance['file_resource'])
            if not blob_storage.exists(file_resource['filename']):
                print "Invalid filepath!"
                tantalus_filecheck_result.write("ERROR: " + file_instance['filepath'] + " is not a valid filepath \n")
                fail_flag = True

            else:
                print str(file_resource['size'])
                print str(blob_storage.get_size(file_resource['filename']))
                if not blob_storage.get_size(file_resource['filename']) == file_resource['size']:
                    print "Expected size do not match the actual size!"
                    tantalus_filecheck_result.write("ERROR: Size mismatch in: " + file_instance['filepath'] + \
                                                    "\nActual Size: " + str(blob_storage.get_size(file_resource['filename'])) + \
                                                    "\nExpected Size: " + str(file_resource['size']) + "\n")
                    fail_flag = True

                else:
                    print "Passed"



    tantalus_filecheck_result.close()


    print "TEST" + "FAILED! PLEASE CHECK THE tantalus_filecheck_result.txt" if fail_flag else "PASSED!"

