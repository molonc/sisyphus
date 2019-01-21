import os
import sys
from dbclients.tantalus import TantalusApi


def init():
    print "STARTING, NOT DELETING"

    tantalus_api = TantalusApi()
    tantalus_filecheck_result = open("tantalus_filepath_check_result.txt", "w+")
    fail_flag = False

    print "Collecting File Resources..."

    file_resources = tantalus_api.list('file_resource', fileinstance__storage__name="shahlab")

    print "File Resources collected..."

    assert file_resources, "Empty File Resources is empty"

    shahlabclient = tantalus_api.get_storage_client("shahlab")
    singlecellblob = tantalus_api.get_storage_client("singlecellblob")

    print "Blob Storage collected..."

    for file_resource in file_resources:
        file_instances = {}
        date_flag = False
        for file_instance in file_resource["file_instances"]:
            file_instances[file_instance['storage']['name']] = file_instance

        print "Checking " + file_instances['shahlab']['filepath']
        if shahlabclient.exists(file_instances['shahlab']['filepath']):
            if  shahlabclient.get_size(file_instances['shahlab']["filepath"]) != file_resource["size"] :
                print "Expected size do not match the actual size!"

                tantalus_filecheck_result.write(
                    "\n ERROR: Size mismatch in: " + file_instances['shahlab']['filepath'] +
                    "\nActual Size: " + str(shahlabclient.get_size(file_instances['shahlab']['filepath'])) +
                    "\nExpected Size: " + str(file_resource["size"]) + "\n"
                )

                tantalus_filecheck_result.write("Tantalus: ")
                tantalus_filecheck_result.write(file_resource["created"])
                tantalus_filecheck_result.write("\nShahlab: ")
                tantalus_filecheck_result.write(shahlabclient.get_created_time(file_instances['shahlab']['filepath']))

                if 'singlecellblob' in file_instances.keys():
                    file_instances['singlecellblob']['filepath'] =  file_instances['singlecellblob']['filepath'].replace("singlecelldata/data/","")
                    if singlecellblob.exists(file_instances['singlecellblob']['filepath']):
                        tantalus_filecheck_result.write("\nSinglecellBlob: ")
                        tantalus_filecheck_result.write(singlecellblob.get_created_time(file_instances['singlecellblob']['filepath']))
                        if singlecellblob.get_created_time(file_instances['singlecellblob']['filepath']) == file_resource["created"]:
                            date_flag = True

                if file_resource["created"] > shahlabclient.get_created_time(file_instances['shahlab']['filepath']) or date_flag:
                    tantalus_filecheck_result.write("\nDeleted outdated filepath on Tantalus ")
                    print "Removed outdated filepath from tantalus, and file from shahlab"
                    # tantalus_api.delete("file_instance", file_instances['shahlab']["id"])
                    # shahlabclient.delete(file_instances['shahlab']['filepath'])
                tantalus_filecheck_result.write("\n")
                fail_flag = True


            else:
                print "Passed"
        else:
            print "file path does not exist or not valid"
            tantalus_filecheck_result.write("ERROR: " + file_instances['shahlab']['filepath'] + " is not a valid filepath \n")
            fail_flag = True
            try:
                pass
               #tantalus_api.delete("file_instance", file_instances['shahlab']["id"])
            except:
                print "Deletion failed. Ignoring"
                continue


    tantalus_filecheck_result.close()

    print "TEST" + " FAILED! PLEASE CHECK THE tantalus_filepath_check_result.txt" if fail_flag else " PASSED!"


if __name__ == '__main__':
    init()



