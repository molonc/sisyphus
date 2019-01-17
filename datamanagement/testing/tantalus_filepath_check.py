import os
import sys
from dbclients.tantalus import TantalusApi


def init():
    print "STARTING"

    tantalus_api = TantalusApi()
    tantalus_filecheck_result = open("tantalus_filepath_check_result.txt", "w+")
    file_instances = {}
    fail_flag = False

    print "Collecting File Resources..."

    file_resources = tantalus_api.list('file_resource', fileinstance__storage__name="shahlab")

    print "File Resources collected..."

    assert file_resources, "Empty File Resources is empty"

    shahlabclient = tantalus_api.get_storage_client("shahlab")
    singlecellblob = tantalus_api.get_storage_client("singlecellblob")

    print "Blob Storage collected..."

    for file_resource in file_resources:
        for file_instance in file_resource["file_instances"]:
            file_instances[file_instance['storage']['name']] = file_instance

            if file_instance['storage']['name'] ==  "shahlab":
                print "Checking " + file_instance['filepath'] + "... \n"

                if shahlabclient.exists(file_instance['filepath']):
                    if  shahlabclient.get_size(file_instance["filepath"]) != file_resource["size"] :
                        print "Expected size do not match the actual size!"

                        tantalus_filecheck_result.write(
                            "\n ERROR: Size mismatch in: " + file_instance['filepath'] +
                            "\nActual Size: " + str(shahlabclient.getsize(file_instance["filepath"])) +
                            "\nExpected Size: " + str(file_resource["size"]) + "\n"
                        )

                        tantalus_filecheck_result.write("Tantalus: ")
                        tantalus_filecheck_result.write(file_resource["created"])
                        tantalus_filecheck_result.write("\nShahlab: ")
                        tantalus_filecheck_result.write(shahlabclient.get_created_time(file_instances['shahlab']['filepath']))

                        if 'singlecellblob' in file_instances.keys() and singlecellblob.exists(
                                file_instances['singlecellblob']['filepath']):
                            tantalus_filecheck_result.write("\nSinglecellBlob: ")
                            tantalus_filecheck_result.write(singlecellblob.get_created_time(file_instances['singlecellblob']['filepath']))

                        tantalus_filecheck_result.write("\n")
                        fail_flag = True

                    else:
                        print "Passed"
                else:
                    print "file path does not exist or not valid"
                    tantalus_filecheck_result.write("DELETED: " + file_instance['filepath'] + " is not a valid filepath \n")
                    ail_flag = True
                    try:
                        tantalus_api.delete("file_instance", file_instance["id"])
                    except:
                        print "Deletion failed. Ignoring"
                        continue
            else:
                pass


    tantalus_filecheck_result.close()

    print "TEST" + " FAILED! PLEASE CHECK THE tantalus_filepath_check_result.txt" if fail_flag else " PASSED!"


if __name__ == '__main__':
    init()



