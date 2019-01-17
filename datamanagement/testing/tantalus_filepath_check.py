import os
import sys
from dbclients.tantalus import TantalusApi


def init():
    print "STARTING"

    tantalus_api = TantalusApi()
    tantalus_filecheck_result = open("tantalus_filepath_check_result.txt", "w+")

    fail_flag = False

    print "Collecting File Resources..."

    file_resources = tantalus_api.list('file_resource', fileinstance__storage__name="shahlab")

    print "File Resources collected..."

    assert file_resources, "Empty File Resources is empty"

    shahlab_blob = tantalus_api.get_storage_client("shahlab")
    singlecell_blob = tantalus_api.get_storage_client("singlecellblob")

    print "Blob Storage collected..."

    for file_resource in file_resources:
        for file_instance in file_resource["file_instances"]:
            if file_instance['storage']['name'] ==  "shahlab":
                print "Checking " + file_instance['filepath'] + "... \n"
                print file_resource["created"]
                if shahlab_blob.exists(file_instance['filepath']):
                    if  shahlab_blob.get_size(file_instance["filepath"]) != file_resource["size"] :
                        print "Expected size do not match the actual size!"
                        tantalus_filecheck_result.write(
                            "ERROR: Size mismatch in: " + file_instance['filepath'] +
                            "\nActual Size: " + str(os.path.getsize(file_instance["filepath"])) +
                            "\nExpected Size: " + str(file_resource["size"]) + "\n"
                        )
                        tantalus_filecheck_result.write("Tantalus: ")
                        tantalus_filecheck_result.write(file_resource["created"])
                        tantalus_filecheck_result.write("\nShahlab: ")
                        tantalus_filecheck_result.write(shahlab_blob.get_created_time(file_instance["filepath"]))
                        tantalus_filecheck_result.write("\nSinglecellBlob: ")
                        tantalus_filecheck_result.write(singlecell_blob.get_created_time(file_instance["filepath"]))
                        tantalus_filecheck_result.write("\n")
                        fail_flag = True

                    else:
                        print "Passed"
                else:
                    print "file path does not exist or not valid"
                    tantalus_filecheck_result.write("DELETED: " + file_instance['filepath'] + " is not a valid filepath \n")
                    try:
                        tantalus_api.delete("file_instance", file_instance["id"])
                    except:
                        print "Deletion failed. Ignoring"
                        continue
                    fail_flag = True
            else:
                pass


    # tantalus_filecheck_result.write("\n\n\n ---------- AZURE --------- \n\n\n")
    #
    # print "Testing filepath on Azure now"
    # for storage in storage_to_test_blob:
    #     file_instances = tantalus_api.list('file_instance', storage__name=storage['name'])
    #
    #     assert file_instances, storage['name'] + " is empty"
    #
    #     for file_instance in file_instances:
    #         if "singlecelldata/data" in file_instance['filepath']:
    #             print "Checking filepath: " + str(file_instance['filepath'])
    #             file_resource = tantalus_api.get('file_resource', id=file_instance['file_resource'])
    #             if not blob_storage.exists(file_resource['filename']):
    #                 print "Invalid filepath!"
    #                 tantalus_filecheck_result.write("ERROR: " + file_instance['filepath'] + " is not a valid filepath \n")
    #                 fail_flag = True
    #
    #             else:
    #                 print str(file_resource['size'])
    #                 print str(blob_storage.get_size(file_resource['filename']))
    #                 if not blob_storage.get_size(file_resource['filename']) == file_resource['size']:
    #                     print "Expected size do not match the actual size!"
    #                     tantalus_filecheck_result.write("ERROR: Size mismatch in: " +
    #                                                     file_instance['filepath'] +  "\nActual Size: " +
    #                                                     str(blob_storage.get_size(file_resource['filename'])) +
    #                                                     "\nExpected Size: " + str(file_resource['size']) + "\n")
    #                     fail_flag = True
    #
    #                 else:
    #                     print "Passed"
    #

    tantalus_filecheck_result.close()

    print "TEST" + "FAILED! PLEASE CHECK THE tantalus_filepath_check_result.txt" if fail_flag else "PASSED!"


if __name__ == '__main__':
    init()



