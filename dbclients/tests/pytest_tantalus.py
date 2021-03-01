from sisyphus.dbclients.tantalus import BlobStorageClient
import os
import azure.core.exceptions
import pytest
import random
import string
import datetime
import time
import dateutil.parser


@pytest.fixture
def blob_name():
    return 'testblob.txt'

@pytest.fixture
def client1(blob_name):
    client = BlobStorageClient('sisyphustest1', 'data', '') 
    _check_delete(client, blob_name)
    return client

@pytest.fixture
def client2(blob_name):
    client = BlobStorageClient('sisyphustest2', 'data', '') 
    _check_delete(client, blob_name)
    return client


def _random_name():
    letters = string.ascii_lowercase
    return ''.join(random.choice(letters) for i in range(random.randint(5,10))) 

def _check_delete(client, blob):
    if client.exists(blob):
        client.delete(blob)

def _raises_correct_error(function, *args,expected_error,
                        **kwargs):
    raised = False
    try:
        function(*args, **kwargs)
    except Exception as e:
        if type(e) == expected_error:
            raised = True
        else:
            print("raised wrong error: raised: {}, expected: {}"
                   .format(type(e), expected_error))
    finally:
        print(raised)
        return raised


###########################
## tests ##
##########################

def test_create_time(client1,blob_name, timeallowance=datetime.timedelta(seconds=1.0, days=0.0)):
    from pytz import timezone

    precreate_time = datetime.datetime.now()
    client1.create(blob_name, 'testfile.txt')
    # time = list(client1.list(''))[0]["creation_time"]
    date = datetime.datetime.now().astimezone(timezone('US/Pacific'))
    blobdate = dateutil.parser.isoparse(client1.get_created_time(blob_name)).astimezone(timezone('US/Pacific'))
    
    print(date, blobdate, date-blobdate < timeallowance)
    assert (date - blobdate) < timeallowance


def test_open_file(client1, blob_name):
    client1.create(blob_name, 'testfile.txt')
    content = open('testfile.txt', "r").readline()
    assert content == client1.open_file(blob_name).read().decode('utf-8')
    client1.delete(blob_name)

def test_open_empty_file(client1, blob_name):
    client1.create(blob_name, 'testfile_empty.txt')
    assert '' == client1.open_file(blob_name).read().decode('utf-8')
    client1.delete(blob_name)

def test_open_non_existant_blob(client1):
    assert _raises_correct_error(client1.open_file, blob_name,
                              expected_error=TypeError)   

def test_list(client1, blob_name):
    client1.create(blob_name, 'testfile.txt')
    list_data = list(client1.list(''))[0]
    assert list_data["lease"]["status"] == "unlocked" 
    assert list_data["lease"]["state"] == "available"
    assert list_data["deleted_time"] == None
    client1.delete(blob_name)


def test_list_multiple(client1, blob_name):
    client1.create(blob_name, 'testfile.txt')
    client1.create(blob_name + "2", 'testfile_empty.txt')
    client1.create(blob_name + "3", 'testfile_empty.txt')
    assert len(list(client1.list(''))) == 4
    client1.delete(blob_name)
    client1.delete(blob_name + "2")
    client1.delete(blob_name + "3")

def test_write(client1, blob_name):
    client1.create(blob_name, 'testfile.txt')
    client1.write_data(blob_name, open("testfile2.txt", "rb"))
    content = open("testfile2.txt", "r").readline()
    assert content == client1.open_file(blob_name).read().decode('utf-8')
    client1.delete(blob_name)


def test_download(client1, blob_name):
    client1.create(blob_name, 'testfile.txt')
    content = client1.open_file(blob_name).read().decode('utf-8')
    client1.download(blob_name, "testdownloaded.txt")
    assert content == open("testdownloaded.txt", "r").readline()
    client1.delete(blob_name)
    os.remove("testdownloaded.txt")
    assert not os.path.exists("testdownloaded.txt")


def test_download_non_existant(client1, blob_name):
    assert _raises_correct_error(client1.download,blob_name,"testdownloaded.txt",
                              expected_error= azure.core.exceptions.ResourceNotFoundError)   


def test_copy_non_existant(client1):
    assert client1.exists("non_existant_blob") == False
    assert _raises_correct_error(client1.delete,blob_name,
                              expected_error=TypeError)   


def test_copy(client1, blob_name):
    client1.create(blob_name, 'testfile.txt')
    client1.copy(blob_name, blob_name+ "_copied")
    client1.delete(blob_name)
    assert client1.exists(blob_name+ "_copied")
    client1.delete(blob_name+ "_copied")

def test_create(client1, blob_name):
    client1.create(blob_name, 'testfile.txt')
    assert client1.exists(blob_name)
    client1.delete(blob_name)

def test_create_already_exists_no_update(client1, blob_name):
    client1.create(blob_name, 'testfile_empty.txt')
    assert client1.exists(blob_name)
    assert _raises_correct_error(client1.create, blob_name,
                              expected_error=TypeError)   


def test_create_already_exists_update(client1, blob_name):
    client1.create(blob_name, 'testfile_empty.txt')
    assert client1.exists(blob_name)
    client1.create(blob_name, 'testfile.txt', update=True)
    assert client1.exists(blob_name)


def test_size(client1, blob_name):
    client1.create(blob_name, 'testfile.txt')
    assert os.path.getsize('testfile.txt') == client1.get_size(blob_name)
    client1.delete(blob_name)

def test_size_empty(client1, blob_name):
    client1.create(blob_name, "testfile_empty.txt")
    assert 0 == client1.get_size(blob_name)
    client1.delete(blob_name)


def test_delete(client1, blob_name ):
    client1.create(blob_name, "testfile.txt")
    assert client1.exists(blob_name) == True
    client1.delete(blob_name)
    assert client1.exists(blob_name) == False


def test_delete_non_existant_blob(client1,blob_name):
    client1.create(blob_name, "testfile.txt")
    client1.delete(blob_name)
    assert client1.exists(blob_name) == False
    assert _raises_correct_error(client1.delete,blob_name,
                              expected_error=azure.core.exceptions.ResourceNotFoundError)   


def test_exists(client1, client2):
    client1.create("test_blob", "testfile.txt")
    assert client1.exists("test_blob") == True


def test_empty_exists(client1, client2):
    assert client1.exists('testblob_non_existant.txt') == False
    assert client2.exists('testblob_non_existant.txt') == False
