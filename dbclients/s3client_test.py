import datetime
import os
import unittest

import dateutil

from .tantalus import S3StorageClient, IncorrectFileMode


class TestS3StorageClient(unittest.TestCase):

    def setUp(self):
        self.data = "The quick brown fox jumps over the lazy dog"

        self.local_name = 'testfile'

        self.key = 'testfile'

        self.file_size = 43

        self.bucketname = 'testbucketscp'

        with open(self.local_name, "wt") as output:
            output.write(self.data)

    def tearDown(self):
        os.remove(self.local_name)

    def test_create(self):
        s3client = S3StorageClient(self.bucketname, self.bucketname)

        if s3client.exists(self.key):
            s3client.delete(self.key)

        s3client.create(self.key, self.local_name)

        size = s3client.get_size(self.key)

        self.assertEqual(size, self.file_size)

    def test_create_time(self):

        s3client = S3StorageClient(self.bucketname, self.bucketname)

        if s3client.exists(self.key):
            s3client.delete(self.key)

        time_now = datetime.datetime.now()

        s3client.create(self.key, self.local_name)

        create_time = s3client.get_created_time(self.key)
        create_time = dateutil.parser.parse(create_time).replace(tzinfo=None)

        delta = create_time - time_now
        delta = delta.total_seconds()

        self.assertLessEqual(delta, 30)

    def test_delete(self):

        s3client = S3StorageClient(self.bucketname, self.bucketname)

        if s3client.exists(self.key):
            s3client.delete(self.key)
            self.assertEqual(s3client.exists(self.key), False)
        else:
            s3client.create(self.key, self.local_name)
            self.assertEqual(s3client.exists(self.key), False)
            s3client.delete(self.key)
            self.assertEqual(s3client.exists(self.key), False)

    def test_open_file(self):
        s3client = S3StorageClient(self.bucketname, self.bucketname)

        if not s3client.exists(self.key):
            s3client.create(self.key, self.local_name)

        data = s3client.open_file('testfile').readlines()

        self.assertEqual(len(data), 1)

        data = data[0].strip().decode()

        self.assertEqual(data, self.data)

    def test_list(self):
        s3client = S3StorageClient(self.bucketname, self.bucketname)

        if not s3client.exists(self.key):
            s3client.create(self.key, self.local_name)

        files = [filename for filename in s3client.list("")]
        self.assertEqual(files, [self.key])

        files = [filename for filename in s3client.list("somethingelse")]
        self.assertEqual(files, [])

        files = [filename for filename in s3client.list(self.key[:3])]
        self.assertEqual(files, [self.key])

    def test_write_data(self):
        s3client = S3StorageClient(self.bucketname, self.bucketname)

        if s3client.exists(self.key):
            s3client.delete(self.key)

        try:
            with open(self.local_name, 'rb') as infile:
                s3client.write_data(self.key, infile)
        except IncorrectFileMode:
            pass
        except:
            self.assertEqual(0, 1)

        with open(self.local_name, 'rb') as infile:
            s3client.write_data(self.key, infile)

        data = s3client.open_file(self.key).readlines()
        self.assertEqual(len(data), 1)
        data = data[0].strip().decode()
        self.assertEqual(data, self.data)


if __name__ == '__main__':
    suite = unittest.TestSuite()
    loader = unittest.TestLoader()
    tests = loader.loadTestsFromTestCase(TestS3StorageClient)
    suite.addTests(tests)
    runner = unittest.TextTestRunner(verbosity=2)
    runner.run(suite)
