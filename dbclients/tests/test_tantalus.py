import os
import unittest
import dbclients.tantalus


TANTALUS_API_URL = 'http://localhost:8000/api/'


class TestSum(unittest.TestCase):

    def setUp(self):
        os.environ['TANTALUS_API_URL'] =  TANTALUS_API_URL
        os.environ['TANTALUS_API_USERNAME'] = 'simong'
        os.environ['TANTALUS_API_PASSWORD'] = 'pinchpinch'

        self.tantalus_api = dbclients.tantalus.TantalusApi()

        assert 'localhost' in self.tantalus_api.base_api_url
        assert 'tantalus' not in self.tantalus_api.base_api_url

    def test_create(self):
        try:
            result = self.tantalus_api.get2('sample', {'sample_id':'TEST1'})
        except:
            result = None
            pass

        if result is not None:
            self.tantalus_api.delete('sample', id=result['id'])

        self.tantalus_api.create('sample', {'sample_id':'TEST1'}, keys=['sample_id'])

        with self.assertRaises(dbclients.basicclient.ExistsError):
            self.tantalus_api.create('sample', {'sample_id':'TEST1'}, keys=['sample_id'])

        result, _ = self.tantalus_api.create('sample', {'sample_id':'TEST1'}, keys=['sample_id'], get_existing=True)
        self.assertEqual(result['sample_id'], 'TEST1')

        with self.assertRaises(dbclients.basicclient.FieldMismatchError):
            result, _ = self.tantalus_api.create(
                'sample', {'sample_id':'TEST1', 'external_sample_id': 'TESTA'},
                keys=['sample_id'], get_existing=True)

        result, _ = self.tantalus_api.create(
            'sample', {'sample_id':'TEST1', 'external_sample_id': 'TESTA'},
            keys=['sample_id'], get_existing=True, do_update=True)
        self.assertEqual(result['sample_id'], 'TEST1')
        self.assertEqual(result['external_sample_id'], 'TESTA')


if __name__ == '__main__':
    unittest.main()