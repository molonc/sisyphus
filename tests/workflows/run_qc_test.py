import unittest

from workflows.run_qc import (
	generate_alhena_loader_projects_cli_args,
)

class RunQCTestCase(unittest.TestCase):
	def test_generate_alhena_cli_args(self):
		empty_args = [
			{'id': 1, 'name': 'foo'},
			{'id': 2, 'name': 'bar'},
			{'id': 3, 'name': 'doe'},
		]

		valid_args = [
			{'id': 1, 'name': 'DLP'},
			{'id': 2, 'name': 'fitness'},
			{'id': 3, 'name': 'doe'},
		]

		empty_expected = ''

		valid_expected = '--project DLP --project fitness'

		empty_obs = generate_alhena_loader_projects_cli_args(empty_args)
		valid_obs = generate_alhena_loader_projects_cli_args(valid_args)

		self.assertEqual(empty_obs, empty_expected)
		self.assertEqual(valid_obs, valid_expected)

if __name__ == '__main__':
	unittest.main()