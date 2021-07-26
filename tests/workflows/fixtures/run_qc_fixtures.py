import pytest

@pytest.fixture
def invalid_alhena_project_args():
	return [
		{'id': 1, 'name': 'foo'},
		{'id': 2, 'name': 'bar'},
		{'id': 3, 'name': 'doe'},
	]

@pytest.fixture
def valid_loader_args():
	return '--project DLP --project Brugge'

@pytest.fixture
def empty_loader_args():
	return ""


@pytest.fixture
def jira():
	return 'SC-1111'