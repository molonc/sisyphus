import pytest

@pytest.fixture
def invalid_alhena_project_args():
	return [
		{'id': 1, 'name': 'foo'},
		{'id': 2, 'name': 'bar'},
		{'id': 3, 'name': 'doe'},
	]

@pytest.fixture
def project_args():
	return '--project DLP --project Brugge'

@pytest.fixture
def empty_project_args():
	return ""

@pytest.fixture
def reload_args():
	return True

@pytest.fixture
def no_reload_args():
	return False

@pytest.fixture
def filter_args():
	return True

@pytest.fixture
def no_filter_args():
	return False

@pytest.fixture
def jira():
	return 'SC-1111'