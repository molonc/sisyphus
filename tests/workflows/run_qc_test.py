import pytest

from workflows.run_qc import (
	generate_alhena_loader_projects_cli_args,
	generate_loader_command,
)

from tests.workflows.fixtures.run_qc_fixtures import (
	invalid_alhena_project_args,
	valid_loader_args,
	empty_loader_args,
	jira,
)

def test_generate_alhena_cli_args_empty(invalid_alhena_project_args):
	empty_expected = ''
	empty_obs = generate_alhena_loader_projects_cli_args(invalid_alhena_project_args)

	assert (empty_obs == empty_expected)

def test_generate_loader_command(jira, valid_loader_args):
	obs = generate_loader_command(jira, valid_loader_args)
	expected = 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 "--project DLP --project Brugge"'

	assert (obs == expected)

def test_generate_loader_command_empty_projects(jira, empty_loader_args):
	obs = generate_loader_command(jira, empty_loader_args)
	expected = 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8'

	assert (obs == expected)