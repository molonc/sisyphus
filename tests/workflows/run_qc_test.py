import pytest

from workflows.run_qc import (
	generate_alhena_loader_projects_cli_args,
	generate_loader_command,
)

from tests.workflows.fixtures.run_qc_fixtures import (
	invalid_alhena_project_args,
	project_args,
	empty_project_args,
	reload_args,
	no_reload_args,
	filter_args,
	no_filter_args,
	jira,
)

def test_generate_alhena_cli_args_empty(invalid_alhena_project_args):
	empty_expected = ''
	empty_obs = generate_alhena_loader_projects_cli_args(invalid_alhena_project_args)

	assert (empty_obs == empty_expected)

def test_generate_loader_command_projects(jira, project_args, no_reload_args, no_filter_args):
	obs = generate_loader_command(jira, project_args, no_reload_args, no_filter_args)
	expected = 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 false false "--project DLP --project Brugge"'

	assert (obs == expected)

def test_generate_loader_command_empty_projects(jira, empty_project_args, no_reload_args, no_filter_args):
	obs = generate_loader_command(jira, empty_project_args, no_reload_args, no_filter_args)
	expected = 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 false false'

	assert (obs == expected)

def test_generate_loader_command_reload(jira, empty_project_args, reload_args, no_filter_args):
	obs = generate_loader_command(jira, empty_project_args, reload_args, no_filter_args)
	expected = 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 true false'

	assert (obs == expected)

def test_generate_loader_command_no_reload(jira, empty_project_args, no_reload_args, no_filter_args):
	obs = generate_loader_command(jira, empty_project_args, no_reload_args, no_filter_args)
	expected = 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 false false'

	assert (obs == expected)

def test_generate_loader_command_filter(jira, empty_project_args, no_reload_args, filter_args):
	obs = generate_loader_command(jira, empty_project_args, no_reload_args, filter_args)
	expected = 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 false true'

	assert (obs == expected)

def test_generate_loader_command_no_filter(jira, empty_project_args, no_reload_args, no_filter_args):
	obs = generate_loader_command(jira, empty_project_args, no_reload_args, no_filter_args)
	expected = 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 false false'

	assert (obs == expected)