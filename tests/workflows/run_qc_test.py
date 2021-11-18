import pytest

from workflows.run_qc import (
	generate_alhena_loader_projects_cli_args,
	generate_loader_command,
)


@pytest.mark.parametrize(
	"projects,expected",
	[
		([
			{'id': 1, 'name': 'foo'},
			{'id': 2, 'name': 'bar'},
			{'id': 3, 'name': 'doe'},
		], ''),
		([
			{'id': 1, 'name': 'Collab-Brugge'},
			{'id': 2, 'name': 'bar'},
			{'id': 3, 'name': 'DLP'},
		], '--view Brugge --view DLP'),
		([
			{'id': 1, 'name': 'DLP'},
			{'id': 2, 'name': 'Collab-Brugge'},
		], '--view Brugge --view DLP'),
	],
)
def test_generate_alhena_loader_projects_cli_args(projects, expected):
	observed = generate_alhena_loader_projects_cli_args(projects)

	# split by --view, and sort to make sure correct projects are generated
	observed_list = observed.split('--view').sort()
	expected_list = expected.split('--view').sort()
	assert observed_list == expected_list

@pytest.mark.parametrize(
	"jira,project_args,reload_args,filter_args,expected",
	[
		('SC-1111', '--view DLP --view Brugge', False, False, 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 false false "--view DLP --view Brugge"'),
		('SC-1111', '', False, False, 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 false false'),
		('SC-1111', '', True, False, 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 true false'),
		('SC-1111', '', False, False, 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 false false'),
		('SC-1111', '', False, True, 'bash /home/spectrum/alhena-loader/load_ticket.sh SC-1111 10.1.0.8 false true'),
	],
)
def test_generate_loader_command(jira, project_args, reload_args, filter_args, expected):
	observed = generate_loader_command(jira, project_args, reload_args, filter_args)

	assert observed == expected