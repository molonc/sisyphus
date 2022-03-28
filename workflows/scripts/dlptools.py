import os
import glob
import click
from dbclients.colossus import ColossusApi


@click.group()
def main():
    pass

@main.command()
@click.argument('analysis_tickets', type=str, nargs=-1)
def reset_analysis_run(analysis_tickets):
    for jira_ticket_i in analysis_tickets:
        print("Setting analysis run for ticket " + jira_ticket_i + " to idle")
        colossus_api = ColossusApi()
        analysis = colossus_api.get("analysis_information", analysis_jira_ticket=jira_ticket_i)
        analysis_run_id = analysis["analysis_run"]["id"]
        colossus_api.update(
        "analysis_run",
        id=analysis_run_id,
        run_status="idle",
    )

@main.command()
@click.argument('analysis_ids', type=int, nargs=-1)
def restart_analysis(
    analysis_ids
):
	for analysis_i in analysis_ids:
		locks_path = glob.glob("/home/prod/saltant/singlecelllogs/pipeline/analysis_" + str(analysis_i) + "/*/locks")
		if len(locks_path) > 0:
			command = "sudo rm -rf " + locks_path[0] + "/*"
			os.system(command)
		try:
			command = "python /home/prod/sisyphus/workflows/scripts/update_analyses.py  --status ready " + str(analysis_i)
			os.system(command)
		except:
			print("An error has occured could not update analyis " + str(analysis_i))
		try:
			command = "python /home/prod/sisyphus/workflows/scripts/start_saltant_analyses.py from-ids prod prod " + str(analysis_i) + " --update"
			os.system(command)
		except:
		  print("An error has occured could not start analyis " + str(analysis_i))

if __name__ == "__main__":
    main()

