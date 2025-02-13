import os
import glob
import click
from dbclients.colossus import ColossusApi
from dbclients.tantalus import TantalusApi

@click.command()
@click.argument('analysis_ids', type=int, nargs=-1)
@click.option("--update", is_flag=True, show_default=False, default=False, help="Flag to apply update term to analysis")
@click.option("--clear", is_flag=True, show_default=False, default=False, help="Flag to clear all previously generated files associated with analysis id, does not clear from azure singlecelltemp blob")
def main(
    analysis_ids,
	update,
	clear
):
	for analysis_i in analysis_ids:
		locks_path = glob.glob("/home/prod/saltant/singlecelllogs/pipeline/analysis_" + str(analysis_i) + "/*/locks")
		if len(locks_path) > 0:
			command = "sudo rm -rf " + locks_path[0] + "/*"
			os.system(command)

		#Try to delete all previous files associated with analysis id
		if clear:
			#delete associated folder and files from /home/sc_runs
			analysis = tantalus_api.get("analysis", id=11434)
			jira_ticket = analysis['jira_ticket']
			#command = "sudo rm -rf" + os.path.join("/home/sc_runs", str(analysis_id))
			command = "rm -r " + os.path.join("/home/prod/sc_runs", str(jira_ticket))
			os.system(command)

			#delete associate folder and files from /home/prod/saltant/singlecelllogs/pipeline
			command = "sudo rm -rf " + os.path.join("/home/prod/saltant/singlecelllogs/pipeline", "analysis_" + str(analysis_i))
			os.system(command)

			#delete associate folder and files from /home/prod/saltant/singlecelltemp/temp
			command = "sudo rm -rf " + os.path.join("/home/prod/saltant/singlecelltemp/temp", "analysis_" + str(analysis_i))
			os.system(command)
		try:
			command = "python /home/prod/sisyphus/workflows/scripts/update_analyses.py  --status ready " + str(analysis_i)
			os.system(command)
		except:
			print("An error has occured could not update analyis " + str(analysis_i))
		
		if update:
			try:
				command = "python /home/prod/sisyphus/workflows/scripts/start_saltant_analyses.py from-ids prod prod " + str(analysis_i) + " --update"
				os.system(command)
			except:
				print("An error has occured could not start analyis " + str(analysis_i))
		else:
			try:
				command = "python /home/prod/sisyphus/workflows/scripts/start_saltant_analyses.py from-ids prod prod " + str(analysis_i)
				os.system(command)
			except:
				print("An error has occured could not start analyis " + str(analysis_i)) 


if __name__ == "__main__":
    main()

colossus_api = ColossusApi()
tantalus_api = TantalusApi()

test = tantalus_api.get("analysis", id=11434)
jira_ticket = test['jira_ticket']

#jira_i = "SC-7632"
#analysis = colossus_api.get("analysis_information", analysis_jira_ticket=jira_i)

#analysis_run_id = analysis["analysis_run"]["id"]
#analysis_run = colossus_api.get(
#	"analysis_run",
#	id=analysis_run_id,
#)
#colossus_api.update(
#	"analysis_run",
#	id=analysis_run_id,
#	run_status="idle",
#)
