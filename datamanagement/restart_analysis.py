import os
import glob
import click
<<<<<<< HEAD
from dbclients.colossus import ColossusApi
@click.command()
@click.argument('analysis_ids', type=int, nargs=-1)
@click.option("--update", is_flag=True, show_default=False, default=False, help="Flag to apply update term to analysis")
def main(
    analysis_ids,
	update
=======
@click.command()
@click.argument('analysis_ids', type=int, nargs=-1)
def main(
    analysis_ids
>>>>>>> 3c6d8700328837ab9f54c082a5bcd0b0eda868c0
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
<<<<<<< HEAD
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
=======
		  print("An error has occured could not update analyis " + str(analysis_i))

		try:
			command = "python /home/prod/sisyphus/workflows/scripts/start_saltant_analyses.py from-ids prod prod " + str(analysis_i) + " --update"
			os.system(command)
		except:
		  print("An error has occured could not start analyis " + str(analysis_i))

if __name__ == "__main__":
    main()
>>>>>>> 3c6d8700328837ab9f54c082a5bcd0b0eda868c0
