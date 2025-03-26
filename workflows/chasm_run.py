import subprocess
import os
import shutil
import glob

#log = logging.getLogger('sisyphus')
#log.setLevel(logging.DEBUG)
#stream_handler = logging.StreamHandler()
#formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')



def download_metrics(jira):
	try:
		a= f"https://singlecellresults.blob.core.windows.net/results/{jira}/results/annotation/?sv=2020-10-02&st=2022-08-19T16%3A27%3A54Z&se=2050-01-20T17%3A27%3A00Z&sr=c&sp=rl&sig=0k4%2FjNHlASI9d4ydoPEeCmIbJ2ehiodZV6kCHnOxUz4%3D"
		subprocess.run([
		"azcopy",
		"copy",
                a,
                f"/home/prod/sisyphus/workflows/chasmbot/{jira}",
		"--include-pattern",
		"*metrics.csv.gz",
		"--recursive"
		])


		for path in glob.glob(f"/home/prod/sisyphus/workflows/chasmbot/{jira}/annotation/*metrics.csv.gz"):
			shutil.move(path, "/home/prod/sisyphus/workflows/chasmbot")

	except Exception as e:
		raise Exception(f"failed to download :{e}")

def chasmbot_analysis(jira):
	try:
		output=subprocess.run(["Rscript",
			"/home/prod/sisyphus/workflows/chasmbot/compute_recovery.R"],capture_output=True,text=True).stdout.strip("\n")
		return output

	except Exception as e:
		raise Exception(f"failed to check status of {name} in {resourcegroupname}: {e}")

def post_to_jira(jira, output):
	parent = get_parent_issue(jira)
	comment_jira(parent,output)


def clean_up(jira):
	shutil.rmtree(f"/home/prod/sisyphus/workflows/chasmbot/{jira}")
	for path in glob.glob("/home/prod/sisyphus/workflows/chasmbot/*metrics.csv.gz"):
		os.remove(path)

def chasmbot_run(jira):
	download_metrics(jira)
	output = chasmbot_analysis(jira)
	#print(output)
	post_to_jira(jira,output)
	clean_up(jira)

if __name__ == "__main__":
	chasmbot_run("SC-7995")
