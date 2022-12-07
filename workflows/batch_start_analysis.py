import csv
import subprocess

with open('./hap_analysis.csv', newline='') as csvfile:
	reader = csv.DictReader(csvfile)
	for row in reader:
		a =row['analysis_id']
		t = "hap_count"
		j = row['jira_id']
		v = "v0.8.18"
		l = row['library_id']
		n = row['aligner']
		command=["python", "/home/prod/sisyphus/workflows/start_analysis.py" ,a,t,j,v,l,n] 
		subprocess.run(command)

