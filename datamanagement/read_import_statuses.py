import os

file = open("/home/jpham/sisyphus/datamanagement/import_statuses.txt", "r")

lines = file.readlines()

for line in lines:
	print(line)