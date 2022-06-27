import re
import os

from dbclients.tantalus import TantalusApi
from dbclients.colossus import ColossusApi
colossus_api = ColossusApi()
tantalus_api = TantalusApi()

file_instances = tantalus_api.list(
    'file_instance',
    storage__name="singlecellblob",
    is_deleted=True)
i = 1
for file_instance_i in file_instances:
	if i % 1000 == 0:
		print("files deleted =" + str(i))
	tantalus_api.delete('file_instance', id=file_instance_i['id'])
	i += 1