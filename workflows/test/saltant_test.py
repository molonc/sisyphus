import os
import sys

from workflows.utils import saltant_utils
from workflows.utils import file_utils

def main():

	config_filename = '/home/prod/sisyphus/workflows/config/normal_config.json'
	config = file_utils.load_json(config_filename)
	print("testing saltant")
	saltant_utils.test("testing", config)

if __name__ == "__main__":
	main()
