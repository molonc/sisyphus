import os

from constants.url_constants import (
	DEFAULT_TANTALUS_BASE_URL,
	DEFAULT_TANTALUS_API_URL,
	DEVELOPMENT_TANTALUS_BASE_URL,
	DEVELOPMENT_TANTALUS_API_URL,
	TESTING_TANTALUS_BASE_URL,
	TESTING_TANTALUS_API_URL,
	STAGING_TANTALUS_BASE_URL,
	STAGING_TANTALUS_API_URL,
	DEFAULT_COLOSSUS_BASE_URL,
	DEFAULT_COLOSSUS_API_URL,
	DEVELOPMENT_COLOSSUS_BASE_URL,
	DEVELOPMENT_COLOSSUS_API_URL,
	TESTING_COLOSSUS_BASE_URL,
	TESTING_COLOSSUS_API_URL,
	STAGING_COLOSSUS_BASE_URL,
	STAGING_COLOSSUS_API_URL,
)

# get mode from environment variable. Production by default
mode = os.environ.get("MODE", "production")
mode = mode.lower()

def get_tantalus_base_url():
	"""
	Get Tantalus base URL based on the mode.
	Mode can be one of 'development', 'testing', 'staging', and 'production'
	"""
	# by default, use production server
	if(mode not in ["production", "staging", "development", "testing"]):
		raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")

	# use different api endpoint depending on mode
	if(mode == "development"):
		TANTALUS_BASE_URL = DEVELOPMENT_TANTALUS_API_URL 
	elif(mode == "testing"):
		TANTALUS_BASE_URL = TESTING_TANTALUS_API_URL
	elif(mode == "staging"):
		TANTALUS_BASE_URL = STAGING_TANTALUS_API_URL
	elif(mode == "production"):
		TANTALUS_BASE_URL = DEFAULT_TANTALUS_API_URL
	else:
		raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")

	return TANTALUS_BASE_URL

def get_tantalus_api_url():
	"""
	Get Tantalus API URL based on the mode.
	Mode can be one of 'development', 'testing', 'staging', and 'production'
	"""
	# by default, use production server
	if(mode not in ["production", "staging", "development", "testing"]):
		raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")

	# use different api endpoint depending on mode
	if(mode == "development"):
		TANTALUS_API_URL = DEVELOPMENT_TANTALUS_API_URL 
	elif(mode == "testing"):
		TANTALUS_API_URL = TESTING_TANTALUS_API_URL
	elif(mode == "staging"):
		TANTALUS_API_URL = STAGING_TANTALUS_API_URL
	elif(mode == "production"):
		TANTALUS_API_URL = DEFAULT_TANTALUS_API_URL
	else:
		raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")

	return TANTALUS_API_URL

def get_colossus_base_url():
	"""
	Get Colossus base URL based on the mode.
	Mode can be one of 'development', 'testing', 'staging', and 'production'
	"""
	# by default, use production server
	if(mode not in ["production", "staging", "development", "testing"]):
		raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")

	# use different api endpoint depending on mode
	if(mode == "development"):
		COLOSSUS_BASE_URL = DEVELOPMENT_COLOSSUS_BASE_URL 
	elif(mode == "testing"):
		COLOSSUS_BASE_URL = TESTING_COLOSSUS_BASE_URL
	elif(mode == "staging"):
		COLOSSUS_BASE_URL = STAGING_COLOSSUS_BASE_URL
	elif(mode == "production"):
		COLOSSUS_BASE_URL = DEFAULT_COLOSSUS_BASE_URL
	else:
		raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")

	return COLOSSUS_BASE_URL

def get_colossus_api_url():
	"""
	Get Colossus API URL based on the mode.
	Mode can be one of 'development', 'testing', 'staging', and 'production'
	"""
	# by default, use production server
	if(mode not in ["production", "staging", "development", "testing"]):
		raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")

	# use different api endpoint depending on mode
	if(mode == "development"):
		COLOSSUS_API_URL = DEVELOPMENT_COLOSSUS_API_URL 
	elif(mode == "testing"):
		COLOSSUS_API_URL = TESTING_COLOSSUS_API_URL
	elif(mode == "staging"):
		COLOSSUS_API_URL = STAGING_COLOSSUS_API_URL
	elif(mode == "production"):
		COLOSSUS_API_URL = DEFAULT_COLOSSUS_API_URL
	else:
		raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")

	return COLOSSUS_API_URL
