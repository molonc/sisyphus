from datetime import datetime, timedelta
import urllib.parse

def get_today():
	"""
	Return today's date
	"""
	return datetime.today()

def get_last_n_days(days):
	"""
	Get n days before today.
	For example, for n=10, today=May 11th, return May 1st
	"""
	diff = timedelta(days=days)

	last_date = get_today() - diff

	return last_date

def build_url(base_url, path, args_dict=None):
	"""
	Build valid URL from parts

	Input:
		base_url: Base URL
		path: path to append to base URL
		args_dict: query string in dictionary format
	"""
	# urllib.parse.urlparse returns ParseResult object
	url_parts = list(urllib.parse.urlparse(base_url))
	url_parts[2] = path
	if(args_dict is not None):
		url_parts[4] = urllib.parse.urlencode(args_dict)

	return urllib.parse.urlunparse(url_parts)

def validate_mode(mode):
	"""
	Validate user specified mode is valid.
	Mode must be one of development, testing, staging, or production.
	"""
	if not (mode in ['development', 'testing', 'staging', 'production']):
		raise ValueError("Invalid mode. Must be one of development, testing, staging, or production!")