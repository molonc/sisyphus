from datetime import datetime
import urllib.parse

def get_today():
	"""
	Return today's date
	"""
	return datetime.today()

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