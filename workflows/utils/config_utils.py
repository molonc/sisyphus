def update_config(config, k, v):
	"""
	Replace configuration field with user specified value.
	Assume configuration file is loaded as dictionary.
	Dictionary is mutable so don't return anything.

	config: config file loaded as dictionary.
	k: config field to edit.
	v: value to replace with
	"""
	try:
		config[k] = v
	except KeyError:
		raise("Field, '{}', does not exist in the config file".format(k))
	except (TypeError, AttributeError) as err:
		raise("Config file needs to be loaded as dictionary...")