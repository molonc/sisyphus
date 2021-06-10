import os
from slack_sdk import WebClient

# User IDs
DMIN_ID = 'U01H3GRGMNF'
SBEATTY_ID = 'UMK7Z7C30'

class SlackClient(object):
	"""
	Initialize Slack Bot Client.
	Requries
		1. slack_sdk package
		2. Slack App needs to be set up (https://api.slack.com/apps?new_app=1)
		3. 'SLACK_BOT_TOKEN' environment variable
	"""
	def __init__(self):
		self.token = os.environ.get("SLACK_BOT_TOKEN", None)

		if(self.token is None):
			raise ValueError("Please obtain 'Bot User OAuth Token', and set 'SLACK_BOT_TOKEN' environment variable.")

		self.client = WebClient(token=self.token)

	def post(self, text, channel="yvr-production-status"):
		# Mention people of interest
		text = " ".join([f"<@{DMIN_ID}>", f"<@{SBEATTY_ID}>", text])
		self.client.chat_postMessage(channel=channel, text=text)