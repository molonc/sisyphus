from google.oauth2.service_account import Credentials
import gspread
import string
import os


scope = [
    'https://spreadsheets.google.com/feeds',
    'https://www.googleapis.com/auth/drive',
]


def update_gsheet(sheet_name, tab_name, data):
    credentials = Credentials.from_service_account_file(os.environ['GOOGLE_AUTH_FILE'], scopes=scope)
    gc = gspread.authorize(credentials)

    wks = gc.open(sheet_name).worksheet(tab_name)

    col_start = 'A'
    col_end = string.ascii_uppercase[data.shape[1] + 1]
    row_start = 1
    row_end = data.shape[0] + 3

    cell_range = f'{col_start}{row_start}:{col_end}{row_end}'

    values = (
      [data.columns.astype(str).tolist()] +
      data.astype(str).values.tolist() +
      [['---'] * data.shape[1]])

    wks.batch_update([{
      'range': cell_range,
      'values': values}])

