import os
import json
from google.oauth2.service_account import Credentials
import gspread

FONTE_PLANILHA = os.getenv("SPREADSHEET_ID_INTERNO")

creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
creds_dict = json.loads(creds_json)
creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(creds)

ws_fonte = gc.open_by_key(FONTE_PLANILHA).worksheet("Debates")
all_rows = ws_fonte.get_all_values()
for i, r in enumerate(all_rows):
    if len(r) > 9 and "Ciro" in r[9]:
        print(f"Row {i+1}:", r)

