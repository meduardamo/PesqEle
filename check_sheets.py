import os
import json
from google.oauth2.service_account import Credentials
import gspread

PLANILHA = os.getenv("SPREADSHEET_ID_DEBATES")
FONTE_PLANILHA = os.getenv("SPREADSHEET_ID_INTERNO")

creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
creds_dict = json.loads(creds_json)
creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=["https://www.googleapis.com/auth/drive", "https://www.googleapis.com/auth/spreadsheets"],
)
gc = gspread.authorize(creds)

ws = gc.open_by_key(PLANILHA).worksheet("debates")
print("Mapeamento de Debates - Últimas 5 linhas:")
for row in ws.get_all_values()[-5:]:
    print(row)

print("\n---")
ws_fonte = gc.open_by_key(FONTE_PLANILHA).worksheet("Debates")
print("[Interno] - Últimas 5 linhas:")
for row in ws_fonte.get_all_values()[-5:]:
    print(row)
