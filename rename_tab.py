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

try:
    ws = gc.open_by_key(PLANILHA).worksheet("debates")
    ws.update_title("eventos")
    print("Aba em Mapeamento renomeada para 'eventos'.")
except Exception as e:
    print("Mapeamento aba debates não encontrada ou já renomeada:", e)

try:
    ws_fonte = gc.open_by_key(FONTE_PLANILHA).worksheet("Debates")
    ws_fonte.update_title("Eventos")
    print("Aba em Interno renomeada para 'Eventos'.")
except Exception as e:
    print("Interno aba Debates não encontrada ou já renomeada:", e)
    
# Rename file titles?
try:
    sh = gc.open_by_key(PLANILHA)
    sh.update_title("Mapeamento de Eventos (Debates e Sabatinas)")
except:
    pass
