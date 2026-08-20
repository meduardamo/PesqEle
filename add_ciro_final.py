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

new_row = [
    "D999", # id_debate
    "2026-08-20", # data
    "10h", # horario
    "Governador", # cargo
    "CE", # uf
    "1", # turno
    "PontoPoder", # emissora
    "https://www.youtube.com/watch?v=h5HBtPxILIU", # url
    "", # mediador
    "Ciro Gomes", # participantes
    "Sabatina", # tipo
    "Teste de sabatina automatizado", # observacoes
]

all_values = ws_fonte.get_all_values()
insert_idx = len(all_values) + 1

# resize grid
ws_fonte.add_rows(1)
ws_fonte.update(f"A{insert_idx}:L{insert_idx}", [new_row])

print(f"Linha adicionada no index {insert_idx}!")
