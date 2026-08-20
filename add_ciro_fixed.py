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
# Find first completely empty row to avoid format-only rows
all_values = ws_fonte.get_all_values()
insert_idx = len(all_values) + 1
for i, row in enumerate(all_values):
    if not any(cell.strip() for cell in row):
        insert_idx = i + 1
        break

print(f"Inserting at row {insert_idx}")
ws_fonte.insert_row(new_row, insert_idx)
