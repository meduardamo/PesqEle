import os
import json
from google.oauth2.service_account import Credentials
import gspread
import time

FONTE_PLANILHA = os.getenv("SPREADSHEET_ID_INTERNO")
FONTE_ABA = "Debates"

creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON")
creds_dict = json.loads(creds_json)
creds = Credentials.from_service_account_info(
    creds_dict,
    scopes=[
        "https://www.googleapis.com/auth/drive",
        "https://www.googleapis.com/auth/spreadsheets",
    ],
)
gc = gspread.authorize(creds)

ws_fonte = gc.open_by_key(FONTE_PLANILHA).worksheet(FONTE_ABA)

# Append row
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
ws_fonte.append_row(new_row)
print("Linha do Ciro Gomes adicionada com sucesso na fonte!")
