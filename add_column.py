import os
import json
from google.oauth2.service_account import Credentials
import gspread

# Parse secrets from env
PLANILHA = os.getenv("SPREADSHEET_ID_DEBATES")
FONTE_PLANILHA = os.getenv("SPREADSHEET_ID_INTERNO")
ABA = "debates"
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

# 1. Modify Mapeamento de Debates
ws = gc.open_by_key(PLANILHA).worksheet(ABA)
headers = ws.row_values(1)
print("debates headers antes:", headers)

if "tipo" not in headers:
    # insert after participantes (which is index 9, so insert at 11 in 1-based)
    col_idx = headers.index("participantes") + 2 if "participantes" in headers else 11
    ws.insert_cols([["tipo"] + ["Debate"] * (ws.row_count - 1)], col_idx)
    
    # Add Data Validation (lista suspensa)
    body = {
        "requests": [
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "startColumnIndex": col_idx - 1,
                        "endColumnIndex": col_idx,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": "Debate"}, {"userEnteredValue": "Sabatina"}],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    }
                }
            }
        ]
    }
    ws.spreadsheet.batch_update(body)
    print("Coluna 'tipo' adicionada a Mapeamento de Debates.")
else:
    print("Coluna 'tipo' já existe em Mapeamento de Debates.")


# 2. Modify [Interno] ELEIÇÕES 2026
ws_fonte = gc.open_by_key(FONTE_PLANILHA).worksheet(FONTE_ABA)
headers_fonte = ws_fonte.row_values(1)
print("fonte headers antes:", headers_fonte)

if "tipo" not in headers_fonte:
    col_idx_fonte = headers_fonte.index("participantes") + 2 if "participantes" in headers_fonte else 11
    ws_fonte.insert_cols([["tipo"] + ["Debate"] * (ws_fonte.row_count - 1)], col_idx_fonte)
    
    body = {
        "requests": [
            {
                "setDataValidation": {
                    "range": {
                        "sheetId": ws_fonte.id,
                        "startRowIndex": 1,
                        "startColumnIndex": col_idx_fonte - 1,
                        "endColumnIndex": col_idx_fonte,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": "Debate"}, {"userEnteredValue": "Sabatina"}],
                        },
                        "showCustomUi": True,
                        "strict": True,
                    }
                }
            }
        ]
    }
    ws_fonte.spreadsheet.batch_update(body)
    print("Coluna 'tipo' adicionada a [Interno] ELEIÇÕES 2026.")
else:
    print("Coluna 'tipo' já existe em [Interno] ELEIÇÕES 2026.")

