"""Baixa as bases que o Índice de competitividade eleitoral ao Senado consome.

Quatro fontes: a matriz de polling (média móvel, resultados e pesquisas), o
DivulgaCand espelhado na planilha do TSE (registros e apoios), a composição das
coligações direto da API do TSE e as três abas Radar da planilha Recandidaturas.
Grava CSVs no diretório de trabalho, que os outros passos leem.
"""
import os
from pathlib import Path

import gspread
import pandas as pd
from google.oauth2.service_account import Credentials

from outros import coligacoes_governador as cg

ESCOPO = ["https://www.googleapis.com/auth/spreadsheets"]
CRED = Path(os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"))
# O repositório é público: id de planilha vem de variável de ambiente, nunca
# embutido no código.
def _id(nome):
    valor = os.getenv(nome, "").strip()
    if not valor:
        raise RuntimeError(f"Variável {nome} não configurada.")
    return valor

ID_POLLING = _id("SPREADSHEET_ID_POLLINGDATA")
ID_TSE = _id("SPREADSHEET_ID_TSE")
ID_RADAR = _id("SPREADSHEET_ID_RECANDIDATURAS")

# A API do TSE devolve 403 sem cabeçalho de navegador em versão XHR.
CABECALHOS_TSE = {
    "User-Agent": ("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 "
                   "(KHTML, like Gecko) Chrome/139.0.0.0 Safari/537.36"),
    "Accept": "application/json, text/plain, */*",
    "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8",
    "Referer": "https://divulgacandcontas.tse.jus.br/divulga/",
    "Origin": "https://divulgacandcontas.tse.jus.br",
    "Sec-Fetch-Dest": "empty", "Sec-Fetch-Mode": "cors", "Sec-Fetch-Site": "same-origin",
}

ABAS = [
    (ID_POLLING, "resultados_bi", "bi.csv"),
    (ID_POLLING, "resultados", "resultados.csv"),
    (ID_POLLING, "pesquisas", "pesquisas.csv"),
    (ID_TSE, "chapas_divulgacand", "chapas.csv"),
    (ID_TSE, "apoio por candidatura", "apoios.csv"),
    (ID_RADAR, "Radar Senado Atual", "Radar_Senado_Atual.csv"),
    (ID_RADAR, "Radar Câmara Atual", "Radar_Câmara_Atual.csv"),
    (ID_RADAR, "Radar Assembleias Atual", "Radar_Assembleias_Atual.csv"),
]


def baixar():
    gc = gspread.authorize(Credentials.from_service_account_file(str(CRED), scopes=ESCOPO))
    for planilha, aba, arquivo in ABAS:
        valores = gc.open_by_key(planilha).worksheet(aba).get_all_values()
        df = pd.DataFrame(valores[1:], columns=valores[0])
        df.to_csv(arquivo, index=False)
        print(f"{aba}: {df.shape}")

    cg.HEADERS_HTTP = CABECALHOS_TSE
    coligacoes = pd.DataFrame(cg.extract_majoritarias())
    coligacoes.to_csv("coligacoes.csv", index=False)
    print("coligações:", coligacoes.cargo.value_counts().to_dict())


if __name__ == "__main__":
    baixar()
