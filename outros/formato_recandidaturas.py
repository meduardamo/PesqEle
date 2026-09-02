"""Tamanho e cor do cabeçalho das abas da planilha Radar do Congresso 2026.

Roda sozinho, da raiz:  python -m outros.formato_recandidaturas

Existe por dois motivos. O primeiro é que as abas estavam com larguras e alturas
diferentes entre si, e as do Senado estavam deslocadas uma coluna, porque a lista
de larguras era posicional e uma coluna nova entrou no meio. Aqui tudo é
endereçado por NOME de coluna: coluna que entra ou sai não desalinha o resto.

O segundo é a regra dos três tons do Radar: cinza em quem é a pessoa, vinho nas
colunas do índice, marinho no resto.

Linha de dado tem 21 pixels em todas as abas. Texto longo fica cortado de
propósito: quem precisa do texto inteiro clica na célula. Cabeçalho tem 54, que
é o bastante para três linhas.
"""
import os

import gspread
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

ESCOPO = ["https://www.googleapis.com/auth/spreadsheets"]

MARINHO = {"red": 25 / 255, "green": 45 / 255, "blue": 78 / 255}
VINHO = {"red": 150 / 255, "green": 46 / 255, "blue": 77 / 255}
CINZA = {"red": 225 / 255, "green": 225 / 255, "blue": 225 / 255}
FUNDO = {"red": .9569, "green": .9529, "blue": .9373}
LINHA = {"red": .855, "green": .855, "blue": .831}
TEXTO = {"red": .0667, "green": .0667, "blue": .0667}
BRANCO = {"red": 1, "green": 1, "blue": 1}

# As abas que o Radar desenha. As de cliente são editadas à mão e só recebem
# tamanho, não cor.
COM_COR = {"Competitividade Câmara (em exercício)", "Competitividade Senado (em exercício)",
           "Competitividade Assembleias (em exercício)", "Bancadas Temáticas",
           "Competitividade Senado (todas as candidaturas)"}

ALTURA_CABECALHO = 54
ALTURA_LINHA = 21
LARGURA_PADRAO = 150

# Quem é a pessoa: cinza.
IDENTIFICACAO = {"Parlamentar", "Candidato", "Cliente", "Casa", "Partido", "UF",
                 "Partido em 2022", "Partido em 2026", "Nome do Ator de Interesse"}
# Colunas do índice: vinho.
INDICE = {"Índice de competitividade à reeleição", "Índice de competitividade eleitoral",
          "Alta, média ou baixa", "Nota da régua", "Cenário eleitoral (banda)",
          "É competitivo?"}

LARGURA = {
    # identificação
    "Parlamentar": 195, "Candidato": 195, "Cliente": 230, "Casa": 80,
    "Partido": 100, "UF": 45, "Partido em 2022": 105, "Partido em 2026": 105,
    # índice, nas três abas de competitividade
    "Índice de competitividade à reeleição": 130,
    "Índice de competitividade eleitoral": 130,
    "Alta, média ou baixa": 110, "Nota da régua": 90,
    "Cenário eleitoral (banda)": 215, "É competitivo?": 105,
    "Como o índice foi calculado": 420,
    # mandato, comum às três
    "O que disputa em 2026": 265, "Se perder, o que acontece": 265,
    # Senado
    "Posição na disputa": 110, "Média das pesquisas": 120,
    "Distância para a linha de corte": 130, "Chapa presidencial": 130,
    "Base do vínculo de chapa": 230, "Coligação no estado": 300,
    "Cabeça de chapa no estado": 170, "Apoio presidencial declarado": 170,
    "Fonte do apoio declarado": 200, "Mandato legislativo atual": 170,
    "Situação do registro no TSE": 130, "Pesquisas na conta": 105,
    "Última pesquisa do estado": 120,
    # Câmara e assembleias
    "Votos nominais em 2022": 120, "Como entrou em 2022": 190,
    "Quociente eleitoral em 2022": 130, "Votos divididos pelo quociente": 140,
    "Posição entre os eleitos da UF": 130,
    "Votos nos 10 municípios onde mais votou": 165, "Cargo na Mesa": 150,
    "Em exercício na assembleia?": 120, "Página da assembleia": 230,
    "Data da consulta": 105, "Relevante para o ISG?": 110,
    "Poder institucional (0 a 3)": 120, "Cargos que geraram essa nota": 280,
    "Emendas totais (0 a 2)": 110, "Cabeças do Congresso 2026 (DIAP)": 140,
    "Comissões de Educação, Saúde e Esporte": 190,
    # bancadas temáticas
    "Bancada da Educação": 110, "Vínculo (Educação)": 240,
    "Bancada da Saúde": 105, "Vínculo (Saúde)": 240,
    "Bancada do Esporte": 110, "Vínculo (Esporte)": 240,
    "Bancada da Primeira Infância": 130, "Vínculo (Primeira Infância)": 240,
    "Resultado em 2026": 120,
    # abas de cliente
    "Cargo / Partido / UF": 160, "Alinhamento (Aliado/Neutro/Opositor)": 200,
    "Subtema / Pauta Específica": 200, "Relação (Ex: Frente X, Comissão Y)": 220,
    "PLs Relacionados": 160, "Análise / Observações da Eixo": 350,
    "Nome do Ator de Interesse": 200,
}

# A aba de metodologia é um documento, não uma grade: duas colunas largas e a
# linha crescendo com o parágrafo.
LEIAME = {"largura": {"A": 220, "B": 780}}


def tom(coluna):
    if coluna in IDENTIFICACAO:
        return CINZA
    if coluna in INDICE:
        return VINHO
    return MARINHO


def pedidos_da_aba(ws, cabecalho, linhas, pintar=True, congelar_coluna=True):
    """Requisições de formato de uma aba de dados, endereçadas por nome de coluna.

    `congelar_coluna` sai fora em aba com célula mesclada de ponta a ponta: o
    Sheets recusa congelar coluna que corta uma mescla pela metade.
    """
    sid = ws.id
    grade = {"frozenRowCount": 1}
    campos = "gridProperties.frozenRowCount"
    if congelar_coluna:
        grade["frozenColumnCount"] = 1
        campos += ",gridProperties.frozenColumnCount"
    req = [{"updateSheetProperties": {"properties": {"sheetId": sid, "gridProperties": grade},
                                      "fields": campos}}]
    for i, nome in enumerate(cabecalho):
        fundo = tom(nome)
        escuro = fundo is not CINZA
        if pintar:
            req.append({"repeatCell": {
                "range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1,
                          "startColumnIndex": i, "endColumnIndex": i + 1},
                "cell": {"userEnteredFormat": {
                    "backgroundColor": fundo, "horizontalAlignment": "LEFT",
                    "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP",
                    "textFormat": {"bold": True, "fontSize": 10, "fontFamily": "Montserrat",
                                   "foregroundColor": BRANCO if escuro else TEXTO}}},
                "fields": "userEnteredFormat"}})
        req.append({"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "COLUMNS",
                      "startIndex": i, "endIndex": i + 1},
            "properties": {"pixelSize": LARGURA.get(nome, LARGURA_PADRAO)},
            "fields": "pixelSize"}})
    req.append({"updateDimensionProperties": {
        "range": {"sheetId": sid, "dimension": "ROWS", "startIndex": 0, "endIndex": 1},
        "properties": {"pixelSize": ALTURA_CABECALHO}, "fields": "pixelSize"}})
    if linhas > 1:
        req.append({"updateDimensionProperties": {
            "range": {"sheetId": sid, "dimension": "ROWS", "startIndex": 1,
                      "endIndex": linhas},
            "properties": {"pixelSize": ALTURA_LINHA}, "fields": "pixelSize"}})
    return req


def corpo(sid, primeira_linha, ultima_linha, colunas):
    """Formato do corpo da aba: fundo, borda, fonte e quebra de linha."""
    borda = {"style": "SOLID", "width": 1, "color": LINHA}
    return {"repeatCell": {
        "range": {"sheetId": sid, "startRowIndex": primeira_linha, "endRowIndex": ultima_linha,
                  "startColumnIndex": 0, "endColumnIndex": colunas},
        "cell": {"userEnteredFormat": {
            "numberFormat": {"type": "TEXT", "pattern": "@"},
            "backgroundColor": FUNDO,
            "borders": {"top": borda, "bottom": borda, "left": borda, "right": borda},
            "horizontalAlignment": "LEFT", "verticalAlignment": "MIDDLE",
            "wrapStrategy": "WRAP",
            "textFormat": {"bold": False, "fontSize": 9, "fontFamily": "Montserrat",
                           "foregroundColor": TEXTO}}},
        "fields": "userEnteredFormat"}}


def main():
    cred = Credentials.from_service_account_file(
        os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"), scopes=ESCOPO)
    ident = os.getenv("SPREADSHEET_ID_RECANDIDATURAS", "").strip()
    if not ident:
        raise RuntimeError("Variável SPREADSHEET_ID_RECANDIDATURAS não configurada.")
    gc = gspread.authorize(cred)
    svc = build("sheets", "v4", credentials=cred)
    sh = gc.open_by_key(ident)
    meta = svc.spreadsheets().get(spreadsheetId=ident,
                                  fields="sheets(properties.title,merges)").execute()
    mescladas = {a["properties"]["title"]: bool(a.get("merges")) for a in meta["sheets"]}

    req = []
    for ws in sh.worksheets():
        if ws.title == "LEIA-ME":
            # Documento: a linha cresce com o parágrafo, senão o texto fica cortado.
            for letra, px in LEIAME["largura"].items():
                i = ord(letra) - ord("A")
                req.append({"updateDimensionProperties": {
                    "range": {"sheetId": ws.id, "dimension": "COLUMNS",
                              "startIndex": i, "endIndex": i + 1},
                    "properties": {"pixelSize": px}, "fields": "pixelSize"}})
            req.append({"autoResizeDimensions": {"dimensions": {
                "sheetId": ws.id, "dimension": "ROWS", "startIndex": 0,
                "endIndex": ws.row_count}}})
            print(f"{ws.title}: 2 colunas, linha ajustada ao parágrafo")
            continue
        cabecalho = ws.row_values(1)
        if not cabecalho:
            continue
        req += pedidos_da_aba(ws, cabecalho, ws.row_count,
                              pintar=ws.title in COM_COR,
                              congelar_coluna=not mescladas.get(ws.title))
        sem_medida = [c for c in cabecalho if c not in LARGURA]
        print(f"{ws.title}: {len(cabecalho)} colunas"
              + (f", sem largura própria: {sem_medida}" if sem_medida else ""))
    svc.spreadsheets().batchUpdate(spreadsheetId=ident, body={"requests": req}).execute()
    print(f"\n{len(req)} ajustes aplicados.")


if __name__ == "__main__":
    main()
