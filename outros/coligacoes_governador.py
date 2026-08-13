"""Atualiza a aba de coligações das candidaturas a governador de 2026.

A fonte é a API oficial do DivulgaCand/TSE. A rotina consulta todas as UFs,
expande os partidos integrantes de federações e só reescreve a aba quando os
campos visíveis mudam.
"""

import os
import re
import time
from collections import Counter
from pathlib import Path

import gspread
import requests


ANO = 2026
API = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1"
HEADERS_HTTP = {"User-Agent": "Mozilla/5.0"}
CREDS_FILE = Path(os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"))
SPREADSHEET_ID = os.getenv("SPREADSHEET_ID_TSE", "").strip()
SHEET_TITLE = "coligacoes_governador"

UFS = [
    "AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT",
    "MS", "MG", "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO",
    "RR", "SC", "SP", "SE", "TO",
]

HEADERS = [
    "ano",
    "uf",
    "cargo",
    "candidato",
    "partido_candidato",
    "nome_coligacao",
    "partidos_coligacao",
    "quantidade_partidos",
    "tipo_chapa",
    "situacao",
    "fonte_tse",
]


def get_json(session, url, attempts=5):
    for attempt in range(1, attempts + 1):
        try:
            response = session.get(url, headers=HEADERS_HTTP, timeout=45)
            response.raise_for_status()
            return response.json()
        except (requests.RequestException, ValueError):
            if attempt == attempts:
                raise
            time.sleep(attempt * 2)


def election_id(session, year=ANO):
    elections = get_json(session, f"{API}/eleicao/ordinarias")
    return next(
        election["id"]
        for election in elections
        if election.get("ano") == year
        and election.get("tipoAbrangencia") == "F"
    )


def split_parties(composition, listed_party="", coalition_name=""):
    """Converte a composição do TSE em uma lista plana de siglas.

    Em candidaturas de federação, o TSE pode devolver ``**`` na composição e
    guardar as siglas apenas no nome, como
    ``FEDERAÇÃO PSOL REDE(PSOL/REDE)``.
    """
    raw = str(composition or "").strip()
    if not raw or raw == "**":
        coalition_name = str(coalition_name or "").strip()
        federation = re.search(
            r"FEDERA(?:ÇÃO|CAO)[^(]*\(([^)]*)\)",
            coalition_name,
            flags=re.IGNORECASE,
        )
        if not federation:
            return [listed_party] if listed_party else []
        raw = federation.group(1)

    expanded = re.sub(
        r"FEDERA(?:ÇÃO|CAO)[^(]*\(([^)]*)\)",
        r"\1",
        raw,
        flags=re.IGNORECASE,
    )
    parties = []
    for token in re.split(r"\s*/\s*", expanded):
        party = re.sub(r"^\d+\s*-\s*", "", token).strip()
        if party and party != "**" and party not in parties:
            parties.append(party)
    return parties


def classify(parties):
    if len(parties) > 1:
        return "COLIGAÇÃO"
    if len(parties) == 1:
        return "PARTIDO ISOLADO"
    return "COMPOSIÇÃO NÃO INFORMADA"


def extract_governors(session=None):
    session = session or requests.Session()
    election = election_id(session)
    rows = []
    seen = set()

    for uf in UFS:
        listing_url = (
            f"{API}/candidatura/listar/{ANO}/{uf}/{election}/3/candidatos"
        )
        listing = get_json(session, listing_url)
        candidates = listing.get("candidatos") or []
        for candidate in candidates:
            candidate_id = str(candidate.get("id") or "").strip()
            if not candidate_id or candidate_id in seen:
                continue
            seen.add(candidate_id)
            source_url = (
                f"{API}/candidatura/buscar/{ANO}/{uf}/{election}"
                f"/candidato/{candidate_id}"
            )
            detail = get_json(session, source_url)
            party_data = candidate.get("partido") or detail.get("partido") or {}
            listed_party = str(party_data.get("sigla") or "").strip()
            coalition_name = str(
                detail.get("nomeColigacao") or candidate.get("nomeColigacao") or ""
            ).strip()
            parties = split_parties(
                detail.get("composicaoColigacao"),
                listed_party,
                coalition_name,
            )
            rows.append({
                "ano": ANO,
                "uf": uf,
                "cargo": "GOVERNADOR",
                "candidato": str(
                    candidate.get("nomeUrna") or detail.get("nomeUrna") or ""
                ).strip(),
                "partido_candidato": listed_party,
                "nome_coligacao": coalition_name,
                "partidos_coligacao": " / ".join(parties),
                "quantidade_partidos": len(parties),
                "tipo_chapa": classify(parties),
                "situacao": str(
                    detail.get("descricaoSituacao")
                    or candidate.get("descricaoSituacao")
                    or ""
                ).strip(),
                "fonte_tse": source_url,
                "sq_titular": candidate_id,
            })
        print(f"{uf}: {len(candidates)} candidatura(s)", flush=True)

    return sorted(rows, key=lambda row: (row["uf"], row["candidato"], row["sq_titular"]))


def validate(rows):
    if not rows:
        raise RuntimeError("O TSE não devolveu candidaturas a governador.")
    present_ufs = {row["uf"] for row in rows}
    missing_ufs = sorted(set(UFS) - present_ufs)
    if missing_ufs:
        raise RuntimeError(f"UFs sem candidatura na extração: {missing_ufs}")
    missing_composition = [
        f"{row['uf']}/{row['candidato']}"
        for row in rows
        if not row["partidos_coligacao"]
    ]
    if missing_composition:
        raise RuntimeError(
            "Candidaturas sem composição partidária: "
            + ", ".join(missing_composition)
        )
    ids = [row["sq_titular"] for row in rows]
    duplicates = [candidate_id for candidate_id, count in Counter(ids).items() if count > 1]
    if duplicates:
        raise RuntimeError(f"Sequenciais duplicados na extração: {duplicates}")


def rgb(hex_color):
    value = hex_color.lstrip("#")
    return {
        "red": int(value[0:2], 16) / 255,
        "green": int(value[2:4], 16) / 255,
        "blue": int(value[4:6], 16) / 255,
    }


def visible_values(rows):
    return [HEADERS] + [
        [str(row.get(header, "")) for header in HEADERS]
        for row in rows
    ]


def row_signature(record):
    return tuple(str(record.get(header, "")).strip() for header in HEADERS)


def update_sheet(rows):
    if not SPREADSHEET_ID:
        raise RuntimeError("Secret SPREADSHEET_ID_TSE não configurado.")
    client = gspread.service_account(filename=str(CREDS_FILE))
    spreadsheet = client.open_by_key(SPREADSHEET_ID)
    try:
        worksheet = spreadsheet.worksheet(SHEET_TITLE)
        old_records = worksheet.get_all_records()
        existing_values = worksheet.get_all_values()
        if existing_values and existing_values[0] != HEADERS:
            raise RuntimeError(
                f"A aba {SHEET_TITLE!r} tem outro cabeçalho; nada foi sobrescrito."
            )
    except gspread.WorksheetNotFound:
        worksheet = spreadsheet.add_worksheet(
            title=SHEET_TITLE,
            rows=len(rows) + 1,
            cols=len(HEADERS),
        )
        old_records = []
        existing_values = []

    values = visible_values(rows)
    old_signatures = Counter(row_signature(record) for record in old_records)
    new_records = [dict(zip(HEADERS, value)) for value in values[1:]]
    new_signatures = Counter(row_signature(record) for record in new_records)
    if existing_values and old_signatures == new_signatures:
        print(f"Sem mudanças: {len(rows)} candidaturas; aba mantida.")
        return False, [], []

    added = list((new_signatures - old_signatures).elements())
    removed = list((old_signatures - new_signatures).elements())

    worksheet.clear()
    worksheet.resize(rows=len(values), cols=len(HEADERS))
    worksheet.update(values=values, range_name="A1", value_input_option="RAW")
    worksheet.freeze(rows=1)
    worksheet.set_basic_filter(f"A1:K{len(values)}")

    navy = rgb("#192D4E")
    white = rgb("#FFFFFF")
    light_border = rgb("#D9E1E8")
    worksheet.batch_format([
        {
            "range": f"A1:K{len(values)}",
            "format": {
                "textFormat": {"fontFamily": "Montserrat", "fontSize": 10},
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "wrapStrategy": "WRAP",
                "borders": {
                    "bottom": {"style": "SOLID", "color": light_border},
                },
            },
        },
        {
            "range": "A1:K1",
            "format": {
                "backgroundColor": navy,
                "textFormat": {
                    "foregroundColor": white,
                    "fontFamily": "Montserrat",
                    "fontSize": 10,
                    "bold": True,
                },
                "horizontalAlignment": "CENTER",
                "verticalAlignment": "MIDDLE",
                "wrapStrategy": "WRAP",
            },
        },
    ])

    widths = [70, 55, 130, 190, 145, 220, 300, 110, 155, 155, 420]
    requests_body = [
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": worksheet.id,
                    "dimension": "COLUMNS",
                    "startIndex": index,
                    "endIndex": index + 1,
                },
                "properties": {"pixelSize": width},
                "fields": "pixelSize",
            }
        }
        for index, width in enumerate(widths)
    ]
    requests_body.extend([
        {
            "updateDimensionProperties": {
                "range": {
                    "sheetId": worksheet.id,
                    "dimension": "ROWS",
                    "startIndex": 0,
                    "endIndex": 1,
                },
                "properties": {"pixelSize": 42},
                "fields": "pixelSize",
            }
        },
        {
            "updateSheetProperties": {
                "properties": {
                    "sheetId": worksheet.id,
                    "gridProperties": {"hideGridlines": True},
                    "tabColor": navy,
                },
                "fields": "gridProperties.hideGridlines,tabColor",
            }
        },
    ])
    spreadsheet.batch_update({"requests": requests_body})
    return True, added, removed


def describe(signatures):
    return [f"{row[1]} — {row[3]} ({row[4]})" for row in signatures]


def main():
    rows = extract_governors()
    validate(rows)
    changed, added, removed = update_sheet(rows)
    coalitions = sum(row["tipo_chapa"] == "COLIGAÇÃO" for row in rows)
    print(
        f"Resultado: {len(rows)} candidaturas, "
        f"{len({row['uf'] for row in rows})} UFs, {coalitions} coligações."
    )
    if changed:
        print(f"Adicionadas/alteradas: {describe(added)}")
        print(f"Removidas/alteradas: {describe(removed)}")
    rr = [row for row in rows if row["uf"] == "RR"]
    print(f"RR: {[(row['candidato'], row['partidos_coligacao']) for row in rr]}")


if __name__ == "__main__":
    main()
