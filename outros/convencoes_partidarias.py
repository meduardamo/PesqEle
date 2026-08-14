"""
Busca datas e locais de convencoes partidarias para pre-candidatos.

Uso:
  python convencoes_partidarias.py
  python convencoes_partidarias.py --max-linhas 50
  python convencoes_partidarias.py --force

Secrets/env:
  GOOGLE_CREDENTIALS_JSON
  GEMINI_API_KEY
  SPREADSHEET_ID_CONVENCOES

O script le a aba "Convencoes partidarias" por padrao e preenche apenas campos
vazios, mantendo qualquer informacao manual ja digitada. Use --force apenas para
reconsultar e sobrescrever campos existentes.
"""

import argparse
import json
import os
import re
import unicodedata
from datetime import datetime, timedelta, timezone

import gspread
from google import genai
from google.genai import types
from google.oauth2.service_account import Credentials
from compartilhado.relatorios_sheets_utils import autorizar_com_retry as _autorizar


BRT = timezone(timedelta(hours=-3))
GEMINI_MODEL = os.getenv("GEMINI_MODEL_CONVENCOES", "gemini-3.6-flash")
ABA_PADRAO = "Convenções partidárias"

# Uso acumulado de tokens do Gemini nesta execução (processo novo a cada rodada).
USO_TOKENS = {"chamadas": 0, "entrada": 0, "saida": 0, "pensamento": 0}


def _registrar_uso(resp):
    meta = getattr(resp, "usage_metadata", None)
    if not meta:
        return
    USO_TOKENS["chamadas"] += 1
    USO_TOKENS["entrada"] += getattr(meta, "prompt_token_count", 0) or 0
    USO_TOKENS["saida"] += getattr(meta, "candidates_token_count", 0) or 0
    USO_TOKENS["pensamento"] += getattr(meta, "thoughts_token_count", 0) or 0


def _custo_estimado(entrada, saida, pensamento):
    # Faixa "flash" (~$0,30/1M tokens de entrada, ~$2,50/1M de saída; pensamento cobra
    # na mesma tabela de saída). ATENÇÃO: este script usa Google Search grounding, que o
    # Google cobra POR REQUISIÇÃO, à parte dos tokens. Logo, este valor é um PISO; a
    # fatura real com busca ativada é maior. Confira o billing do Google pro valor exato.
    return (entrada / 1_000_000 * 0.30) + ((saida + pensamento) / 1_000_000 * 2.50)


def _resumo_uso_tokens():
    if not USO_TOKENS["chamadas"]:
        return
    custo = _custo_estimado(USO_TOKENS["entrada"], USO_TOKENS["saida"], USO_TOKENS["pensamento"])
    print(f"\nGemini (convenções): {USO_TOKENS['chamadas']} chamada(s) · "
          f"{USO_TOKENS['entrada']:,} tokens entrada · {USO_TOKENS['saida']:,} saída · "
          f"{USO_TOKENS['pensamento']:,} pensamento · custo estimado ${custo:.4f} "
          f"(sem a tarifa de busca do Google)")

CABECALHO_BASE = [
    "Estado",
    "Pré-candidato",
    "Cargo",
    "Partido",
    "Data convenção",
    "Local",
    "Escopo",
]

CABECALHO_AUDITORIA = [
    "Fonte convenção",
    "Título fonte",
    "Status busca",
    "Data da busca",
    "Evidência",
]

ALIASES = {
    "Pré-candidato": ["Pre-candidato", "Pré candidato", "Pre candidato", "Candidato"],
    "Data convenção": ["Data convencao", "Data da convenção", "Data da convencao"],
    "Fonte convenção": ["Fonte convencao", "Link fonte", "Fonte"],
    "Título fonte": ["Titulo fonte", "Título", "Titulo"],
    "Status busca": ["Status da busca", "Status"],
    "Data da busca": ["Última busca", "Ultima busca", "Data busca"],
}


def _sem_acento(valor):
    return unicodedata.normalize("NFKD", str(valor or "")).encode("ascii", "ignore").decode()


def _norm_header(valor):
    s = _sem_acento(valor).strip().lower()
    return re.sub(r"[^a-z0-9]+", "", s)


def _creds_info():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não encontrado.")
    return json.loads(raw)


def _sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    creds = Credentials.from_service_account_info(_creds_info(), scopes=scopes)
    return _autorizar(creds)


def _service_account_email():
    return str(_creds_info().get("client_email", "")).strip()


def _gemini():
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY não encontrada.")
    return genai.Client(api_key=api_key)


def _extrair_json_objeto(texto):
    bruto = (texto or "").strip()
    bruto = re.sub(r"^```json\s*", "", bruto, flags=re.I)
    bruto = re.sub(r"^```\s*", "", bruto)
    bruto = re.sub(r"\s*```$", "", bruto)
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", bruto):
        try:
            obj, _ = decoder.raw_decode(bruto[match.start():])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    raise RuntimeError("JSON não encontrado na resposta do Gemini")


def _mapear_header(header):
    alvo = CABECALHO_BASE + CABECALHO_AUDITORIA
    mapa_norm = {}
    for nome in alvo:
        mapa_norm[_norm_header(nome)] = nome
        for alias in ALIASES.get(nome, []):
            mapa_norm[_norm_header(alias)] = nome
    return {nome: mapa_norm.get(_norm_header(nome), nome) for nome in header}


def _garantir_colunas(ws):
    header = ws.row_values(1)
    if not header:
        header = CABECALHO_BASE + CABECALHO_AUDITORIA
        ws.update(range_name="A1", values=[header])
        return header

    canonical_by_norm = {}
    for nome in CABECALHO_BASE + CABECALHO_AUDITORIA:
        canonical_by_norm[_norm_header(nome)] = nome
        for alias in ALIASES.get(nome, []):
            canonical_by_norm[_norm_header(alias)] = nome

    novo_header = []
    mudou = False
    for col in header:
        canonical = canonical_by_norm.get(_norm_header(col), col)
        novo_header.append(canonical)
        mudou = mudou or canonical != col

    for col in CABECALHO_BASE + CABECALHO_AUDITORIA:
        if col not in novo_header:
            novo_header.append(col)
            mudou = True

    if mudou:
        if ws.col_count < len(novo_header):
            ws.add_cols(len(novo_header) - ws.col_count)
        ws.update(range_name="A1", values=[novo_header], value_input_option="RAW")
    return novo_header


def _normalizar_spreadsheet_id(valor):
    s = str(valor or "").strip()
    match = re.search(r"/spreadsheets/d/([A-Za-z0-9_-]+)", s)
    if match:
        return match.group(1)
    match = re.search(r"\b([A-Za-z0-9_-]{30,})\b", s)
    if match:
        return match.group(1)
    return s


def _abrir_aba():
    sheet_id = _normalizar_spreadsheet_id(os.getenv("SPREADSHEET_ID_CONVENCOES", ""))
    if not sheet_id:
        raise RuntimeError("Defina SPREADSHEET_ID_CONVENCOES.")
    aba_nome = os.getenv("CONVENCOES_ABA", ABA_PADRAO)
    try:
        sh = _sheets().open_by_key(sheet_id)
    except gspread.exceptions.SpreadsheetNotFound as exc:
        email = _service_account_email() or "<client_email do GOOGLE_CREDENTIALS_JSON>"
        raise RuntimeError(
            "Planilha de convenções não encontrada pela service account. "
            "Confira se SPREADSHEET_ID_CONVENCOES tem o ID correto "
            f"({sheet_id}) e compartilhe a planilha com {email}."
        ) from exc
    try:
        return sh.worksheet(aba_nome)
    except gspread.exceptions.WorksheetNotFound as exc:
        abas = ", ".join(ws.title for ws in sh.worksheets())
        raise RuntimeError(
            f"Aba '{aba_nome}' não encontrada na planilha de convenções. "
            f"Abas disponíveis: {abas}"
        ) from exc


def _data_busca():
    return datetime.now(BRT).strftime("%Y-%m-%d %H:%M")


def _limpar_data(valor):
    s = str(valor or "").strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except ValueError:
            pass
    return s


def _normalizar_status(valor):
    s = _sem_acento(valor).strip().lower()
    if s == "provavel":
        return "provável"
    if s in ("confirmado", "pendente", "conflito", "erro"):
        return s
    return "pendente"


def _texto_sem_dado_especifico(valor):
    s = str(valor or "").strip()
    if not s:
        return False
    if len(s) > 90:
        return True
    return bool(re.search(
        r"nao foi encontrad|nao ha|sem informacao|calendario|justica eleitoral|"
        r"ocorrerao entre|20 de julho|5 de agosto|no entanto",
        _sem_acento(s).lower(),
    ))


def _sanitizar_campos_principais(status, data_conv, local, escopo):
    if status not in ("confirmado", "provável"):
        return "", "", ""

    if _texto_sem_dado_especifico(local):
        local = ""
    if _texto_sem_dado_especifico(escopo):
        escopo = ""

    # Sem data ou local especifico, a busca ainda nao encontrou o dado que importa.
    if not data_conv and not local:
        return "", "", ""

    return data_conv, local, escopo


def _prompt_busca(row):
    estado = row.get("Estado", "")
    candidato = row.get("Pré-candidato", "")
    cargo = row.get("Cargo", "")
    partido = row.get("Partido", "")

    return f"""
Você é um pesquisador eleitoral. Busque na web informações sobre a convenção partidária de 2026 relacionada ao seguinte pré-candidato:

Estado/UF: {estado}
Pré-candidato: {candidato}
Cargo: {cargo}
Partido: {partido}

O objetivo é preencher uma planilha interna com data, local e escopo da convenção.

REGRAS:
1. Priorize fontes oficiais: site/Instagram do partido, federação, diretório estadual, pré-candidato, assessoria ou agenda oficial.
2. Aceite fonte jornalística confiável quando não houver fonte oficial.
3. Não confunda filiação, lançamento de pré-candidatura, encontro partidário, reunião interna ou pesquisa eleitoral com convenção partidária.
4. A data deve ser da convenção partidária que deve oficializar candidatura/chapas para a eleição de 2026.
5. Só preencha data_convencao, local e escopo quando houver informação específica sobre a convenção desse pré-candidato/partido/UF.
6. Calendário geral da Justiça Eleitoral, janela legal de convenções ou texto dizendo que nada específico foi encontrado NÃO conta como resultado.
7. Se não encontrar data/local/escopo específico, retorne status="pendente" e deixe data_convencao="", local="", escopo="", fonte_url="", fonte_titulo="" e evidencia="".
8. Se a fonte trouxer apenas previsão específica para esse pré-candidato/partido/UF, retorne status="provável". Se confirmar data e/ou local, status="confirmado".
9. Escopo deve ser curto, por exemplo: "convenção estadual", "convenção nacional", "federação/coligação" ou "lançamento de pré-candidatura".
10. Se houver conflito entre fontes, não escolha no chute: status="conflito" e explique em evidencia.
11. Use data no formato DD/MM/AAAA quando o ano estiver claro. Se só houver dia e mês, assuma 2026 apenas quando o contexto for claramente Eleições 2026.

Retorne APENAS JSON válido:
{{
  "data_convencao": "",
  "local": "",
  "escopo": "",
  "status": "confirmado|provável|pendente|conflito",
  "fonte_url": "",
  "fonte_titulo": "",
  "evidencia": "frase curta explicando o que foi encontrado"
}}
""".strip()


def buscar_convencao(client, row):
    config = types.GenerateContentConfig(
        temperature=0.1,
        tools=[types.Tool(google_search=types.GoogleSearch())],
    )
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=_prompt_busca(row),
        config=config,
    )
    _registrar_uso(resp)
    return _extrair_json_objeto(getattr(resp, "text", "") or "")


def _records(ws, header):
    mapa = _mapear_header(header)
    registros = []
    valores = ws.get_all_values()[1:]
    for idx, row in enumerate(valores, start=2):
        item = {}
        for col_idx, nome in enumerate(header):
            chave = mapa.get(nome, nome)
            item[chave] = row[col_idx] if col_idx < len(row) else ""
        registros.append((idx, item))
    return registros


def _deve_processar(row, force=False):
    if force:
        return True
    if not str(row.get("Pré-candidato", "")).strip():
        return False
    if not str(row.get("Data convenção", "")).strip():
        return True
    if not str(row.get("Local", "")).strip():
        return True
    if not str(row.get("Escopo", "")).strip():
        return True
    return False


def atualizar(max_linhas=80, force=False, dry_run=False):
    ws = _abrir_aba()
    header = _garantir_colunas(ws)
    idx = {nome: pos + 1 for pos, nome in enumerate(header)}
    client = _gemini()
    registros = [(row_i, row) for row_i, row in _records(ws, header) if _deve_processar(row, force=force)]
    registros = registros[:max_linhas]
    print(f"Convenções: {len(registros)} linha(s) para buscar.")

    updates = []
    ok, pendentes, erros = 0, 0, 0
    for row_i, row in registros:
        candidato = row.get("Pré-candidato", "")
        estado = row.get("Estado", "")
        partido = row.get("Partido", "")
        print(f"linha {row_i}: {estado} / {candidato} ({partido})...")
        try:
            dados = buscar_convencao(client, row)
        except Exception as e:
            erros += 1
            msg = f"erro técnico: {str(e)[:180]}"
            print(f"  [erro] {msg}")
            updates.extend([
                gspread.Cell(row_i, idx["Status busca"], "erro"),
                gspread.Cell(row_i, idx["Data da busca"], _data_busca()),
                gspread.Cell(row_i, idx["Evidência"], msg),
            ])
            continue

        status = _normalizar_status(dados.get("status") or "pendente")
        data_conv = _limpar_data(dados.get("data_convencao", ""))
        local = str(dados.get("local") or "").strip()
        escopo = str(dados.get("escopo") or "").strip()
        data_conv, local, escopo = _sanitizar_campos_principais(status, data_conv, local, escopo)
        if status in ("confirmado", "provável") and not (data_conv or local):
            status = "pendente"

        if status in ("confirmado", "provável"):
            ok += 1
        else:
            pendentes += 1

        def _set_if_needed(col, valor):
            if valor == "":
                return
            if force or not str(row.get(col, "")).strip():
                updates.append(gspread.Cell(row_i, idx[col], valor))

        if status in ("confirmado", "provável"):
            _set_if_needed("Data convenção", data_conv)
            _set_if_needed("Local", local)
            _set_if_needed("Escopo", escopo)
            updates.extend([
                gspread.Cell(row_i, idx["Fonte convenção"], str(dados.get("fonte_url") or "").strip()),
                gspread.Cell(row_i, idx["Título fonte"], str(dados.get("fonte_titulo") or "").strip()),
                gspread.Cell(row_i, idx["Status busca"], status),
                gspread.Cell(row_i, idx["Data da busca"], _data_busca()),
                gspread.Cell(row_i, idx["Evidência"], str(dados.get("evidencia") or "").strip()[:500]),
            ])
        elif status == "conflito":
            updates.extend([
                gspread.Cell(row_i, idx["Fonte convenção"], str(dados.get("fonte_url") or "").strip()),
                gspread.Cell(row_i, idx["Título fonte"], str(dados.get("fonte_titulo") or "").strip()),
                gspread.Cell(row_i, idx["Status busca"], status),
                gspread.Cell(row_i, idx["Data da busca"], _data_busca()),
                gspread.Cell(row_i, idx["Evidência"], str(dados.get("evidencia") or "").strip()[:500]),
            ])
        else:
            updates.extend([
                gspread.Cell(row_i, idx["Fonte convenção"], ""),
                gspread.Cell(row_i, idx["Título fonte"], ""),
                gspread.Cell(row_i, idx["Status busca"], "pendente"),
                gspread.Cell(row_i, idx["Data da busca"], ""),
                gspread.Cell(row_i, idx["Evidência"], ""),
            ])
            if _texto_sem_dado_especifico(row.get("Escopo", "")):
                updates.append(gspread.Cell(row_i, idx["Escopo"], ""))

        print(f"  [{status}] data={data_conv or '-'} local={local or '-'}")
        if len(updates) >= 120 and not dry_run:
            ws.update_cells(updates, value_input_option="USER_ENTERED")
            updates.clear()

    if updates and not dry_run:
        ws.update_cells(updates, value_input_option="USER_ENTERED")

    print("\nResumo:")
    print(f"* encontrados/prováveis: {ok}")
    print(f"* pendentes/conflitos: {pendentes}")
    print(f"* erros técnicos: {erros}")
    if dry_run:
        print("* dry-run: nada foi gravado.")

    _resumo_uso_tokens()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-linhas", type=int, default=int(os.getenv("CONVENCOES_MAX_LINHAS", "80")))
    parser.add_argument("--force", action="store_true", default=os.getenv("CONVENCOES_FORCE", "").lower() == "true")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    atualizar(max_linhas=args.max_linhas, force=args.force, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
