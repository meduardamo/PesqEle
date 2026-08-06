"""
Coleta as atas de convenção partidária no DivulgaCandContas e extrai os
candidatos listados dentro dos PDFs.

Por que existe: a candidatura só aparece na API do DivulgaCand depois do
registro, mas a ata da convenção é publicada antes — ela antecipa quem vai
ser candidato. O PDF é gerado pelo módulo de atas do TSE e traz uma seção
estruturada "Lista de candidatos" (nome, nome de urna, número, gênero, CPF,
por cargo), mesmo quando o corpo da ata é texto livre do partido.

Grava duas abas no Google Sheets (SPREADSHEET_ID_TSE, credentials.json):
  - atas_divulgacand: uma linha por ata (partido, UF, tipo, link do PDF)
  - atas_candidatos:  uma linha por candidato extraído do PDF

Incremental: só baixa e reparseia o PDF quando a ata é nova ou o TSE
ressincronizou (dtSincronizacao mudou); o resto reaproveita o que já está
na planilha.

Tipos de ata (código do DivulgaCand): C = Convenção, E = Executiva,
R = Retificadora. O site agrupa retificadoras sob a ata principal pelo uid;
aqui cada ata vira uma linha própria, com o uid preservado para agrupar.
"""

import os
import re
import time

import fitz  # PyMuPDF
import gspread
import pandas as pd
import requests

from compartilhado.relatorios_sheets_utils import reescrever_aba

ANO = 2026

API_ATA = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1/ata"
ARQUIVO = "https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/eleicao"

HEADERS = {"User-Agent": "Mozilla/5.0"}
PAUSA = 0.3   # o TSE bloqueia se as requisições vierem rápido demais

TIPO_ATA = {"C": "Convenção", "E": "Executiva", "R": "Retificadora"}

ABA_ATAS = "atas_divulgacand"
ABA_CANDIDATOS = "atas_candidatos"

COLS_ATAS = ["ano", "uf", "partido_numero", "partido_sigla", "partido_nome",
             "tipo_ata", "dt_sincronizacao", "sq_ata", "uid", "link_pdf",
             "status_extracao", "n_candidatos", "cargos"]
COLS_CANDIDATOS = ["ano", "uf", "partido_numero", "partido_sigla", "tipo_ata",
                   "cargo", "concorre", "ordem", "nome", "nome_urna", "numero",
                   "genero", "cpf", "sq_ata", "link_pdf", "dt_sincronizacao"]


def _get_json(url):
    # 5 tentativas com backoff: a API do TSE tem blips curtos (ver
    # tse_candidaturas, 2 rodadas falharam em 22/07 com retry de 3x1s).
    for tentativa in range(1, 6):
        try:
            r = requests.get(url, headers=HEADERS, timeout=40)
            r.raise_for_status()
            time.sleep(PAUSA)
            return r.json()
        except Exception:
            time.sleep(2 * tentativa)
    return None


def cod_eleicao(ano=ANO):
    # ordinariasAta lista só as eleições que já têm ata publicada
    eleicoes = _get_json(f"{API_ATA}/ordinariasAta")
    if eleicoes is None:
        # API fora do ar mesmo após as 5 tentativas (blip longo do TSE, como
        # em 23/07): sai limpo, a próxima rodada do schedule cobre. Erro de
        # verdade é só quando a API responde SEM a eleição (ramo abaixo).
        print("API do TSE indisponível; encerrando sem erro, próxima rodada cobre")
        raise SystemExit(0)
    for e in eleicoes:
        if e.get("ano") == ano and e.get("tipoAbrangencia") == "F":
            return e["id"]
    raise ValueError(f"Eleição geral {ano} sem atas publicadas")


def listar_atas(eleicao):
    """Uma linha por ata, varrendo as UFs que têm ata (inclui BR = nacional)."""
    ufs = _get_json(f"{API_ATA}/uf/{eleicao}") or []
    linhas = []
    for uf in ufs:
        d = _get_json(f"{API_ATA}/partidos/{eleicao}/{uf}") or {}
        for a in d.get("atas") or []:
            linhas.append({
                "ano": ANO,
                "uf": uf,
                "partido_numero": a.get("nrPartido"),
                "partido_sigla": a.get("sgPartido"),
                "partido_nome": a.get("nmPartido"),
                "tipo_ata": TIPO_ATA.get(a.get("tipoAta"), a.get("tipoAta")),
                "dt_sincronizacao": a.get("dtSincronizacao"),
                "sq_ata": a.get("sqAta"),
                "uid": a.get("uid"),
                "link_pdf": f"{ARQUIVO}/{a.get('sqEleicao')}/ata/{a.get('sqAta')}",
                "sq_eleicao": a.get("sqEleicao"),
            })
        print(f"{uf}: {len(d.get('atas') or [])} ata(s)")
    return linhas


def baixar_pdf(sq_eleicao, sq_ata):
    for _ in range(3):
        try:
            r = requests.get(f"{ARQUIVO}/{sq_eleicao}/ata/{sq_ata}",
                             headers=HEADERS, timeout=60)
            r.raise_for_status()
            time.sleep(PAUSA)
            return r.content
        except Exception:
            time.sleep(1)
    return None


# Linhas de rodapé que o TSE intercala no meio da lista de candidatos e que
# quebrariam o pareamento valor/rótulo se não fossem descartadas.
_RUIDO = re.compile(r"^(Página \d+ de \d+|Hash: [0-9a-f]+)$")

_SECAO_CARGO = re.compile(
    r"^Candidato\(s\) ao cargo de (.+?) concorrer\S*\s+(isolado|coligado)", re.I)

# Rótulos que aparecem NA LINHA SEGUINTE ao valor no texto extraído do PDF.
_ROTULOS = {"Nome": "nome", "Nome para Urna": "nome_urna",
            "Número": "numero", "Gênero": "genero", "CPF": "cpf"}


def extrair_candidatos(pdf_bytes):
    """Lê a seção "Lista de candidatos" do PDF.

    O texto extraído vem como pares valor→rótulo (o valor na linha de cima do
    rótulo). Um bloco de candidato começa em "N - FULANO" seguido de "Nome" e
    termina em "Assinatura". Só coleta dentro de uma seção de cargo: a "Lista
    de Presença" no fim da ata repete os mesmos nomes sem seção de cargo e
    duplicaria tudo se não fosse ignorada.
    """
    try:
        doc = fitz.open(stream=pdf_bytes, filetype="pdf")
    except Exception:
        return [], "erro: PDF ilegível"

    linhas = []
    for page in doc:
        for l in page.get_text().splitlines():
            l = l.strip()
            if l and not _RUIDO.match(l):
                linhas.append(l)

    if len("".join(linhas)) < 100:
        return [], "sem texto (PDF escaneado?)"

    candidatos = []
    cargo, concorre = "", ""
    atual, anterior = None, ""
    for i, l in enumerate(linhas):
        m = _SECAO_CARGO.match(l)
        if m:
            cargo, concorre = m.group(1).strip(), m.group(2).lower()
            anterior = ""
            continue
        if l in ("Lista de Presença", "Declaração"):
            cargo, atual = "", None
            continue

        m = re.match(r"^(\d+) - (.+)$", l)
        if m and cargo and i + 1 < len(linhas) and linhas[i + 1] == "Nome":
            atual = {"ordem": int(m.group(1)), "nome": m.group(2).strip(),
                     "cargo": cargo, "concorre": concorre,
                     "nome_urna": "", "numero": "", "genero": "", "cpf": ""}
            anterior = ""
            continue

        if atual is not None:
            if l in _ROTULOS:
                campo = _ROTULOS[l]
                if campo != "nome":   # nome já veio da linha "N - FULANO"
                    valor = anterior.strip()
                    atual[campo] = "" if valor in ("-", "") else valor
                anterior = ""
                continue
            if l == "Assinatura":
                candidatos.append(atual)
                atual = None
                anterior = ""
                continue
        anterior = l

    if not candidatos:
        if any("Não há candidatos a declarar" in l for l in linhas):
            return [], "sem candidatos declarados"
        return [], "sem lista estruturada"
    return candidatos, "ok"


def _ler_aba(sh, aba):
    try:
        ws = sh.worksheet(aba)
    except gspread.WorksheetNotFound:
        return pd.DataFrame()
    valores = ws.get_all_values()
    if len(valores) < 2:
        return pd.DataFrame()
    return pd.DataFrame(valores[1:], columns=valores[0])


def salvar_no_sheets(sh, df, aba):
    """Sobrescreve a aba no Google Sheets com os dados do DataFrame."""
    try:
        ws = sh.worksheet(aba)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=aba, rows=len(df) + 1, cols=len(df.columns))
    valores = df.where(df.notna(), "").astype(str)
    reescrever_aba(ws, [valores.columns.tolist()] + valores.values.tolist(), aba)
    print(f"Sheets atualizado: aba '{aba}' ({len(df)} linhas)")


def _dedup_candidatos(df_cand):
    """Um candidato aparece em várias atas do mesmo partido: a retificadora
    repete a lista da convenção (204 casos em 23/07/2026, pedido da Manuela).
    Fica só a linha da ata mais recente por uf + partido + cargo + nome
    (dt_sincronizacao; empate decidido pelo sq_ata maior)."""
    if df_cand.empty:
        return df_cand
    df = df_cand.copy()
    df["_dt"] = pd.to_datetime(df["dt_sincronizacao"], dayfirst=True, errors="coerce")
    df["_sq"] = pd.to_numeric(df["sq_ata"], errors="coerce")
    df = df.sort_values(["_dt", "_sq"])
    df = df.drop_duplicates(subset=["uf", "partido_numero", "cargo", "nome"], keep="last")
    return df.drop(columns=["_dt", "_sq"])


def coletar(sh):
    eleicao = cod_eleicao()
    atas = listar_atas(eleicao)
    print(f"\ntotal: {len(atas)} ata(s) listadas")

    # Cache: o que já está na planilha, chaveado por sq_ata + dt_sincronizacao.
    # Ata inalterada não é rebaixada nem reparseada.
    df_atas_old = _ler_aba(sh, ABA_ATAS)
    df_cand_old = _ler_aba(sh, ABA_CANDIDATOS)
    cache = {}
    if not df_atas_old.empty and {"sq_ata", "dt_sincronizacao"} <= set(df_atas_old.columns):
        for _, r in df_atas_old.iterrows():
            cache[str(r["sq_ata"])] = r.to_dict()

    linhas_atas, linhas_cand = [], []
    reaproveitadas, parseadas = 0, 0
    for a in atas:
        chave = str(a["sq_ata"])
        antiga = cache.get(chave)
        inalterada = (antiga is not None
                      and antiga.get("dt_sincronizacao") == a["dt_sincronizacao"]
                      and not str(antiga.get("status_extracao", "")).startswith("erro"))
        if inalterada:
            linhas_atas.append({**a, "status_extracao": antiga.get("status_extracao"),
                                "n_candidatos": antiga.get("n_candidatos"),
                                "cargos": antiga.get("cargos")})
            if not df_cand_old.empty and "sq_ata" in df_cand_old.columns:
                velhas = df_cand_old[df_cand_old["sq_ata"] == chave]
                linhas_cand.extend(velhas.to_dict("records"))
            reaproveitadas += 1
            continue

        pdf = baixar_pdf(a["sq_eleicao"], a["sq_ata"])
        if pdf is None:
            candidatos, status = [], "erro: download falhou"
        else:
            candidatos, status = extrair_candidatos(pdf)
        parseadas += 1
        cargos = sorted({c["cargo"] for c in candidatos})
        linhas_atas.append({**a, "status_extracao": status,
                            "n_candidatos": len(candidatos),
                            "cargos": ", ".join(cargos)})
        for c in candidatos:
            linhas_cand.append({
                "ano": a["ano"], "uf": a["uf"],
                "partido_numero": a["partido_numero"],
                "partido_sigla": a["partido_sigla"],
                "tipo_ata": a["tipo_ata"],
                "cargo": c["cargo"], "concorre": c["concorre"],
                "ordem": c["ordem"], "nome": c["nome"],
                "nome_urna": c["nome_urna"], "numero": c["numero"],
                "genero": c["genero"], "cpf": c["cpf"],
                "sq_ata": a["sq_ata"], "link_pdf": a["link_pdf"],
                "dt_sincronizacao": a["dt_sincronizacao"],
            })
        print(f"  {a['uf']} {a['partido_sigla']} ({a['tipo_ata']}): "
              f"{status}, {len(candidatos)} candidato(s)")

    print(f"\n{parseadas} ata(s) baixadas/parseadas, {reaproveitadas} reaproveitadas")
    df_atas = pd.DataFrame(linhas_atas, columns=COLS_ATAS + ["sq_eleicao"])[COLS_ATAS]
    df_cand = _dedup_candidatos(pd.DataFrame(linhas_cand, columns=COLS_CANDIDATOS))
    df_atas = df_atas.sort_values(["uf", "partido_numero", "sq_ata"])
    df_cand = df_cand.sort_values(["uf", "partido_numero", "sq_ata", "ordem"],
                                  key=lambda s: pd.to_numeric(s, errors="coerce")
                                  if s.name in ("partido_numero", "sq_ata", "ordem") else s)
    return df_atas, df_cand


if __name__ == "__main__":
    sheets_id = os.getenv("SPREADSHEET_ID_TSE", "")
    if not sheets_id:
        raise SystemExit("Defina SPREADSHEET_ID_TSE.")
    gc = gspread.service_account(filename="credentials.json")
    sh = gc.open_by_key(sheets_id)
    df_atas, df_cand = coletar(sh)
    salvar_no_sheets(sh, df_atas, ABA_ATAS)
    salvar_no_sheets(sh, df_cand, ABA_CANDIDATOS)
