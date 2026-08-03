# -*- coding: utf-8 -*-
"""Processa os planos de governo fora do painel e grava o resultado na planilha.

O painel deixa de analisar na abertura da página: quem abre só lê o que já está
gravado. Este script é quem produz esse conteúdo, rodado por quem administra o
painel conforme novos planos entram no TSE.

Cada candidato é baixado e extraído UMA vez, e o mesmo texto alimenta a
classificação por tema e a avaliação de coerência, que antes eram duas passagens
separadas (a coerência nem rodava no botão "Analisar todos os governadores").

Uso:
    export GEMINI_API_KEY=...
    export SPREADSHEET_ID_TSE=...
    python -m outros.processar_planos
    python -m outros.processar_planos --uf CE --forcar
    python -m outros.processar_planos --limite 5

Grava em lotes: uma interrupção no meio perde no máximo o lote corrente, e a
execução seguinte retoma de onde parou.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analise_planos import (  # noqa: E402
    PlanoIndisponivel, RespostaIlegivel,
    avaliar_coerencia, classificar_plano, extrair_texto_url,
)

# Convenções deste repo: credenciais em credentials.json (escrito pelo
# workflow a partir do secret) e planilha em SPREADSHEET_ID_TSE.

# Mesmos nomes e regras do painel. Mantidos em sincronia à mão porque a página
# importa streamlit e não dá para importá-la aqui.
ANALISE_ABA = "analise_planos"
COERENCIA_ABA = "coerencia_planos"
LIMIAR_CHARS = 1500
VERSAO_ANALISE = "2"
VERSAO_COERENCIA = "2"

COLS = ["ano", "sq_candidato", "candidato", "partido", "uf", "cargo", "link",
        "tema", "nivel", "trecho", "responsavel", "prazo", "publico_alvo",
        "programa_nome", "chars", "versao"]
COLS_COE = ["ano", "sq_candidato", "candidato", "partido", "uf", "cargo", "link",
            "score_coerencia", "justificativa_coerencia", "chars", "versao"]

# Só 2026. A aba planos_2022 continua na planilha, mas saiu do fluxo.
ANO = "2026"
ABA_BASE = "base_dadosabertos"
LOTE = 5           # candidatos entre uma gravação e outra
ESCOPOS = ["https://www.googleapis.com/auth/spreadsheets"]


# ─── Planilha ────────────────────────────────────────────────────────────────

def cliente(caminho_credenciais: str = ""):
    import gspread
    from google.oauth2.service_account import Credentials
    if caminho_credenciais:
        with open(caminho_credenciais, encoding="utf-8") as f:
            info = json.load(f)
    else:
        bruto = os.getenv("GOOGLE_SHEETS_CREDS", "").strip()
        if not bruto:
            caminho = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
            if not os.path.exists(caminho):
                raise SystemExit(
                    "Sem credenciais. Defina GOOGLE_SHEETS_CREDS com o JSON da conta "
                    "de serviço, ou passe --credenciais caminho/para/credentials.json"
                )
            with open(caminho, encoding="utf-8") as f:
                info = json.load(f)
        else:
            info = json.loads(bruto)
    return gspread.authorize(Credentials.from_service_account_info(info, scopes=ESCOPOS))


def ler_aba(sh, nome: str) -> pd.DataFrame:
    try:
        valores = sh.worksheet(nome).get_all_values()
    except Exception:
        return pd.DataFrame()
    if not valores:
        return pd.DataFrame()
    return pd.DataFrame(valores[1:], columns=[c.strip() for c in valores[0]])


def _aba_ou_cria(sh, nome: str, colunas: list[str]):
    try:
        return sh.worksheet(nome)
    except Exception:
        ws = sh.add_worksheet(nome, rows=200, cols=len(colunas) + 2)
        ws.update(values=[colunas])
        return ws


def anexar(sh, nome: str, colunas: list[str], linhas: list[dict]) -> None:
    """Acrescenta linhas no fim da aba, sem apagar nada.

    O painel reescreve a aba inteira (clear + update), o que perde dados se duas
    escritas se cruzarem. Aqui só acrescentamos: é seguro rodar enquanto alguém
    está com o painel aberto.
    """
    if not linhas:
        return
    ws = _aba_ou_cria(sh, nome, colunas)
    cab = [c.strip() for c in (ws.row_values(1) or [])]
    if not cab:
        ws.update(values=[colunas])
        cab = colunas
    df = pd.DataFrame(linhas).reindex(columns=cab).fillna("").astype(str)
    ws.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")


def reescrever(sh, nome: str, colunas: list[str], df: pd.DataFrame) -> None:
    """Reescreve a aba inteira. Só usado quando há reanálise, porque aí é
    preciso remover as linhas antigas do candidato."""
    ws = _aba_ou_cria(sh, nome, colunas)
    d = df.reindex(columns=colunas).fillna("").astype(str)
    ws.clear()
    ws.update(values=[colunas] + d.values.tolist())


# ─── Regra de reprocessamento (igual à do painel) ────────────────────────────

def versao_salva(salvas: pd.DataFrame, sq: str) -> str:
    if salvas.empty or "versao" not in salvas.columns:
        return ""
    linhas = salvas[salvas["sq_candidato"].astype(str) == str(sq)]
    return "" if linhas.empty else str(linhas["versao"].iloc[0]).strip()


def pendentes(cands: pd.DataFrame, salvas: pd.DataFrame, coes: pd.DataFrame,
              forcar: bool) -> list:
    if forcar:
        return [r for _, r in cands.iterrows()]
    ja_a = ({str(r["sq_candidato"]): str(r["link"]) for _, r in salvas.iterrows()}
            if not salvas.empty else {})
    ja_c = ({str(r["sq_candidato"]): str(r["link"]) for _, r in coes.iterrows()}
            if not coes.empty else {})
    fila = []
    for _, r in cands.iterrows():
        sq, link = str(r["SQ_CANDIDATO"]), str(r["LINK_PLANO"])
        falta_analise = (ja_a.get(sq) != link
                         or versao_salva(salvas, sq) != VERSAO_ANALISE)
        falta_coerencia = (ja_c.get(sq) != link
                           or versao_salva(coes, sq) != VERSAO_COERENCIA)
        if falta_analise or falta_coerencia:
            fila.append(r)
    return fila


# ─── Processamento ───────────────────────────────────────────────────────────

def processar(r, ano: str) -> tuple[list[dict], dict | None, str]:
    """Baixa, extrai e roda classificação e coerência sobre o MESMO texto.

    Devolve (linhas de análise, linha de coerência, motivo de pulo).
    """
    link = str(r["LINK_PLANO"])
    texto = extrair_texto_url(link)          # levanta PlanoIndisponivel se cair
    chars = len((texto or "").strip())
    if chars < LIMIAR_CHARS:
        return [], None, f"extração pobre ({chars} caracteres)"

    classif = classificar_plano(texto)       # levanta RespostaIlegivel
    coe = avaliar_coerencia(texto)

    comum = {"ano": ano, "sq_candidato": str(r["SQ_CANDIDATO"]),
             "candidato": r["NM_URNA_CANDIDATO"], "partido": r["SG_PARTIDO"],
             "uf": r["SG_UF"], "cargo": r["DS_CARGO"], "link": link, "chars": chars}

    linhas = [dict(comum, tema=tema, versao=VERSAO_ANALISE,
                   nivel=res["nivel"], trecho=res["trecho"],
                   responsavel=res.get("responsavel", ""),
                   prazo=res.get("prazo", ""),
                   publico_alvo=res.get("publico_alvo", ""),
                   programa_nome=res.get("programa_nome", ""))
              for tema, res in classif.items()]
    linha_coe = dict(comum, versao=VERSAO_COERENCIA,
                     score_coerencia=coe["score"],
                     justificativa_coerencia=coe["justificativa"])
    return linhas, linha_coe, ""


def main() -> int:
    p = argparse.ArgumentParser(description=__doc__,
                                formatter_class=argparse.RawDescriptionHelpFormatter)
    p.add_argument("--cargo", default="GOVERNADOR",
                   help="GOVERNADOR, PRESIDENTE ou TODOS")
    p.add_argument("--uf", default="", help="restringe a uma UF")
    p.add_argument("--limite", type=int, default=0, help="processa no máximo N candidatos")
    p.add_argument("--forcar", action="store_true",
                   help="reprocessa mesmo quem já está gravado e atualizado")
    p.add_argument("--planilha",
                   default=os.getenv("SPREADSHEET_ID_TSE",
                                     os.getenv("COLETA_SHEET_ID", "")),
                   help="ID da planilha (ou env SPREADSHEET_ID_TSE)")
    p.add_argument("--credenciais", default="", help="caminho do credentials.json")
    args = p.parse_args()

    if not args.planilha:
        raise SystemExit("Informe --planilha ou defina SPREADSHEET_ID_TSE.")
    if not os.getenv("GEMINI_API_KEY", "").strip():
        raise SystemExit("Defina GEMINI_API_KEY: é ela que roda a análise.")

    gc = cliente(args.credenciais)
    sh = gc.open_by_key(args.planilha)

    base = ler_aba(sh, ABA_BASE)
    if base.empty or "LINK_PLANO" not in base.columns:
        raise SystemExit(f"A aba {ABA_BASE} está vazia ou sem LINK_PLANO.")
    base = base[base["LINK_PLANO"].astype(str).str.startswith("http")]
    if args.cargo.upper() != "TODOS":
        base = base[base["DS_CARGO"].astype(str).str.strip().str.upper()
                    == args.cargo.upper()]
    if args.uf:
        base = base[base["SG_UF"].astype(str).str.strip().str.upper() == args.uf.upper()]

    salvas = ler_aba(sh, ANALISE_ABA)
    coes = ler_aba(sh, COERENCIA_ABA)
    fila = pendentes(base, salvas, coes, args.forcar)
    if args.limite:
        fila = fila[:args.limite]

    print(f"Base {ABA_BASE} ({ANO}) · cargo {args.cargo}"
          + (f" · UF {args.uf.upper()}" if args.uf else ""))
    print(f"{len(base)} candidatos com plano · {len(fila)} a processar")
    if not fila:
        print("Nada a fazer: tudo já analisado e na versão atual.")
        return 0

    ja_gravados = set(salvas["sq_candidato"].astype(str)) if not salvas.empty else set()
    buffer_a, buffer_c = [], []
    reanalisados, indisponiveis, ilegiveis, erros = set(), [], [], []
    feitos = 0
    inicio = time.time()

    def descarrega():
        """Grava o que está no buffer. Só acrescenta; reanálise fica para o fim."""
        nonlocal buffer_a, buffer_c
        novos_a = [l for l in buffer_a if l["sq_candidato"] not in reanalisados]
        novos_c = [l for l in buffer_c if l["sq_candidato"] not in reanalisados]
        anexar(sh, ANALISE_ABA, COLS, novos_a)
        anexar(sh, COERENCIA_ABA, COLS_COE, novos_c)
        buffer_a, buffer_c = [l for l in buffer_a if l["sq_candidato"] in reanalisados], \
                             [l for l in buffer_c if l["sq_candidato"] in reanalisados]

    for n, r in enumerate(fila, 1):
        nome = str(r["NM_URNA_CANDIDATO"])
        sq = str(r["SQ_CANDIDATO"])
        print(f"[{n}/{len(fila)}] {r['SG_UF']} · {nome}...", end=" ", flush=True)
        try:
            linhas, coe, pulo = processar(r, ANO)
        except PlanoIndisponivel as e:
            indisponiveis.append(nome)
            # Sem o motivo no log, "fora do ar" fica indistinguível de bloqueio
            # por IP, timeout e erro de TLS. Em 03/08/2026 o runner do Actions
            # não baixou nenhum plano enquanto os mesmos links respondiam 200
            # fora dele, e o log não dizia por quê.
            print(f"plano fora do ar, tenta depois ({e})")
            continue
        except RespostaIlegivel as e:
            ilegiveis.append(nome)
            print(f"resposta ilegível ({e})")
            continue
        except Exception as e:                       # noqa: BLE001
            erros.append(f"{nome}: {e}")
            print(f"erro: {e}")
            continue
        if pulo:
            ilegiveis.append(f"{nome} ({pulo})")
            print(f"pulado: {pulo}")
            continue

        if sq in ja_gravados:
            reanalisados.add(sq)
        buffer_a.extend(linhas)
        buffer_c.append(coe)
        feitos += 1
        niveis = sum(1 for l in linhas if l["nivel"] != "Não menciona")
        print(f"ok ({niveis}/{len(linhas)} temas com conteúdo, "
              f"coerência {coe['score_coerencia']})")

        if feitos % LOTE == 0:
            descarrega()
            print(f"    ... {feitos} gravados ({time.time() - inicio:.0f}s)")

    descarrega()

    if reanalisados:
        # Reanálise exige tirar as linhas antigas do candidato: só aqui a aba é
        # reescrita, uma vez, no fim.
        print(f"Reescrevendo as abas para {len(reanalisados)} reanálise(s)...")
        base_a = salvas[~salvas["sq_candidato"].astype(str).isin(reanalisados)] \
            if not salvas.empty else pd.DataFrame(columns=COLS)
        base_c = coes[~coes["sq_candidato"].astype(str).isin(reanalisados)] \
            if not coes.empty else pd.DataFrame(columns=COLS_COE)
        atual_a = ler_aba(sh, ANALISE_ABA)
        atual_c = ler_aba(sh, COERENCIA_ABA)
        novas_a = atual_a[~atual_a["sq_candidato"].astype(str).isin(reanalisados)] \
            if not atual_a.empty else base_a
        novas_c = atual_c[~atual_c["sq_candidato"].astype(str).isin(reanalisados)] \
            if not atual_c.empty else base_c
        reescrever(sh, ANALISE_ABA, COLS,
                   pd.concat([novas_a, pd.DataFrame(buffer_a)], ignore_index=True))
        reescrever(sh, COERENCIA_ABA, COLS_COE,
                   pd.concat([novas_c, pd.DataFrame(buffer_c)], ignore_index=True))

    print(f"\n{feitos} plano(s) processados em {time.time() - inicio:.0f}s.")
    if indisponiveis:
        print(f"{len(indisponiveis)} fora do ar (rode de novo mais tarde): "
              f"{', '.join(indisponiveis[:8])}")
    if ilegiveis:
        print(f"{len(ilegiveis)} não puderam ser lidos: {', '.join(ilegiveis[:8])}")
    if erros:
        print(f"{len(erros)} com erro: {'; '.join(erros[:5])}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
