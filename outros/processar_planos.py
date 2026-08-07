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
from datetime import datetime, timedelta, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analise_planos import (  # noqa: E402
    NIVEIS, TEMAS, PlanoIndisponivel, RespostaIlegivel,
    _norm_busca, avaliar_coerencia, citacao_sustenta, classificar_plano,
    contexto_do_tema,
    extrair_paginas_url, ocorrencias_ancora, paginas_do_trecho, reanalisar_tema,
    tem_alvo_mensuravel, verificar_trecho,
)

# Convenções deste repo: credenciais em credentials.json (escrito pelo
# workflow a partir do secret) e planilha em SPREADSHEET_ID_TSE.

# Mesmos nomes e regras do painel. Mantidos em sincronia à mão porque a página
# importa streamlit e não dá para importá-la aqui.
ANALISE_ABA = "analise_planos"
COERENCIA_ABA = "coerencia_planos"
LIMIAR_CHARS = 1500
# Subiu em 04/08/2026 com a taxonomia nova: 10 eixos e 43 temas, contra 8 e 26.
# A coerência sobe junto porque o prompt dela lista os eixos e temas, e julgar
# "articulação entre eixos" com dez eixos não é a mesma pergunta que com oito.
# Subiu de novo em 05/08/2026: a descrição de "Saúde Mental" passou a citar
# álcool e outras drogas, ludopatia, apostas de quota fixa (bets) e imposto
# seletivo. Sem reprocessar, os planos já lidos não conhecem essas palavras. A
# coerência fica em 3: o prompt dela lista eixos e temas, que não mudaram.
# Subiu para 5 em 06/08/2026: o prompt passou a exigir [...] em toda junção de
# partes do plano, e a coluna `verificacao` nasce junto com a análise. Sem
# reprocessar, os trechos antigos ficariam sem o selo e com a junção não marcada.
# Subiu para 6 em 07/08/2026: o plano deixou de ser cortado em 80 mil caracteres
# e passa inteiro, em blocos; a classificação passa por conferir_classificacao
# antes de gravar; a coerência lê a análise verificada e não mais o texto cru.
# Metade do conteúdo da base nunca tinha sido lida, então nada de antes vale.
# Subiu para 7 no mesmo dia, depois de uma revisão que achou nível afirmado sem
# citação em quatro caminhos (trecho vazio, citação de três palavras, reanálise
# sem trecho e merge que descartava a citação boa) e a citação genérica, frase
# literal do plano que não é evidência de tema nenhum.
# Subiu para 8 ainda em 07/08/2026: "Define meta" passou a exigir alvo que dê
# para conferir depois. O modelo lia quantificador vago como número, e um terço
# dos 192 "Define meta" da base não tinha alvo nenhum.
VERSAO_ANALISE = "8"
VERSAO_COERENCIA = "6"

COLS = ["ano", "sq_candidato", "candidato", "partido", "uf", "cargo", "link",
        "tema", "nivel", "trecho", "responsavel", "prazo", "publico_alvo",
        "programa_nome", "pagina", "verificacao", "chars", "chars_analisados",
        "versao", "analisado_em"]
COLS_COE = ["ano", "sq_candidato", "candidato", "partido", "uf", "cargo", "link",
            "score_coerencia", "justificativa_coerencia", "chars",
            "chars_analisados", "versao", "analisado_em"]

# Só 2026. A aba planos_2022 continua na planilha, mas saiu do fluxo.
ANO = "2026"
ABA_BASE = "base_dadosabertos"
LOTE = 5           # candidatos entre uma gravação e outra
# Segundos entre um candidato e outro. Até 02/08/2026 a fila tinha 1 candidato por
# rodada e a pausa não fazia falta. Quando a versão subiu para 2 e os 16 entraram
# de uma vez, o DivulgaCand recusou tudo. Pedir 16 PDFs em sequência é um padrão
# de acesso diferente do que vinha sendo feito.
PAUSA_ENTRE_PLANOS = int(os.getenv("PAUSA_ENTRE_PLANOS", "5"))
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
    # Coluna que o código grava e a aba ainda não tem entra no cabeçalho antes
    # da escrita. Sem isso o reindex abaixo descarta o campo em silêncio: foi
    # assim que a coluna `tipo` sumiu no scraper de notícias, classificada e
    # cobrada do Gemini a cada rodada sem nunca chegar à planilha.
    faltantes = [c for c in colunas if c not in cab]
    if faltantes:
        if len(cab) + len(faltantes) > ws.col_count:
            ws.add_cols(len(cab) + len(faltantes) - ws.col_count)
        cab = cab + faltantes
        ws.update(values=[cab], range_name="A1")
    df = pd.DataFrame(linhas).reindex(columns=cab).fillna("").astype(str)
    ws.append_rows(df.values.tolist(), value_input_option="USER_ENTERED")


def reescrever(sh, nome: str, colunas: list[str], df: pd.DataFrame) -> None:
    """Reescreve a aba inteira. Só usado quando há reanálise, porque aí é
    preciso remover as linhas antigas do candidato.

    Repete em falha transitória porque clear + update não é atômico: se o
    update cai, a aba fica vazia, e aqui isso significaria perder a análise
    inteira depois de uma rodada de Gemini já paga. Em 06/08/2026 o workflow 10
    perdeu a aba de candidaturas exatamente assim, com um 409 do Sheets no meio
    da gravação. O clear é idempotente, então repetir o par é seguro.
    """
    from compartilhado.relatorios_sheets_utils import reescrever_aba
    ws = _aba_ou_cria(sh, nome, colunas)
    d = df.reindex(columns=colunas).fillna("").astype(str)
    reescrever_aba(ws, [colunas] + d.values.tolist(), nome)


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

def conferir_classificacao(classif: dict, texto: str,
                           paginas_norm: list[str]) -> dict:
    """Passa cada tema por uma conferência contra o texto antes de gravar.

    Duas coisas iam para a planilha sem ninguém conferir, e as duas foram
    medidas em 07/08/2026:

    - citação que o modelo escreveu em vez de copiar. Eram 8,1% da base, e 88%
      das citações do Kalil (MG). A que abria Primeira Infância falava em
      "ampliação do acesso à creche" e a palavra creche não aparece uma vez nas
      147 páginas do plano dele.
    - ausência que ninguém checou. No plano do Zema, 12 dos 15 temas gravados
      como "Não menciona" tinham ocorrência no texto.

    Nos dois casos o tema volta para reanalisar_tema, agora com o entorno do
    plano onde o assunto aparece. O que a segunda passagem não sustentar com
    citação verificável não vira nível: o trecho é descartado e o tema fica como
    "Não menciona", que é o que o texto sustenta quando nem o termo aparece.
    """
    texto_norm = _norm_busca(texto)

    def sem_lastro(item):
        """O tema volta para 'Não menciona': nada do que estava ali se sustenta."""
        return dict(item, nivel="Não menciona", score=0, trecho="",
                    responsavel="", prazo="", publico_alvo="", programa_nome="")

    for tema, item in classif.items():
        if item["nivel"] == "Não menciona":
            if not ocorrencias_ancora(texto_norm, tema):
                continue                      # ausência conferida, nada a fazer
        elif citacao_sustenta(paginas_norm, item["trecho"]):
            continue                          # citação bate com o plano

        contexto = contexto_do_tema(texto, texto_norm, tema)
        if not contexto:
            # Sem nem o termo no plano, não há o que reperguntar: o nível que
            # estava lá vinha de citação que o plano não tem.
            classif[tema] = sem_lastro(item)
            continue
        try:
            novo = reanalisar_tema(contexto, tema, TEMAS.get(tema, ""))
        except (RespostaIlegivel, json.JSONDecodeError, ValueError):
            novo = None
        if novo and (novo["nivel"] == "Não menciona"
                     or citacao_sustenta(paginas_norm, novo["trecho"])):
            classif[tema] = novo
        else:
            classif[tema] = sem_lastro(item)

    classif = _conferir_citacao_generica(classif, texto, texto_norm, paginas_norm)
    return _conferir_meta(classif)


def _conferir_meta(classif: dict) -> dict:
    """Desce para 'Propõe ação' a meta que não diz quanto nem até quando.

    A régua da escala é "alvo mensurável: número, percentual, prazo". O modelo
    vinha lendo quantificador vago como número: no plano da Samara (UP),
    "geração de milhões de empregos" virou Define meta, e "Revogação da Lei do
    Novo Ensino Médio", que é ação, também. Um terço dos 192 "Define meta" da
    base de 07/08/2026 não tinha alvo nenhum.

    Só desce um degrau, nunca apaga: a citação existe e a ação está lá, o que
    não se sustenta é chamar aquilo de meta.
    """
    for tema, item in classif.items():
        if item["nivel"] == "Define meta" and not tem_alvo_mensuravel(item["trecho"]):
            classif[tema] = dict(item, nivel="Propõe ação",
                                 score=NIVEIS.index("Propõe ação"))
    return classif


# Quantos temas a mesma citação pode sustentar antes de deixar de ser evidência.
# Dois é normal e legítimo: uma frase sobre educação básica serve a "Fundamental"
# e a "Ensino Médio" ao mesmo tempo, e esse par respondeu por 7 das repetições de
# 07/08/2026. Três já não descreve tema nenhum.
LIMITE_TEMAS_POR_CITACAO = 3


def _conferir_citacao_generica(classif: dict, texto: str, texto_norm: str,
                               paginas_norm: list[str]) -> dict:
    """Tira o nível do tema que se apoia numa frase genérica do plano.

    verificar_trecho responde "essa frase está no plano", não "essa frase é sobre
    esse tema", e a diferença aparecia na planilha: em 07/08/2026, uma frase do
    plano do Prof Witer Naves (TO), "O primeiro princípio é o Direito da gente
    Tocantinense", sustentava sozinha 14 temas, entre eles alfabetização, saúde
    mental e crime organizado. A citação é transcrição fiel e não é evidência de
    nada. Eram 3,5% das linhas da base.

    O tema volta para a pergunta dirigida, que recebe só o entorno do assunto no
    plano. Se de lá vier outra citação, verificável e não genérica, o tema fica;
    senão cai para "Não menciona".
    """
    from collections import defaultdict
    por_trecho = defaultdict(list)
    for tema, item in classif.items():
        chave = _norm_busca(item.get("trecho", ""))
        if chave and item["nivel"] != "Não menciona":
            por_trecho[chave].append(tema)

    for chave, temas_do_trecho in por_trecho.items():
        if len(temas_do_trecho) < LIMITE_TEMAS_POR_CITACAO:
            continue
        for tema in temas_do_trecho:
            item = classif[tema]
            contexto = contexto_do_tema(texto, texto_norm, tema)
            novo = None
            if contexto:
                try:
                    novo = reanalisar_tema(contexto, tema, TEMAS.get(tema, ""))
                except (RespostaIlegivel, json.JSONDecodeError, ValueError):
                    novo = None
            vale = (novo and novo["nivel"] != "Não menciona"
                    and _norm_busca(novo["trecho"]) != chave
                    and citacao_sustenta(paginas_norm, novo["trecho"]))
            classif[tema] = novo if vale else dict(
                item, nivel="Não menciona", score=0, trecho="", responsavel="",
                prazo="", publico_alvo="", programa_nome="")
    return classif


def processar(r, ano: str) -> tuple[list[dict], dict | None, str]:
    """Baixa, extrai e roda classificação e coerência sobre o MESMO texto.

    Devolve (linhas de análise, linha de coerência, motivo de pulo).
    """
    link = str(r["LINK_PLANO"])
    # Extrai página a página: o mesmo texto alimenta a classificação (tudo
    # junto) e o cálculo de em que página está cada trecho citado.
    paginas = extrair_paginas_url(link)      # levanta PlanoIndisponivel se cair
    texto = " ".join(paginas)
    paginas_norm = [_norm_busca(p) for p in paginas]
    chars = len((texto or "").strip())
    if chars < LIMIAR_CHARS:
        return [], None, f"extração pobre ({chars} caracteres)"

    try:
        classif = classificar_plano(texto)   # levanta RespostaIlegivel
    except RespostaIlegivel as e:
        # A resposta do modelo não é determinística: o plano do Lucien Rezende
        # voltou ilegível em 03/08/2026 numa reanálise que já tinha dado certo
        # antes. Uma segunda tentativa usa o mesmo texto, sem novo download.
        print(f"(resposta ilegível, tentando de novo: {e})", end=" ", flush=True)
        time.sleep(3)
        classif = classificar_plano(texto)
    classif = conferir_classificacao(classif, texto, paginas_norm)
    try:
        coe = avaliar_coerencia(classif)
    except RespostaIlegivel as e:
        # A coerência não tinha segunda chance, e é a última chamada do
        # candidato: cair aqui jogava fora a classificação inteira, já paga.
        print(f"(coerência ilegível, tentando de novo: {e})", end=" ", flush=True)
        time.sleep(3)
        coe = avaliar_coerencia(classif)

    # Data em que esta análise foi feita. Sem ela, olhando a planilha ou o painel
    # não dá para saber se o que está lá é de hoje ou de duas semanas atrás.
    agora = datetime.now(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M")
    comum = {"ano": ano, "sq_candidato": str(r["SQ_CANDIDATO"]),
             "candidato": r["NM_URNA_CANDIDATO"], "partido": r["SG_PARTIDO"],
             "uf": r["SG_UF"], "cargo": r["DS_CARGO"], "link": link, "chars": chars,
             # Quanto do plano foi de fato lido. Enquanto só existia `chars`, o
             # corte em 80 mil não aparecia em lugar nenhum: a planilha registrava
             # os 145 mil caracteres do plano do Zema e o modelo tinha visto 80
             # mil. Com as duas colunas lado a lado, truncamento vira dado.
             "chars_analisados": chars,
             "analisado_em": agora}

    linhas = [dict(comum, tema=tema, versao=VERSAO_ANALISE,
                   nivel=res["nivel"], trecho=res["trecho"],
                   responsavel=res.get("responsavel", ""),
                   prazo=res.get("prazo", ""),
                   publico_alvo=res.get("publico_alvo", ""),
                   programa_nome=res.get("programa_nome", ""),
                   # Vazio quando o modelo resumiu em vez de citar: aí a frase
                   # não está literal no PDF e não há página para apontar.
                   pagina=", ".join(
                       str(n) for n in paginas_do_trecho(paginas_norm,
                                                         res["trecho"])),
                   # Conferida aqui, contra o mesmo texto que o modelo acabou de
                   # ler. É o que deixa o painel dizer quais trechos são
                   # transcrição sem que ninguém tenha que abrir o PDF.
                   verificacao=verificar_trecho(paginas_norm, res["trecho"]))
              for tema, res in classif.items()]
    linha_coe = dict(comum, versao=VERSAO_COERENCIA,
                     score_coerencia=coe["score"],
                     justificativa_coerencia=coe["justificativa"])
    return linhas, linha_coe, ""


def preencher_paginas(sh, uf: str = "", limite: int = 0) -> int:
    """Preenche a coluna `pagina` das análises já gravadas, sem chamar o modelo.

    A coluna nasceu depois das primeiras análises. Reanalisar tudo só para ela
    custaria uma rodada inteira de Gemini à toa: aqui o plano é baixado, o
    trecho de cada tema é casado com a página e só essa célula é escrita.

    Não escreve `verificacao` de propósito. Aqui o PDF é extraído de novo, e a
    extração de hoje não sai igual à que gerou a análise: nos 1.261 trechos
    medidos em 06/08/2026, 41 apareceram como não localizados só por caractere
    quebrado que a extração original não tinha. Selo de verificação errado é
    pior que selo ausente, então ele só nasce junto da análise, em processar().
    """
    salvas = ler_aba(sh, ANALISE_ABA)
    if salvas.empty:
        print("Nada gravado ainda.")
        return 0
    if "pagina" not in salvas.columns:
        salvas["pagina"] = ""
    alvo = salvas[(salvas["pagina"].astype(str).str.strip() == "")
                  & (salvas["trecho"].astype(str).str.strip() != "")]
    if uf:
        alvo = alvo[alvo["uf"].astype(str).str.upper() == uf.upper()]
    sqs = list(dict.fromkeys(alvo["sq_candidato"].astype(str)))
    if limite:
        sqs = sqs[:limite]
    print(f"{len(alvo)} linhas sem página, em {len(sqs)} planos")
    if not sqs:
        return 0

    for n, sq in enumerate(sqs, 1):
        linhas = salvas[salvas["sq_candidato"].astype(str) == sq]
        link = str(linhas.iloc[0].get("link", "") or "")
        nome = str(linhas.iloc[0].get("candidato", ""))
        print(f"[{n}/{len(sqs)}] {linhas.iloc[0].get('uf','')} · {nome}...",
              end=" ", flush=True)
        if not link.startswith("http"):
            print("sem link")
            continue
        try:
            paginas_norm = [_norm_busca(x) for x in extrair_paginas_url(link)]
        except Exception as e:
            print(f"não abriu ({e})")
            continue
        achou = 0
        for i, r in linhas.iterrows():
            if str(r.get("pagina", "") or "").strip():
                continue
            pgs = paginas_do_trecho(paginas_norm, str(r.get("trecho", "")))
            salvas.at[i, "pagina"] = ", ".join(str(x) for x in pgs)
            achou += 1 if pgs else 0
        print(f"{achou} de {len(linhas)} trechos localizados")

    reescrever(sh, ANALISE_ABA, COLS, salvas)
    print("Coluna `pagina` gravada.")
    return 0


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
    p.add_argument("--so-paginas", action="store_true",
                   help="só preenche a coluna `pagina` do que já está gravado, "
                        "sem chamar o modelo")
    args = p.parse_args()

    if not args.planilha:
        raise SystemExit("Informe --planilha ou defina SPREADSHEET_ID_TSE.")
    if not args.so_paginas and not os.getenv("GEMINI_API_KEY", "").strip():
        raise SystemExit("Defina GEMINI_API_KEY: é ela que roda a análise.")

    gc = cliente(args.credenciais)
    sh = gc.open_by_key(args.planilha)

    if args.so_paginas:
        return preencher_paginas(sh, args.uf, args.limite)

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
        time.sleep(PAUSA_ENTRE_PLANOS)

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

    # Rodada que tinha fila e não gravou nada precisa sair vermelha. Antes ela
    # terminava em 0 e o Actions marcava sucesso, do mesmo jeito que quando não
    # havia nada a fazer. Foi assim que a recusa do DivulgaCand em 01/08/2026
    # passou despercebida.
    if feitos == 0 and fila:
        print("Nenhum plano da fila foi processado. Saindo com erro.")
        return 1
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
