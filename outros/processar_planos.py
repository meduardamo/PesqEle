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
    NIVEIS, NIVEIS_LEGADO, TEMAS, PlanoIndisponivel, RespostaIlegivel,
    _norm_busca, _sem_espaco, citacao_sustenta, classificar_plano, resumir_plano,
    contexto_do_trecho,
    contexto_do_tema, contexto_do_vocabulario, posicoes_do_tema,
    extrair_paginas_url, ocorrencias_ancora, paginas_do_trecho, reanalisar_tema,
    normalizar_responsavel, tem_alvo_mensuravel, tema_e_item_de_enumeracao,
    verificar_trecho,
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
# Subiu para 9: ancoras reforcadas nos cinco temas em que a guarda de ausencia
# mais falhava, triagem de tema que e item de enumeracao na citacao, e JSON
# quebrado do modelo virou retentativa em vez de candidato perdido.
# Subiu para 12 em 09/08/2026, depois da conferência da base inteira: dígito
# sozinho deixou de valer como alvo em "Define meta" (12 das 299 metas eram
# numeração de item, número de página ou ano no nome do plano), a mesma citação
# passou a exigir o mesmo nível em todos os temas que sustenta (11 casos
# divergiam) e título de seção deixou de contar como proposta (3 casos, todos do
# Alexandre Kalil).
# Subiu para 13 em 10/08/2026: entraram três temas que a lista não tinha, e a
# falta deles jogava a proposta no tema errado ou no nada. Financiamento da
# Educação e Financiamento e Gestão do SUS, porque os temas de educação são por
# etapa e os de saúde por nível de atenção, então "10% do PIB para educação" e
# "Aumento do Orçamento que permita o funcionamento de 100% da saúde pública"
# não tinham onde cair. E Ensino Superior, que morava dentro da descrição de
# Subiu para 14 em 19/08/2026: inclusão de Educação, Arte e Cultura e Recomposição das Aprendizagens.
# Subiu para 15 no mesmo dia, antes de a leva rodar. Os dois temas novos entraram
# com descrição que não dizia o que NÃO era deles, e "Educação, Arte e Cultura"
# ficou disputando a mesma citação com "Cultura", que já existia no eixo de
# Cultura, esporte e turismo. É o erro que Ensino Superior e Ciência, Tecnologia
# e Inovação já tinham dado em 10/08, e a correção é a mesma: cada descrição
# aponta para a outra. "patrimônio cultural" saiu das âncoras do tema escolar
# pelo mesmo motivo, é política cultural e não escola.
# Subiu para 16 antes de a leva de 15 sair do lugar. A varredura de vocabulário
# entre os 49 temas apontou dois pares que dividiam palavra discriminante sem
# nenhuma das descrições dizer de quem era o quê. "Tempo Integral" e "Educação,
# Arte e Cultura" dividiam "educação integral", e é exatamente a frase que o
# Itaú citou ao pedir o tema ("arte e cultura na educação integral", do plano do
# Lula): sem separar, o pedido deles cairia no tema errado. "Valorização
# Docente" e "Servidores e Municípios" dividiam carreira e concurso, e professor
# é servidor estadual.
VERSAO_ANALISE = "16"
# Subiu para 11 em 09/08/2026: a justificativa passou a receber nome de urna e
# gênero do cadastro, então fala "a candidata" quando é mulher e alterna o
# sujeito em vez de abrir toda frase com "o candidato". O painel já corrige isso
# na tela; a versão nova é para o texto gravado nascer assim.
# Subiu para 12 em 10/08/2026, com a régua reescrita. A nota tinha virado
# contagem de temas com proposta: 56 dos 79 planos estavam em 4. Agora o modelo
# nomeia os pares de temas que se sustentam e o instrumento que os liga, e o
# score é limitado pelos pares válidos. No plano da Samara Martins, uma lista de
# reivindicações, a nota saiu de 3 para 2.
# Subiu para 13 em 17/08/2026, na conferência da base antes de abrir o painel
# para cliente: 93 dos 201 resumos escreviam o nome dos temas da análise no meio
# da frase ("o Planserv, que abrange Financiamento e Gestão do SUS e Média e
# Alta Complexidade"), porque o prompt mandava dizer quais temas o programa
# cobria. O prompt agora pede o que o programa faz, e `temas_no_texto` confere
# depois da resposta. Junto, sigla de nome de urna parou de virar nome próprio
# ("Acm Neto" na BA, "Jhc" em AL). O resumo é o primeiro texto que o cliente lê
# de cada plano, então nada de antes serve.
VERSAO_COERENCIA = "13"

# `contexto` entrou em 19/08/2026: o texto do plano em volta da citação, para o
# painel mostrar a proposta inteira sem obrigar a abrir o PDF. Fica na análise, e
# não é calculado na visita, pelo mesmo motivo de `pagina`: o PDF já está aberto
# aqui, e lá custaria um download de até 10 MB por pessoa que abre a página.
COLS = ["ano", "sq_candidato", "candidato", "partido", "uf", "cargo", "link",
        "tema", "nivel", "trecho", "contexto", "responsavel", "prazo",
        "publico_alvo", "programa_nome", "pagina", "verificacao", "entes",
        "chars", "chars_analisados", "versao", "analisado_em"]
# `resumos_eixos` não entra aqui: quem escreve a coluna é a aba de coerência,
# uma linha por candidato. Listada em COLS, o reindex de `gravar` criava uma
# coluna 23 sempre vazia na aba de análise, que tem uma linha por tema.
# score_coerencia saiu em 10/08/2026 e a coluna fica, vazia, para não quebrar
# quem já leu a aba. O que a tela usa agora é `resumo`, que descreve o plano em
# vez de justificar um número.
COLS_COE = ["ano", "sq_candidato", "candidato", "partido", "uf", "cargo", "link",
            "resumo", "pontes", "score_coerencia", "justificativa_coerencia",
            "chars", "chars_analisados", "versao", "analisado_em", "resumos_eixos"]

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


def gravar(sh, nome: str, colunas: list[str], chave: list[str],
           novas: list[dict]) -> None:
    """Lê a aba, substitui o que é dos candidatos gravados agora e regrava tudo.

    Aqui havia um `anexar` que mandava "acrescente no fim" (values.append) e
    deixava a API do Sheets achar onde é o fim. Em 09/08/2026 ela achou a linha
    1722, no meio do dado: as 215 linhas da rodada das 20:18 caíram em cima de
    cinco candidatos, e o Lula, analisado às 19:30, sumiu do painel na rodada
    seguinte. A aba ficava sempre com 65 candidatos e trocava quem estava
    dentro, então a fila dizia "7 a processar" de hora em hora e o Gemini era
    pago de novo pelos mesmos planos.

    O append não erra sozinho: com buraco no meio da aba ele acha o fim certo,
    conferido em 09/08/2026. O que confunde é a rodada escrever de duas formas
    na mesma aba, `anexar` e a reescrita do fim, cada uma partindo de uma foto
    diferente do estado. A reescrita é clear + update, que não é atômico: entre
    um e outro a aba fica curta, e um append que caia nessa janela grava numa
    linha baixa.

    Agora é uma forma só. Sai mais caro (a aba inteira a cada gravação) e não
    depende de a API adivinhar nada: as linhas que vão para a planilha são as
    que estão aqui na memória.

    `chave` dedupe o que sobrar, guardando sempre a análise mais recente. É o
    que colapsa as 108 linhas repetidas que a aba de coerência acumulou, oito
    delas do Lula.
    """
    if not novas:
        return
    sqs = {str(l.get("sq_candidato", "")) for l in novas}
    atual = ler_aba(sh, nome)
    if not atual.empty and "sq_candidato" in atual.columns:
        # O candidato regravado sai inteiro: reanálise pode devolver menos temas
        # que a anterior, e sobra de linha antiga viraria tema fantasma.
        atual = atual[~atual["sq_candidato"].astype(str).isin(sqs)]
    juntas = pd.concat([atual, pd.DataFrame(novas)], ignore_index=True) \
        if not atual.empty else pd.DataFrame(novas)
    juntas = juntas.reindex(columns=colunas).fillna("").astype(str)
    if "analisado_em" in juntas.columns:
        # Ordem por data de análise, não por posição na aba: com keep="last" a
        # linha que vence tem que ser a mais nova, não a que calhou de estar
        # embaixo.
        _ordem = pd.to_datetime(juntas["analisado_em"], format="%d/%m/%Y %H:%M",
                                errors="coerce")
        juntas = (juntas.assign(_ordem=_ordem)
                  .sort_values("_ordem", kind="stable", na_position="first")
                  .drop(columns="_ordem"))
    juntas = juntas.drop_duplicates(subset=chave, keep="last")
    reescrever(sh, nome, colunas, juntas)


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
        por_vocabulario = False
        if item["nivel"] == "Não menciona":
            if not ocorrencias_ancora(texto_norm, tema):
                # A âncora é enxuta de propósito e por isso deixa passar a
                # ausência do plano que trata do tema sem usar a palavra do
                # tema. Antes de aceitar, confere o vocabulário largo.
                if len(posicoes_do_tema(texto_norm, tema)) < LIMIAR_VOCABULARIO:
                    continue                  # ausência conferida, nada a fazer
                por_vocabulario = True
        elif citacao_sustenta(paginas_norm, item["trecho"]):
            continue                          # citação bate com o plano

        contexto = (contexto_do_vocabulario(texto, texto_norm, tema)
                    if por_vocabulario else contexto_do_tema(texto, texto_norm, tema))
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
    classif = _conferir_enumeracao(classif, texto, texto_norm, paginas_norm)
    classif = _conferir_subclassificacao(classif, texto, texto_norm, paginas_norm)
    classif = _conferir_meta(classif)
    return _conferir_nivel_por_citacao(classif)


# A partir de quantas posições do vocabulário largo a ausência é reperguntada.
#
# Por que 2 e não 1: medido em 40 planos e 125 ausências gravadas em 11/08/2026.
# Com corte em 1 são 28 reperguntas novas, ~64 chamadas por rodada de 91 planos,
# e a maioria é menção de passagem em que "Não menciona" está certo (um
# "aplicativo" virando Governo Digital). Com corte em 2 são 12, ~27 chamadas, e
# elas concentram os casos fortes: Daniel Vilela (GO) com 14 ocorrências de
# vocabulário de Financiamento do SUS marcado como "Não menciona", Kalil (MG)
# com 11 em Violência contra a Mulher, Cadu de Lula (RN) com 10 em Desmatamento.
#
# Efeito medido rodando as 12 reperguntas: 10 voltaram com nível e citação
# literal do plano, 2 confirmaram a ausência. Duas das 10 atribuíram o trecho ao
# tema errado (uma seção sobre idosos e pessoas com deficiência virou Educação
# Inclusiva e EJA). Testei exigir que a citação devolvida contivesse termo do
# próprio tema e a regra reprovou zero dos 12, então não serve como filtro e não
# está aqui.
#
# Remedido em 14/08/2026 no gemini-3.6-flash, que substituiu o 2.5 à força
# (outros/medir_limiar_vocabulario.py, 40 planos, 290 ausências): as mesmas 12
# reperguntas, agora 7 voltando com citação verificada e 5 confirmando a
# ausência. O rendimento caiu de 10 em 12 para 7 em 12; nenhuma foi descartada
# por citação fora do plano. O corte fica em 2: baixar para 1 dobra as chamadas
# atrás de ruído, e isso não mudou com o modelo.
#
# A VERSAO_ANALISE NÃO subiu junto de propósito. Subir força reprocessar os 91
# planos com Gemini para mexer em ~23 linhas de 4.186, e a guarda vale a partir
# do próximo plano que entrar. A base fica com dois critérios convivendo, e é
# uma troca consciente.
LIMIAR_VOCABULARIO = 2

# A partir de quantas ocorrências do termo o tema deixa de ser menção de
# passagem. ocorrencias_ancora para de contar em 12, então este é o teto dela: o
# tema aparece doze vezes ou mais no plano.
OCORRENCIAS_TEMA_TRATADO = 12


def _conferir_subclassificacao(classif: dict, texto: str, texto_norm: str,
                               paginas_norm: list[str]) -> dict:
    """Repergunta o tema dado como vago que o plano trata a fundo.

    Toda a conferência olhava para um lado só: citação sem lastro e ausência
    falsa, os dois erros que inflam ou zeram. Faltava o oposto. Medindo os 43
    planos em 07/08/2026, 50 temas gravados como "Menciona vagamente" tinham o
    termo aparecendo doze vezes ou mais no plano, e ninguém conferia nenhum.

    Só pode subir, e só com citação que passe em verificar_trecho. Se a segunda
    passagem não achar proposta, fica o que estava: um tema pode ser citado
    muitas vezes e nunca ganhar proposta, e isso é resultado legítimo.
    """
    for tema, item in classif.items():
        if item["nivel"] != "Menciona vagamente":
            continue
        if len(ocorrencias_ancora(texto_norm, tema)) < OCORRENCIAS_TEMA_TRATADO:
            continue
        contexto = contexto_do_tema(texto, texto_norm, tema)
        if not contexto:
            continue
        try:
            novo = reanalisar_tema(contexto, tema, TEMAS.get(tema, ""))
        except (RespostaIlegivel, json.JSONDecodeError, ValueError):
            continue
        if (novo["score"] > item["score"]
                and citacao_sustenta(paginas_norm, novo["trecho"])
                and not tema_e_item_de_enumeracao(novo["trecho"], tema)):
            classif[tema] = novo
    return classif


def _conferir_enumeracao(classif: dict, texto: str, texto_norm: str,
                         paginas_norm: list[str]) -> dict:
    """Repergunta o tema que parece ser item de lista dentro da própria citação.

    A peneira é tema_e_item_de_enumeracao, que erra em cerca de um terço dos
    casos por não distinguir "esporte no meio de uma lista de outras coisas" de
    "proposta de saúde mental que enumera seus componentes". Por isso ela não
    decide nada: o tema volta ao modelo com o entorno do assunto no plano, e só
    desce se a segunda passagem também não achar proposta própria.

    Desce para "Menciona vagamente", não para "Não menciona": o tema está
    escrito na citação, o que falta é proposta para ele.
    """
    for tema, item in classif.items():
        if item["nivel"] in ("Não menciona", "Menciona vagamente"):
            continue
        if not tema_e_item_de_enumeracao(item["trecho"], tema):
            continue
        contexto = contexto_do_tema(texto, texto_norm, tema)
        novo = None
        if contexto:
            try:
                novo = reanalisar_tema(contexto, tema, TEMAS.get(tema, ""))
            except (RespostaIlegivel, json.JSONDecodeError, ValueError):
                novo = None
        vale = (novo and novo["nivel"] not in ("Não menciona", "Menciona vagamente")
                and not tema_e_item_de_enumeracao(novo["trecho"], tema)
                and citacao_sustenta(paginas_norm, novo["trecho"]))
        classif[tema] = novo if vale else dict(
            item, nivel="Menciona vagamente", score=NIVEIS.index("Menciona vagamente"))
    return classif


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
    # Nome e gênero vão para a justificativa da coerência: é ela que atribui a
    # proposta a alguém, e "o candidato" escrito sobre uma mulher é erro de
    # fato. A base de dados abertos pode não trazer a coluna de gênero; sem ela,
    # o prompt escreve sem pronome em vez de deduzir do nome.
    nome_urna = str(r.get("NM_URNA_CANDIDATO", "") or "").strip()
    genero = str(r.get("DS_GENERO", "") or "").strip()
    try:
        coe = resumir_plano(classif, nome=nome_urna, genero=genero)
    except RespostaIlegivel as e:
        # O resumo não tinha segunda chance, e é a última chamada do
        # candidato: cair aqui jogava fora a classificação inteira, já paga.
        print(f"(resumo ilegível, tentando de novo: {e})", end=" ", flush=True)
        time.sleep(3)
        coe = resumir_plano(classif, nome=nome_urna, genero=genero)

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
                   # O entorno da citação no PDF. Vazio quando a citação não foi
                   # localizada, que é o mesmo caso em que `pagina` fica vazia.
                   contexto=contexto_do_trecho(paginas, paginas_norm,
                                               res["trecho"]),
                   responsavel=res.get("responsavel", ""),
                   # `responsavel` é texto livre e chegou a 198 valores
                   # distintos. `entes` é a mesma informação reduzida à esfera
                   # que executa, que é o que dá para filtrar e contar.
                   entes=normalizar_responsavel(res.get("responsavel", "")),
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
                     resumo=coe["resumo"],
                     pontes=coe.get("pontes_texto", ""),
                     resumos_eixos=coe.get("resumos_eixos", ""))
    return linhas, linha_coe, ""


def refazer_coerencia(sh, uf: str = "", limite: int = 0, sq: str = "") -> int:
    """Refaz só o resumo do plano, a partir da análise já gravada.

    Por que existe: mudar a régua da coerência custava um reprocessamento
    inteiro, porque a fila só tem um caminho e ele baixa o PDF, roda OCR e
    reclassifica os 46 temas antes de chegar na coerência. Em 10/08/2026 a
    régua nova saiu com 19 dos 40 planos em nota 5, e corrigir o degrau ia
    custar outras três horas de Gemini pela classificação que já estava boa.

    O resumo não lê o plano: resumir_plano recebe a classificação, que
    está na aba de análise. Então dá para remontar o dicionário do que já foi
    gravado e refazer só a chamada da coerência. São 79 chamadas, minutos, sem
    download e sem OCR.

    Fica barato errar o degrau, que é o que a régua precisa: mudar o corte,
    rodar, olhar a distribuição, ajustar.
    """
    salvas = ler_aba(sh, ANALISE_ABA)
    if salvas.empty:
        print(f"A aba {ANALISE_ABA} está vazia.")
        return 1
    salvas = salvas[salvas["ano"].astype(str).str.strip() == ANO]
    if uf:
        salvas = salvas[salvas["uf"].astype(str).str.strip().str.upper() == uf.upper()]
    if sq:
        # Lista separada por vírgula, e não um SQ só: a correção de 21/08/2026
        # pegou 33 planos espalhados por 20 UFs, e refazer um por um é uma
        # leitura da aba inteira por candidato.
        alvos = {x.strip() for x in str(sq).split(",") if x.strip()}
        salvas = salvas[salvas["sq_candidato"].astype(str).str.strip().isin(alvos)]

    # O gênero não está na análise, e sem ele a justificativa escreve "o
    # candidato" sobre uma mulher.
    base = ler_aba(sh, ABA_BASE)
    genero_de = {}
    if not base.empty and "DS_GENERO" in base.columns:
        genero_de = {str(r["SQ_CANDIDATO"]).strip(): str(r.get("DS_GENERO", "")).strip()
                     for _, r in base.iterrows()}

    sqs = list(dict.fromkeys(salvas["sq_candidato"].astype(str).str.strip()))
    if limite:
        sqs = sqs[:limite]
    print(f"{len(sqs)} planos para refazer a coerência")

    buffer, erros = [], []
    for i, sq_cand in enumerate(sqs, start=1):
        linhas = salvas[salvas["sq_candidato"].astype(str).str.strip() == sq_cand]
        if linhas.empty:
            continue
        primeira = linhas.iloc[0]
        nome = str(primeira.get("candidato", "")).strip()
        print(f"[{i}/{len(sqs)}] {primeira.get('uf','')} · {nome}...", end=" ", flush=True)

        classif = {}
        for _, l in linhas.iterrows():
            tema = str(l.get("tema", "")).strip()
            if not tema:
                continue
            nivel = NIVEIS_LEGADO.get(str(l.get("nivel", "")).strip(),
                                      str(l.get("nivel", "")).strip())
            classif[tema] = {"nivel": nivel or "Não menciona",
                             "trecho": str(l.get("trecho", "")),
                             "responsavel": str(l.get("responsavel", "")),
                             "prazo": str(l.get("prazo", "")),
                             "publico_alvo": str(l.get("publico_alvo", "")),
                             "programa_nome": str(l.get("programa_nome", ""))}
        try:
            coe = resumir_plano(classif, nome=nome,
                                genero=genero_de.get(sq_cand, ""))
        except Exception as e:
            print(f"ERRO: {type(e).__name__}: {e}")
            erros.append(nome)
            continue

        agora = datetime.now(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M")
        buffer.append({
            "ano": ANO, "sq_candidato": sq_cand, "candidato": nome,
            "partido": primeira.get("partido", ""), "uf": primeira.get("uf", ""),
            "cargo": primeira.get("cargo", ""), "link": primeira.get("link", ""),
            "resumo": coe["resumo"],
            "pontes": coe.get("pontes_texto", ""),
            "resumos_eixos": coe.get("resumos_eixos", ""),
            "chars": primeira.get("chars", ""),
            "chars_analisados": primeira.get("chars_analisados", ""),
            "versao": VERSAO_COERENCIA, "analisado_em": agora,
        })
        print("ok (resumo escrito)")
        if len(buffer) >= LOTE:
            gravar(sh, COERENCIA_ABA, COLS_COE, ["sq_candidato"], buffer)
            buffer = []
        time.sleep(PAUSA_ENTRE_PLANOS)

    if buffer:
        gravar(sh, COERENCIA_ABA, COLS_COE, ["sq_candidato"], buffer)
    if erros:
        print(f"\n{len(erros)} com erro: {', '.join(erros)}")
    print("Coerência refeita.")
    return 1 if erros else 0


def _conferir_nivel_por_citacao(classif: dict) -> dict:
    """Alinha o nível dos temas que se apoiam na MESMA citação.

    O texto é um só, então o degrau tem que ser um só: o que muda de tema para
    tema é se aquela frase trata do tema, não o quanto ela promete. A regra
    entrou no prompt na versão 12 e continuou furando: em 10/08/2026, 21 das
    3.412 citações da base saíram com níveis diferentes, como o plano do Saulo
    Arcangeli, em que "Realizaremos um amplo plano de investimentos para
    recuperar, modernizar..." valeu Propõe ação em Média e Alta Complexidade e
    Menciona vagamente em Saúde Mental.

    Alinha pelo MENOR, como todas as outras guardas daqui: elas só rebaixam.
    Subir espalharia para um tema o que a frase promete a outro.
    """
    por_citacao = {}
    for tema, item in classif.items():
        trecho = _sem_espaco(_norm_busca(str((item or {}).get("trecho", ""))))
        if not trecho or (item or {}).get("nivel") == "Não menciona":
            continue
        por_citacao.setdefault(trecho, []).append(tema)

    for temas in por_citacao.values():
        if len(temas) < 2:
            continue
        niveis = {classif[t]["nivel"] for t in temas}
        if len(niveis) < 2:
            continue
        menor = min(niveis, key=lambda n: NIVEIS.index(n) if n in NIVEIS else 0)
        for t in temas:
            if classif[t]["nivel"] != menor:
                classif[t] = dict(classif[t], nivel=menor, score=NIVEIS.index(menor))
    return classif


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
    # Para reprocessar um candidato só, sem subir versão e sem arrastar a base
    # inteira junto. Serve quando um tema novo entra e o efeito precisa ser visto
    # em um plano antes de valer para todos.
    p.add_argument("--sq", default="",
                   help="restringe a um SQ_CANDIDATO (ou a vários, separados "
                        "por vírgula, com --so-coerencia)")
    p.add_argument("--forcar", action="store_true",
                   help="reprocessa mesmo quem já está gravado e atualizado")
    p.add_argument("--planilha",
                   default=os.getenv("SPREADSHEET_ID_TSE",
                                     os.getenv("COLETA_SHEET_ID", "")),
                   help="ID da planilha (ou env SPREADSHEET_ID_TSE)")
    p.add_argument("--credenciais", default="", help="caminho do credentials.json")
    p.add_argument("--sem-espelho", action="store_true",
                   help="ignora a cópia do Drive e baixa sempre do TSE")
    # Refaz só a coerência a partir da análise gravada. É o que torna
    # barato mexer na régua: 79 chamadas em minutos, sem baixar PDF nem
    # reclassificar tema.
    p.add_argument("--so-coerencia", action="store_true",
                   help="refaz só a nota e a justificativa de coerência, a "
                        "partir da análise já gravada")
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

    if args.so_coerencia:
        return refazer_coerencia(sh, args.uf, args.limite, args.sq)

    base = ler_aba(sh, ABA_BASE)
    if base.empty or "LINK_PLANO" not in base.columns:
        raise SystemExit(f"A aba {ABA_BASE} está vazia ou sem LINK_PLANO.")
    base = base[base["LINK_PLANO"].astype(str).str.startswith("http")]
    if args.cargo.upper() != "TODOS":
        base = base[base["DS_CARGO"].astype(str).str.strip().str.upper()
                    == args.cargo.upper()]
    if args.uf:
        base = base[base["SG_UF"].astype(str).str.strip().str.upper() == args.uf.upper()]
    if args.sq:
        # Lista separada por vírgula, e não um SQ só. Antes a lista só valia com
        # --so-coerencia, e no caminho completo a string inteira era comparada
        # como se fosse um SQ: em 21/08/2026 uma rodada com 15 SQs devolveu
        # "0 candidatos com plano · 0 a processar" e não reanalisou nada.
        _alvos = {x.strip() for x in str(args.sq).split(",") if x.strip()}
        base = base[base["SQ_CANDIDATO"].astype(str).str.strip().isin(_alvos)]

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

    # Cópia do PDF no Drive antes do TSE. Quando a versão da análise sobe, a fila
    # é a base inteira, e 206 downloads seguidos do DivulgaCand é justamente o
    # padrão que o WAF dele corta (19/08/2026, dia inteiro em 403). Sem espelho
    # gravado, isto não faz nada e o download segue como antes.
    if not args.sem_espelho:
        try:
            from outros.espelhar_planos import registrar_espelho
            n_espelho = registrar_espelho(sh=sh, caminho_credenciais=args.credenciais)
            if n_espelho:
                print(f"espelho do Drive ativo para {n_espelho} planos")
        except Exception as e:
            print(f"espelho não pôde ser ligado ({str(e)[:120]}); seguindo no TSE")

    buffer_a, buffer_c = [], []
    indisponiveis, ilegiveis, erros = [], [], []
    processados = set()          # (sq, nome) de quem a rodada analisou inteiro
    feitos = 0
    inicio = time.time()

    def descarrega():
        """Grava o que está no buffer, aba inteira, e esvazia o buffer.

        Continua descarregando a cada LOTE e não só no fim: análise já paga não
        pode depender de o job chegar vivo ao final.
        """
        nonlocal buffer_a, buffer_c
        gravar(sh, ANALISE_ABA, COLS, ["sq_candidato", "tema"], buffer_a)
        gravar(sh, COERENCIA_ABA, COLS_COE, ["sq_candidato"], buffer_c)
        buffer_a, buffer_c = [], []

    for n, r in enumerate(fila, 1):
        nome = str(r["NM_URNA_CANDIDATO"])
        sq = str(r["SQ_CANDIDATO"])
        print(f"[{n}/{len(fila)}] {r['SG_UF']} · {nome}...", end=" ", flush=True)
        try:
            linhas, coe, pulo = processar(r, ANO)
        except PlanoIndisponivel as e:
            indisponiveis.append(nome)
            print(f"plano fora do ar, tenta depois ({e})")
            if len(indisponiveis) >= 8 and feitos == 0:
                print("\nDivulgaCand/TSE está recusando requisições consecutivas (bloqueio/WAF). "
                      "Interrompendo rodada para tentar no próximo horário.")
                break
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

        buffer_a.extend(linhas)
        buffer_c.append(coe)
        processados.add((sq, nome))
        feitos += 1
        niveis = sum(1 for l in linhas if l["nivel"] != "Não menciona")
        # O que chega aqui é a linha já formatada para a planilha, onde 'pontes'
        # é o texto do pontes_texto, junto por " | ", e não o dicionário. Contar
        # com len() direto media caracteres: em 14/08/2026 o log deu "807
        # programas" para um plano do DF, que eram 807 caracteres de texto.
        pontes_txt = str(coe.get("pontes", "")).strip()
        n_pontes = len([p for p in pontes_txt.split(" | ") if p]) if pontes_txt else 0
        print(f"ok ({niveis}/{len(linhas)} temas com conteúdo, "
              f"{n_pontes} programas atravessando temas)")

        if feitos % LOTE == 0:
            descarrega()
            print(f"    ... {feitos} gravados ({time.time() - inicio:.0f}s)")
        time.sleep(PAUSA_ENTRE_PLANOS)

    descarrega()

    # Confere o que ficou na planilha, e não o que o script acha que gravou. A
    # rodada de 09/08/2026 terminou dizendo "7 plano(s) processados" com a aba
    # intacta do dia anterior, e ninguém soube até alguém notar que o Lula tinha
    # sumido do painel.
    if processados:
        conferidos = ler_aba(sh, ANALISE_ABA)
        gravados = (set(conferidos["sq_candidato"].astype(str))
                    if not conferidos.empty else set())
        sumidos = [nome for sq_p, nome in processados if sq_p not in gravados]
        if sumidos:
            print(f"ATENÇÃO: {len(sumidos)} analisado(s) não estão na aba depois "
                  f"da gravação: {', '.join(str(s) for s in sumidos[:8])}")
            return 1

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
