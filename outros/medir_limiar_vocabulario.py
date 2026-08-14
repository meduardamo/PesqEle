# -*- coding: utf-8 -*-
"""Mede a guarda do LIMIAR_VOCABULARIO contra a base já gravada.

Por que existe: o corte em 2 foi calibrado em 11/08/2026, sobre 40 planos e 125
ausências, rodando gemini-2.5-flash. Em 14/08/2026 o modelo virou 3.6-flash por
imposição do Google (o 2.5 responde 404 para chave de projeto novo), e o número
que justificava o corte, 10 de 12 reperguntas voltando com nível e citação
literal, não vale mais por herança: foi medido em outro modelo.

O que faz: lê a aba de análise, refaz o teste de ausência sobre o texto do plano
e repergunta ao modelo só o que a guarda pegaria. Não escreve nada em lugar
nenhum. Serve para decidir se o corte continua em 2, sobe, desce, ou se a guarda
sai.

Uso:
    python -m outros.medir_limiar_vocabulario --planos 40
    python -m outros.medir_limiar_vocabulario --planos 0 --limiar 1   # base toda
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import time
from collections import Counter

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from analise_planos import (  # noqa: E402
    PlanoIndisponivel, RespostaIlegivel, TEMAS,
    _norm_busca, citacao_sustenta, contexto_do_vocabulario,
    extrair_paginas_url, ocorrencias_ancora, posicoes_do_tema, reanalisar_tema,
)
from processar_planos import ANALISE_ABA, cliente, ler_aba  # noqa: E402


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--planos", type=int, default=40,
                   help="quantos planos medir (0 = todos)")
    p.add_argument("--limiar", type=int, default=2,
                   help="posições de vocabulário a partir das quais repergunta")
    p.add_argument("--uf", default="")
    args = p.parse_args()

    sh = cliente().open_by_key(os.environ["SPREADSHEET_ID_TSE"])
    df = ler_aba(sh, ANALISE_ABA)
    if args.uf:
        df = df[df["uf"] == args.uf]
    if df.empty:
        print("aba vazia")
        return 1

    # Um plano por sq_candidato, na ordem em que aparecem, para a amostra ser a
    # mesma entre duas execuções e os números serem comparáveis.
    planos: dict[str, str] = {}
    for _, r in df.iterrows():
        sq = str(r["sq_candidato"])
        if sq not in planos:
            planos[sq] = str(r["link"])
    ordem = list(planos)
    if args.planos:
        ordem = ordem[:args.planos]

    modelo = os.getenv("GEMINI_MODEL", "(padrão do script)")
    print(f"modelo: {modelo} | limiar: {args.limiar} | planos: {len(ordem)}\n")

    tot_ausencias = tot_candidatas = 0
    resultado = Counter()
    inicio = time.time()

    for n, sq in enumerate(ordem, 1):
        linhas = df[df["sq_candidato"].astype(str) == sq]
        nome = str(linhas.iloc[0]["candidato"])
        uf = str(linhas.iloc[0]["uf"])
        try:
            paginas = extrair_paginas_url(planos[sq])
        except (PlanoIndisponivel, Exception) as e:  # noqa: BLE001
            print(f"[{n}/{len(ordem)}] {uf} · {nome}... plano indisponível ({e})")
            continue
        texto = " ".join(paginas)
        texto_norm = _norm_busca(texto)
        paginas_norm = [_norm_busca(pg) for pg in paginas]

        ausencias = [r for _, r in linhas.iterrows()
                     if str(r["nivel"]) == "Não menciona"]
        tot_ausencias += len(ausencias)
        candidatas = []
        for r in ausencias:
            tema = str(r["tema"])
            if ocorrencias_ancora(texto_norm, tema):
                continue          # a âncora já pegaria, não é caso da guarda
            if len(posicoes_do_tema(texto_norm, tema)) < args.limiar:
                continue          # ausência conferida, a guarda não age
            candidatas.append(tema)

        tot_candidatas += len(candidatas)
        print(f"[{n}/{len(ordem)}] {uf} · {nome}... "
              f"{len(ausencias)} ausências, {len(candidatas)} reperguntas")

        for tema in candidatas:
            contexto = contexto_do_vocabulario(texto, texto_norm, tema)
            if not contexto:
                resultado["sem contexto"] += 1
                continue
            try:
                novo = reanalisar_tema(contexto, tema, TEMAS.get(tema, ""))
            except (RespostaIlegivel, json.JSONDecodeError, ValueError) as e:
                resultado["resposta ilegível"] += 1
                print(f"      {tema}: ilegível ({e})")
                continue
            if not novo:
                resultado["sem resposta"] += 1
            elif novo["nivel"] == "Não menciona":
                resultado["confirmou a ausência"] += 1
                print(f"      {tema}: confirmou ausência")
            elif citacao_sustenta(paginas_norm, novo["trecho"]):
                resultado["achou, com citação verificada"] += 1
                print(f"      {tema}: {novo['nivel']} · "
                      f"\"{novo['trecho'][:90]}\"")
            else:
                # Voltou com nível mas a citação não está no plano. É o caso que
                # a guarda descarta: vira "Não menciona" de novo.
                resultado["citação não confere, descartado"] += 1
                print(f"      {tema}: descartado, citação fora do plano")

    print(f"\n--- {time.time() - inicio:.0f}s")
    print(f"ausências olhadas: {tot_ausencias}")
    print(f"reperguntas disparadas: {tot_candidatas} "
          f"({tot_candidatas / tot_ausencias:.1%} das ausências)"
          if tot_ausencias else "")
    for k, v in resultado.most_common():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
