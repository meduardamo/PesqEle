"""Afere se reperguntar corrige os "Propõe ação" que são só menção.

Conjunto de aferição fechado: as 20 linhas que a leitura de 250 citações
aleatórias, em 25/08/2026, marcou como classificadas em "Propõe ação" sem que
a citação proponha ação nenhuma. São título de seção ("PROGRAMA 6 —
AGROINDÚSTRIA BAIANA"), fragmento de lista ("Qualificação e reinserção
profissional"), diagnóstico ("Essa centralização sobrecarrega o transporte
sanitário") e intenção sem meio ("Acelerar e expandir a redução do tempo de
espera, consolidando os avanços já alcançados").

Existe porque três peneiras já reprovaram para achar essa classe por regra:
citação sem termo-âncora do tema (47% em todos os níveis), citação sem verbo de
ação (14,7%, mas proposta escrita como substantivo é legítima) e citação curta
(pega 6 dos 20 e marca 222 linhas). Ver [[project-planos-guardas]]. Sobrou
reperguntar ao modelo, e antes de gastar a leva inteira nos 207 planos convém
saber quanto ela rende: se reperguntar corrigir 3 de 20, não vale a rodada.

NÃO ESCREVE NADA. Só imprime o antes e o depois, linha a linha.
"""
from __future__ import annotations

import json
import os
import pathlib
import sys
from collections import Counter

sys.path.insert(0, str(pathlib.Path(__file__).resolve().parent.parent))

from outros.analise_planos import (  # noqa: E402
    TEMAS, RespostaIlegivel, _norm_busca, contexto_do_tema, extrair_paginas_url,
    paginas_do_trecho, reanalisar_tema, verificar_trecho,
)


def contexto_pela_citacao(paginas: list[str], paginas_norm: list[str],
                          trecho: str, vizinhas: int = 1) -> str:
    """As páginas onde a citação gravada está, mais as vizinhas.

    Metade do conjunto de aferição não tem nenhum termo-âncora do tema no plano,
    e aí contexto_do_tema volta vazio e não há o que reperguntar. Isso não quer
    dizer que o plano não trate o assunto: TERMOS_ANCORA pega 31% das ausências
    e o resto passa porque o plano usa outras palavras, limite conhecido desde
    07/08/2026. A citação gravada, essa sim, está no plano e foi verificada.
    Então quando a âncora falha o entorno vem de onde a frase mora.
    """
    nums = paginas_do_trecho(paginas_norm, trecho)
    if not nums:
        return ""
    querer = set()
    for n in nums:
        for k in range(n - vizinhas, n + vizinhas + 1):
            if 1 <= k <= len(paginas):
                querer.add(k)
    return " ".join(paginas[k - 1] for k in sorted(querer))

CONJUNTO = pathlib.Path(__file__).with_name("aferir_acao.json")


def main() -> int:
    casos = json.loads(CONJUNTO.read_text(encoding="utf-8"))
    # Agrupa por plano: baixar e OCRar o PDF é o caro, e dois casos do mesmo
    # candidato não podem custar dois downloads.
    por_plano: dict[str, list[dict]] = {}
    for c in casos:
        por_plano.setdefault(c["sq_candidato"], []).append(c)

    conta = Counter()
    for n, (sq, itens) in enumerate(por_plano.items(), 1):
        cab = f"[{n}/{len(por_plano)}] {itens[0]['uf']} · {itens[0]['candidato']}"
        try:
            paginas = extrair_paginas_url(itens[0]["link"])
        except Exception as e:  # noqa: BLE001
            print(f"{cab}: plano indisponível ({e})")
            conta["indisponível"] += len(itens)
            continue
        texto = " ".join(paginas)
        texto_norm = _norm_busca(texto)
        paginas_norm = [_norm_busca(pg) for pg in paginas]

        for c in itens:
            tema = c["tema"]
            contexto = contexto_do_tema(texto, texto_norm, tema)
            origem = "âncora"
            if not contexto:
                contexto = contexto_pela_citacao(paginas, paginas_norm, c["trecho"])
                origem = "página da citação"
            if not contexto:
                print(f"{cab} · {tema}: sem contexto (sem âncora e citação não localizada)")
                conta["sem contexto"] += 1
                continue
            try:
                novo = reanalisar_tema(contexto, tema, TEMAS.get(tema, ""))
            except (RespostaIlegivel, json.JSONDecodeError, ValueError) as e:
                print(f"{cab} · {tema}: resposta ilegível ({e})")
                conta["ilegível"] += 1
                continue

            ver = verificar_trecho(paginas_norm, novo["trecho"])
            mudou = novo["nivel"] != c["nivel"]
            conta[f"{c['nivel']} -> {novo['nivel']}"] += 1
            if mudou:
                conta["mudou"] += 1
            conta[f"contexto por {origem}"] += 1
            print(f"\n{cab} · {tema}  (contexto por {origem})")
            print(f"   antes ({c['nivel']}): {c['trecho'][:150]}")
            print(f"   depois ({novo['nivel']}, {ver or 'sem citação'}): "
                  f"{novo['trecho'][:150]}")

    print("\n" + "=" * 70)
    print(f"conjunto: {len(casos)} linhas em {len(por_plano)} planos")
    for k, v in sorted(conta.items(), key=lambda x: -x[1]):
        print(f"  {k}: {v}")
    corrigidas = sum(v for k, v in conta.items()
                     if k.startswith("Propõe ação -> ")
                     and not k.endswith("-> Propõe ação"))
    print(f"\nreperguntar corrigiu {corrigidas} de {len(casos)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
