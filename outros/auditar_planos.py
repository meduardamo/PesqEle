# -*- coding: utf-8 -*-
"""Passa as guardas da análise sobre os planos já gravados.

Por que existe: as guardas rodam na hora da análise. Quando uma entra sem a
VERSAO_ANALISE subir junto, ela nunca alcança o que já está na planilha, e a
base fica com dois critérios convivendo. Desde a última subida de versão
(10/08/2026, 11:32) entraram pelo menos duas: a guarda de nível por citação e as
âncoras que faltavam em policiamento, às 11:11 do mesmo dia, e o
LIMIAR_VOCABULARIO em 14/08/2026.

Como funciona: monta a classificação a partir das linhas da aba e chama a MESMA
`conferir_classificacao` que a produção usa, sobre o texto do mesmo plano. Não
reimplementa guarda nenhuma, então não tem como divergir do pipeline. O que a
função devolver diferente do que está gravado é o que estava errado.

Dois modos:

    --contar   só mede, sem chamar o modelo. Diz quantas linhas cada guarda
               pegaria, para saber o tamanho antes de gastar.
    --gravar   roda as guardas de verdade (com repergunta ao modelo onde elas
               mandam) e corrige as linhas divergentes.

Sem nenhum dos dois, roda as guardas e só mostra o que mudaria.

A `versao` não muda: pendentes() lê a versão da primeira linha do candidato, e
mexer nela mandaria o plano inteiro para o Gemini de novo. Quem marca a
correção é o `analisado_em`.
"""

from __future__ import annotations

import argparse
import os
import sys
import time
from collections import Counter
from datetime import datetime, timedelta, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__)))

from analise_planos import (  # noqa: E402
    NIVEIS, NIVEIS_LEGADO, PlanoIndisponivel,
    _norm_busca, citacao_sustenta, extrair_paginas_url, normalizar_responsavel,
    ocorrencias_ancora, paginas_do_trecho, posicoes_do_tema, verificar_trecho,
)
from processar_planos import (  # noqa: E402
    ANALISE_ABA, LIMIAR_VOCABULARIO, cliente, conferir_classificacao,
)

CAMPOS = ("nivel", "trecho", "responsavel", "prazo", "publico_alvo",
          "programa_nome")


def _col_a1(n: int) -> str:
    s, n = "", n + 1
    while n:
        n, r = divmod(n - 1, 26)
        s = chr(65 + r) + s
    return s


def _nivel_canonico(nivel: str) -> str:
    """O nome de nível como o código de hoje escreve."""
    nivel = (nivel or "").strip()
    return NIVEIS_LEGADO.get(nivel, nivel)


def _item_da_linha(v: list, col: dict) -> dict:
    nivel = _nivel_canonico(v[col["nivel"]])
    return {
        "nivel": nivel,
        "score": NIVEIS.index(nivel) if nivel in NIVEIS else 0,
        "trecho": v[col["trecho"]],
        "responsavel": v[col["responsavel"]],
        "prazo": v[col["prazo"]],
        "publico_alvo": v[col["publico_alvo"]],
        "programa_nome": v[col["programa_nome"]],
    }


def _medir_atribuicao(linhas: list, col: dict, n: int) -> int:
    """Mede quantos trechos não sustentam o tema em que foram arquivados.

    Nenhuma guarda cobre isso: a citação existe no plano e é literal, mas fala
    de outro assunto. Na medição de 11/08/2026 foram 2 em 10 (uma seção sobre
    idosos e pessoas com deficiência arquivada em Educação Inclusiva e EJA).

    Como mede: mostra ao modelo só o trecho gravado e pergunta o tema, com a
    mesma reanalisar_tema da produção. Se ele responder "Não menciona" olhando
    justamente o trecho que sustenta o nível, o trecho provavelmente não é dali.

    É uma triagem, não um veredito: trecho sem o entorno da página pode perder
    o que o ligava ao tema, então o número aqui é teto, não contagem de erro.
    """
    import random
    from analise_planos import TEMAS, reanalisar_tema

    alvo = [v for v in linhas
            if v[col["nivel"]] != "Não menciona"
            and v[col["verificacao"]] == "literal"
            and v[col["trecho"]].strip()]
    random.seed(20260814)                     # amostra repetível entre execuções
    amostra = random.sample(alvo, min(n, len(alvo)))
    print(f"amostra: {len(amostra)} de {len(alvo)} linhas com citação literal\n")

    suspeitos = 0
    for k, v in enumerate(amostra, 1):
        tema = v[col["tema"]]
        try:
            r = reanalisar_tema(v[col["trecho"]], tema, TEMAS.get(tema, ""))
        except Exception as e:                # noqa: BLE001
            print(f"[{k}/{len(amostra)}] {tema}: erro ({e})")
            continue
        if r and r["nivel"] == "Não menciona":
            suspeitos += 1
            print(f"[{k}/{len(amostra)}] SUSPEITO · {v[col['candidato']][:20]} · {tema}")
            print(f"      \"{v[col['trecho']][:120]}\"")
    print(f"\ntrechos que não sustentam o próprio tema: {suspeitos} de "
          f"{len(amostra)} ({suspeitos / len(amostra):.1%})" if amostra else "")
    return 0


def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--planos", type=int, default=0, help="0 = base inteira")
    p.add_argument("--uf", default="")
    p.add_argument("--contar", action="store_true",
                   help="só mede, sem chamar o modelo")
    p.add_argument("--gravar", action="store_true",
                   help="corrige na aba as linhas divergentes")
    p.add_argument("--atribuicao", type=int, default=0, metavar="N",
                   help="mede em N linhas se o trecho é mesmo do tema em que está")
    args = p.parse_args()

    aba = cliente().open_by_key(os.environ["SPREADSHEET_ID_TSE"]).worksheet(ANALISE_ABA)
    valores = aba.get_all_values()
    cab, linhas = valores[0], valores[1:]
    col = {n: k for k, n in enumerate(cab)}

    planos: dict[str, dict] = {}
    for n, v in enumerate(linhas, start=2):
        sq = v[col["sq_candidato"]]
        if args.uf and v[col["uf"]] != args.uf:
            continue
        d = planos.setdefault(sq, {"link": v[col["link"]],
                                   "nome": v[col["candidato"]],
                                   "uf": v[col["uf"]], "temas": {}})
        d["temas"][v[col["tema"]]] = (n, v)

    if args.atribuicao:
        return _medir_atribuicao(linhas, col, args.atribuicao)

    ordem = list(planos)[:args.planos] if args.planos else list(planos)
    print(f"planos: {len(ordem)} | modo: "
          f"{'contar' if args.contar else ('gravar' if args.gravar else 'simular')}\n")

    conta = Counter()
    mudancas = Counter()
    correcoes: list[dict] = []
    inicio = time.time()

    for n, sq in enumerate(ordem, 1):
        d = planos[sq]
        try:
            paginas = extrair_paginas_url(d["link"])
        except (PlanoIndisponivel, Exception) as e:  # noqa: BLE001
            print(f"[{n}/{len(ordem)}] {d['uf']} · {d['nome']}... indisponível ({e})")
            conta["plano indisponível"] += 1
            continue
        texto = " ".join(paginas)
        texto_norm = _norm_busca(texto)
        paginas_norm = [_norm_busca(pg) for pg in paginas]
        antes = {tema: _item_da_linha(v, col) for tema, (_, v) in d["temas"].items()}

        if args.contar:
            # As mesmas três perguntas que conferir_classificacao faz, só que
            # sem reperguntar: aqui interessa o tamanho, não a correção.
            for tema, item in antes.items():
                if item["nivel"] == "Não menciona":
                    if ocorrencias_ancora(texto_norm, tema):
                        conta["ausência com a âncora no texto"] += 1
                    elif len(posicoes_do_tema(texto_norm, tema)) >= LIMIAR_VOCABULARIO:
                        conta["ausência com vocabulário no texto"] += 1
                elif not citacao_sustenta(paginas_norm, item["trecho"]):
                    conta["citação que não confere com o plano"] += 1
            print(f"[{n}/{len(ordem)}] {d['uf']} · {d['nome']}... "
                  f"{len(antes)} temas")
            continue

        depois = conferir_classificacao(dict(antes), texto, paginas_norm)
        agora = datetime.now(timezone(timedelta(hours=-3))).strftime("%d/%m/%Y %H:%M")
        mudou = 0
        for tema, novo in depois.items():
            velho = antes.get(tema)
            if velho is None or all(str(velho.get(c, "")) == str(novo.get(c, ""))
                                    for c in CAMPOS):
                continue
            mudou += 1
            mudancas[f"{velho['nivel']} -> {novo['nivel']}"] += 1
            linha, v = d["temas"][tema]
            nova = list(v)
            campos = {
                "nivel": novo["nivel"], "trecho": novo["trecho"],
                "responsavel": novo.get("responsavel", ""),
                "entes": normalizar_responsavel(novo.get("responsavel", "")),
                "prazo": novo.get("prazo", ""),
                "publico_alvo": novo.get("publico_alvo", ""),
                "programa_nome": novo.get("programa_nome", ""),
                "pagina": ", ".join(str(x) for x in
                                    paginas_do_trecho(paginas_norm, novo["trecho"])),
                "verificacao": verificar_trecho(paginas_norm, novo["trecho"]),
                "analisado_em": agora,
            }
            for nome, valor in campos.items():
                if nome in col:
                    while len(nova) <= col[nome]:
                        nova.append("")
                    nova[col[nome]] = valor
            correcoes.append({"range": f"A{linha}:{_col_a1(len(nova) - 1)}{linha}",
                              "values": [nova]})
            print(f"      {tema}: {velho['nivel']} -> {novo['nivel']}")
        print(f"[{n}/{len(ordem)}] {d['uf']} · {d['nome']}... "
              f"{len(antes)} temas, {mudou} corrigidos")

    if correcoes and args.gravar:
        for i in range(0, len(correcoes), 50):
            aba.batch_update(correcoes[i:i + 50], value_input_option="USER_ENTERED")
            time.sleep(1)
        print(f"\ngravadas {len(correcoes)} correções")
    elif correcoes:
        print(f"\n{len(correcoes)} linhas mudariam (rode com --gravar para aplicar)")

    print(f"\n--- {time.time() - inicio:.0f}s")
    for k, v in (conta or mudancas).most_common():
        print(f"  {k}: {v}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
