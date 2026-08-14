"""
Assuntos de um debate por dicionário de termos, sem julgamento do modelo.

Substitui a codificação por escolha única, que foi medida e reprovada: em três
desenhos (46 temas, 46 desacoplados, 12 eixos) o acordo entre duas passadas do
modelo ficou em ~80%, contra a régua de ~95% do Comparative Agendas, e o
agregado por candidato oscilava até 10 pontos percentuais entre rodadas. Número
que muda 10 pontos ao rodar de novo não vai para entrega.

Aqui não há escolha única nem modelo: cada fala é varrida por uma lista de
termos e recebe todos os assuntos que aparecem nela. O resultado é o mesmo em
toda rodada, por construção, e cada marcação vem com o termo que a disparou,
para conferir uma a uma.

O que isto NÃO mede: se a fala é *sobre* o assunto. "Não vou falar de saúde"
conta como saúde. Em compensação a contagem é de menção, que é o que se pode
afirmar sem interpretar.

Os termos vêm de TERMOS_ANCORA, do analise_planos, que já roda contra os planos
de governo. Reaproveitar é o que mantém debate e plano comparáveis.

Uso:
    python -m outros.assuntos_debates --csv transcricoes/debate.csv
    python -m outros.assuntos_debates --csv debate.csv --por-tema
"""

import argparse
import collections
import csv
import re
import sys
from pathlib import Path

from outros.analise_planos import EIXOS, TERMOS_ANCORA, _norm_acentos, _regex_ancora

# Os dois assuntos que o debate discute e o plano de governo não tem. Medidos
# no debate da Band de 09/08: dívida em 27 das 153 falas, privatização em 13.
TERMOS_EXTRA = {
    "Finanças estaduais e dívida": [
        "divida", "servico da divida", "endividamento", "renegociacao",
        "renegociar", "propague", "regime de recuperacao fiscal",
        "equilibrio fiscal", "ajuste fiscal", "emprestimo", "financiamento",
        "banco mundial", "banco europeu", "bid", "aval da uniao",
        "icms", "carga tributaria", "arrecadacao", "precatorio",
        "teto de gastos", "capacidade de pagamento", "capag",
    ],
    "Privatizações e concessões": [
        "privatiza*", "desestatiza*", "concessao", "concessionaria",
        "parceria publico-privada", "ppp", "leilao", "estatal",
        "venda de acoes", "participacao acionaria", "sabesp",
        "tarifa", "outorga", "reestatiza*",
    ],
}


# Termos que o debate usa e o plano não. Plano escreve a locução ("efetivo
# policial", "unidade de pronto atendimento"); em debate fala-se a palavra
# solta. Achados conferindo, uma a uma, as falas que o modelo temou e o
# dicionário não pegou no debate da Band de 09/08.
TERMOS_FALADOS = {
    "Policiamento e Efetivo": ["policia", "pm", "delegado", "delegacia", "corporacao"],
    "Enfrentamento ao Crime Organizado": ["faccao", "traficante", "bandido", "criminoso"],
    "Média e Alta Complexidade": ["hospital", "cirurgia", "leito"],
    "Urgência e Emergência": ["ambulancia"],
    "Fundamental": ["escola", "aluno", "sala de aula"],
    "Valorização Docente": ["professor"],
    "Transporte e Rodovias": ["rodovia", "estrada", "pedagio"],
    "Mobilidade Urbana": ["metro", "trem", "onibus", "linha do metro"],
    "Saneamento e Recursos Hídricos": ["agua", "esgoto", "reservatorio", "seca"],
    "Eficiência e Gasto Público": ["orcamento", "gasto publico", "investimento"],
}


def montar_termos(por_tema):
    """{rótulo: [termos]} no nível pedido, com os assuntos de debate no fim."""
    base = {t: list(v) + TERMOS_FALADOS.get(t, []) for t, v in TERMOS_ANCORA.items()}
    if por_tema:
        base.update(TERMOS_EXTRA)
        return base
    d = {}
    for eixo, temas in EIXOS.items():
        termos = []
        for t in temas:
            termos.extend(base.get(t, []))
        d[eixo] = termos
    d.update(TERMOS_EXTRA)
    return d


# Plural que muda o radical. O _regex_ancora dos planos cobre só "s" e "es", e
# em plano isso basta, porque o texto é escrito em locução ("efetivo policial").
# Em fala solta o plano quebra: "policial" não acha "policiais", e "upa" não
# acha "UPAs" porque termo com menos de 4 letras não ganha sufixo nenhum.
#
# Não mexo no _regex_ancora: ele é compartilhado com o pipeline dos planos e o
# comentário dele registra medição em 15 planos. Aqui é uma segunda regra, só
# para fala.
PLURAIS = [
    ("al", "ais"), ("el", "eis"), ("ol", "ois"), ("ul", "uis"),
    ("ao", "oes"), ("ao", "aos"), ("ao", "aes"),
    ("m", "ns"), ("r", "res"), ("z", "zes"), ("s", "ses"),
]


def _regex_debate(termo):
    """Como o _regex_ancora, mais os plurais irregulares do português."""
    if termo.endswith("*") or " " in termo:
        return _regex_ancora(termo)

    base = _norm_acentos(termo)
    formas = {base, base + "s", base + "es"}
    for fim, plural in PLURAIS:
        if base.endswith(fim):
            formas.add(base[: -len(fim)] + plural)
    # Termo curto vira palavra inteira e nada mais: "upa" pode virar "upas",
    # mas não pode casar dentro de "ocupacao".
    return re.compile(r"\b(?:" + "|".join(sorted(map(re.escape, formas), key=len, reverse=True)) + r")\b")


def compilar(termos):
    """Curinga e locução seguem o _regex_ancora dos planos; palavra solta usa a
    regra de plural própria de fala."""
    return {rotulo: [(t, _regex_debate(t)) for t in lista]
            for rotulo, lista in termos.items()}


def assuntos_da_fala(texto, compilados):
    """{rótulo: [termos que dispararam]} para uma fala."""
    alvo = _norm_acentos(texto or "")
    achados = {}
    for rotulo, pares in compilados.items():
        disparou = sorted({t for t, rx in pares if rx.search(alvo)})
        if disparou:
            achados[rotulo] = disparou
    return achados


def main():
    ap = argparse.ArgumentParser(description="Marca os assuntos citados em cada fala")
    ap.add_argument("--csv", required=True, help="CSV da transcrição")
    ap.add_argument("--saida", default=None)
    ap.add_argument("--por-tema", action="store_true",
                    help="usa os 46 temas em vez dos 12 eixos")
    ap.add_argument("--min-palavras", type=int, default=0,
                    help="ignora falas mais curtas que isto na varredura")
    args = ap.parse_args()

    entrada = Path(args.csv)
    if not entrada.exists():
        sys.exit(f"não encontrei {entrada}")

    with open(entrada, encoding="utf-8") as f:
        falas = [r for r in csv.DictReader(f) if r.get("fala", "").strip()]
    if not falas:
        sys.exit("CSV sem falas.")

    termos = montar_termos(args.por_tema)
    compilados = compilar(termos)
    nivel = "tema" if args.por_tema else "eixo"

    print("=" * 68)
    print("CONFIGURAÇÃO")
    print("=" * 68)
    print(f"entrada : {entrada}  ({len(falas)} falas)")
    print(f"nível   : {nivel}  ({len(termos)} rótulos, "
          f"{sum(len(v) for v in termos.values())} termos)")
    print(f"          {len(TERMOS_EXTRA)} próprios de debate: {', '.join(TERMOS_EXTRA)}")

    linhas = []
    for i, r in enumerate(falas, 1):
        texto = r["fala"]
        curta = len(texto.split()) < args.min_palavras
        achados = {} if curta else assuntos_da_fala(texto, compilados)
        linhas.append({
            "id": i,
            "segundos": r.get("segundos", ""),
            "tempo": r.get("tempo", ""),
            "falante": r.get("falante", ""),
            "palavras": len(texto.split()),
            "assuntos": "; ".join(sorted(achados)),
            "n_assuntos": len(achados),
            "termos": "; ".join(f"{k}: {', '.join(v)}" for k, v in sorted(achados.items())),
            "fala": texto,
        })

    destino = Path(args.saida) if args.saida else entrada.with_name(
        entrada.stem + f"_assuntos_{nivel}.csv")
    campos = ["id", "segundos", "tempo", "falante", "palavras",
              "assuntos", "n_assuntos", "termos", "fala"]
    with open(destino, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        w.writerows(linhas)

    print()
    print("=" * 68)
    print("COBERTURA")
    print("=" * 68)
    com = sum(1 for l in linhas if l["n_assuntos"])
    print(f"falas com pelo menos um assunto : {com} de {len(linhas)} ({com / len(linhas):.0%})")
    print(f"falas sem nenhum                : {len(linhas) - com}")
    dist = collections.Counter(l["n_assuntos"] for l in linhas)
    print("assuntos por fala: " + ", ".join(
        f"{n}: {q} fala(s)" for n, q in sorted(dist.items())))

    print()
    print("=" * 68)
    print("ASSUNTOS POR CANDIDATO")
    print("=" * 68)
    # Contado em falas, não em palavras: uma fala pode citar dois assuntos, e
    # dividir as palavras entre eles seria inventar um rateio que o dado não
    # sustenta. Por isso as colunas somam mais de 100%.
    por_falante = collections.defaultdict(collections.Counter)
    total_falas = collections.Counter()
    for l in linhas:
        total_falas[l["falante"]] += 1
        for a in l["assuntos"].split("; "):
            if a:
                por_falante[l["falante"]][a] += 1

    for falante in sorted(por_falante, key=lambda f: -total_falas[f]):
        n = total_falas[falante]
        print(f"\n{falante}  ({n} falas)")
        for assunto, q in por_falante[falante].most_common(10):
            print(f"   {q / n:>5.0%}  {q:>3} falas  {assunto}")

    print()
    print("=" * 68)
    print("O QUE CONFERIR ANTES DE USAR")
    print("=" * 68)
    print(f"{destino}")
    print("a coluna 'termos' diz qual palavra disparou cada marcação: confira")
    print("por amostra, principalmente onde o termo é curto ou ambíguo")
    print("isto conta MENÇÃO, não assunto da fala: negar um assunto conta igual")
    print("uma fala pode citar vários assuntos, então as porcentagens somam")
    print("mais de 100%")


if __name__ == "__main__":
    main()
