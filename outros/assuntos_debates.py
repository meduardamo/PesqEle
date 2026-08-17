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

Duas exceções à regra de "todo termo marca", ambas medidas no debate da Band de
09/08 e documentadas onde estão definidas: termo que diz o eixo mas não diz o
tema (TERMOS_SO_EIXO) só marca no nível eixo, e termo que sozinho não diz nada
(TERMOS_AMBIGUOS) só confirma rótulo que outro termo já marcou.

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

# Os assuntos que o debate discute e o plano de governo não tem. Dívida e
# privatização foram medidos no debate da Band de 09/08 (dívida em 27 falas,
# privatização em 13, sobre a transcrição de 153 falas de então).
#
# Criminalidade e economia nacional entraram depois, conferindo as falas que
# ficaram sem tema nenhum na transcrição atual, de 287 falas:
#
#  - linha 203, Tarcísio, 342 palavras inteiras sobre "menor taxa de homicídio,
#    menor taxa de latrocínio, menor taxa de roubo, menor taxa de furto", com
#    zero temas. O dicionário dos planos fala de política ("efetivo policial",
#    "enfrentamento ao crime organizado") e não de resultado, que é como o
#    debate discute segurança.
#  - linhas 231 a 235, um bloco inteiro sobre a taxação americana, também sem
#    tema. Os temas de 'Economia e emprego' são de programa estadual e não
#    cobrem conjuntura nacional.
#
# Os dois são eixo e tema ao mesmo tempo, como os outros daqui: não cabem em
# nenhum eixo do plano sem distorcer o que o eixo significa lá.
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
    "Criminalidade e violência": [
        "homicidio", "latrocinio", "roubo", "furto", "assalto",
        "assassinato", "chacina", "criminalidade", "letalidade",
        "morte violenta", "sequestro", "estupro", "violencia",
        "taxa de homicidio", "indice de criminalidade", "seguranca publica",
    ],
    "Economia nacional e comércio exterior": [
        "tarifaco", "taxacao americana", "sobretaxa", "comercio exterior",
        "exportacao", "importacao", "balanca comercial", "dolar",
        "inflacao", "juros", "taxa selic", "pib", "recessao",
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
    # O plano escreve "geração de emprego"; em debate fala-se "emprego" e
    # "desemprego" soltos. Sem estes o eixo Economia ficava quase vazio.
    "Geração de Emprego": ["emprego", "desemprego", "carteira assinada",
                           "vaga de trabalho", "mercado de trabalho"],
}


# Termos que dizem o eixo mas não dizem o tema. "escola" é educação, mas não
# diz se é fundamental, médio ou tempo integral; "hospital" é saúde, mas não
# diz se é atenção primária ou alta complexidade.
#
# Medido no debate da Band de 09/08: 20 das 27 marcações de 'Fundamental' (74%)
# e 8 das 10 de 'Valorização Docente' (80%) se sustentavam só nestes termos. O
# eixo estava certo e o tema era chute do dicionário.
#
# Regra: valem no nível eixo e não valem no nível tema. Um tema só sai se algum
# termo próprio dele tiver disparado.
TERMOS_SO_EIXO = {"escola", "aluno", "sala de aula", "professor", "hospital"}

# Termos que sozinhos não dizem nem o eixo. Só confirmam um rótulo que outro
# termo já tenha disparado na mesma fala.
#
# 'investimento' era o caso grave: 36 das 46 falas marcadas com o eixo 'Gestão
# pública e transparência' (78%) tinham só essa palavra, e ela aparece em fala
# de qualquer área ("o investimento em Defesa Civil não avança", linha 257).
# 'tarifa' está em Mobilidade Urbana pelo dicionário dos planos e marcou como
# mobilidade a fala sobre "as tarifas impostas pelos Estados Unidos" (linha 235).
TERMOS_AMBIGUOS = {"investimento", "tarifa"}


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


def assuntos_da_fala(texto, compilados, por_tema=False):
    """{rótulo: [termos que dispararam]} para uma fala.

    `por_tema` diz em que nível a varredura está rodando, porque a régua muda:
    termo que diz o eixo e não diz o tema vale num nível e não vale no outro.
    A lista devolvida guarda todos os termos que casaram, inclusive os que não
    sustentam a marcação sozinhos, para a conferência ver de onde ela veio.
    """
    alvo = _norm_acentos(texto or "")
    achados = {}
    for rotulo, pares in compilados.items():
        disparou = sorted({t for t, rx in pares if rx.search(alvo)})
        if not disparou:
            continue
        sustentam = [t for t in disparou if t not in TERMOS_AMBIGUOS
                     and not (por_tema and t in TERMOS_SO_EIXO)]
        if sustentam:
            achados[rotulo] = disparou
    return achados


# Acima disto o turno se explica sozinho e não lê o anterior. O corte é o
# mesmo número que justificou a janela: das 24 falas que o dicionário perdia e
# o modelo pegava, 14 tinham menos de 30 palavras.
PALAVRAS_TURNO_CURTO = 30


def janela(falas, i, anteriores, curto=PALAVRAS_TURNO_CURTO):
    """O turno mais os `anteriores` que vieram antes dele, se ele for curto.

    Turno curto muitas vezes não tem assunto próprio: "Você deve tá arrependido
    mesmo" só é sobre segurança porque o turno anterior era.

    Turno longo não precisa disso e a janela só fazia ele importar o tema do
    adversário. Nos turnos acima de 200 palavras do debate da Band de 09/08,
    39 das 167 marcações vinham do turno anterior: o de 564 palavras do
    Tarcísio herdava Ensino Médio, Fundamental, Igualdade Racial e Tempo
    Integral do turno do Haddad.

    A janela é fixa de propósito. A primeira versão agrupava por bloco, cortando
    a cada fala do mediador, e no trecho de confronto livre o mediador não fala:
    saiu um bloco de 64 turnos e 7.175 palavras, um terço do debate, marcado com
    10 dos 12 eixos e propagado para os 64 turnos. A cobertura subiu para 88% e
    não queria dizer nada.
    """
    if curto and len(falas[i]["fala"].split()) >= curto:
        return [falas[i]]
    ini = max(0, i - anteriores)
    return falas[ini:i + 1]


def main():
    ap = argparse.ArgumentParser(description="Marca os assuntos citados em cada fala")
    ap.add_argument("--csv", required=True, help="CSV da transcrição")
    ap.add_argument("--saida", default=None)
    ap.add_argument("--por-tema", action="store_true",
                    help="usa os 46 temas em vez dos 12 eixos")
    ap.add_argument("--min-palavras", type=int, default=0,
                    help="ignora falas mais curtas que isto na varredura")
    ap.add_argument("--anteriores", type=int, default=1,
                    help="turnos anteriores lidos junto com cada turno "
                         "(0 = turno isolado; padrão 1)")
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

    print(f"unidade : turno + {args.anteriores} anterior(es)"
          if args.anteriores else "unidade : turno isolado")

    linhas = []
    for i, r in enumerate(falas):
        grupo = janela(falas, i, args.anteriores)
        texto = " ".join(f["fala"] for f in grupo)
        achados = ({} if len(texto.split()) < args.min_palavras
                   else assuntos_da_fala(texto, compilados, args.por_tema))
        # O próprio turno é marcado à parte: separa o que ele diz do que
        # herdou do turno anterior, e quem for citar em entrega precisa saber
        # a diferença.
        proprios = assuntos_da_fala(r["fala"], compilados, args.por_tema)
        linhas.append({
            "id": i + 1,
            "segundos": r.get("segundos", ""),
            "tempo": r.get("tempo", ""),
            "falante": r.get("falante", ""),
            "palavras": len(r["fala"].split()),
            "assuntos": "; ".join(sorted(achados)),
            "n_assuntos": len(achados),
            "proprios": "; ".join(sorted(proprios)),
            "termos": "; ".join(f"{k}: {', '.join(v)}" for k, v in sorted(achados.items())),
            "fala": r["fala"],
        })

    destino = Path(args.saida) if args.saida else entrada.with_name(
        entrada.stem + f"_assuntos_{nivel}.csv")
    campos = ["id", "segundos", "tempo", "falante", "palavras",
              "assuntos", "n_assuntos", "proprios", "termos", "fala"]
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
