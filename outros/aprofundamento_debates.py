"""
Aprofundamento de um eixo do debate: o que cada candidato disse sobre um tema.

Vem depois da transcrição e do dicionário de assuntos. O dicionário filtra as
falas que tocam o eixo pedido, e só essas vão para o modelo. É por isso que
aqui ele funciona, e na classificação não funcionou: lá era escolha forçada
entre 12 rótulos para uma fala ambígua, e o modelo oscilava porque a pergunta
não tinha resposta única. Aqui o recorte já veio pronto e a tarefa é ler e
citar, com o trecho conferido contra o texto original.

Toda afirmação sai com o horário e a citação literal. Se o trecho não estiver,
palavra por palavra, numa fala daquele candidato, a afirmação é descartada e o
descarte aparece no log. É a mesma disciplina das guardas dos planos.

O confronto entre os dois não é julgado pelo modelo: é contado. Um tema onde os
dois têm achado é um tema em que os dois falaram, e isso o dado sustenta.

Uso:
    python -m outros.aprofundamento_debates --csv debate.csv --eixo "Educação"
    python -m outros.aprofundamento_debates --csv debate.csv --min-falas 12
"""

import argparse
import collections
import csv
import json
import os
import re
import sys
import time
from pathlib import Path

from google import genai
from google.genai import types

from outros.analise_planos import EIXOS, _norm_acentos
from outros.assuntos_debates import (TERMOS_EXTRA, assuntos_da_fala, compilar,
                                     janela, montar_termos)


def norm_texto(t):
    """Sem acento, minúsculo e com espaço colapsado.

    O colapso importa aqui e não no dicionário: o modelo devolve a citação com
    o espaçamento dele, e comparar espaço a espaço reprovaria trecho correto."""
    return re.sub(r"\s+", " ", _norm_acentos(t or "")).strip()

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")
TENTATIVAS = 3

# Abaixo disto o eixo não rende documento: vira uma citação solta e a leitura
# fica pior do que não ter. Medido no debate da Band de 09/08, onde os eixos
# com material real ficaram todos acima de 10 falas.
MIN_FALAS = 10

PROMPT = """Você está lendo as falas de um debate eleitoral brasileiro que
tratam de {eixo}. As falas abaixo já foram filtradas: são todas as que tocam
neste assunto.

# Falas
{falas}

# Tarefa
Extraia o que cada participante afirmou sobre {eixo}. Uma entrada por
afirmação distinta.

Categorias:
- "Proposta": promete ou anuncia uma ação de governo para o futuro
- "Balanço": afirma o que já foi feito, com ou sem número
- "Contestação": nega ou disputa o que o outro afirmou

Regras, todas obrigatórias:
- "trecho" é copiado LITERALMENTE de uma das falas acima, entre 5 e 40 palavras
  seguidas, do jeito que está, inclusive repetição e hesitação. Não parafraseie,
  não corrija a gramática, não junte pedaços de lugares diferentes.
- "falante" é exatamente o nome que aparece na fala de onde o trecho saiu.
- "id" é o número entre colchetes daquela fala.
- "resumo" é uma frase curta, factual, dizendo o que foi afirmado. Não avalie,
  não julgue se é viável, não compare com o adversário.
- Se um número foi citado, reproduza o número no resumo.
- Não invente afirmação que não esteja nas falas acima.

Responda só com um array JSON:
[{{"id": 12, "falante": "...", "categoria": "Proposta", "resumo": "...", "trecho": "..."}}]"""


_T0 = time.time()


def log(msg=""):
    if msg == "":
        print(flush=True)
        return
    m, s = divmod(int(time.time() - _T0), 60)
    print(f"[{m:02d}:{s:02d}] {msg}", flush=True)


def secao(t):
    log()
    log("=" * 68)
    log(t)
    log("=" * 68)


def filtrar(falas, eixo, anteriores=1):
    """Falas que tocam o eixo, pela janela do dicionário."""
    compilados = compilar(montar_termos(por_tema=False))
    escolhidas = []
    for i, f in enumerate(falas):
        texto = " ".join(x["fala"] for x in janela(falas, i, anteriores))
        if eixo in assuntos_da_fala(texto, compilados):
            escolhidas.append(f)
    return escolhidas


def temas_da_fala(texto):
    """Subtemas do eixo citados numa fala, para o mapa de confronto."""
    compilados = compilar(montar_termos(por_tema=True))
    return set(assuntos_da_fala(texto, compilados))


def extrair(client, eixo, falas):
    listadas = "\n\n".join(
        f'[{f["id"]}] {f["falante"]} ({f["tempo"]}): {f["fala"]}' for f in falas
    )
    prompt = PROMPT.format(eixo=eixo, falas=listadas)
    for n in range(1, TENTATIVAS + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0, response_mime_type="application/json"),
            )
            dados = json.loads(resp.text)
            if isinstance(dados, dict):
                dados = next((v for v in dados.values() if isinstance(v, list)), [])
            return dados
        except Exception as e:
            log(f"    tentativa {n}/{TENTATIVAS} falhou: {e}")
            if n < TENTATIVAS:
                time.sleep(5 * n)
    return []


def conferir(achado, por_id, por_falante):
    """O trecho tem de estar, literal, numa fala DAQUELE falante.

    Duas guardas em uma. A do texto pega paráfrase, que é o erro comum. A do
    falante pega atribuição trocada, que é o erro caro: citação real posta na
    boca do candidato errado passa despercebida na leitura e é justamente o
    que não pode sair em entrega.
    """
    trecho = (achado.get("trecho") or "").strip()
    falante = (achado.get("falante") or "").strip()
    if not trecho:
        return None, "sem trecho"
    if len(trecho.split()) < 5:
        return None, "trecho curto demais para conferir"

    alvo = norm_texto(trecho)
    fala = por_id.get(achado.get("id"))
    if fala and alvo in norm_texto(fala["fala"]):
        return fala, ""

    # O id pode ter vindo errado e o trecho estar certo: procura nas falas do
    # falante antes de descartar.
    for f in por_falante.get(falante, []):
        if alvo in norm_texto(f["fala"]):
            return f, ""

    for f in por_id.values():
        if alvo in norm_texto(f["fala"]):
            return None, f'trecho é de {f["falante"]}, atribuído a {falante}'
    return None, "trecho não confere com nenhuma fala"


def documento(eixo, achados, falas, avisos):
    """Markdown pronto para virar Doc no Drive."""
    por_falante = collections.defaultdict(lambda: collections.defaultdict(list))
    for a in achados:
        por_falante[a["falante"]][a["categoria"]].append(a)

    L = [f"# {eixo}", ""]
    L.append(f"Debate analisado a partir de {len(falas)} falas que tocam este eixo, "
             f"de um total de {len(set(f['id'] for f in falas))} identificadas pelo "
             "dicionário de termos.")
    L.append("")

    for falante in sorted(por_falante, key=lambda f: -sum(
            len(v) for v in por_falante[f].values())):
        L.append(f"## {falante}")
        L.append("")
        for cat in ("Proposta", "Balanço", "Contestação"):
            itens = por_falante[falante].get(cat, [])
            if not itens:
                continue
            L.append(f"### {cat}")
            L.append("")
            for a in itens:
                L.append(f"- {a['resumo']}")
                L.append(f'  - [{a["tempo"]}] "{a["trecho"]}"')
            L.append("")

    # Confronto contado, não julgado: subtema em que os dois deixaram achado.
    temas_por_falante = {}
    for a in achados:
        temas_por_falante.setdefault(a["falante"], set()).update(temas_da_fala(a["trecho"]))
    falantes = [f for f in temas_por_falante if len(temas_por_falante[f]) > 0]
    if len(falantes) >= 2:
        comuns = set.intersection(*(temas_por_falante[f] for f in falantes))
        so_de = {f: temas_por_falante[f] - set().union(
            *(temas_por_falante[o] for o in falantes if o != f)) for f in falantes}
        L.append("## Onde os dois falaram, e onde só um falou")
        L.append("")
        L.append(f"Subtemas citados pelos dois: {', '.join(sorted(comuns)) or 'nenhum'}")
        L.append("")
        for f in falantes:
            if so_de[f]:
                L.append(f"Só {f} citou: {', '.join(sorted(so_de[f]))}")
        L.append("")

    L.append("## Como isto foi feito")
    L.append("")
    L.append("As falas foram selecionadas por um dicionário de termos, não por "
             "julgamento: a lista de termos é a mesma que roda sobre os planos de "
             "governo. Cada afirmação acima traz o horário e a citação literal, e "
             "foi conferida contra a transcrição antes de entrar: trecho que não "
             "aparecia palavra por palavra na fala do candidato citado foi "
             "descartado.")
    L.append("")
    L.append("O que isto não mede: quanto tempo cada candidato dedicou ao assunto. "
             "Uma fala pode tocar vários eixos, e dividir o tempo dela entre eles "
             "seria um rateio que o dado não sustenta.")
    if avisos:
        L.append("")
        L.append(f"Descartes na conferência: {len(avisos)}.")
    L.append("")
    return "\n".join(L)


def main():
    ap = argparse.ArgumentParser(description="Aprofunda um eixo do debate")
    ap.add_argument("--csv", required=True, help="CSV da transcrição")
    ap.add_argument("--eixo", default=None, help="um eixo (vazio = todos com material)")
    ap.add_argument("--min-falas", type=int, default=MIN_FALAS)
    ap.add_argument("--saida", default=None, help="pasta de saída")
    ap.add_argument("--drive", action="store_true", help="sobe os documentos para o Drive")
    args = ap.parse_args()

    if not os.getenv("GEMINI_API_KEY", "").strip():
        sys.exit("GEMINI_API_KEY não definido.")

    entrada = Path(args.csv)
    if not entrada.exists():
        sys.exit(f"não encontrei {entrada}")
    with open(entrada, encoding="utf-8") as f:
        falas = [r for r in csv.DictReader(f) if r.get("fala", "").strip()]
    for i, f in enumerate(falas, 1):
        f["id"] = i

    saida = Path(args.saida) if args.saida else entrada.parent / "aprofundamento"
    saida.mkdir(parents=True, exist_ok=True)

    todos = list(EIXOS) + list(TERMOS_EXTRA)
    alvos = [args.eixo] if args.eixo else todos
    if args.eixo and args.eixo not in todos:
        sys.exit(f"eixo desconhecido: {args.eixo!r}. Opções: {', '.join(todos)}")

    secao("CONFIGURAÇÃO")
    log(f"modelo  : {GEMINI_MODEL}")
    log(f"entrada : {entrada}  ({len(falas)} falas)")
    log(f"eixos   : {len(alvos)} candidato(s), mínimo de {args.min_falas} falas")
    log(f"saída   : {saida}")

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    gerados = []

    for eixo in alvos:
        recorte = filtrar(falas, eixo)
        if len(recorte) < args.min_falas:
            log(f"{eixo}: {len(recorte)} falas, abaixo do mínimo, pulado")
            continue

        secao(eixo.upper())
        log(f"{len(recorte)} falas tocam o eixo")
        bruto = extrair(client, eixo, recorte)
        log(f"modelo devolveu {len(bruto)} afirmação(ões)")

        por_id = {f["id"]: f for f in recorte}
        por_falante = collections.defaultdict(list)
        for f in recorte:
            por_falante[f["falante"]].append(f)

        achados, avisos = [], []
        for a in bruto:
            fala, nota = conferir(a, por_id, por_falante)
            if not fala:
                avisos.append(f'{a.get("falante","?")}: {nota}')
                continue
            achados.append({
                "falante": fala["falante"],
                "categoria": a.get("categoria", "Balanço"),
                "resumo": (a.get("resumo") or "").strip(),
                "trecho": a["trecho"].strip(),
                "tempo": fala.get("tempo", ""),
            })

        log(f"{len(achados)} passaram na conferência, {len(avisos)} descartadas")
        for av in avisos[:5]:
            log(f"    descartada: {av}")
        if not achados:
            log("nenhuma afirmação conferível, documento não gerado")
            continue

        cont = collections.Counter((a["falante"], a["categoria"]) for a in achados)
        for (f, c), n in sorted(cont.items()):
            log(f"    {f}: {n} {c}")

        md = documento(eixo, achados, recorte, avisos)
        arq = saida / f"{entrada.stem}_{eixo.replace(' ', '_').replace('/', '-')}.md"
        arq.write_text(md, encoding="utf-8")
        log(f"{arq.name}  ({arq.stat().st_size / 1024:.0f} KB)")
        gerados.append(arq)

    if args.drive and gerados:
        secao("DRIVE")
        from outros.transcricao_debates import PASTA_DRIVE, clientes_google, enviar_drive
        if not PASTA_DRIVE:
            log("PASTA_DRIVE_DEBATES não definido, upload pulado")
        else:
            _, drive = clientes_google()
            for arq in gerados:
                link = enviar_drive(drive, arq, arq.stem,
                                    "application/vnd.google-apps.document")
                log(f"{arq.stem} -> {link}")

    secao("FIM")
    log(f"{len(gerados)} documento(s) em {saida}")


if __name__ == "__main__":
    main()
