"""
Resumo em prosa de um debate, a partir da transcrição e dos assuntos já marcados.

Vem depois da transcrição (transcricao_debates) e usa o mesmo CSV que já está
no Drive: a coluna 'eixo' dele é o dicionário de termos, e é dela que sai tanto
a medição de tempo por assunto quanto o recorte de falas que vai ao modelo.

A divisão de trabalho é a regra da casa: número é contado em Python, prosa é
escrita pelo modelo, e o modelo só redige o que já passou pela conferência de
citação literal do aprofundamento_debates. O parágrafo redigido ainda passa por
uma segunda guarda: todo número que ele escrever tem de existir nas afirmações
conferidas, senão o eixo cai para a lista de achados, que é determinística.

O que este resumo NÃO afirma: quem venceu, se o tom foi duro ou cordial, se a
proposta é viável. O fechamento é contagem (quantas propostas, quantos
balanços, quantas contestações por candidato), não julgamento.

Sobre os minutos por assunto: uma fala que cita dois assuntos conta inteira nos
dois, e por isso a soma dos assuntos passa da duração do debate. Ratear o tempo
da fala entre os assuntos citados seria inventar proporção que o dado não tem.
O texto diz isso onde os minutos aparecem.

Uso:
    python -m outros.resumo_debates --csv transcricoes/band_sp.csv
    python -m outros.resumo_debates --csv band_sp.csv --sem-modelo
    python -m outros.resumo_debates --fila
    python -m outros.resumo_debates --id 2026-band-sp-gov-t1 --drive
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

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")
TENTATIVAS = 3

# Quantos eixos ganham parágrafo próprio. Seis é o que coube no resumo que a
# Manu escreveu à mão para o debate da Band de 09/08; do sétimo em diante o
# eixo aparece só na frase de medição.
EIXOS_NO_TEXTO = 6

# Abaixo disto o eixo não rende parágrafo, mesmo entrando no top. É o mesmo
# corte do aprofundamento_debates, medido no mesmo debate.
MIN_FALAS = 10

# Quem fica acima disto do tempo total é tratado como candidato. No debate da
# Band de 09/08 os dois candidatos ficaram em 46% e 45% do tempo, o mediador em
# 5% e o resto (jornalistas, vinheta) abaixo de 2%. Quando a fila roda, quem
# manda é a planilha, e este corte só vale para o CSV avulso.
FATIA_CANDIDATO = 0.15

DIAS = ["segunda-feira", "terça-feira", "quarta-feira", "quinta-feira",
        "sexta-feira", "sábado", "domingo"]
MESES = ["janeiro", "fevereiro", "março", "abril", "maio", "junho", "julho",
         "agosto", "setembro", "outubro", "novembro", "dezembro"]
UFS = {
    "AC": "Acre", "AL": "Alagoas", "AP": "Amapá", "AM": "Amazonas",
    "BA": "Bahia", "CE": "Ceará", "DF": "Distrito Federal",
    "ES": "Espírito Santo", "GO": "Goiás", "MA": "Maranhão",
    "MT": "Mato Grosso", "MS": "Mato Grosso do Sul", "MG": "Minas Gerais",
    "PA": "Pará", "PB": "Paraíba", "PR": "Paraná", "PE": "Pernambuco",
    "PI": "Piauí", "RJ": "Rio de Janeiro", "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul", "RO": "Rondônia", "RR": "Roraima",
    "SC": "Santa Catarina", "SP": "São Paulo", "SE": "Sergipe",
    "TO": "Tocantins", "BR": "Brasil",
}
ORDINAIS = ["1º", "2º", "3º", "4º", "5º", "6º", "7º", "8º", "9º", "10º"]

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


def num(x):
    """Minuto com uma casa, sem o '.0' quando é redondo, com vírgula decimal."""
    t = f"{x:.1f}".replace(".", ",")
    return t[:-2] if t.endswith(",0") else t


def duracoes(falas):
    """Segundos de cada turno, pela distância até o turno seguinte.

    O CSV traz o começo de cada fala e não o fim, então a duração é a diferença
    para a próxima. Serve porque debate é fala colada em fala; onde há pausa de
    vinheta ela entra na fala anterior, e isso não muda a leitura de quem falou
    mais.

    A última fala não tem seguinte e recebe o tempo estimado pelo ritmo médio
    do próprio debate (palavras por segundo). É uma fala de encerramento em
    duas horas de áudio: qualquer critério aqui mexe na terceira casa.
    """
    segs = [int(float(f.get("segundos") or 0)) for f in falas]
    total_palavras = sum(len(f["fala"].split()) for f in falas)
    span = max(segs[-1] - segs[0], 1)
    ritmo = total_palavras / span
    saida = []
    for i, s in enumerate(segs):
        if i + 1 < len(segs):
            saida.append(max(0, segs[i + 1] - s))
        else:
            saida.append(int(len(falas[i]["fala"].split()) / ritmo) if ritmo else 0)
    return saida


def eixos_da_fala(fala):
    """Os eixos marcados na fala, venha o CSV da transcrição ou do assuntos.

    São dois nomes de coluna para a mesma coisa: o CSV que a transcrição sobe
    para o Drive chama de 'eixo', e o que o assuntos_debates escreve à parte
    chama de 'assuntos'. Aceitar os dois evita ter de dizer, na hora de rodar,
    qual dos arquivos é o que está na mão.
    """
    marcado = (fala.get("eixo") or fala.get("assuntos") or "")
    return [e.strip() for e in marcado.split(";") if e.strip() and e.strip() != "Sem tema"]


def medir(falas):
    """Tempo total, tempo por falante e tempo por eixo, tudo em segundos."""
    dur = duracoes(falas)
    por_falante = collections.Counter()
    por_eixo = collections.Counter()
    falas_por_eixo = collections.Counter()
    for f, d in zip(falas, dur):
        por_falante[(f.get("falante") or "DESCONHECIDO").strip()] += d
        for e in eixos_da_fala(f):
            por_eixo[e] += d
            falas_por_eixo[e] += 1
    return {
        "total": sum(dur),
        "por_falante": por_falante,
        "por_eixo": por_eixo,
        "falas_por_eixo": falas_por_eixo,
        "duracoes": dur,
    }


def elenco(medida, participantes=None, mediador=None):
    """Quem é candidato e quem é mediador.

    Com a fila, os dois vêm da planilha e o CSV não opina: nome escrito pelo
    modelo bloco a bloco não é chave confiável, e a planilha é preenchida por
    gente. Sem a planilha, cai no corte de tempo, que separa candidato de
    mediador em debate de dois, e loga o que decidiu para poder ser conferido.
    """
    tempos = medida["por_falante"]
    total = medida["total"] or 1

    def casa(nome):
        """Nomes do CSV que são a mesma pessoa que 'nome' da planilha.

        Por token e não por igualdade: a planilha escreve 'Fernando Haddad' e o
        modelo transcreve 'FERNANDO HADDAD', mas também 'HADDAD' sozinho quando
        o mediador chama assim. Token de até três letras não conta, senão 'de'
        e 'da' casariam candidato com mediador.
        """
        alvo = {t for t in _chave(nome).split() if len(t) > 3}
        return [n for n in tempos if alvo & {t for t in _chave(n).split() if len(t) > 3}]

    cands, med = [], None
    for p in (participantes or []):
        novos = [n for n in casa(p) if n not in cands]
        cands += novos
        EXIBICAO.update({n: p.strip() for n in novos})
    if mediador:
        med = next(iter(casa(mediador)), None)
        if med:
            EXIBICAO[med] = mediador.strip()
    if not cands:
        cands = [n for n, t in tempos.most_common() if t / total >= FATIA_CANDIDATO]
        log(f"candidatos por tempo de fala (acima de {FATIA_CANDIDATO:.0%}): "
            f"{', '.join(cands) or 'nenhum'}")
    if not med:
        resto = [n for n, _ in tempos.most_common() if n not in cands]
        med = resto[0] if resto else None
        if med:
            log(f"mediador presumido pelo tempo de fala: {med}")
    return cands, med


def _chave(nome):
    import unicodedata
    n = unicodedata.normalize("NFKD", (nome or "").upper())
    return "".join(c for c in n if not unicodedata.combining(c)).strip()


PROMPT_EIXO = """Você está escrevendo o trecho de um resumo jornalístico de um
debate eleitoral brasileiro, sobre {eixo}. As afirmações abaixo já foram
extraídas do debate e conferidas contra a transcrição.

# Afirmações
{achados}

# Tarefa
Escreva UM parágrafo dizendo o que cada participante afirmou sobre {eixo} e,
quando os dois trataram do mesmo ponto, onde eles divergiram.

Regras, todas obrigatórias:
- Use somente o que está nas afirmações acima. Não acrescente contexto, não
  explique quem é o candidato, não diga o que ele quis dizer.
- Não avalie: nada de dizer se a proposta é boa, viável, vaga ou contraditória.
- Reproduza os números como estão. Não arredonde, não converta, não crie número
  que não esteja acima.
- Entre 40 e 120 palavras, um parágrafo só, em português do Brasil.
- Trate cada participante pelo sobrenome, como aparece nas afirmações.
- Discurso indireto: "defendeu", "citou", "rebateu", "cobrou". Sem aspas.
- Não use travessão nem emoji.
- Texto corrido. Sem lista, sem marcador, sem subtítulo.
- Não comece repetindo o nome do assunto: ele já vai antes do seu texto.
- A divergência é a que aparece nas afirmações, não a que você deduzir. Se os
  dois não trataram do mesmo ponto, não invente confronto.

Responda só com o parágrafo, sem título."""


def filtrar(falas, eixo):
    """Falas cuja coluna 'eixo' traz este eixo.

    Usa a coluna do CSV em vez de recompilar o dicionário: é a mesma marcação
    que a Manu vê no link de temas, e é a mesma que contou os minutos aqui em
    cima. Recompilar abriria a chance de o texto do resumo discordar da tabela
    que está ao lado dele.
    """
    return [f for f in falas if eixo in eixos_da_fala(f)]


def numeros(texto):
    """Sequências de dígitos do texto, para conferir número inventado."""
    return re.findall(r"\d+", (texto or "").replace(".", "").replace(",", ""))


def conferir_paragrafo(texto, achados, candidatos):
    """Motivo pelo qual o parágrafo não pode sair, ou string vazia.

    Duas guardas. Número que não está na fonte é o erro que mais custa numa
    entrega de dado, e é o que o modelo faz quando arredonda ('quase 30%' por
    '28,4%') ou quando soma sozinho. Nome que não é de ninguém do debate pega a
    fala posta na boca de terceiro que nem estava lá.
    """
    fonte = " ".join(f'{a["resumo"]} {a["trecho"]}' for a in achados)
    perm = set(numeros(fonte))
    faltando = [n for n in numeros(texto) if n not in perm]
    if faltando:
        return f"número fora das afirmações: {', '.join(sorted(set(faltando))[:5])}"

    sobrenomes = set()
    for n in candidatos:
        sobrenomes |= {t for t in _chave(n).split() if len(t) > 3}
    citados = {t for t in _chave(texto).split() if len(t) > 3}
    # Só palavra que começa com maiúscula no texto original interessa; o _chave
    # já subiu tudo, então a comparação é contra os sobrenomes conhecidos e o
    # que sobra é ignorado. Aqui a guarda é a inversa: pelo menos um dos
    # candidatos precisa aparecer, senão o parágrafo não é sobre eles.
    if sobrenomes and not (sobrenomes & citados):
        return "parágrafo não cita nenhum dos participantes"
    return ""


def redigir(client, eixo, achados, candidatos):
    """Parágrafo do eixo, ou string vazia se não passou nas guardas."""
    from google.genai import types

    listadas = "\n".join(
        f'- {a["falante"]} ({a["categoria"]}): {a["resumo"]} | citação: "{a["trecho"]}"'
        for a in achados)
    prompt = PROMPT_EIXO.format(eixo=eixo, achados=listadas)
    for n in range(1, TENTATIVAS + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL, contents=prompt,
                config=types.GenerateContentConfig(temperature=0.2),
            )
            texto = " ".join((resp.text or "").split())
            problema = conferir_paragrafo(texto, achados, candidatos)
            if not problema:
                return texto
            log(f"    tentativa {n}/{TENTATIVAS} reprovada: {problema}")
        except Exception as e:
            log(f"    tentativa {n}/{TENTATIVAS} falhou: {e}")
        if n < TENTATIVAS:
            time.sleep(3 * n)
    return ""


def lista_de_achados(achados):
    """Saída determinística de um eixo, quando o parágrafo não passou.

    Vale mais que parágrafo bonito com número errado: é o mesmo conteúdo
    conferido, só sem a costura em prosa, e quem lê vê na hora que ali não
    houve redação.
    """
    L = []
    por_falante = collections.defaultdict(list)
    for a in achados:
        por_falante[a["falante"]].append(a)
    for falante, itens in sorted(por_falante.items(), key=lambda kv: -len(kv[1])):
        L.append(f"{falante}:")
        for a in itens:
            L.append(f"- {a['resumo']} [{a['tempo']}]")
    return "\n".join(L)


def hm(seg):
    """1h57, ou 57min quando não chega a uma hora."""
    h, m = divmod(int(round(seg / 60)), 60)
    return f"{h}h{m:02d}" if h else f"{m}min"


def por_extenso(data):
    """'domingo, 9 de agosto de 2026' a partir de dd/mm/aaaa ou aaaa-mm-dd."""
    from datetime import date
    t = (data or "").strip()
    m = re.match(r"(\d{2})/(\d{2})/(\d{4})", t) or re.match(r"(\d{4})-(\d{2})-(\d{2})", t)
    if not m:
        return t
    d, mes, ano = ((int(m.group(3)), int(m.group(2)), int(m.group(1)))
                   if "-" in t else (int(m.group(1)), int(m.group(2)), int(m.group(3))))
    dt = date(ano, mes, d)
    return f"{DIAS[dt.weekday()]}, {d} de {MESES[mes - 1]} de {ano}"


# Artigo definido de cada estado, para o título não sair como "ao governo de
# Ceará". Ficam de fora os que não levam artigo: São Paulo, Minas Gerais,
# Goiás, Pernambuco, Alagoas, Sergipe, Rondônia, Roraima, Santa Catarina,
# Mato Grosso e Mato Grosso do Sul.
ARTIGO_UF = {
    "AC": "o", "AP": "o", "AM": "o", "BA": "a", "CE": "o", "DF": "o",
    "ES": "o", "MA": "o", "PA": "o", "PB": "a", "PR": "o", "PI": "o",
    "RJ": "o", "RN": "o", "RS": "o", "TO": "o", "BR": "o",
}


def cargo_por_extenso(cargo, uf):
    sigla = (uf or "").strip().upper()
    lugar = UFS.get(sigla, (uf or "").strip())
    artigo = ARTIGO_UF.get(sigla, "")
    de = {"o": "do", "a": "da"}.get(artigo, "de")
    por = {"o": "pelo", "a": "pela"}.get(artigo, "por")
    c = (cargo or "").strip().lower()
    if c.startswith("gov"):
        return f"ao governo {de} {lugar}" if lugar else "ao governo"
    if c.startswith("pref"):
        return f"à prefeitura {de} {lugar}" if lugar else "à prefeitura"
    if c.startswith("sen"):
        return f"ao Senado {por} {lugar}" if lugar else "ao Senado"
    if c.startswith("pres"):
        return "à Presidência da República"
    return f"{c} {lugar}".strip() or "ao cargo em disputa"


def artigo_do_dia(quando):
    """'No domingo', 'Na quinta-feira'. Sem isto sai 'Em domingo'."""
    return "Na " if quando.startswith(("segunda", "terça", "quarta", "quinta", "sexta")) else "No "


def abertura(meta, medida, cands, med):
    quando = por_extenso(meta.get("data"))
    ordem = meta.get("ordinal")
    qual = f"o {ORDINAIS[ordem - 1]} debate" if ordem and ordem <= len(ORDINAIS) else "um debate"
    turno = (meta.get("turno") or "").strip()
    turno = f" do {turno}º turno" if turno in ("1", "2") else ""
    emissora = (meta.get("emissora") or "").strip()
    quem = " e ".join(nome_curto(c) for c in cands) if cands else "os participantes"
    frase = (f"{artigo_do_dia(quando)}{quando}, " if quando else "")
    frase += (f"a {emissora} realizou " if emissora else "foi realizado ")
    frase += f"{qual}{turno} {cargo_por_extenso(meta.get('cargo'), meta.get('uf'))}"
    frase += f", com {quem}"
    if med:
        frase += f" e mediação de {nome_curto(med)}"
    frase += "."

    tempos = medida["por_falante"]
    if cands:
        ordem_fala = sorted(cands, key=lambda c: -tempos[c])
        partes = [f"{num(tempos[c] / 60)} minutos para {nome_curto(c)}"
                  for c in ordem_fala]
        frase += (f" O debate durou {hm(medida['total'])}, e o tempo de fala ficou em "
                  + ", ".join(partes[:-1]) + (" e " if len(partes) > 1 else "")
                  + partes[-1] + ".")
    else:
        frase += f" O debate durou {hm(medida['total'])}."
    return frase


# Grafia de exibição de cada falante, preenchida quando a planilha diz quem é
# quem. O modelo transcreve o nome como ouviu, e no debate de SP de 09/08 saiu
# 'TARCISIO DE FREITAS', sem acento; num texto que vai para cliente o nome sai
# como a planilha escreve.
EXIBICAO = {}


def nome_curto(nome):
    """'FERNANDO HADDAD' vira 'Fernando Haddad'.

    Title() e não capitalize(): o CSV vem em caixa alta e capitalize deixaria
    'Fernando haddad'. Preposição volta para minúscula porque 'De Freitas' lê
    como erro de digitação.
    """
    if nome in EXIBICAO:
        return EXIBICAO[nome]
    t = (nome or "").title()
    for p in (" De ", " Da ", " Do ", " Dos ", " Das ", " E "):
        t = t.replace(p, p.lower())
    return t


def medicao(medida, total_falas):
    por_eixo = medida["por_eixo"]
    if not por_eixo:
        return ("O CSV não traz a coluna de assuntos preenchida, então este "
                "resumo sai sem a medição por tema.")
    itens = [f"{e.lower()} ({num(s / 60)} min)" for e, s in por_eixo.most_common(8)]
    return (f"A marcação de assuntos, feita por dicionário de termos sobre as "
            f"{total_falas} falas, mostra {itens[0]} como o assunto mais presente, "
            f"seguido de {', '.join(itens[1:])}. Uma fala que cita dois assuntos "
            f"conta inteira nos dois, então a soma dos assuntos passa da duração "
            f"do debate.")


def plural(n, singular, plural_):
    return f"{n} {singular if n == 1 else plural_}"


def fechamento(por_eixo_achados, cands):
    """Contagem de categoria por candidato. Não é julgamento de tom.

    Responde à pergunta do resumo executivo ('trouxe proposta ou foi ataque ao
    histórico do adversário?') com o que dá para contar: quantas afirmações de
    cada tipo cada um deixou. Quem não é candidato fica de fora, senão o
    mediador aparece na conta com as falas de encaminhamento dele.
    """
    cont = collections.defaultdict(collections.Counter)
    for achados in por_eixo_achados.values():
        for a in achados:
            if not cands or a["falante"] in cands:
                cont[a["falante"]][a["categoria"]] += 1
    if not cont:
        return ""
    partes = []
    for falante in sorted(cont, key=lambda f: -sum(cont[f].values())):
        c = cont[falante]
        partes.append(
            f"{nome_curto(falante)} somou {plural(c['Balanço'], 'balanço de gestão', 'balanços de gestão')}, "
            f"{plural(c['Proposta'], 'proposta', 'propostas')} e "
            f"{plural(c['Contestação'], 'contestação', 'contestações')} ao adversário")
    return ("Nas afirmações que passaram na conferência de citação, "
            + "; ".join(partes) + ".")


# Uma linha por parágrafo, sem quebra interna: o Drive converte o .md em Doc, e
# quebra no meio do parágrafo às vezes vira parágrafo novo lá dentro.
METODOLOGIA = "\n\n".join([
    "Os minutos vêm da transcrição: cada fala dura até o início da fala seguinte, e o assunto de cada fala é marcado por um dicionário de termos, o mesmo que roda sobre os planos de governo. Não há classificação por modelo, e rodar de novo dá o mesmo número.",
    "O que cada candidato disse foi extraído por modelo de linguagem e conferido uma a uma contra a transcrição: afirmação cujo trecho não aparecia palavra por palavra na fala daquele candidato foi descartada antes de entrar no texto. Os parágrafos foram redigidos a partir dessas afirmações conferidas, e todo número escrito neles foi checado contra elas.",
    "O que este resumo não diz: quem ganhou o debate, se o tom foi duro ou cordial e se as propostas são viáveis. A contagem de menção também não separa quem defende de quem ataca um assunto: negar um tema conta como citá-lo.",
])


def para_conferir(por_eixo_achados, quantos=5):
    """As afirmações com número que sustentam o texto, com horário.

    Existe para o que a Manu pediu: antes de mandar para cliente, alguém abre a
    transcrição no horário e confere. São as com mais dígitos, uma por eixo
    enquanto der, para não sair cinco linhas do mesmo assunto.
    """
    candidatas = []
    for eixo, achados in por_eixo_achados.items():
        for a in achados:
            n = len(numeros(a["trecho"]))
            if n:
                candidatas.append((n, eixo, a))
    candidatas.sort(key=lambda t: -t[0])
    escolhidas, eixos_usados = [], set()
    for _, eixo, a in candidatas:
        if eixo in eixos_usados:
            continue
        escolhidas.append((eixo, a))
        eixos_usados.add(eixo)
        if len(escolhidas) == quantos:
            break
    for _, eixo, a in candidatas:
        if len(escolhidas) == quantos:
            break
        if not any(a is x for _, x in escolhidas):
            escolhidas.append((eixo, a))
    return [f'- [{a["tempo"]}] {nome_curto(a["falante"])}, {eixo.lower()}: "{a["trecho"]}"'
            for eixo, a in escolhidas]


def titulo(meta, eh_sabatina=False):
    ordem = meta.get("ordinal")
    if eh_sabatina:
        qual = "sabatina"
    else:
        qual = f"{ORDINAIS[ordem - 1]} debate" if ordem and ordem <= len(ORDINAIS) else "Debate"
    emissora = (meta.get("emissora") or "").strip()
    data = (meta.get("data") or "").strip()
    t = f"Resumo da {qual} {cargo_por_extenso(meta.get('cargo'), meta.get('uf'))}" if eh_sabatina else f"Resumo do {qual} {cargo_por_extenso(meta.get('cargo'), meta.get('uf'))}"
    if emissora:
        t += f", {emissora}"
    if data:
        # A planilha guarda aaaa-mm-dd, e a capa do documento que vai ao
        # cliente não é lugar de data em formato de banco.
        m = re.match(r"(\d{4})-(\d{2})-(\d{2})$", data)
        t += f", {m.group(3)}/{m.group(2)}/{m.group(1)}" if m else f", {data}"
    return t[0].upper() + t[1:]


def gerar(falas, meta, sem_modelo=False, min_falas=MIN_FALAS, quantos=EIXOS_NO_TEXTO, eh_sabatina=False):
    """Gera o Resumo Executivo completo do evento no formato oficial para clientes."""
    from outros.resumo_debate import (
        limpar_classificacao_procedimental,
        calcular_metricas_debate,
        formatar_prompt,
    )

    falas_limpas = limpar_classificacao_procedimental(falas)
    metricas = calcular_metricas_debate(falas_limpas)

    secao("MEDIÇÃO DO EVENTO")
    log(f"{len(falas)} falas, ~{metricas.get('duracao_total_min')} min de evento")
    for c in metricas.get("candidatos", []):
        if eh_sabatina:
            log(f"    {c['minutos']} min  {c['nome']}")
        else:
            log(f"    {c['minutos']} min ({c['percentual']}%)  {c['nome']}")
    for t in metricas.get("ranking_temas", [])[:8]:
        log(f"    {t['minutos']} min  {t['tema']} ({t['qtd_falas']} falas)")

    titulo_evento = titulo(meta, eh_sabatina)

    if sem_modelo:
        L = [f"# {titulo_evento}", ""]
        L.append(f"Duração estimada: ~{metricas.get('duracao_total_min')} min\n")
        L.append("**Tempo de fala:**")
        for c in metricas.get("candidatos", []):
            if eh_sabatina:
                L.append(f"- {c['nome']}: {c['minutos']} min")
            else:
                L.append(f"- {c['nome']}: {c['minutos']} min ({c['percentual']}%)")
        L.append("\n**Temas mais abordados:**")
        for t in metricas.get("ranking_temas", [])[:quantos]:
            L.append(f"- {t['tema']}: {t['minutos']} min ({t['qtd_falas']} falas)")
        return "\n".join(L)

    chave = os.getenv("GEMINI_API_KEY", "").strip()
    if not chave:
        sys.exit("GEMINI_API_KEY não definido (use --sem-modelo para só a medição).")

    from google import genai
    from google.genai import types
    client = genai.Client(api_key=chave)

    secao("GERAÇÃO DO RESUMO EXECUTIVO (GEMINI)")
    prompt = formatar_prompt(titulo_evento, metricas, falas_limpas, eh_sabatina=eh_sabatina)

    for n in range(1, TENTATIVAS + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.2,
                    max_output_tokens=8192,
                ),
            )
            texto = (resp.text or "").strip()
            if texto and len(texto.split()) >= 250:
                log(f"Resumo executivo gerado com sucesso ({len(texto.split())} palavras)")
                return texto
            log(f"tentativa {n}/{TENTATIVAS}: resposta incompleta ({len(texto.split()) if texto else 0} palavras)")
        except Exception as e:
            log(f"tentativa {n}/{TENTATIVAS} falhou: {e}")
        if n < TENTATIVAS:
            time.sleep(3 * n)

    return f"# {titulo_evento}\n\n[ERRO] Falha ao gerar o resumo executivo via modelo."


def ler_csv_local(caminho):
    with open(caminho, encoding="utf-8") as f:
        return [r for r in csv.DictReader(f) if (r.get("fala") or "").strip()]


def ler_csv_drive(gc, link):
    """Falas a partir da planilha de transcrição que está no Drive."""
    from outros.transcricao_debates import com_retentativa, extrair_id_drive
    aba = com_retentativa(
        "abertura do csv do debate",
        lambda: gc.open_by_key(extrair_id_drive(link)).sheet1,
    )
    valores = com_retentativa("leitura do csv", aba.get_all_values)
    if not valores:
        raise RuntimeError("csv vazio")
    cab = [c.strip() for c in valores[0]]
    linhas = [dict(zip(cab, l + [""] * (len(cab) - len(l)))) for l in valores[1:]]
    return [r for r in linhas if (r.get("fala") or "").strip()]


def meta_da_linha(linha, COL, todas):
    """Metadados do debate a partir da linha da planilha de mapeamento.

    O ordinal é contado aqui e não digitado: é a posição deste debate entre os
    do mesmo cargo, UF e turno, por data. Assim o segundo debate de SP sai como
    '2º debate' sem ninguém lembrar de atualizar a planilha.
    """
    def campo(nome, l=linha):
        j = COL[nome]
        return (l[j] if j < len(l) else "").strip()

    data = campo("data")
    mesmos = []
    for l in todas[1:]:
        if not l:
            continue
        def c(nome, ll=l):
            j = COL[nome]
            return (ll[j] if j < len(ll) else "").strip()
        if (c("cargo"), c("uf"), c("turno")) == (campo("cargo"), campo("uf"), campo("turno")) and c("data"):
            mesmos.append(c("data"))
    ordem = sorted(set(mesmos)).index(data) + 1 if data and data in mesmos else None

    participantes = [p.strip() for p in re.split(r"[;,/]| e ", campo("participantes"))
                     if p.strip()]
    return {
        "id": campo("id"), "data": data, "emissora": campo("emissora"),
        "cargo": campo("cargo"), "uf": campo("uf"), "turno": campo("turno"),
        "mediador": campo("mediador"), "participantes": participantes,
        "ordinal": ordem, "link_csv": campo("link_csv"),
        "tipo": campo("tipo") if "tipo" in COL else "",
    }


# O painel lê o texto do resumo desta coluna. Guardar o markdown na própria
# planilha em vez de fazer o painel exportar o Google Doc: o .docx timbrado
# formata título e subtítulo com run direto (negrito azul), não com estilo de
# heading, então a exportação do Doc volta sem a estrutura de markdown e o
# resumo chega ao painel como um bloco de texto corrido.
COLUNA_TEXTO = "resumo_md"

# Limite de uma célula do Sheets é 50 mil caracteres. O resumo mais longo até
# agora tem cerca de 9 mil, mas a célula que estoura derruba a escrita da linha
# inteira, então o texto grande fica só no Drive e o painel avisa.
MAX_CELULA = 45000


def indice_da_coluna(cabecalho, nome):
    """Índice 1-based da coluna, ou None se ela ainda não existe."""
    nomes = [c.strip().lower() for c in cabecalho]
    return nomes.index(nome) + 1 if nome in nomes else None


def indice_do_resumo(cabecalho):
    """Índice 1-based da coluna link_resumo, ou None se ela ainda não existe."""
    return indice_da_coluna(cabecalho, "link_resumo")


def coluna_por_nome(ws, cabecalho, nome):
    """Índice 1-based da coluna, criada no fim se ainda não existir.

    Criar em vez de exigir mexer na planilha à mão: o script já escreve nessa
    aba, e uma coluna a mais no fim não desloca nenhuma das que o
    transcricao_debates endereça por posição.

    `cabecalho` é atualizado no lugar quando a coluna é criada, para a chamada
    seguinte não calcular o mesmo índice a partir de um cabeçalho velho.
    """
    ja = indice_da_coluna(cabecalho, nome)
    if ja:
        return ja
    col = len(cabecalho) + 1
    if col > ws.col_count:
        ws.add_cols(col - ws.col_count)
    from outros.transcricao_debates import escrever_celula
    escrever_celula(ws, 1, col, nome)
    cabecalho.append(nome)
    log(f"coluna '{nome}' criada na planilha (coluna {col})")
    return col


def coluna_do_resumo(ws, cabecalho):
    """Índice 1-based da coluna link_resumo, criada no fim se ainda não existir."""
    return coluna_por_nome(ws, cabecalho, "link_resumo")


def criar_docx_timbrado(texto_md: str, titulo: str, subtitulo: str, template_path: Path, saida_path: Path) -> Path | None:
    """Gera um DOCX formatado em Montserrat usando o modelo Timbrado eleições."""
    import zipfile, html, re
    if not template_path.exists():
        return None
    with zipfile.ZipFile(template_path, "r") as zin:
        file_dict = {item.filename: zin.read(item.filename) for item in zin.infolist()}

    doc_xml_str = file_dict["word/document.xml"].decode("utf-8")
    m_sect = re.search(r"(<w:sectPr>.*?</w:sectPr>)", doc_xml_str)
    sect_xml = m_sect.group(1) if m_sect else ""

    body_paragraphs = []
    # 1. Título
    body_paragraphs.append(f"""<w:p w:rsidR="00000000" w14:paraId="00000001">
        <w:pPr>
            <w:pStyle w:val="Title"/>
            <w:spacing w:after="120" w:before="0" w:line="280" w:lineRule="auto"/>
            <w:rPr>
                <w:rFonts w:ascii="Montserrat" w:cs="Montserrat" w:eastAsia="Montserrat" w:hAnsi="Montserrat"/>
                <w:b w:val="1"/><w:bCs w:val="1"/>
                <w:color w:val="202124"/>
                <w:sz w:val="32"/><w:szCs w:val="32"/>
            </w:rPr>
        </w:pPr>
        <w:r>
            <w:rPr>
                <w:rFonts w:ascii="Montserrat" w:cs="Montserrat" w:eastAsia="Montserrat" w:hAnsi="Montserrat"/>
                <w:b w:val="1"/><w:bCs w:val="1"/>
                <w:color w:val="202124"/>
                <w:sz w:val="32"/><w:szCs w:val="32"/>
            </w:rPr>
            <w:t>{html.escape(titulo)}</w:t>
        </w:r>
    </w:p>""")

    # 2. Subtítulo
    if subtitulo:
        body_paragraphs.append(f"""<w:p w:rsidR="00000000" w14:paraId="00000002">
            <w:pPr>
                <w:pStyle w:val="Heading1"/>
                <w:spacing w:after="240" w:before="0" w:line="260" w:lineRule="auto"/>
                <w:rPr>
                    <w:rFonts w:ascii="Montserrat" w:cs="Montserrat" w:eastAsia="Montserrat" w:hAnsi="Montserrat"/>
                    <w:color w:val="5F6368"/>
                    <w:sz w:val="22"/><w:szCs w:val="22"/>
                </w:rPr>
            </w:pPr>
            <w:r>
                <w:rPr>
                    <w:rFonts w:ascii="Montserrat" w:cs="Montserrat" w:eastAsia="Montserrat" w:hAnsi="Montserrat"/>
                    <w:color w:val="5F6368"/>
                    <w:sz w:val="22"/><w:szCs w:val="22"/>
                </w:rPr>
                <w:t>{html.escape(subtitulo)}</w:t>
            </w:r>
        </w:p>""")

    for p_idx, linha in enumerate(texto_md.splitlines(), start=3):
        linha = linha.strip()
        if not linha:
            continue
        if linha.startswith("---"):
            body_paragraphs.append(f"""<w:p w:rsidR="00000000" w14:paraId="{p_idx:08x}">
                <w:pPr>
                    <w:pBdr><w:bottom w:val="single" w:sz="6" w:space="1" w:color="CCCCCC"/></w:pBdr>
                    <w:spacing w:after="180" w:before="180"/>
                </w:pPr>
            </w:p>""")
            continue
        if linha.startswith("#"):
            texto_head = re.sub(r"^#+\s*", "", linha)
            body_paragraphs.append(f"""<w:p w:rsidR="00000000" w14:paraId="{p_idx:08x}">
                <w:pPr>
                    <w:spacing w:after="120" w:before="200" w:line="280" w:lineRule="auto"/>
                    <w:rPr>
                        <w:rFonts w:ascii="Montserrat" w:cs="Montserrat" w:eastAsia="Montserrat" w:hAnsi="Montserrat"/>
                        <w:b w:val="1"/>
                        <w:color w:val="1A73E8"/>
                        <w:sz w:val="24"/>
                    </w:rPr>
                </w:pPr>
                <w:r>
                    <w:rPr>
                        <w:rFonts w:ascii="Montserrat" w:cs="Montserrat" w:eastAsia="Montserrat" w:hAnsi="Montserrat"/>
                        <w:b w:val="1"/>
                        <w:color w:val="1A73E8"/>
                        <w:sz w:val="24"/>
                    </w:rPr>
                    <w:t>{html.escape(texto_head)}</w:t>
                </w:r>
            </w:p>""")
            continue

        partes = re.split(r"(\*\*.*?\*\*)", linha)
        runs_xml = []
        for parte in partes:
            if not parte:
                continue
            if parte.startswith("**") and parte.endswith("**"):
                runs_xml.append(f"""<w:r>
                    <w:rPr>
                        <w:rFonts w:ascii="Montserrat" w:cs="Montserrat" w:eastAsia="Montserrat" w:hAnsi="Montserrat"/>
                        <w:b w:val="1"/><w:bCs w:val="1"/>
                        <w:color w:val="202124"/>
                        <w:sz w:val="21"/><w:szCs w:val="21"/>
                    </w:rPr>
                    <w:t xml:space="preserve">{html.escape(parte[2:-2])}</w:t>
                </w:r>""")
            else:
                runs_xml.append(f"""<w:r>
                    <w:rPr>
                        <w:rFonts w:ascii="Montserrat Light" w:cs="Montserrat Light" w:eastAsia="Montserrat Light" w:hAnsi="Montserrat Light"/>
                        <w:color w:val="3C4043"/>
                        <w:sz w:val="21"/><w:szCs w:val="21"/>
                    </w:rPr>
                    <w:t xml:space="preserve">{html.escape(parte)}</w:t>
                </w:r>""")

        body_paragraphs.append(f"""<w:p w:rsidR="00000000" w14:paraId="{p_idx:08x}">
            <w:pPr>
                <w:spacing w:after="180" w:before="0" w:line="340" w:lineRule="auto"/>
                <w:jc w:val="both"/>
            </w:pPr>
            {''.join(runs_xml)}
        </w:p>""")

    novo_doc = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main" xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships" xmlns:m="http://schemas.openxmlformats.org/officeDocument/2006/math" xmlns:wp="http://schemas.openxmlformats.org/wordprocessingDrawing" xmlns:w14="http://schemas.microsoft.com/office/word/2010/wordml" xmlns:a="http://schemas.openxmlformats.org/drawingml/2006/main" xmlns:pic="http://schemas.openxmlformats.org/drawingml/2006/picture">
<w:body>
{''.join(body_paragraphs)}
{sect_xml}
</w:body>
</w:document>"""

    file_dict["word/document.xml"] = novo_doc.encode("utf-8")
    with zipfile.ZipFile(saida_path, "w", compression=zipfile.ZIP_DEFLATED) as zout:
        for fname, content in file_dict.items():
            zout.writestr(fname, content)
    return saida_path


def preencher_texto(_args):
    """Copia para a planilha o texto dos resumos que só existem no Drive.

    Existe para o histórico: os resumos gerados antes da coluna `resumo_md`
    estão só como documento no Drive, e o painel lê da planilha. Exportar o
    documento e colar o texto é mais barato e mais fiel que mandar o modelo
    redigir tudo de novo, e não muda nem uma palavra do que já foi conferido.

    O que sai daqui é o texto do documento, sem a estrutura de markdown: o
    .docx timbrado formata título e destaque com formatação direta, não com
    estilo de heading, e a exportação não tem como adivinhar isso. Resumo novo
    já nasce com o markdown certo pela mão do rodar_fila.
    """
    from outros.transcricao_debates import (PLANILHA, clientes_google,
                                            com_retentativa, escrever_celula,
                                            extrair_id_drive)
    if not PLANILHA:
        sys.exit("defina SPREADSHEET_ID_DEBATES (secret do repo).")
    gc, drive = clientes_google()
    ws = com_retentativa("abertura da planilha de eventos",
                         lambda: gc.open_by_key(PLANILHA).worksheet("eventos"))
    todas = com_retentativa("leitura da planilha", ws.get_all_values)
    cabecalho = list(todas[0])
    col_link = indice_do_resumo(cabecalho)
    if not col_link:
        log("nenhum resumo na planilha ainda.")
        return
    col_texto = coluna_por_nome(ws, cabecalho, COLUNA_TEXTO)

    def campo(linha, col):
        return (linha[col - 1] if col - 1 < len(linha) else "").strip()

    feitos = 0
    for i, linha in enumerate(todas[1:], start=2):
        if not linha:
            continue
        link = campo(linha, col_link)
        if not link.startswith("http") or campo(linha, col_texto):
            continue
        try:
            bruto = com_retentativa(
                f"exportação do resumo da linha {i}",
                lambda l=link: drive.files().export(
                    fileId=extrair_id_drive(l), mimeType="text/plain").execute(),
            )
        except Exception as e:
            log(f"linha {i}: não deu para exportar ({type(e).__name__}: {e})")
            continue
        texto = bruto.decode("utf-8", "replace").strip() if isinstance(bruto, bytes) else str(bruto).strip()
        if not texto:
            log(f"linha {i}: documento veio vazio")
            continue
        if len(texto) > MAX_CELULA:
            log(f"linha {i}: {len(texto)} caracteres, acima do limite da célula")
            continue
        escrever_celula(ws, i, col_texto, texto)
        feitos += 1
        log(f"linha {i}: {len(texto.split())} palavras gravadas")
    log(f"{feitos} resumo(s) preenchido(s).")


def rodar_fila(args):
    import outros.transcricao_debates as td
    from outros.transcricao_debates import (COL, PLANILHA, clientes_google,
                                            com_retentativa, escrever_celula,
                                            enviar_drive, pasta_do_debate)
    aba_alvo = "eventos"
    
    if not PLANILHA:
        sys.exit("defina SPREADSHEET_ID_DEBATES (secret do repo).")
    gc, drive = clientes_google()
    ws = com_retentativa(f"abertura da planilha de {aba_alvo}",
                         lambda: gc.open_by_key(PLANILHA).worksheet(aba_alvo))
    todas = com_retentativa("leitura da fila", ws.get_all_values)

    # Evento que já tem resumo fica de fora da fila: cada rodada chama o modelo
    # uma vez por tema e paga de novo por texto que já está no Drive. Com --id
    # ou --refazer o resumo sai mesmo assim, e sobe como arquivo novo.
    ja_tem = indice_do_resumo(todas[0])

    fila = []
    for i, linha in enumerate(todas[1:], start=2):
        if not linha:
            continue
        meta = meta_da_linha(linha, COL, todas)
        status = (linha[COL["status"]] if COL["status"] < len(linha) else "").strip().lower()
        if args.id and meta["id"] != args.id:
            continue
        if not args.id and status != "pronto":
            continue
        if not meta["link_csv"]:
            log(f"linha {i} ({meta['id']}) sem link_csv, pulando")
            continue
        if (not args.id and not args.refazer and ja_tem
                and (linha[ja_tem - 1] if ja_tem - 1 < len(linha) else "").strip()):
            log(f"{meta['id']} já tem resumo, pulando (use --refazer)")
            continue
        fila.append((i, meta))

    if not fila:
        log("nada para resumir.")
        return
    log(f"{len(fila)} evento(s): {', '.join(m['id'] or f'linha {i}' for i, m in fila)}")

    saida = Path(args.saida or "transcricoes")
    saida.mkdir(parents=True, exist_ok=True)
    cabecalho = list(todas[0])
    col_resumo = coluna_do_resumo(ws, cabecalho) if args.drive else None
    col_texto = coluna_por_nome(ws, cabecalho, COLUNA_TEXTO) if args.drive else None
    template_docx = Path(__file__).parent / "templates" / "timbrado_eleicoes.docx"

    for i, meta in fila:
        secao(f"RESUMO {meta['id']}  (linha {i})")
        try:
            falas = ler_csv_drive(gc, meta["link_csv"])
        except Exception as e:
            log(f"csv não pôde ser lido: {type(e).__name__}: {e}")
            continue
        sabatina_flag = meta.get("tipo", "") == "Sabatina" or args.sabatina
        md = gerar(falas, meta, sem_modelo=args.sem_modelo,
                   min_falas=args.min_falas, quantos=args.eixos, eh_sabatina=sabatina_flag)
        arq_md = saida / f"{meta['id'] or 'debate'}_resumo.md"
        arq_md.write_text(md, encoding="utf-8")
        log(f"{arq_md}  ({len(md.split())} palavras)")

        arq_upload = arq_md
        # Se o template timbrado existir, gera o .docx com Montserrat e timbrado oficial
        if template_docx.exists():
            arq_docx = saida / f"{meta['id'] or 'debate'}_resumo.docx"
            # O título do documento é o mesmo do texto: 'Resumo do 1º debate ao
            # governo de São Paulo, Band, 09/08/2026'. Antes caía no `else` de
            # um meta_titulo que nunca existiu e o cliente recebia o PDF com o
            # id da linha na capa ('Resumo — 2026-band-sp-gov-t1').
            titulo_doc = titulo(meta, sabatina_flag)
            if criar_docx_timbrado(md, titulo_doc, "Eleições 2026 • Monitoramento de Debates", template_docx, arq_docx):
                arq_upload = arq_docx
                log(f"DOCX Timbrado gerado: {arq_docx.name} (fonte Montserrat)")

        if args.drive:
            pasta = pasta_do_debate(drive, meta["uf"], meta["data"], ident=meta["id"])
            link = enviar_drive(drive, arq_upload, f"{meta['id']}_resumo",
                                "application/vnd.google-apps.document", pasta)
            escrever_celula(ws, i, col_resumo, link)
            log(f"resumo no Drive: {link}")
            # O texto vai para a planilha porque é de lá que o painel lê. O
            # Drive continua guardando o documento timbrado, que é o que se
            # manda para fora.
            if len(md) > MAX_CELULA:
                log(f"resumo com {len(md)} caracteres, acima do limite da célula: "
                    f"texto não gravado na planilha (só o link do Drive)")
            else:
                escrever_celula(ws, i, col_texto, md)
                log(f"texto do resumo gravado na coluna '{COLUNA_TEXTO}'")


def main():
    ap = argparse.ArgumentParser(description="Resumo executivo de um debate")
    ap.add_argument("--csv", default=None, help="CSV da transcrição (modo avulso)")
    ap.add_argument("--fila", action="store_true", help="roda a planilha de debates")
    ap.add_argument("--id", default=None, help="um id da planilha, ignorando o status")
    ap.add_argument("--drive", action="store_true",
                    help="sobe o resumo e grava o link na planilha")
    ap.add_argument("--refazer", action="store_true",
                    help="gera de novo mesmo para quem já tem link_resumo")
    ap.add_argument("--sem-modelo", action="store_true",
                    help="só a parte medida, sem chamar a API nem gastar")
    ap.add_argument("--eixos", type=int, default=EIXOS_NO_TEXTO,
                    help=f"quantos temas ganham parágrafo (padrão {EIXOS_NO_TEXTO})")
    ap.add_argument("--min-falas", type=int, default=MIN_FALAS)
    ap.add_argument("--saida", default=None)
    ap.add_argument("--sabatina", action="store_true", help="O evento é uma sabatina (apenas um candidato)")
    ap.add_argument("--preencher-texto", action="store_true",
                    help="copia para a planilha o texto dos resumos que já estão "
                         "no Drive, sem chamar o modelo")
    # Só no modo avulso: sem a planilha, ninguém sabe a data nem quem mediou.
    ap.add_argument("--data", default=None)
    ap.add_argument("--emissora", default=None)
    ap.add_argument("--cargo", default=None)
    ap.add_argument("--uf", default=None)
    ap.add_argument("--turno", default=None)
    ap.add_argument("--mediador", default=None)
    ap.add_argument("--participantes", default=None, help="separados por vírgula")
    ap.add_argument("--ordinal", type=int, default=None)
    args = ap.parse_args()

    secao("CONFIGURAÇÃO")
    log(f"modelo : {GEMINI_MODEL if not args.sem_modelo else 'nenhum (--sem-modelo)'}")

    if args.preencher_texto:
        preencher_texto(args)
    elif args.fila or args.id:
        rodar_fila(args)
    elif args.csv:
        entrada = Path(args.csv)
        if not entrada.exists():
            sys.exit(f"não encontrei {entrada}")
        falas = ler_csv_local(entrada)
        if not falas:
            sys.exit("CSV sem falas.")
        meta = {
            "data": args.data, "emissora": args.emissora, "cargo": args.cargo,
            "uf": args.uf, "turno": args.turno, "mediador": args.mediador,
            "ordinal": args.ordinal,
            "participantes": [p.strip() for p in (args.participantes or "").split(",")
                              if p.strip()],
        }
        md = gerar(falas, meta, sem_modelo=args.sem_modelo,
                   min_falas=args.min_falas, quantos=args.eixos, eh_sabatina=args.sabatina)
        destino = Path(args.saida) if args.saida else entrada.with_name(
            entrada.stem + "_resumo.md")
        destino.write_text(md, encoding="utf-8")
        secao("FIM")
        log(f"{destino}  ({len(md.split())} palavras)")
    else:
        sys.exit("informe --csv, --fila ou --id.")


if __name__ == "__main__":
    main()
