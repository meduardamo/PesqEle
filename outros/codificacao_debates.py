"""
Codificação temática de debate: em que tema cada fala se enquadra.

A unidade é o turno de fala, e cada turno recebe exatamente um tema. Rótulo
único é o padrão do Manifesto Project e do Comparative Agendas, e é o que
mantém as contagens somáveis: com multirrótulo o total de menções passa de
100% e a comparação entre candidatos perde o sentido.

A taxonomia é a mesma dos planos de governo (10 eixos, 46 temas), importada
de analise_planos e não copiada. É isso que permite cruzar o que o candidato
escreveu com o que ele falou.

Boa parte de um debate não é sobre tema nenhum: cumprimento, regra de tempo,
bate-boca sobre o formato. Esses turnos vão para 'Sem tema', e forçá-los num
eixo inflaria o tema mais próximo.

Cada fala é codificada duas vezes, em ordens diferentes. Onde as duas passadas
discordam é onde alguém precisa olhar, e a taxa de acordo é o número que se
reporta. É o equivalente barato aos dois codificadores independentes que o
Comparative Agendas usa; lá a régua é ~95% de acordo no eixo e ~75% no tema.

Uso:
    python -m outros.codificacao_debates --csv transcricoes/teste.csv
    python -m outros.codificacao_debates --csv teste.csv --passada-unica
"""

import argparse
import csv
import json
import os
import random
import re
import sys
import time
import unicodedata
from pathlib import Path

from google import genai
from google.genai import types

from outros.analise_planos import EIXOS

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

TEMAS = {tema: desc for temas in EIXOS.values() for tema, desc in temas.items()}
EIXO_DO_TEMA = {tema: eixo for eixo, temas in EIXOS.items() for tema in temas}
SEM_TEMA = "Sem tema"

# Segunda dimensão, que a taxonomia dos planos não precisava ter. Plano é só
# proposta; debate é proposta, defesa de gestão, contestação de número e
# acusação, e a maior parte do tempo não é proposta.
#
# Sem esta coluna o modelo era obrigado a enfiar "nenhuma indicação o Eduardo
# Bolsonaro fez no nosso governo" em algum dos 46 temas, e oscilava entre
# 'Sem tema' e 'Servidores e Municípios' porque a escolha era arbitrária.
# Metade do desacordo entre as passadas vinha daí.
TIPOS = {
    "Proposta": "promete, anuncia ou defende uma ação de governo para o futuro",
    "Balanço": "afirma o que já foi feito, apresenta número ou resultado de gestão, própria ou do adversário",
    "Contestação": "nega, desmente ou disputa um fato, número ou versão apresentada pelo outro",
    "Ataque pessoal": "acusa, atribui má-fé ou fala da vida, das relações e das alianças do adversário, sem tratar de política pública",
    "Procedimental": "cumprimento, regra de tempo, direito de resposta, discussão sobre o formato do debate",
}

# Falas por chamada. Lote grande economiza tokens (o codebook vai junto em toda
# chamada), mas a literatura registra queda de desempenho em entrada longa, e
# lote demais aumenta a chance de o modelo pular fala.
LOTE = 12
TENTATIVAS = 3

# Turno mais curto que isto quase nunca tem tema: é "obrigado", "trinta
# segundos", "posso responder". Vai direto para Sem tema, sem gastar chamada.
MIN_PALAVRAS = 5

_T0 = time.time()


def log(msg=""):
    if msg == "":
        print(flush=True)
        return
    m, s = divmod(int(time.time() - _T0), 60)
    print(f"[{m:02d}:{s:02d}] {msg}", flush=True)


def secao(titulo):
    log()
    log("=" * 68)
    log(titulo)
    log("=" * 68)


def norm(texto):
    """Minúscula, sem acento, espaço colapsado. Para comparar trecho com fala."""
    t = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode()
    return re.sub(r"\s+", " ", t.lower()).strip()


# Nome normalizado -> nome canônico, para o casamento tolerante em conferir().
_POR_NOME_NORM = {norm(t): t for t in list(TEMAS) + [SEM_TEMA]}
_POR_TIPO_NORM = {norm(t): t for t in TIPOS}


def montar_codebook():
    linhas = []
    for eixo, temas in EIXOS.items():
        linhas.append(f"\n## {eixo}")
        for tema, desc in temas.items():
            linhas.append(f"- {tema}: {desc}")
    return "\n".join(linhas)


PROMPT = """Você está codificando as falas de um debate eleitoral brasileiro,
seguindo um codebook fixo. Cada fala recebe um tipo e um tema.

# Tipos
{tipos}

# Temas
{codebook}

# Regras
- Escolha para cada fala EXATAMENTE UM tipo e EXATAMENTE UM tema, escritos
  igual à lista, sem reformular o nome.
- O tipo vem primeiro: veja o que a fala está fazendo antes de ver do que ela
  trata.
- Fala de tipo "Procedimental" ou "Ataque pessoal" recebe tema "{sem_tema}".
- Fala de tipo "Proposta", "Balanço" ou "Contestação" recebe um tema da lista,
  o da política pública em jogo. Use "{sem_tema}" apenas se realmente não
  houver política pública nenhuma na fala.
- Se a fala cruzar dois temas, escolha aquele em que o falante gasta mais
  tempo. Não invente tema fora da lista.
- O campo "trecho" deve ser copiado LITERALMENTE da fala, entre 4 e 25
  palavras seguidas, e ser a parte que justifica o tema escolhido. Copie do
  jeito que está, inclusive repetição e hesitação. Não parafraseie, não
  corrija, não junte pedaços separados de lugares diferentes da fala. Quando
  o tema for "{sem_tema}", deixe "".

# Falas
{falas}

Responda só com um array JSON, um objeto por fala, nesta forma:
[{{"id": 1, "tipo": "...", "tema": "...", "trecho": "..."}}]
Devolva um objeto para CADA fala listada, na mesma ordem dos ids."""


def classificar_lote(client, falas, codebook):
    """Devolve {id: (tipo, tema, trecho)} para um lote de falas."""
    listadas = "\n\n".join(
        f'[{f["id"]}] {f["falante"]}: {f["fala"]}' for f in falas
    )
    tipos = "\n".join(f"- {t}: {d}" for t, d in TIPOS.items())
    prompt = PROMPT.format(codebook=codebook, tipos=tipos,
                           sem_tema=SEM_TEMA, falas=listadas)

    for n in range(1, TENTATIVAS + 1):
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=prompt,
                config=types.GenerateContentConfig(
                    temperature=0.0,
                    response_mime_type="application/json",
                ),
            )
            dados = json.loads(resp.text)
            if isinstance(dados, dict):
                dados = next((v for v in dados.values() if isinstance(v, list)), [])
            return {int(d["id"]): (str(d.get("tipo", "")).strip(),
                                   str(d.get("tema", "")).strip(),
                                   str(d.get("trecho", "")).strip())
                    for d in dados if "id" in d}
        except Exception as e:
            log(f"    tentativa {n}/{TENTATIVAS} falhou: {e}")
            if n < TENTATIVAS:
                time.sleep(5 * n)
    return {}


def conferir(fala, tipo, tema, trecho):
    """Guardas antes de aceitar a classificação. Devolve (tipo, tema, trecho, nota).

    Mesma disciplina das guardas dos planos: o que o modelo devolve só entra
    se der para conferir. Tipo e tema fora da lista, e trecho que não está na
    fala, são os jeitos de a classificação parecer boa e não ser.

    Os nomes são casados sem acento e sem caixa, e devolvidos no canônico: o
    modelo às vezes escreve "Primeira Infancia", e descartar por acento
    perdido é jogar fora classificação boa.
    """
    tipo = _POR_TIPO_NORM.get(norm(tipo), tipo)
    tema = _POR_NOME_NORM.get(norm(tema), tema)

    if tipo not in TIPOS:
        return "Procedimental", SEM_TEMA, "", f"tipo fora da lista: {tipo!r}"

    # Regra mecânica, não julgamento: o que fecha a fronteira do Sem tema, que
    # era a maior fonte de desacordo entre as passadas.
    if tipo in ("Procedimental", "Ataque pessoal"):
        return tipo, SEM_TEMA, "", ""

    if tema == SEM_TEMA:
        return tipo, SEM_TEMA, "", ""
    if tema not in TEMAS:
        return tipo, SEM_TEMA, "", f"tema fora do codebook: {tema!r}"
    if not trecho:
        return tipo, tema, "", "sem trecho de apoio"
    if norm(trecho) not in norm(fala):
        return tipo, tema, "", "trecho não confere com a fala"
    return tipo, tema, trecho, ""


def codificar(client, falas, codebook, ordem):
    """Uma passada completa. `ordem` é a lista de índices em que os lotes são
    montados: passadas com agrupamento diferente é o que dá independência de
    verdade entre elas, porque o modelo é sensível à ordem e à vizinhança."""
    resultado, avisos = {}, []
    grandes = [f for f in falas if len(f["fala"].split()) >= MIN_PALAVRAS]
    curtas = [f for f in falas if len(f["fala"].split()) < MIN_PALAVRAS]
    for f in curtas:
        resultado[f["id"]] = ("Procedimental", SEM_TEMA, "")

    fila = [grandes[i] for i in ordem]
    lotes = [fila[i:i + LOTE] for i in range(0, len(fila), LOTE)]
    log(f"  {len(grandes)} falas em {len(lotes)} lote(s), "
        f"{len(curtas)} curtas direto para Procedimental")

    for i, lote in enumerate(lotes, 1):
        bruto = classificar_lote(client, lote, codebook)
        faltando = [f["id"] for f in lote if f["id"] not in bruto]
        if faltando:
            avisos.append(f"lote {i}: modelo não devolveu {len(faltando)} fala(s)")
            log(f"    lote {i}: faltaram {len(faltando)} fala(s), ficaram sem código")
        for f in lote:
            tipo, tema, trecho = bruto.get(f["id"], ("", SEM_TEMA, ""))
            tipo, tema, trecho, nota = conferir(f["fala"], tipo, tema, trecho)
            if nota:
                avisos.append(f'fala {f["id"]}: {nota}')
            resultado[f["id"]] = (tipo, tema, trecho)
        log(f"    lote {i}/{len(lotes)} ok")
    return resultado, avisos


def main():
    ap = argparse.ArgumentParser(description="Codifica o tema de cada fala de um debate")
    ap.add_argument("--csv", required=True, help="CSV da transcrição (segundos,tempo,falante,fala)")
    ap.add_argument("--saida", default=None, help="CSV de saída (padrão: <entrada>_temas.csv)")
    ap.add_argument("--passada-unica", action="store_true",
                    help="codifica uma vez só, sem medir acordo")
    args = ap.parse_args()

    if not os.getenv("GEMINI_API_KEY", "").strip():
        sys.exit("GEMINI_API_KEY não definido.")

    entrada = Path(args.csv)
    if not entrada.exists():
        sys.exit(f"não encontrei {entrada}")

    with open(entrada, encoding="utf-8") as f:
        linhas = list(csv.DictReader(f))
    falas = [
        {"id": i, "segundos": r.get("segundos", ""), "tempo": r.get("tempo", ""),
         "falante": r.get("falante", ""), "fala": r.get("fala", "")}
        for i, r in enumerate(linhas, 1) if r.get("fala", "").strip()
    ]
    if not falas:
        sys.exit("CSV sem falas.")

    codebook = montar_codebook()

    secao("CONFIGURAÇÃO")
    log(f"modelo   : {GEMINI_MODEL}")
    log(f"entrada  : {entrada}  ({len(falas)} falas)")
    log(f"codebook : {len(EIXOS)} eixos, {len(TEMAS)} temas")
    log(f"passadas : {'1' if args.passada_unica else '2 (mede acordo)'}")

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    grandes = [f for f in falas if len(f["fala"].split()) >= MIN_PALAVRAS]

    secao("PASSADA 1")
    p1, av1 = codificar(client, falas, codebook, list(range(len(grandes))))

    p2, av2, acordo_tema, acordo_eixo = {}, [], None, None
    if not args.passada_unica:
        secao("PASSADA 2")
        # Ordem embaralhada, semente fixa: a comparação é reproduzível, e o
        # modelo vê cada fala com vizinhos diferentes.
        ordem = list(range(len(grandes)))
        random.Random(20260813).shuffle(ordem)
        p2, av2 = codificar(client, falas, codebook, ordem)

        secao("ACORDO ENTRE AS PASSADAS")
        comparaveis = [f["id"] for f in falas if f["id"] in p1 and f["id"] in p2]
        eixo_de = lambda t: EIXO_DO_TEMA.get(t, t)
        igual_tipo = sum(1 for i in comparaveis if p1[i][0] == p2[i][0])
        igual_tema = sum(1 for i in comparaveis if p1[i][1] == p2[i][1])
        igual_eixo = sum(1 for i in comparaveis if eixo_de(p1[i][1]) == eixo_de(p2[i][1]))
        n = len(comparaveis) or 1
        acordo_tipo, acordo_tema, acordo_eixo = igual_tipo / n, igual_tema / n, igual_eixo / n

        log(f"acordo no tipo : {acordo_tipo:.1%}")
        log(f"acordo no eixo : {acordo_eixo:.1%}   (régua do Comparative Agendas: ~95%)")
        log(f"acordo no tema : {acordo_tema:.1%}   (régua do Comparative Agendas: ~75%)")
        log()
        divergentes = [i for i in comparaveis if p1[i][1] != p2[i][1]]
        if divergentes:
            log(f"{len(divergentes)} fala(s) divergem no tema, marcadas em 'conferir':")
            for i in divergentes[:10]:
                log(f"  fala {i}: {p1[i][0]}/{p1[i][1]!r}  vs  {p2[i][0]}/{p2[i][1]!r}")
            if len(divergentes) > 10:
                log(f"  e mais {len(divergentes) - 10}")

    secao("SAÍDA")
    destino = Path(args.saida) if args.saida else entrada.with_name(entrada.stem + "_temas.csv")
    campos = ["id", "segundos", "tempo", "falante", "fala", "palavras",
              "tipo", "tema", "eixo", "trecho", "tema_passada2", "conferir"]
    with open(destino, "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=campos)
        w.writeheader()
        for fa in falas:
            tipo, tema, trecho = p1.get(fa["id"], ("", SEM_TEMA, ""))
            t2 = p2.get(fa["id"], ("", "", ""))[1] if p2 else ""
            w.writerow({
                **{k: fa[k] for k in ("id", "segundos", "tempo", "falante", "fala")},
                "palavras": len(fa["fala"].split()),
                "tipo": tipo,
                "tema": tema,
                "eixo": EIXO_DO_TEMA.get(tema, "" if tema == SEM_TEMA else "?"),
                "trecho": trecho,
                "tema_passada2": t2,
                "conferir": "sim" if (t2 and t2 != tema) else "",
            })
    log(f"{destino}  ({destino.stat().st_size / 1024:.0f} KB)")

    secao("TIPO DE FALA POR CANDIDATO")
    # Em palavras, e não em número de falas: turno de debate varia muito de
    # tamanho, e contar turno faz réplica de 5 segundos pesar igual a resposta
    # de 90.
    por_tipo = {}
    for fa in falas:
        tipo = p1.get(fa["id"], ("", SEM_TEMA, ""))[0] or "Procedimental"
        d = por_tipo.setdefault(fa["falante"], {})
        d[tipo] = d.get(tipo, 0) + len(fa["fala"].split())

    for falante, tipos in sorted(por_tipo.items()):
        tot = sum(tipos.values()) or 1
        log(f"\n{falante}  ({tot} palavras)")
        for tipo, pal in sorted(tipos.items(), key=lambda x: -x[1]):
            log(f"   {pal / tot:>5.1%}  {pal:>5}  {tipo}")

    secao("TEMAS POR CANDIDATO")
    por = {}
    for fa in falas:
        tema = p1.get(fa["id"], ("", SEM_TEMA, ""))[1]
        if tema == SEM_TEMA:
            continue
        eixo = EIXO_DO_TEMA.get(tema, "?")
        d = por.setdefault(fa["falante"], {})
        d[eixo] = d.get(eixo, 0) + len(fa["fala"].split())

    for falante, eixos in sorted(por.items()):
        tot = sum(eixos.values()) or 1
        log(f"\n{falante}  ({tot} palavras com tema)")
        for eixo, pal in sorted(eixos.items(), key=lambda x: -x[1]):
            log(f"   {pal / tot:>5.1%}  {pal:>5}  {eixo}")

    sem = sum(1 for fa in falas if p1.get(fa["id"], ("", SEM_TEMA, ""))[1] == SEM_TEMA)
    secao("O QUE CONFERIR ANTES DE USAR")
    log(f"{SEM_TEMA}: {sem} de {len(falas)} falas ({sem / len(falas):.0%})")
    if acordo_eixo is not None:
        log(f"acordo eixo {acordo_eixo:.1%} | acordo tema {acordo_tema:.1%}")
        if acordo_eixo < 0.90:
            log("acordo no eixo abaixo de 90%: o codebook não está separando bem "
                "estas falas, não use antes de olhar as divergências")
    for a in (av1 + av2)[:15]:
        log(f"aviso: {a}")
    log("cada tema tem um trecho literal da fala ao lado: confira por amostra "
        "antes de citar em entrega")


if __name__ == "__main__":
    main()
