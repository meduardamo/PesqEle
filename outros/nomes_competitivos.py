"""Alimenta a aba de nomes competitivos ao Senado a partir do Radar do Congresso.

    python -m outros.nomes_competitivos            confere e imprime o diff
    python -m outros.nomes_competitivos --apply    grava a aba

A planilha `Eleições 2026 - Nomes competitivos` tinha uma lista de Senado feita
na mão, com 104 nomes. Quem manda agora é a aba
`Competitividade Senado (todas as candidaturas)` do Radar do Congresso 2026:
entram as candidaturas que a régua marca como competitivas (bandas 5 a 7, a
coluna `É competitivo?`), com a média móvel de 30 dias no lugar do percentual
da última pesquisa. Ver `outros/indice_senado.py`, que publica aquela aba.

O bloco de Governador da aba não sai do Radar e é copiado sem tocar: o índice
só cobre Senado.

Duas coisas da lista velha sobrevivem, e é o que este script se dá ao trabalho
de casar nome a nome: a grafia do nome (o Radar usa o nome de urna do TSE, e
`Fufuca` lê pior que `André Fufuca`) e o texto da coluna `atualizações`, que a
Manu escreve na mão.
"""
import os, re, sys, unicodedata
from difflib import SequenceMatcher

import gspread
from google.oauth2.service_account import Credentials

ESCOPO = ["https://www.googleapis.com/auth/spreadsheets"]
# O repositório é público: id de planilha vem de variável de ambiente.
ID_RADAR = os.getenv("SPREADSHEET_ID_RECANDIDATURAS", "").strip()
ID_ALVO = os.getenv("SPREADSHEET_ID_NOMES_COMPETITIVOS", "").strip()
if not ID_RADAR or not ID_ALVO:
    raise RuntimeError("Faltam SPREADSHEET_ID_RECANDIDATURAS e/ou "
                       "SPREADSHEET_ID_NOMES_COMPETITIVOS.")
ABA_RADAR = "Competitividade Senado (todas as candidaturas)"
ABA_ALVO = "Nomes competitivos Gov e Senado"

# A aba escreve o partido como a Manu escreve, não como a sigla do TSE vem.
PARTIDO = {"PODE": "Podemos", "UNIÃO": "União", "REPUBLICANOS": "Republicanos",
           "NOVO": "Novo", "REDE": "Rede", "SOLIDARIEDADE": "Solidariedade",
           "CIDADANIA": "Cidadania", "AVANTE": "Avante", "MOBILIZA": "Mobiliza",
           "PC do B": "PCdoB", "PRD": "PRD", "AGIR": "Agir", "DC": "DC"}
# Nome de urna que não se parece com o nome pelo qual a pessoa é conhecida.
# Sem isso o script perde a grafia e a anotação da lista velha e escreve o nome
# do TSE, e o diff acusa a mesma pessoa saindo e entrando. Mesma dor que a
# NOME_NO_RADAR do `outros/indice_senado.py`.
NO_RADAR = {("PB", "Veneziano Vital do Rêgo"): "Veneziano",
            ("DF", "Leila Barros"): "Leila Do Vôlei",
            ("RN", "Samanda Alves"): "Samanda De Lula",
            ("RR", "Hélio Bolsonaro"): "Helio Fernando Barbosa Lopes"}
MANDATO = {"Senador hoje": "Senador",
           "Deputado federal hoje": "Deputado Federal",
           "Deputado estadual/distrital hoje": "Deputado Estadual",
           "Sem mandato legislativo hoje": ""}
# O bloco de Senado da aba é ordenado por região, e não por sigla: mantido.
UF_ORDEM = ["AC", "AP", "AM", "PA", "RO", "RR", "TO",
            "AL", "BA", "CE", "MA", "PB", "PE", "PI", "RN", "SE",
            "DF", "GO", "MS", "MT", "ES", "MG", "RJ", "SP", "PR", "RS", "SC"]
CABECALHO = ["Cargo - disputa", "UF", "Partido", "Candidato",
             "Situação eleitoral em 2026", "Mandato Legislativo atual",
             "atualizações", "% nas pesquisas", "data atualização",
             "Cenário eleitoral (banda)", "Chapa presidencial", "", ""]


def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).upper()
    s = "".join(c for c in s if not unicodedata.combining(c))
    # FILHO, JÚNIOR e NETO ficam: são o que separa Renan Calheiros de Renan Filho.
    s = re.sub(r"\b(DEL|DELEGADO|DR|DRA|CAP|CAPITAO|CORONEL|CEL|SGT|SARGENTO|"
               r"PROF|PROFESSOR|PROFESSORA|PASTOR)\b", "", s)
    return " ".join(re.sub(r"[^A-Z0-9 ]", " ", s).split())


def sim(a, b):
    """Semelhança entre dois nomes já normalizados, 0 a 1."""
    if not a or not b:
        return 0.0
    r = SequenceMatcher(None, a, b).ratio()
    A, B = set(a.split()), set(b.split())
    if A & B:
        r = max(r, 0.6 + 0.35 * len(A & B) / max(len(A), len(B)))
    return r


def casa(nome_radar, partido_radar, linha_velha):
    """Diz se a linha da lista velha é a mesma candidatura da linha do Radar.

    Sobrenome em comum sozinho não basta: em MA, `Roberto Rocha` e
    `Weverton Rocha` são duas pessoas. Casamento fraco só passa com o partido
    igual, e o partido do Radar vem como sigla do TSE."""
    uf = linha_velha[1].strip().upper()
    esperado = NO_RADAR.get((uf, linha_velha[3].strip()))
    if esperado:
        return 1.0 if norm(esperado) == norm(nome_radar) else 0.0
    s = sim(norm(nome_radar), norm(linha_velha[3]))
    if s >= 0.85:
        return s
    mesmo_partido = (PARTIDO.get(partido_radar.strip(), partido_radar.strip())
                     == linha_velha[2].strip().rstrip("*"))
    return s if (s >= 0.75 and mesmo_partido) else 0.0


def pct(s):
    """'21,8%' -> 21.8, para ordenar. Vazio vira -1."""
    s = str(s).replace("%", "").replace(",", ".").strip()
    try:
        return float(s)
    except ValueError:
        return -1.0


def main(apply=False):
    cred = Credentials.from_service_account_file(
        os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"), scopes=ESCOPO)
    gc = gspread.authorize(cred)

    bruto = gc.open_by_key(ID_RADAR).worksheet(ABA_RADAR).get_all_values()
    cab = bruto[0]
    radar = [dict(zip(cab, l)) for l in bruto[1:]]
    comp = [r for r in radar if r["É competitivo?"].strip() == "Sim"]

    ws = gc.open_by_key(ID_ALVO).worksheet(ABA_ALVO)
    atual = ws.get_all_values()
    gov = [l for l in atual[1:] if l and l[0].strip().lower().startswith("governador")]
    sen_velho = [l for l in atual[1:] if l and l[0].strip().lower().startswith("senador")]

    # Casa a lista velha com o Radar dentro da UF, só para herdar grafia e nota.
    # Casamento fraco não vale: preferível escrever o nome de urna do TSE do que
    # colar a anotação da Manu na pessoa errada.
    por_uf = {}
    for l in sen_velho:
        por_uf.setdefault(l[1].strip().upper(), []).append(l)
    herdado, usados, sem_par = {}, set(), []
    for r in comp:
        alvo, melhor = None, 0.0
        for l in por_uf.get(r["UF"].strip().upper(), []):
            if id(l) in usados:
                continue
            s = casa(r["Candidato"], r["Partido"], l)
            if s > melhor:
                alvo, melhor = l, s
        if alvo is not None and melhor > 0:
            usados.add(id(alvo))
            herdado[id(r)] = alvo
        else:
            sem_par.append(r)

    linhas = []
    for r in sorted(comp, key=lambda r: (UF_ORDEM.index(r["UF"].strip().upper()),
                                         -pct(r["Média das pesquisas"]))):
        velho = herdado.get(id(r))
        partido = PARTIDO.get(r["Partido"].strip(), r["Partido"].strip())
        # O asterisco do partido é legenda da própria aba ('*nome inelegível').
        if velho and velho[2].strip().rstrip("*") == partido and velho[2].strip().endswith("*"):
            partido += "*"
        mandato = MANDATO.get(r["Mandato legislativo atual"].strip(), "")
        linhas.append([
            "Senador",
            r["UF"].strip(),
            partido,
            velho[3].strip() if velho else r["Candidato"].strip(),
            "Reeleição Senado" if mandato == "Senador" else "",
            mandato,
            velho[6].strip() if velho else "",
            r["Média das pesquisas"].strip(),
            r["Última pesquisa do estado"].strip(),
            r["Cenário eleitoral (banda)"].strip(),
            r["Chapa presidencial"].strip(),
            "", "",
        ])

    saiu = [l for l in sen_velho if id(l) not in usados]
    print(f"Governador: {len(gov)} linhas copiadas sem alteração.")
    print(f"Senado: {len(sen_velho)} linhas viram {len(linhas)}.")
    print(f"\nSaem da lista ({len(saiu)}):")
    for l in saiu:
        no_radar = [r for r in radar
                    if r["UF"].strip() == l[1].strip()
                    and casa(r["Candidato"], r["Partido"], l) > 0]
        motivo = (f"banda '{no_radar[0]['Cenário eleitoral (banda)']}'"
                  if no_radar else "sem registro de Senado no TSE")
        print(f"  {l[1]} {l[3]} ({l[2]}): {motivo}")
    print(f"\nEntram ({len(sem_par)}):")
    for r in sem_par:
        print(f"  {r['UF']} {r['Candidato']} ({r['Partido']}) "
              f"{r['Média das pesquisas']} — {r['Cenário eleitoral (banda)']}")

    if not apply:
        print("\nNada gravado. Rode com --apply para publicar.")
        return

    # As duas linhas de legenda da coluna M são texto da aba, não vêm do Radar.
    legenda = {i: l[12] for i, l in enumerate(atual) if len(l) > 12 and l[12].strip()}
    corpo = [CABECALHO] + gov + linhas
    corpo = [l + [""] * (len(CABECALHO) - len(l)) for l in corpo]
    for i, texto in legenda.items():
        if i < len(corpo):
            corpo[i][12] = texto
    # RAW, e não USER_ENTERED: o Sheets lê '19,6%' como número e reescreve
    # '19,60%', e '25/08' como data e reescreve '25/08/2026'. A aba do Radar é
    # texto, e a coluna some com a casa decimal se deixar o Sheets converter.
    ws.batch_clear([f"A1:M{max(len(atual), len(corpo))}"])
    ws.update(corpo, "A1", value_input_option="RAW")
    print(f"\nGravado: {len(corpo) - 1} linhas na aba '{ABA_ALVO}'.")


if __name__ == "__main__":
    main(apply="--apply" in sys.argv)
