"""Publica o Índice de competitividade eleitoral ao Senado 2026.

Roda em quatro passos, na ordem, todos do diretório de trabalho:
    python -m outros.indice_senado_dados     baixa as bases e as coligações
    python -m outros.indice_senado_casar     casa registro do TSE com a matriz
    python -m outros.indice_senado           confere; com --apply, publica a aba
    python -m outros.indice_senado_notas     grava as notas de cabeçalho


Universo: todo titular com registro de Senado no TSE, menos renúncias.
Número: média móvel híbrida de 30 dias, na data mais recente de cada UF.
Régua: sete bandas em cima da distância para a linha de corte. 2026 tem duas
vagas por estado, então a linha de corte é o 2º colocado; para quem já está em
1º ou 2º, a referência passa a ser o 3º.

A régua e as taxas por banda vivem em `outros/regua_senado.py`, que é o mesmo
módulo que o `research/calibrar_senado.py` usa para medir. A banda é calculada
na escala comparável, com os percentuais do estado normalizados para somar 100,
porque é assim que o histórico de 2010 e 2018 foi medido; a média móvel bruta
soma cerca de 76 por UF e comprimiria todo mundo para dentro do empate técnico.
"""
import os
import math, re, sys, unicodedata
import numpy as np, pandas as pd, gspread
from difflib import SequenceMatcher
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

from outros import regua_senado as regua
from outros import formato_recandidaturas as formato

# O repositório é público: id de planilha vem de variável de ambiente.
ID = os.getenv("SPREADSHEET_ID_RECANDIDATURAS", "").strip()
if not ID:
    raise RuntimeError("Variável SPREADSHEET_ID_RECANDIDATURAS não configurada.")
ABA = "Competitividade Senado (todas as candidaturas)"
# A janela anda com o dia: congelar a data faria o workflow diário parar de
# enxergar pesquisa nova sem dar erro. DATA_CORTE force a data só em teste.
CORTE = pd.Timestamp(os.getenv("DATA_CORTE") or pd.Timestamp.today().normalize())
JANELA = 90

def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).upper()
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s*\(.*?\)\s*$", "", s)
    s = re.sub(r"\b(DEL|DELEGADO|DR|DRA|CAP|CAPITAO|SGT|SARGENTO|PROF|PROFESSOR|"
               r"PROFESSORA|PASTOR|JR|JUNIOR|CORONEL|CEL|MISSIONARIO|DEPUTADO|SENADOR)\b", "", s)
    return " ".join(re.sub(r"[^A-Z0-9 ]", " ", s).split())

def sim(a, b):
    if not a or not b: return 0.0
    r = SequenceMatcher(None, a, b).ratio(); A, B = set(a.split()), set(b.split())
    if A & B: r = max(r, 0.6 + 0.35 * len(A & B) / max(len(A), len(B)))
    return r

d = pd.read_csv("casado.csv")
d["data_ult"] = pd.to_datetime(d.data_ult, errors="coerce")

# --- unidade da régua ----------------------------------------------------------
# Não é a margem declarada pelo instituto: é o erro absoluto médio das pesquisas
# de Senado contra o resultado, medido em 2010 e 2018 na mesma escala em que a
# régua roda. A mediana da margem declarada por UF chegou a ser calculada aqui e
# nunca foi usada, o que deixou o texto da planilha dizendo "margem de erro do
# estado" para um número nacional.
ME = regua.ME

# --- lado presidencial declarado ----------------------------------------------
# Só o que a base de apoios registra como declaração. Oposição a um lado não é
# adesão ao outro (Caiado, Zema e Ratinho também disputam), então não é inferida.
ap = pd.read_csv("apoios.csv")
ADESAO = {"apoio declarado", "aliança", "ato conjunto"}
LADO = {"LULA": "Lula", "LUIZ INACIO LULA DA SILVA": "Lula", "FLAVIO BOLSONARO": "Flávio"}
decl = {}
for _, r in ap.iterrows():
    tipo = str(r["tipo de relação"]).strip().lower()
    if tipo not in ADESAO or str(r.status).strip().lower() != "confirmado":
        continue
    a, b = norm(r.apoiador), norm(r.apoiado)
    if a in LADO and "senad" in str(r["cargo do apoiado"]).lower():
        pes, lado = b, LADO[a]                     # presidenciável apoia o candidato
    elif b in LADO:
        pes, lado = a, LADO[b]                     # candidato adere ao presidenciável
    else:
        continue
    decl.setdefault((str(r.estado).strip(), pes), set()).add((lado, str(r["link da fonte"])))

def lado_de(uf, nome):
    alvo = norm(nome); achados = set()
    for (est, pes), v in decl.items():
        if est == uf and sim(alvo, pes) >= 0.85: achados |= v
    lados = {l for l, _ in achados}
    if not lados: return "Não declarado", ""
    if len(lados) > 1: return "Registro contraditório", "; ".join(sorted({f for _, f in achados}))
    return lados.pop(), sorted({f for _, f in achados})[0]

d[["lado", "fonte_lado"]] = [lado_de(r.uf, r.candidato) for _, r in d.iterrows()]


# --- chapa presidencial pela coligação registrada no TSE -----------------------
# Fato de registro, não leitura política: a composição da coligação estadual e a
# da coligação nacional saem do DivulgaCand.
col = pd.read_csv("coligacoes.csv"); col["sq"] = col.sq_titular.astype(str)
pres = col[col.cargo == "PRESIDENTE"]
COL_LULA = set(pres[pres.candidato == "LULA"].partidos_coligacao.iloc[0].split(" / "))
COL_FLAVIO = set(pres[pres.candidato == "FLAVIO BOLSONARO"].partidos_coligacao.iloc[0].split(" / "))
proprio = {}
for _, r in pres.iterrows():
    for pt_ in str(r.partidos_coligacao).split(" / "): proprio.setdefault(pt_, r.candidato.title())
gov = col[col.cargo == "GOVERNADOR"].set_index(["uf", "nome_coligacao"]).candidato.to_dict()
sen = col[col.cargo == "SENADOR"].set_index("sq")

def chapa(sq):
    if sq not in sen.index: return "Sem registro de coligação", "", "", ""
    r = sen.loc[sq]
    P = set(str(r.partidos_coligacao).split(" / "))
    cab = gov.get((r.uf, r.nome_coligacao), "")
    coligacao = f"{r.nome_coligacao} ({r.partidos_coligacao})" if r.tipo_chapa != "PARTIDO ISOLADO" \
                else f"{r.nome_coligacao}, partido isolado"
    if "PT" in P and "PL" in P: lado, base = "Contraditório", "a coligação estadual tem PT e PL"
    elif "PT" in P: lado, base = "Lula", "o PT está na coligação estadual"
    elif "PL" in P: lado, base = "Flávio", "o PL está na coligação estadual"
    elif r.partido_candidato in COL_LULA:
        lado, base = "Lula (só nacional)", ("o partido está na coligação nacional de Lula, "
                                            "mas o PT não está na coligação do estado")
    elif r.partido_candidato in COL_FLAVIO: lado, base = "Flávio", "é do PL"
    elif r.partido_candidato in proprio:
        lado, base = "Outro presidenciável", f"o partido lançou {proprio[r.partido_candidato]} à Presidência"
    else: lado, base = "Sem vínculo", "o partido não está em nenhuma das duas coligações presidenciais"
    return lado, base, coligacao, cab

d["sq"] = d.sq.astype(str)
d[["chapa", "chapa_base", "coligacao", "cabeca"]] = [chapa(x) for x in d.sq]


# --- apoio presidencial declarado, listas nominais ----------------------------
# Duas fontes primárias: os 47 nomes que Flávio leu na transmissão de 05/08/2026
# (Poder360 07/08 e Gazeta do Povo 13/08 batem nome a nome) e o "mapa do time de
# Lula" publicado pelo PT em 24/08/2026, que inclui aliados de outros partidos.
from outros.indice_senado_listas import FLAVIO, LULA, FONTE_FLAVIO, FONTE_LULA, UNILATERAL

def casar_lista(lista, rotulo, fonte, destino, avisos):
    for uf, nome in lista:
        pool = d[d.uf == uf]
        notas = sorted(((sim(norm(nome), norm(x.candidato)), x) for _, x in pool.iterrows()),
                       key=lambda z: -z[0])
        sc, x = notas[0]
        segundo = notas[1][0] if len(notas) > 1 else 0
        if sc < 0.70 or sc - segundo < 0.05:
            avisos.append((uf, nome, round(sc, 2), round(segundo, 2))); continue
        chave = (x.uf, x.candidato)
        if chave in destino: avisos.append((uf, nome, "já classificado", destino[chave][0]))
        else: destino[chave] = (rotulo, fonte)

decl_nominal, avisos = {}, []
casar_lista(FLAVIO, "Flávio", FONTE_FLAVIO, decl_nominal, avisos)
casar_lista(LULA, "Lula", FONTE_LULA, decl_nominal, avisos)
if avisos: print("nomes das listas que não casaram com o registro:", avisos)

for (uf_, nome_), v in UNILATERAL.items():
    achou = [(sim(norm(nome_), norm(x.candidato)), x.candidato) for _, x in d[d.uf == uf_].iterrows()]
    sc_, cand_ = max(achou, default=(0, None))
    if sc_ >= 0.85: decl_nominal.setdefault((uf_, cand_), v)
    else: print(f"declaração individual não casou: {uf_} {nome_} ({sc_:.2f})")

def apoio(uf, cand, lado_base, fonte_base):
    x = decl_nominal.get((uf, cand))
    if x: return x[0], x[1]
    return lado_base, fonte_base

d[["lado", "fonte_lado"]] = [apoio(r.uf, r.candidato, r.lado, r.fonte_lado) for _, r in d.iterrows()]

# --- mandato legislativo atual -------------------------------------------------
radar = []
for arq, rot in [("Competitividade_Senado_Atual.csv", "Senador"),
                 ("Competitividade_Câmara_Atual.csv", "Deputado federal"),
                 ("Competitividade_Assembleias_Atual.csv", "Deputado estadual/distrital")]:
    try:
        x = pd.read_csv(arq); x["rot"] = rot; radar.append(x[["Parlamentar", "UF", "rot"]])
    except Exception:
        pass
if radar:
    radar = pd.concat(radar); radar["nb"] = radar.Parlamentar.map(norm)
else:
    radar = pd.DataFrame(columns=["Parlamentar", "UF", "rot", "nb"])

# Nome de urna que o casamento por semelhança não alcança até o nome parlamentar
# das abas Radar. São nove senadores em exercício que, sem esta lista, saíam da
# planilha como "Sem mandato legislativo hoje" nas duas abas de Senado. Baixar o
# limiar não resolve: em AL, "Renan" empata entre Renan Calheiros e Renan Filho,
# que também tem cadeira. A chave é (UF, nome de urna no TSE).
NOME_NO_RADAR = {
    ("PB", "VENEZIANO"): "Veneziano Vital do Rêgo",
    ("AL", "RENAN"): "Renan Calheiros",
    ("DF", "LEILA DO VOLEI"): "Leila Barros",
    ("AP", "RANDOLFE"): "Randolfe Rodrigues",
    ("MA", "WEVERTON ROCHA"): "Weverton",
    ("SE", "ALESSANDRO"): "Alessandro Vieira",
    ("MT", "FAVARO"): "Carlos Fávaro",
    ("AC", "PETECAO"): "Sérgio Petecão",
    ("MS", "SORAYA"): "Soraya Thronicke",
}

def mandato(uf, nome):
    alvo = norm(nome)
    g = radar[radar.UF == uf]
    manual = NOME_NO_RADAR.get((uf, alvo))
    if manual is not None:
        achado = g[g.nb == norm(manual)]
        if len(achado): return f"{achado.iloc[0].rot} hoje"
        print(f"nome do radar não encontrado: {uf} {manual}")
    for _, x in g.iterrows():
        if sim(alvo, x.nb) >= 0.88: return f"{x.rot} hoje"
    return "Sem mandato legislativo hoje"

d["mandato"] = [mandato(r.uf, r.candidato) for _, r in d.iterrows()]

# --- régua ---------------------------------------------------------------------
BANDA = regua.BANDA

def aplicar(g):
    """Banda, posição e distância de um estado.

    A conta roda na escala comparável (`mm_c`), com os percentuais do estado
    normalizados para somar 100 entre as candidaturas medidas. É a escala do
    histórico de 2010 e 2018, e é a única em que o erro de 5,3 pontos e as taxas
    por banda querem dizer alguma coisa. A média móvel bruta (`mm`) continua nas
    colunas de leitura, porque é o número que bate com a pesquisa publicada.
    """
    E = regua.EMPATE
    g = g.sort_values("mm", ascending=False, na_position="last").reset_index(drop=True)
    com = g.mm.dropna().tolist()
    escala = regua.normalizar(com)
    g["mm_c"] = pd.Series(escala + [np.nan] * (len(g) - len(escala)))
    p2 = escala[1] if len(escala) > 1 else 0.0
    p3 = escala[2] if len(escala) > 2 else 0.0
    p2b = com[1] if len(com) > 1 else 0.0
    p3b = com[2] if len(com) > 2 else 0.0
    notas, pos, ref, refc = [], [], [], []
    for i, p in enumerate(g.mm):
        if pd.isna(p):
            notas.append(np.nan); pos.append(np.nan); ref.append(np.nan); refc.append(np.nan)
            continue
        pos.append(i + 1)
        notas.append(regua.nota(i, escala[i], p2))
        # A distância publicada é em pontos da média móvel, que é o que se confere
        # contra a pesquisa; a da escala comparável fica no texto, que é onde a
        # banda pode ser refeita à mão.
        ref.append(round(p - (p3b if i <= 1 else p2b), 1))
        refc.append(round(escala[i] - (p3 if i <= 1 else p2), 1))
    g["nota"], g["posicao"], g["dist"], g["dist_c"] = notas, pos, ref, refc
    g["me_uf"], g["empate"], g["n_uf"] = round(ME, 1), round(E, 1), len(com)
    g["nome2"] = g.candidato.iloc[1] if len(com) > 1 else ""
    g["part2"] = g.partido.iloc[1] if len(com) > 1 else ""
    g["nome3"] = g.candidato.iloc[2] if len(com) > 2 else ""
    g["part3"] = g.partido.iloc[2] if len(com) > 2 else ""
    return g

f = pd.concat([aplicar(g) for _, g in d.groupby("uf")]).sort_values(
    ["uf", "mm"], ascending=[True, False], na_position="last")

# --- texto ---------------------------------------------------------------------
def br(x, c=1): return f"{x:.{c}f}".replace(".", ",")
# Célula vazia numa aba publicada lê como dado faltando. O traço diz que a
# coluna não se aplica àquela linha, que é o caso da coligação sem candidatura
# ao governo e de quem não tem declaração de apoio.
def ou_traco(x): return str(x).strip() or "—"
def brs(x, c=1): return f"{x:+.{c}f}".replace(".", ",")

linhas = []
for _, r in f.iterrows():
    if pd.isna(r.mm):
        texto = ("Sem banda: nenhuma pesquisa dos últimos 90 dias na matriz mede este nome. "
                 "O registro existe no TSE, o número não.")
        linhas.append([r.candidato, r.partido, r.uf, "—", "Sem pesquisa recente", "—",
                       "—", "Sem pesquisa", "—", "—", "—",
                       texto, r.chapa, r.chapa_base, r.coligacao, ou_traco(r.cabeca),
                       r.lado, ou_traco(r.fonte_lado), r.mandato, r.situacao_registro, "0", "—"])
        continue
    if r.posicao <= 2:
        alvo = f", {r.nome3} ({r.part3})" if r.nome3 else ""
        refr = f"{brs(r.dist)} em relação ao 3º colocado{alvo}"
    else:
        refr = f"{brs(r.dist)} em relação ao 2º colocado, {r.nome2} ({r.part2})"
    n = int(r.n_pesq)
    lastro = "1 pesquisa, banda provisória" if n <= 1 else f"{n} pesquisas"
    extra = f"\n• {r.nota_casamento}" if isinstance(r.nota_casamento, str) and r.nota_casamento else ""
    # 2026 tem duas vagas por estado: competitivo é quem está numa delas (banda 6
    # ou 7) ou empatado tecnicamente com quem está na segunda (banda 5).
    chance = regua.TAXA[int(r.nota)]
    classif = regua.classe(r.nota)
    texto = (f"Banda {int(r.nota)} de 7: {BANDA[int(r.nota)].lower()}.\n"
             f"• Média das pesquisas: {br(r.mm)}%, {int(r.posicao)}º entre os {int(r.n_uf)} "
             f"registros do estado que aparecem em pesquisa\n"
             f"• {refr}\n"
             f"• Escala comparável: {br(r.mm_c)}%, {brs(r.dist_c)} para a linha de corte. "
             f"É a média móvel do estado normalizada para somar 100 entre as candidaturas "
             f"medidas, que é a escala em que a régua roda\n"
             f"• Base: {lastro}. Última pesquisa de Senado do estado em {r.data_ult:%d/%m/%Y}\n"
             f"• Unidade da régua: {br(r.me_uf)} pontos, o erro médio das pesquisas de Senado "
             f"contra o resultado em 2010 e 2018. Empate técnico até {br(r.empate)} pontos, "
             f"que é a margem da diferença entre dois percentuais\n"
             f"• {chance} é a taxa de eleição observada nesta banda em 2010 e 2018, não uma "
             f"previsão sobre a pessoa{extra}")
    linhas.append([r.candidato, r.partido, r.uf, chance, classif, str(int(r.nota)),
                   BANDA[int(r.nota)], "Sim" if r.nota >= 5 else "Não",
                   f"{int(r.posicao)}º", br(r.mm) + "%", brs(r.dist), texto,
                   r.chapa, r.chapa_base, r.coligacao, ou_traco(r.cabeca),
                   r.lado, ou_traco(r.fonte_lado), r.mandato, r.situacao_registro, str(n),
                   f"{r.data_ult:%d/%m/%Y}"])

H = ["Candidato", "Partido", "UF", "Índice de competitividade eleitoral", "Alta, média ou baixa", "Nota da régua",
     "Cenário eleitoral (banda)", "É competitivo?", "Posição na disputa", "Média das pesquisas",
     "Distância para a linha de corte",
     "Como o índice foi calculado", "Chapa presidencial", "Base do vínculo de chapa",
     "Coligação no estado", "Cabeça de chapa no estado",
     "Apoio presidencial declarado", "Fonte do apoio declarado",
     "Mandato legislativo atual", "Situação do registro no TSE", "Pesquisas na conta",
     "Última pesquisa do estado"]

print(f"{len(linhas)} linhas, {len(H)} colunas")
print("\nbandas:")
print(f.nota.map(lambda n: BANDA.get(n, "Sem pesquisa recente")).value_counts()
      .reindex(list(BANDA.values()) + ["Sem pesquisa recente"]).to_string())
print("\ncompetitivos:", int((f.nota >= 5).sum()), "de", len(f),
      "| por UF, de", int(f[f.nota >= 5].groupby("uf").size().min()),
      "a", int(f[f.nota >= 5].groupby("uf").size().max()))
print("\nchapa presidencial (registro do TSE):")
print(f.chapa.value_counts().to_string())
print("\napoio presidencial declarado (base de apoios):")
print(f.lado.value_counts().to_string())
if "--apply" not in sys.argv:
    for l in linhas[:2]: print("\n", l[:8], "\n", l[8])
    sys.exit()

c = Credentials.from_service_account_file(os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"),
        scopes=["https://www.googleapis.com/auth/spreadsheets"])
gc = gspread.authorize(c); svc = build("sheets", "v4", credentials=c); sh = gc.open_by_key(ID)
try: sh.del_worksheet(sh.worksheet(ABA))
except Exception: pass
ws = sh.add_worksheet(title=ABA, rows=len(linhas) + 1, cols=len(H))
# RAW e não USER_ENTERED: com USER_ENTERED o Sheets lê "16,6%" como número e
# guarda 0,166, e a coluna, que é toda texto, passa a mostrar 0,166.
ws.update([H] + linhas, "A1", value_input_option="RAW")
# Cor, largura e altura saem do formato_recandidaturas, endereçadas por nome de
# coluna. Lista posicional foi o que deixou as larguras e as cores uma casa
# deslocadas quando entrou a coluna de porcentagem.
req = formato.pedidos_da_aba(ws, H, len(linhas) + 1)
req.append(formato.corpo(ws.id, 1, len(linhas) + 1, len(H)))
svc.spreadsheets().batchUpdate(spreadsheetId=ID, body={"requests": req}).execute()
print(f"\naba '{ABA}' regravada com {len(linhas)} linhas.")


# --- Atualização Parcial das Abas Radar (Senadores, Deputados Federais, Deputados Estaduais) ---
# Adiciona ou atualiza as colunas de Competitividade sem destruir as outras colunas preexistentes
def update_radar_tab(sh, aba_nome, linhas_dict, ausente="Não se aplica"):
    try:
        ws = sh.worksheet(aba_nome)
    except Exception:
        print(f"Aba {aba_nome} não encontrada para update parcial.")
        return
    
    todas_linhas = ws.get_all_values()
    if not todas_linhas: return
    headers = todas_linhas[0]
    
    # As mesmas colunas da aba do índice, da quarta em diante e na mesma ordem.
    colunas_injetar = H[3:]

    # Coluna que falta entra ao lado da anterior da lista, não no fim da aba, para
    # o bloco do índice não ficar picado.
    faltando = [c for c in colunas_injetar if c not in headers]
    for col in faltando:
        i = colunas_injetar.index(col)
        # A primeira da lista nao tem anterior: entra antes da proxima que a aba
        # ja tem, para o bloco do indice nao ficar picado. Sem esta guarda o
        # indice -1 dava a volta e devolvia a ultima coluna da lista, e o pos
        # caia depois do fim da grade: foi o que derrubou a rodada de 03/09 com
        # 'startIndex must be less than the grid size'.
        anterior = next((c for c in reversed(colunas_injetar[:i]) if c in headers), None)
        seguinte = next((c for c in colunas_injetar[i + 1:] if c in headers), None)
        if anterior is not None:
            pos = headers.index(anterior) + 2
        elif seguinte is not None:
            pos = headers.index(seguinte) + 1
        else:
            pos = len(headers) + 1
        # A API recusa insercao alem do fim da grade.
        pos = min(pos, ws.col_count + 1)
        ws.insert_cols([[col]], col=pos)
        headers.insert(pos - 1, col)
    if faltando:
        print(f"{aba_nome}: colunas criadas {faltando}")
        todas_linhas = ws.get_all_values()
        headers = todas_linhas[0]
        
    try:
        idx_parlamentar = headers.index("Parlamentar")
        idx_uf = headers.index("UF")
    except ValueError:
        print(f"Colunas Parlamentar ou UF não encontradas em {aba_nome}.")
        return

    # O nome parlamentar da aba Radar nem sempre é o nome de urna do registro.
    # Sem esta volta, os nove do NOME_NO_RADAR não achavam a própria linha e a
    # aba os publicava como se não tivessem pesquisa nenhuma.
    por_chave = dict(linhas_dict)
    for (uf_, urna), parlamentar in NOME_NO_RADAR.items():
        linha = linhas_dict.get((urna, uf_))
        if linha is not None:
            por_chave.setdefault((norm(parlamentar), uf_), linha)

    updates, achados = [], 0
    for row_idx, row_data in enumerate(todas_linhas[1:], start=2):
        row_data += [''] * (len(headers) - len(row_data))
        nome = norm(row_data[idx_parlamentar])
        uf = str(row_data[idx_uf]).strip()
        linha_completa = por_chave.get((nome, uf))
        achados += linha_completa is not None
        for i, col in enumerate(colunas_injetar):
            col_idx = headers.index(col)
            # Quem não tem linha no índice não disputa o Senado em 2026: a coluna
            # diz isso, em vez de ficar em branco.
            val = str(linha_completa[3 + i]) if linha_completa is not None else ausente
            if row_data[col_idx] != val:
                updates.append({
                    "range": gspread.utils.rowcol_to_a1(row_idx, col_idx + 1),
                    "values": [[val]]
                })

    if updates:
        # RAW e não USER_ENTERED: com USER_ENTERED o Sheets lê "+8,0" como número e
        # a coluna, que é toda texto, passa a mostrar 8 em umas linhas e -3,8 em outras.
        ws.batch_update(updates, value_input_option="RAW")
    print(f"{aba_nome}: {achados} linhas casadas, {len(updates)} células atualizadas.")

if "--apply" in sys.argv:
    # So a aba do Senado em exercicio: este indice e do Senado, e a aba de todas
    # as candidaturas ja foi regravada inteira acima. Camara e Assembleias tem
    # indice proprio, o de competitividade a reeleicao.
    update_radar_tab(sh, "Competitividade Senado (em exercício)", { (norm(r[0]), str(r[2]).strip()): r for r in linhas })

