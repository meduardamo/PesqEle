"""Publica o Índice de competitividade eleitoral ao Senado 2026.

Roda em quatro passos, na ordem, todos do diretório de trabalho:
    python -m outros.indice_senado_dados     baixa as bases e as coligações
    python -m outros.indice_senado_casar     casa registro do TSE com a matriz
    python -m outros.indice_senado           confere; com --apply, publica a aba
    python -m outros.indice_senado_notas     grava as notas de cabeçalho


Universo: todo titular com registro de Senado no TSE, menos renúncias (313).
Número: média móvel híbrida de 30 dias, na data mais recente de cada UF.
Régua: sete bandas em cima da distância para a linha de corte. 2026 tem duas
vagas por estado, então a linha de corte é o 2º colocado; para quem já está em
1º ou 2º, a referência passa a ser o 3º.
"""
import os
import math, re, sys, unicodedata
import numpy as np, pandas as pd, gspread
from difflib import SequenceMatcher
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build

# O repositório é público: id de planilha vem de variável de ambiente.
ID = os.getenv("SPREADSHEET_ID_RECANDIDATURAS", "").strip()
if not ID:
    raise RuntimeError("Variável SPREADSHEET_ID_RECANDIDATURAS não configurada.")
ABA = "Competitividade Senado 2026 (teste)"
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

# --- margem de erro por UF -----------------------------------------------------
pe = pd.read_csv("pesquisas.csv")
pe["dt"] = pd.to_datetime(pe.data_campo, errors="coerce")
pe["me"] = pd.to_numeric(pe.margem_erro, errors="coerce")
ps = pe[pe.cargo.astype(str).str.contains("senad", case=False, na=False)
        & (CORTE - pe.dt).dt.days.between(0, JANELA)]
me_uf = ps.groupby("uf").me.median()
# MAE histórico do Senado calibrado a partir de 2010 e 2018 (média dos 2 anos: ~5.3%)
ME = 5.3

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

def mandato(uf, nome):
    alvo = norm(nome)
    g = radar[radar.UF == uf]
    for _, x in g.iterrows():
        if sim(alvo, x.nb) >= 0.88: return f"{x.rot} hoje"
    return "Sem mandato legislativo hoje"

d["mandato"] = [mandato(r.uf, r.candidato) for _, r in d.iterrows()]

# --- régua ---------------------------------------------------------------------
BANDA = {7: "Lidera isoladamente", 6: "Entre os 2 primeiros", 5: "Empatado tecnicamente com o 2º",
         4: "Até 1 margem de erro atrás", 3: "Entre 1 e 2 margens de erro atrás",
         2: "Entre 2 e 3 margens de erro atrás", 1: "Mais de 3 margens de erro atrás"}

def aplicar(g):
    # Usa o erro histórico calibrado para 2 vagas (5.3%) em vez da margem declarada pelo instituto
    me = ME
    E = me * math.sqrt(2)   # empate é a margem da diferença entre dois percentuais
    g = g.sort_values("mm", ascending=False, na_position="last").reset_index(drop=True)
    com = g.mm.dropna().tolist()
    p2 = com[1] if len(com) > 1 else 0.0
    p3 = com[2] if len(com) > 2 else 0.0
    notas, pos, ref = [], [], []
    for i, p in enumerate(g.mm):
        if pd.isna(p): notas.append(np.nan); pos.append(np.nan); ref.append(np.nan); continue
        pos.append(i + 1)
        if i == 0:   notas.append(7 if (p - p2) > E else 6)
        elif i == 1: notas.append(6)
        else:
            dd = p2 - p
            notas.append(5 if dd <= E else 4 if dd <= E + me else 3 if dd <= E + 2*me
                         else 2 if dd <= E + 3*me else 1)
        ref.append(round(p - (p3 if i <= 1 else p2), 1))
    g["nota"], g["posicao"], g["dist"] = notas, pos, ref
    g["me_uf"], g["empate"], g["n_uf"] = round(me, 1), round(E, 1), len(com)
    g["nome2"] = g.candidato.iloc[1] if len(com) > 1 else ""
    g["part2"] = g.partido.iloc[1] if len(com) > 1 else ""
    g["nome3"] = g.candidato.iloc[2] if len(com) > 2 else ""
    g["part3"] = g.partido.iloc[2] if len(com) > 2 else ""
    return g

f = pd.concat([aplicar(g) for _, g in d.groupby("uf")]).sort_values(
    ["uf", "mm"], ascending=[True, False], na_position="last")

# --- texto ---------------------------------------------------------------------
def br(x, c=1): return f"{x:.{c}f}".replace(".", ",")
def brs(x, c=1): return f"{x:+.{c}f}".replace(".", ",")

linhas = []
for _, r in f.iterrows():
    if pd.isna(r.mm):
        texto = ("Sem banda: nenhuma pesquisa dos últimos 90 dias na matriz mede este nome. "
                 "O registro existe no TSE, o número não.")
        linhas.append([r.candidato, r.partido, r.uf, "Sem pesquisa recente", "—",
                       "Sem pesquisa", "—", "—", "—",
                       texto, r.chapa, r.chapa_base, r.coligacao, r.cabeca,
                       r.lado, r.fonte_lado, r.mandato, r.situacao_registro, "0", "—"])
        continue
    if r.posicao <= 2:
        alvo = f", {r.nome3} ({r.part3})" if r.nome3 else ""
        refr = f"{brs(r.dist)} em relação ao 3º colocado{alvo}"
    else:
        refr = f"{brs(r.dist)} em relação ao 2º colocado, {r.nome2} ({r.part2})"
    n = int(r.n_pesq)
    lastro = "1 pesquisa, banda provisória" if n <= 1 else f"{n} pesquisas"
    extra = f"\n• {r.nota_casamento}" if isinstance(r.nota_casamento, str) and r.nota_casamento else ""
    texto = (f"Banda {int(r.nota)} de 7: {BANDA[int(r.nota)].lower()}.\n"
             f"• Média das pesquisas: {br(r.mm)}%, {int(r.posicao)}º entre os {int(r.n_uf)} "
             f"registros do estado que aparecem em pesquisa\n"
             f"• {refr}\n"
             f"• Base: {lastro}. Última pesquisa de Senado do estado em {r.data_ult:%d/%m/%Y}\n"
             f"• Margem de erro do estado: {br(r.me_uf)} pontos. Empate técnico até {br(r.empate)} "
             f"pontos, que é a margem da diferença entre dois percentuais{extra}")
    # 2026 tem duas vagas por estado: competitivo é quem está numa delas (banda 6
    # ou 7) ou empatado tecnicamente com quem está na segunda (banda 5).
    linhas.append([r.candidato, r.partido, r.uf, BANDA[int(r.nota)], str(int(r.nota)),
                   "Sim" if r.nota >= 5 else "Não",
                   f"{int(r.posicao)}º", br(r.mm) + "%", brs(r.dist), texto,
                   r.chapa, r.chapa_base, r.coligacao, r.cabeca,
                   r.lado, r.fonte_lado, r.mandato, r.situacao_registro, str(n),
                   f"{r.data_ult:%d/%m/%Y}"])

H = ["Candidato", "Partido", "UF", "Índice de competitividade eleitoral", "Nota da régua",
     "É competitivo?", "Posição na disputa", "Média das pesquisas",
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
sid = ws.id
CINZA = {"red": .882, "green": .882, "blue": .882}
MAR = {"red": .098, "green": .176, "blue": .306}
VIN = {"red": .588, "green": .18, "blue": .302}
tons = [CINZA, CINZA, CINZA, VIN, VIN, VIN, MAR, MAR, MAR, MAR,
        VIN, MAR, MAR, MAR, VIN, MAR, MAR, MAR, MAR, MAR]
larg = [195, 105, 45, 250, 95, 105, 110, 120, 150, 480,
        150, 290, 330, 180, 165, 200, 200, 175, 110, 130]
req = [{"updateSheetProperties": {"properties": {"sheetId": sid, "gridProperties":
        {"frozenRowCount": 1, "frozenColumnCount": 1}},
        "fields": "gridProperties.frozenRowCount,gridProperties.frozenColumnCount"}}]
for i, (t, w) in enumerate(zip(tons, larg)):
    escuro = t is not CINZA
    req.append({"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 0, "endRowIndex": 1,
        "startColumnIndex": i, "endColumnIndex": i + 1},
        "cell": {"userEnteredFormat": {"backgroundColor": t, "horizontalAlignment": "LEFT",
            "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP",
            "textFormat": {"bold": True, "fontSize": 10, "fontFamily": "Montserrat",
                "foregroundColor": {"red": 1, "green": 1, "blue": 1} if escuro
                else {"red": .07, "green": .07, "blue": .07}}}}, "fields": "userEnteredFormat"}})
    req.append({"updateDimensionProperties": {"range": {"sheetId": sid, "dimension": "COLUMNS",
        "startIndex": i, "endIndex": i + 1}, "properties": {"pixelSize": w}, "fields": "pixelSize"}})
B = {"style": "SOLID", "width": 1, "color": {"red": .855, "green": .855, "blue": .831}}
req.append({"repeatCell": {"range": {"sheetId": sid, "startRowIndex": 1,
    "endRowIndex": len(linhas) + 1, "startColumnIndex": 0, "endColumnIndex": len(H)},
    "cell": {"userEnteredFormat": {"numberFormat": {"type": "TEXT", "pattern": "@"},
        "backgroundColor": {"red": .9569, "green": .9529, "blue": .9373},
        "borders": {"top": B, "bottom": B, "left": B, "right": B},
        "horizontalAlignment": "LEFT", "verticalAlignment": "MIDDLE", "wrapStrategy": "WRAP",
        "textFormat": {"bold": False, "fontSize": 9, "fontFamily": "Montserrat",
            "foregroundColor": {"red": .0667, "green": .0667, "blue": .0667}}}},
    "fields": "userEnteredFormat"}})
svc.spreadsheets().batchUpdate(spreadsheetId=ID, body={"requests": req}).execute()
print(f"\naba '{ABA}' regravada com {len(linhas)} linhas.")


# --- Atualização Parcial das Abas Radar (Senadores, Deputados Federais, Deputados Estaduais) ---
# Adiciona ou atualiza as colunas de Competitividade sem destruir as outras colunas preexistentes
def update_radar_tab(sh, aba_nome, df_comp):
    try:
        ws = sh.worksheet(aba_nome)
    except Exception:
        print(f"Aba {aba_nome} não encontrada para update parcial.")
        return
    
    todas_linhas = ws.get_all_values()
    if not todas_linhas: return
    headers = todas_linhas[0]
    
    colunas_injetar = ["Índice de competitividade eleitoral", "Alta, média ou baixa", "Nota da régua", "Cenário eleitoral (banda)", "Última pesquisa", "Distância p/ o corte"]
    
    # Garante que os headers existem
    headers_modificados = False
    for col in colunas_injetar:
        if col not in headers:
            headers.append(col)
            headers_modificados = True
    
    if headers_modificados:
        ws.update([headers], "A1", value_input_option="USER_ENTERED")
        
    try:
        idx_parlamentar = headers.index("Parlamentar")
        idx_uf = headers.index("UF")
    except ValueError:
        print(f"Colunas Parlamentar ou UF não encontradas em {aba_nome}.")
        return

    updates = []
    # Usando o DataFrame 'f' (ou 'df_comp') que tem as colunas finais calculadas
    for row_idx, row_data in enumerate(todas_linhas[1:], start=2):
        row_data += [''] * (len(headers) - len(row_data)) # pad
        
        nome = norm(row_data[idx_parlamentar])
        uf = str(row_data[idx_uf]).strip()
        
        match = df_comp[(df_comp["uf"] == uf) & (df_comp["candidato"].apply(norm) == nome)]
        if not match.empty:
            r = match.iloc[0]
            
            # Formatação
            if pd.isna(r["mm"]):
                valores = {
                    "Índice de competitividade eleitoral": "—",
                    "Alta, média ou baixa": "Sem pesquisa recente",
                    "Nota da régua": "—",
                    "Cenário eleitoral (banda)": "—",
                    "Última pesquisa": "—",
                    "Distância p/ o corte": "—"
                }
            else:
                valores = {
                    "Índice de competitividade eleitoral": {7: "100%", 6: "72%", 5: "53%", 4: "44%", 3: "14%", 2: "10%", 1: "4%"}[int(r["nota"])],
                    "Alta, média ou baixa": "Alta" if r["nota"] >= 6 else "Média" if r["nota"] >= 4 else "Baixa",
                    "Nota da régua": str(int(r["nota"])),
                    "Cenário eleitoral (banda)": BANDA[int(r["nota"])],
                    "Última pesquisa": f"{r['data_ult']:%d/%m/%Y}",
                    "Distância p/ o corte": brs(r["dist"])
                }
                
            for col in colunas_injetar:
                col_idx = headers.index(col)
                val = valores[col]
                if row_data[col_idx] != val: # só atualiza se mudou
                    updates.append({
                        "range": gspread.utils.rowcol_to_a1(row_idx, col_idx + 1),
                        "values": [[val]]
                    })

    if updates:
        ws.batch_update(updates, value_input_option="USER_ENTERED")
        print(f"{aba_nome}: {len(updates)} células atualizadas.")
    else:
        print(f"{aba_nome}: nenhuma célula precisou ser atualizada.")

if "--apply" in sys.argv:
    # Update das 3 abas
    update_radar_tab(sh, "Competitividade Senado Atual", f)

