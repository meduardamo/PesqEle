"""Casa registro do TSE com a linha da média móvel, com dois cuidados:
1) variantes de grafia do mesmo nome na matriz viram um só (a com mais pesquisas);
2) casamento fraco ou ambíguo não vira número: vai para revisão manual.
"""
import os
import re, unicodedata
import numpy as np, pandas as pd
from difflib import SequenceMatcher

# A janela anda com o dia: congelar a data faria o workflow diário parar de
# enxergar pesquisa nova sem dar erro. DATA_CORTE force a data só em teste.
CORTE = pd.Timestamp(os.getenv("DATA_CORTE") or pd.Timestamp.today().normalize())
JANELA = 90
ALIAS = {"LEILA BARROS": "LEILA DO VOLEI"}
FORTE, FRACO = 0.85, 0.75   # >=FORTE casa direto; entre FRACO e FORTE exige desempate

def norm(s):
    s = unicodedata.normalize("NFKD", str(s)).upper()
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = re.sub(r"\s*\(.*?\)\s*$", "", s)
    s = re.sub(r"\b(DEL|DELEGADO|DR|DRA|CAP|CAPITAO|SGT|SARGENTO|PROF|PROFESSOR|"
               r"PROFESSORA|PASTOR|JR|JUNIOR|CORONEL|CEL|MISSIONARIO|DEPUTADO|SENADOR)\b", "", s)
    return " ".join(re.sub(r"[^A-Z0-9 ]", " ", s).split())

def sim(a, b):
    if not a or not b: return 0.0
    r = SequenceMatcher(None, a, b).ratio()
    A, B = set(a.split()), set(b.split())
    if A & B: r = max(r, 0.6 + 0.35 * len(A & B) / max(len(A), len(B)))
    return r

def part(s):
    m = re.search(r"\(([^)]*)\)\s*$", str(s))
    return norm(m.group(1)) if m else ""

# a matriz abrevia o que o TSE escreve por extenso
PART_ALIAS = {"REP": "REPUBLICANOS", "CID": "CIDADANIA", "DEM": "DEMOCRATA",
              "MOB": "MOBILIZA", "SD": "SOLIDARIEDADE", "MISSAO": "MISSAO"}
def pnorm(p):
    p = norm(p); return PART_ALIAS.get(p, p)

# casos que o nome sozinho não resolve, conferidos um a um no nome completo do TSE
MANUAL = {
    ("SP", "GUILHERME DERRITE"): "Capitão Derrite (PL)",      # GUILHERME MURARO DERRITE
    ("AL", "MARINA JHC"): "Marina Cândia (PL)",               # MARINA ANTUNES CANDIA E FIGUEIREDO
    ("CE", "LUIZIANNE"): "Luizianne Lins (PT)",               # LUIZIANNE DE OLIVEIRA LINS
    ("AL", "ALEXANDRE FLEMING"): "Fleming (SEM PARTIDO)",     # ALEXANDRE FLEMING VASQUES BASTOS
    ("RN", "CLÓVIS COSTA DO COLETIVO NÓS"): "Clóvis Costa (SEM PARTIDO)",
    ("TO", "HELIO RODRIGUES BOLSONARO"): "Hélio Bolsonaro (SEM PARTIDO)",
}

ch = pd.read_csv("chapas.csv")
tse = ch[ch.cargo == "SENADOR"].drop_duplicates(subset=["uf", "titular", "titular_partido"])
tse = tse[tse.situacao != "Renúncia"].copy()

bi = pd.read_csv("bi.csv")
s = bi[(bi.cargo == "senador") & (bi.turno == "t1") & (bi.tipo == "candidato")].copy()
s["d"] = pd.to_datetime(s.data_campo); s["mm"] = pd.to_numeric(s.media_hibrida_30d, errors="coerce")
s = s.dropna(subset=["mm"])
s = s.merge(s.groupby("uf").d.max().rename("u"), on="uf"); s = s[s.d == s.u].copy()
s["nb"] = s.candidato_partido.map(norm); s["pb"] = s.candidato_partido.map(part).map(pnorm)

res = pd.read_csv("resultados.csv")
res = res[(res.cargo == "senador") & (res.turno == "t1")].copy()
res["d"] = pd.to_datetime(res.data_campo, errors="coerce")
res = res[(CORTE - res.d).dt.days.between(0, JANELA)]
res["nb"] = res.candidato_partido.map(norm)
npesq = res.groupby(["uf", "nb"]).poll_id.nunique()

out, revisar = [], []
for _, t in tse.iterrows():
    a = ALIAS.get(norm(t.titular), norm(t.titular)); b = norm(t.titular_nome_completo)
    pt = pnorm(t.titular_partido)
    pool = s[s.uf == t.uf]
    manual = MANUAL.get((t.uf, t.titular))
    if manual is not None:
        x = pool[pool.candidato_partido == manual].iloc[0]
        out.append(dict(uf=t.uf, candidato=str(t.titular).title(), partido=t.titular_partido,
                        sq=str(t.sq_titular), situacao_registro=t.situacao, mm=round(float(x.mm), 1),
                        nome_na_pesquisa=x.candidato_partido,
                        n_pesq=int(npesq.get((t.uf, x.nb), 0)), data_ult=x.d,
                        score=1.0, nota_casamento="conferido no nome completo do TSE"))
        continue
    cand = sorted(((max(sim(a, x.nb), sim(b, x.nb)), x) for _, x in pool.iterrows()),
                  key=lambda z: -z[0])
    achados = [(sc, x) for sc, x in cand if sc >= FRACO]
    fortes = [(sc, x) for sc, x in achados if sc >= FORTE]
    nota = ""
    if fortes:
        # variantes de grafia do mesmo nome: fica a que tem mais pesquisas atrás
        escolhido = max(fortes, key=lambda z: (npesq.get((t.uf, z[1].nb), 0), z[1].mm))
        if len(fortes) > 1:
            nota = "variantes na matriz: " + "; ".join(
                f"{x.candidato_partido} {x.mm:.1f}" for _, x in fortes)
        sc, x = escolhido
    elif achados:
        # só casamento fraco: aceita se o partido bate e nenhum outro fraco bate também
        compat = [(sc, x) for sc, x in achados if x.pb == pt]
        if len(compat) == 1:
            sc, x = compat[0]; nota = f"casado por partido (similaridade {sc:.2f})"
        else:
            revisar.append((t.uf, t.titular, t.titular_partido,
                            "; ".join(f"{x.candidato_partido} {x.mm:.1f} [{sc:.2f}]" for sc, x in achados)))
            sc, x = 0, None
    else:
        sc, x = 0, None
    out.append(dict(uf=t.uf, candidato=str(t.titular).title(), partido=t.titular_partido,
                    sq=str(t.sq_titular), situacao_registro=t.situacao,
                    mm=(round(float(x.mm), 1) if x is not None else np.nan),
                    nome_na_pesquisa=(x.candidato_partido if x is not None else ""),
                    n_pesq=(int(npesq.get((t.uf, x.nb), 0)) if x is not None else 0),
                    data_ult=(x.d if x is not None else pd.NaT),
                    score=round(sc, 2), nota_casamento=nota))
d = pd.DataFrame(out)
d.to_csv("casado.csv", index=False)
print(f"universo TSE (sem renúncia): {len(d)}")
print(f"com número: {d.mm.notna().sum()} | sem número: {d.mm.isna().sum()}")
print(f"  casados por variante consolidada: {(d.nota_casamento.str.startswith('variantes')).sum()}")
print(f"  casados por partido (similaridade fraca): {(d.nota_casamento.str.startswith('casado por partido')).sum()}")
print(f"\nAMBÍGUOS, foram para revisão manual ({len(revisar)}):")
for r in revisar: print(f"  {r[0]}  {r[1]:<30} {r[2]:<14} -> {r[3]}")
print(f"\nSEM NENHUM CASAMENTO ({(d.mm.isna() & (d.score==0)).sum() - len(revisar)}):")
print(d[d.mm.isna()][~d[d.mm.isna()].apply(lambda r: any(r.uf==x[0] and r.candidato.upper()==str(x[1]).upper() for x in revisar), axis=1)][["uf","candidato","partido"]].to_string(index=False))
