"""Mede a taxa de eleição de cada banda da régua do Senado em 2010 e 2018.

Roda da raiz do repositório:  python -m research.calibrar_senado

Substitui o `calcular_probabilidade_senado.py`, que tinha dois defeitos: cortava
as bandas numa régua própria, diferente da que o `outros/indice_senado.py`
publica, e ordenava os candidatos por (ano, UF), empilhando institutos
diferentes numa lista só. Aqui a banda vem de `outros/regua_senado.py`, a mesma
que publica, e o ranking é dentro de cada rodada.

Escala: percentual publicado normalizado para somar 100 dentro da rodada, entre
as candidaturas medidas. É a escala em que o erro histórico de 5,3 pontos foi
medido e a mesma que o índice de 2026 usa.

Desfecho: eleito é quem terminou entre os 2 primeiros em voto bruto, porque 2010
e 2018 renovaram dois terços do Senado, com duas vagas por estado, como 2026.
"""
import re
import sys
from pathlib import Path

import pandas as pd

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from outros import regua_senado as regua  # noqa: E402

BASE = Path(__file__).parent / "dados_senado_historico"
FONTE = BASE / "pesquisas_com_resultados.csv"
SAIDA = BASE / "taxas_por_banda.csv"


def mes_de_campo(periodo):
    """Mês em que o campo terminou, para separar rodada de agosto de rodada de outubro."""
    achados = re.findall(r"(\d{1,2})/(\d{1,2})", str(periodo))
    return int(achados[-1][1]) if achados else None


def carregar():
    d = pd.read_csv(FONTE)
    d = d[d.ranking_voto_bruto.notna() & d.percentual.notna()].copy()
    d["mes"] = d.periodo_campo.map(mes_de_campo)
    d["rodada"] = (d.ano.astype(str) + "|" + d.uf + "|" + d.instituto.astype(str)
                   + "|" + d.periodo_campo.astype(str))
    bandas = {}
    for _, g in d.groupby("rodada"):
        for i, nota in zip(g.index, regua.notas_da_rodada(g.percentual.tolist())):
            bandas[i] = nota
    d["nota"] = pd.Series(bandas)
    d["eleito"] = d.ranking_voto_bruto <= 2
    return d


def taxas(d):
    t = d.groupby("nota").agg(casos=("eleito", "size"), eleitos=("eleito", "sum"))
    t["taxa"] = (t.eleitos / t.casos * 100).round(0).astype(int)
    return t.sort_index(ascending=False)


def main():
    d = carregar()
    print(f"{d.rodada.nunique()} rodadas, {len(d)} observações, "
          f"{d.groupby(['ano', 'uf']).ngroups} disputas estaduais\n")
    geral = taxas(d)
    print("taxa de eleição por banda:")
    print(geral.to_string())

    print("\nmesma conta, ciclo a ciclo (a ordem das bandas tem que se manter):")
    ciclos = pd.concat({ano: taxas(g).taxa for ano, g in d.groupby("ano")}, axis=1)
    ciclos["casos_2010"] = taxas(d[d.ano == 2010]).casos
    ciclos["casos_2018"] = taxas(d[d.ano == 2018]).casos
    print(ciclos.to_string())

    print("\njanela de campo (2026 publica em setembro, com pesquisa de agosto):")
    for rotulo, sub in [("campo em agosto", d[d.mes == 8]),
                        ("campo em setembro ou outubro", d[d.mes.isin([9, 10])]),
                        ("sem data de campo na fonte", d[d.mes.isna()])]:
        print(f"  {rotulo}: {sub.rodada.nunique()} rodadas, {len(sub)} obs")
    agosto = taxas(d[d.mes == 8])
    print("\n  agosto, banda a banda (28 rodadas, fino demais para calibrar):")
    print("  " + agosto.to_string().replace("\n", "\n  "))

    geral.to_csv(SAIDA)
    print(f"\ngravado em {SAIDA}")
    print("publicado em outros/regua_senado.py:", regua.TAXA)


if __name__ == "__main__":
    main()
