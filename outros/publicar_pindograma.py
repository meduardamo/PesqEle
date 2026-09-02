"""Publica a base do Ranking de Institutos do Pindograma numa aba de planilha.

Existe porque a nota que o painel aplica é uma letra, e a letra esconde
diferença grande dentro da mesma faixa: Quaest e Veritá são os dois B, com erro
médio 3,44 e 4,85. Quem precisa conferir uma nota tem que conseguir ver o número
que está por trás dela sem abrir código.

A fonte é o arquivo que o próprio Pindograma serve para montar o ranking no
site. A aba é reescrita inteira a cada execução: editar na mão não muda nada.

Uso:
    python -m outros.publicar_pindograma <spreadsheet_id> [nome_da_aba]
"""

from __future__ import annotations

import json
import sys
import urllib.request

import pandas as pd

from compartilhado.pollingdata_scraper import (
    CLASSIFICACAO_INSTITUTOS,
    CLASSIFICACAO_SEM_FONTE,
    OVERRIDES_CLASSIFICACAO,
    classificar_instituto,
    garantir_aba,
    gs_client_from_env,
    normalizar_instituto,
    score_instituto,
    sobrescrever_aba,
)

# O mesmo arquivo que https://pindograma.com.br/ranking.html carrega. O
# raw.githubusercontent é o espelho do repositório que publica o site; se um
# cair, o outro serve o mesmo conteúdo.
FONTES = (
    "https://raw.githubusercontent.com/pindograma/pindograma.github.io/"
    "master/assets/ranking_data_4.js",
    "https://pindograma.com.br/assets/ranking_data_4.js",
)

ABA_PADRAO = "Pindograma"
ORDEM_NOTAS = ["A", "B+", "B", "B-", "C", "D"]

# Campos que a metodologia publicada não nomeia. Vão para a aba com o nome
# original, de propósito: traduzir para "viés" ou "desempenho" seria afirmar um
# significado que a fonte não dá.
CAMPOS_CRUS = ("spm", "wpm", "wpm_adj", "rev_mean", "pred_pm", "pred_pm_adj", "quant_bin")


def baixar_base() -> list[dict]:
    """Lê o ranking_data_4.js e devolve a lista de institutos.

    O arquivo é JavaScript, não JSON: começa com uma atribuição. O corpo depois
    do `=` é JSON válido.
    """
    erros = []
    for url in FONTES:
        try:
            with urllib.request.urlopen(url, timeout=60) as r:
                bruto = r.read().decode("utf-8")
            return json.loads(bruto.split("=", 1)[1].strip().rstrip(";"))
        except Exception as erro:  # noqa: BLE001
            erros.append(f"{url}: {erro}")
    raise RuntimeError("não foi possível baixar a base do Pindograma. " + " | ".join(erros))


def montar_df(dados: list[dict]) -> pd.DataFrame:
    """Uma linha por instituto, com a nota da fonte e a que o painel aplica."""
    sim = lambda v: "Sim" if v else "Não"  # noqa: E731

    # Nome repetido na base é nome que não identifica a empresa. Acontece com
    # "Sensor Pesquisas", que aparece com dois CNPJs e duas notas.
    repetidos = {d["pretty_name"] for d in dados
                 if sum(1 for x in dados if x["pretty_name"] == d["pretty_name"]) > 1}

    linhas = []
    for d in dados:
        nome = d["pretty_name"]
        canonico = normalizar_instituto(nome)
        aplicada = classificar_instituto(nome)

        if nome in repetidos:
            situacao = "Fora, nome repetido na base"
        elif canonico in OVERRIDES_CLASSIFICACAO:
            situacao = "Rebaixado pela Eixo"
        elif canonico in CLASSIFICACAO_SEM_FONTE and canonico not in CLASSIFICACAO_INSTITUTOS:
            situacao = "Nota sem fonte confirmada"
        elif aplicada == "Ainda não foi avaliado":
            situacao = "Fora do painel"
        else:
            situacao = "No painel"

        linha = {
            "Instituto": nome,
            "Classificação": d["grade"],
            "Pesquisas Avaliadas": d["n"],
            "Erro Médio": d["avg"],
            "Nacional": sim(d["is_national"]),
            "ABEP": sim(d["has_abep"]),
            "CONRE": sim(d["has_conre"]),
            "Situação no painel": situacao,
            "Nota aplicada no painel": "" if aplicada == "Ainda não foi avaliado" else aplicada,
            "Peso aplicado": score_instituto(aplicada),
            "company_id": d["company_id"],
        }
        linha.update({c: d[c] for c in CAMPOS_CRUS})
        linhas.append(linha)

    df = pd.DataFrame(linhas)
    df["_ordem"] = df["Classificação"].apply(
        lambda g: ORDEM_NOTAS.index(g) if g in ORDEM_NOTAS else len(ORDEM_NOTAS))
    df = (df.sort_values(["_ordem", "Erro Médio"], kind="stable")
            .drop(columns="_ordem")
            .reset_index(drop=True))
    df.insert(0, "#", range(1, len(df) + 1))
    return df


def publicar(spreadsheet_id: str, nome_aba: str = ABA_PADRAO) -> None:
    df = montar_df(baixar_base())
    gc = gs_client_from_env()
    sh = gc.open_by_key(spreadsheet_id)
    aba = garantir_aba(sh, nome_aba, rows=len(df) + 20, cols=len(df.columns))
    sobrescrever_aba(aba, df)

    no_painel = int((df["Situação no painel"] == "No painel").sum())
    print(f"[+] aba '{nome_aba}': {len(df)} institutos, {no_painel} com nota aplicada no painel")


def main() -> None:
    if len(sys.argv) < 2:
        raise SystemExit(__doc__)
    publicar(sys.argv[1], sys.argv[2] if len(sys.argv) > 2 else ABA_PADRAO)


if __name__ == "__main__":
    main()
