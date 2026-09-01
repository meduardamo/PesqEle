"""Coleta resultados estaduais do Senado em 2010 e 2018 na Wikipédia.

As tabelas estaduais citam a Justiça Eleitoral/TSE e são usadas como camada
estruturada enquanto o CDN do TSE bloqueia o download automatizado dos ZIPs.
O coletor preserva a URL e remove tabelas duplicadas da versão responsiva.

Uso:
    python research/coletar_resultados_senado_wikipedia.py
"""

from __future__ import annotations

import argparse
import re
import time
from io import StringIO
from pathlib import Path
from urllib.parse import quote

import pandas as pd
import requests
from lxml import html
from lxml.html import HtmlElement

from coletar_pesquisas_senado_historicas import (
    BASE,
    UFS,
    USER_AGENT,
    limpar,
    texto_elemento,
    titulo_pagina,
)


NAO_CANDIDATOS = (
    "branco",
    "nulo",
    "absten",
    "comparecimento",
    "total",
    "votos válidos",
    "votos nominais",
)


def cabecalho_imediato(tabela: HtmlElement) -> str:
    cabecalhos = tabela.xpath("preceding::*[self::h2 or self::h3 or self::h4]")
    return texto_elemento(cabecalhos[-1]) if cabecalhos else ""


def contexto_cabecalhos(tabela: HtmlElement) -> str:
    cabecalhos = tabela.xpath("preceding::*[self::h2 or self::h3 or self::h4]")
    return " | ".join(texto_elemento(x) for x in cabecalhos[-3:])


def normalizar_coluna(coluna: object) -> str:
    if isinstance(coluna, tuple):
        partes = []
        for parte in coluna:
            texto = limpar(parte)
            if texto and not texto.casefold().startswith("unnamed") and texto not in partes:
                partes.append(texto)
        return " | ".join(partes)
    return limpar(coluna)


def votos_inteiros(valor: object) -> int | None:
    texto = limpar(valor)
    if not texto:
        return None
    digitos = re.sub(r"\D", "", texto)
    return int(digitos) if digitos else None


def percentual_numero(valor: object) -> float | None:
    texto = limpar(valor).replace(",", ".")
    achou = re.search(r"\d+(?:\.\d+)?", texto)
    return float(achou.group()) if achou else None


def eh_tabela_resultado_senado(tabela: HtmlElement, quadro: pd.DataFrame) -> bool:
    imediato = cabecalho_imediato(tabela).casefold()
    secao = contexto_cabecalhos(tabela).casefold()
    colunas = " | ".join(normalizar_coluna(c).casefold() for c in quadro.columns)
    tem_candidato = "candidat" in colunas or ("cargo" in colunas and "nome" in colunas)
    tem_voto = (
        "votos" in colunas
        or "votação" in colunas
        or ("total" in colunas and ("percent" in colunas or "porcent" in colunas))
    )
    outro_cargo = any(cargo in imediato for cargo in ("governador", "deputad", "presiden"))
    referencia_senado = "senad" in secao or "senad" in colunas
    return referencia_senado and tem_candidato and tem_voto and not outro_cargo


def indice_coluna(colunas: list[str], termos: tuple[str, ...]) -> int | None:
    return next(
        (i for i, coluna in enumerate(colunas) if any(t in coluna.casefold() for t in termos)),
        None,
    )


def indice_candidato(quadro: pd.DataFrame, colunas: list[str]) -> int | None:
    candidatas = [
        i for i, coluna in enumerate(colunas) 
        if "candidat" in coluna.casefold() and "suplente" not in coluna.casefold()
    ]
    if not candidatas:
        candidatas = [i for i, coluna in enumerate(colunas) if "candidat" in coluna.casefold()]
    if not candidatas:
        return indice_coluna(colunas, ("nome",))

    def preenchidas(indice: int) -> tuple[int, int]:
        valores = [limpar(x) for x in quadro.iloc[:, indice]]
        uteis = [x for x in valores if x and x.casefold() not in {"nan", "candidato(a)"}]
        return len(uteis), sum(len(x) for x in uteis)

    return max(candidatas, key=preenchidas)


def extrair_tabela(
    tabela: HtmlElement,
    *,
    ano: int,
    uf: str,
    pagina: str,
    url: str,
    numero_tabela: int,
) -> list[dict[str, object]]:
    quadro = pd.read_html(StringIO(html.tostring(tabela, encoding="unicode")))[0]
    if not eh_tabela_resultado_senado(tabela, quadro):
        return []
    colunas = [normalizar_coluna(c) for c in quadro.columns]
    candidato_i = indice_candidato(quadro, colunas)
    votos_i = indice_coluna(colunas, ("votos", "votação", "total"))
    percentual_i = indice_coluna(colunas, ("porcent", "percent", "votos (%)"))
    partido_i = indice_coluna(colunas, ("partido",))
    cargo_i = indice_coluna(colunas, ("cargo",))
    if candidato_i is None or votos_i is None:
        return []

    saida: list[dict[str, object]] = []
    for linha_numero, (_, linha) in enumerate(quadro.iterrows(), start=1):
        if cargo_i is not None and "senad" not in limpar(linha.iloc[cargo_i]).casefold():
            continue
        candidato = limpar(linha.iloc[candidato_i])
        votos_texto = limpar(linha.iloc[votos_i])
        votos = votos_inteiros(votos_texto)
        if (
            not candidato
            or votos is None
            or any(termo in candidato.casefold() for termo in NAO_CANDIDATOS)
            or candidato.casefold().startswith("candidat")
        ):
            continue
        percentual_texto = limpar(linha.iloc[percentual_i]) if percentual_i is not None else ""
        partido = limpar(linha.iloc[partido_i]) if partido_i is not None else ""
        saida.append(
            {
                "ano": ano,
                "uf": uf,
                "pagina": pagina,
                "url": url,
                "tabela": numero_tabela,
                "linha": linha_numero,
                "candidato_resultado": candidato,
                "partido_resultado": partido,
                "votos": votos,
                "percentual_publicado": percentual_numero(percentual_texto),
                "percentual_original": percentual_texto,
                "fonte_primaria": "TSE/Justiça Eleitoral (conforme tabela estadual)",
                "fonte_estruturada": "Wikipédia",
            }
        )
    return saida


def assinatura_tabela(linhas: list[dict[str, object]]) -> tuple[tuple[str, int], ...]:
    return tuple(
        sorted((str(x["candidato_resultado"]).casefold(), int(x["votos"])) for x in linhas)
    )


def coletar(anos: list[int], pausa: float = 0.8) -> tuple[pd.DataFrame, pd.DataFrame]:
    sessao = requests.Session()
    sessao.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "pt-BR,pt;q=0.9"})
    resultados: list[dict[str, object]] = []
    inventario: list[dict[str, object]] = []

    for ano in anos:
        for uf, estado in UFS.items():
            pagina = titulo_pagina(uf, estado, ano)
            url = BASE + quote(pagina.replace(" ", "_"), safe="()_-")
            registro: dict[str, object] = {
                "ano": ano,
                "uf": uf,
                "pagina": pagina,
                "url": url,
                "status": "",
                "tabelas_resultado": 0,
                "candidatos": 0,
                "erro": "",
            }
            try:
                resposta = sessao.get(url, timeout=45)
                resposta.raise_for_status()
                raiz = html.fromstring(resposta.content)
                # Páginas antigas misturam `wikitable`, `sortable` e tabelas sem
                # classe. A combinação de seção + cabeçalhos é o filtro real.
                tabelas = raiz.xpath("//table")
                assinaturas: set[tuple[tuple[str, int], ...]] = set()
                opcoes: list[list[dict[str, object]]] = []
                for numero, tabela in enumerate(tabelas, start=1):
                    try:
                        linhas = extrair_tabela(
                            tabela,
                            ano=ano,
                            uf=uf,
                            pagina=pagina,
                            url=url,
                            numero_tabela=numero,
                        )
                    except (ValueError, IndexError):
                        continue
                    if not linhas:
                        continue
                    assinatura = assinatura_tabela(linhas)
                    if assinatura in assinaturas:
                        continue
                    assinaturas.add(assinatura)
                    opcoes.append(linhas)

                if opcoes:
                    # Algumas páginas publicam a mesma apuração em versões
                    # responsivas com rótulos abreviados. A tabela mais completa
                    # é preferida; os casos ficam sinalizados no inventário.
                    if len(opcoes) > 1 and max(map(len, opcoes)) <= 2:
                        por_chave = {}
                        for linhas in opcoes:
                            for linha in linhas:
                                chave = (
                                    str(linha["candidato_resultado"]).casefold(),
                                    int(linha["votos"]),
                                )
                                por_chave[chave] = linha
                        linhas_uf = list(por_chave.values())
                    else:
                        linhas_uf = max(
                            opcoes,
                            key=lambda linhas: (
                                len(linhas),
                                sum(len(str(x["candidato_resultado"])) for x in linhas),
                            ),
                        )
                else:
                    linhas_uf = []

                if len(opcoes) > 1:
                    registro["status"] = "ok_escolhida_entre_duplicadas"
                elif linhas_uf:
                    registro["status"] = "ok"
                else:
                    registro["status"] = "sem_resultado_senado"
                registro["tabelas_resultado"] = len(opcoes)
                registro["candidatos"] = len(linhas_uf)
                resultados.extend(linhas_uf)
            except Exception as exc:
                registro["status"] = "erro"
                registro["erro"] = f"{type(exc).__name__}: {exc}"
            inventario.append(registro)
            print(
                f"{ano} {uf}: {registro['status']} ({registro['candidatos']} candidato(s))",
                flush=True,
            )
            time.sleep(pausa)

    quadro = pd.DataFrame(resultados)
    if not quadro.empty:
        quadro["ranking_voto_bruto"] = quadro.groupby(["ano", "uf"])["votos"].rank(
            method="first", ascending=False
        ).astype(int)
        quadro["eleito_top2_voto_bruto"] = quadro["ranking_voto_bruto"].le(2)
        total = quadro.groupby(["ano", "uf"])["votos"].transform("sum")
        quadro["share_voto_nominal_calculado"] = quadro["votos"] / total
        quadro["diferenca_percentual_publicado_pp"] = (
            quadro["percentual_publicado"] / 100 - quadro["share_voto_nominal_calculado"]
        ) * 100
    return quadro, pd.DataFrame(inventario)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--anos", nargs="+", type=int, default=[2018, 2010])
    parser.add_argument(
        "--saida", type=Path, default=Path("research/dados_senado_historico")
    )
    args = parser.parse_args()

    resultados, inventario = coletar(args.anos)
    args.saida.mkdir(parents=True, exist_ok=True)
    arquivo_resultados = args.saida / "resultados_senado_2010_2018_wikipedia.csv"
    arquivo_inventario = args.saida / "inventario_resultados_wikipedia.csv"
    resultados.to_csv(arquivo_resultados, index=False)
    inventario.to_csv(arquivo_inventario, index=False)

    print("\nResumo:")
    print(inventario.groupby(["ano", "status"]).size().unstack(fill_value=0).to_string())
    print(f"Resultados: {arquivo_resultados} ({len(resultados)} candidatos)")
    print(f"Inventário: {arquivo_inventario} ({len(inventario)} páginas)")


if __name__ == "__main__":
    main()
