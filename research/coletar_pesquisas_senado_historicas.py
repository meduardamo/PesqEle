"""Coleta tabelas históricas de pesquisas para o Senado na Wikipédia.

A Wikipédia é usada como índice secundário de páginas e tabelas preservadas.
Os CSVs gerados mantêm a URL de origem e não substituem a validação posterior
com PesqEle, relatórios dos institutos e resultados oficiais do TSE.

Uso:
    python research/coletar_pesquisas_senado_historicas.py
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


BASE = "https://pt.wikipedia.org/wiki/"
USER_AGENT = "eixo-eleicoes-historico-senado/1.0 (pesquisa local auditavel)"

UFS = {
    "AC": "Acre",
    "AL": "Alagoas",
    "AP": "Amapá",
    "AM": "Amazonas",
    "BA": "Bahia",
    "CE": "Ceará",
    "DF": "Distrito Federal",
    "ES": "Espírito Santo",
    "GO": "Goiás",
    "MA": "Maranhão",
    "MT": "Mato Grosso",
    "MS": "Mato Grosso do Sul",
    "MG": "Minas Gerais",
    "PA": "Pará",
    "PB": "Paraíba",
    "PR": "Paraná",
    "PE": "Pernambuco",
    "PI": "Piauí",
    "RJ": "Rio de Janeiro",
    "RN": "Rio Grande do Norte",
    "RS": "Rio Grande do Sul",
    "RO": "Rondônia",
    "RR": "Roraima",
    "SC": "Santa Catarina",
    "SP": "São Paulo",
    "SE": "Sergipe",
    "TO": "Tocantins",
}

PREPOSICAO = {
    "AC": "no",
    "AL": "em",
    "AP": "no",
    "AM": "no",
    "BA": "na",
    "CE": "no",
    "DF": "no",
    "ES": "no",
    "GO": "em",
    "MA": "no",
    "MT": "em",
    "MS": "em",
    "MG": "em",
    "PA": "no",
    "PB": "na",
    "PR": "no",
    "PE": "em",
    "PI": "no",
    "RJ": "no",
    "RN": "no",
    "RS": "no",
    "RO": "em",
    "RR": "em",
    "SC": "em",
    "SP": "em",
    "SE": "em",
    "TO": "no",
}


def limpar(valor: object) -> str:
    texto = "" if pd.isna(valor) else str(valor)
    texto = re.sub(r"\[\d+\]", "", texto)
    return " ".join(texto.replace("\xa0", " ").split()).strip()


def percentual(valor: object) -> float | None:
    texto = limpar(valor).replace("−", "-").replace(",", ".")
    if not texto or texto in {"—", "-", "nan"}:
        return None
    achou = re.search(r"-?\d+(?:\.\d+)?", texto)
    return float(achou.group()) if achou else None


def nivel_coluna(coluna: object) -> tuple[str, str]:
    if isinstance(coluna, tuple):
        topo = limpar(coluna[0])
        detalhe = limpar(coluna[-1])
        return topo, detalhe
    texto = limpar(coluna)
    return texto, texto


def titulo_pagina(uf: str, estado: str, ano: int) -> str:
    if uf == "DF":
        return f"Eleições distritais no Distrito Federal em {ano}"
    return f"Eleições estaduais {PREPOSICAO[uf]} {estado} em {ano}"


def texto_elemento(elemento: HtmlElement) -> str:
    return limpar(elemento.text_content())


def cabecalhos_anteriores(tabela: HtmlElement) -> str:
    cabecalhos = tabela.xpath("preceding::*[self::h2 or self::h3 or self::h4]")
    return " | ".join(texto_elemento(x) for x in reversed(cabecalhos[-4:]))


def cabecalho_imediato(tabela: HtmlElement) -> str:
    cabecalhos = tabela.xpath("preceding::*[self::h2 or self::h3 or self::h4]")
    return texto_elemento(cabecalhos[-1]) if cabecalhos else ""


def contexto_tabela(tabela: HtmlElement) -> str:
    partes = [cabecalhos_anteriores(tabela)]
    anterior = tabela.getprevious()
    vistos = 0
    while anterior is not None and vistos < 3:
        if anterior.tag in {"h2", "h3"}:
            break
        if anterior.tag in {"p", "div"}:
            partes.append(texto_elemento(anterior))
            vistos += 1
        anterior = anterior.getprevious()
    return " | ".join(p for p in partes if p)


def eh_tabela_pesquisa_senado(tabela: HtmlElement) -> bool:
    secao = cabecalho_imediato(tabela).casefold()
    texto = texto_elemento(tabela).casefold()
    return (
        "senad" in secao
        and "instituto" in texto
        and (
            "data" in texto
            or "divulgação" in texto
            or "período" in texto
            or "periodo" in texto
        )
    )


METADADOS = (
    "instituto",
    "data",
    "período",
    "periodo",
    "divulgação",
    "margem",
    "amostra",
    "entrevist",
    "fonte",
    "ref.",
    "referência",
    "organizador",
)

NAO_CANDIDATOS = (
    "branco",
    "nulo",
    "não sabe",
    "nao sabe",
    "nenhum",
    "ns/nr",
    "indecis",
    "abst",
    "outros",
    "sem candidato",
    "vantagem",
    "liderança",
    "empate",
    "citou só",
    "citou apenas",
)


def contem(texto: str, termos: tuple[str, ...]) -> bool:
    base = texto.casefold()
    return any(termo in base for termo in termos)


def nome_candidato(topo: str, detalhe: str) -> str | None:
    if "candidat" in topo.casefold() and "candidat" not in detalhe.casefold():
        nome = detalhe
    elif not contem(f"{topo} {detalhe}", METADADOS + NAO_CANDIDATOS):
        nome = topo if detalhe.casefold().startswith("unnamed") or detalhe == topo else detalhe
    else:
        return None
    nome = limpar(nome)
    if (
        not nome
        or nome.casefold().startswith("unnamed")
        or contem(nome, NAO_CANDIDATOS)
    ):
        return None
    return nome


def instituto_do_rotulo(rotulo: str) -> str:
    texto = limpar(rotulo)
    texto = re.sub(r"\[[^]]+\]", "", texto).strip()
    antes_data = re.split(r"\s*\(\s*\d|\s+\d{1,2}\s+de\s+", texto, maxsplit=1)[0]
    return antes_data.strip(" -") or texto


def formato_tabela(contexto: str, quadro: pd.DataFrame) -> str:
    texto = contexto.casefold() + " " + " ".join(map(str, quadro.columns)).casefold()
    if "200%" in texto or "duas vezes" in texto or "duas menções" in texto:
        return "duas_mencoes_agregadas"
    if re.search(r"1[ºo°]?\s*voto|primeiro voto", texto) and re.search(
        r"2[ºo°]?\s*voto|segundo voto", texto
    ):
        return "primeiro_segundo_separados"
    return "nao_identificado"


def extrair_tabela(
    tabela: HtmlElement,
    *,
    ano: int,
    uf: str,
    titulo: str,
    url: str,
    numero: int,
) -> list[dict[str, object]]:
    quadro = pd.read_html(StringIO(html.tostring(tabela, encoding="unicode")))[0]
    contexto = contexto_tabela(tabela)
    formato = formato_tabela(contexto, quadro)
    colunas = [nivel_coluna(c) for c in quadro.columns]

    primeira = " ".join(colunas[0]).casefold() if colunas else ""
    colunas_instituto = [i for i, (a, b) in enumerate(colunas) if "instituto" in f"{a} {b}".casefold()]
    if "candidat" in primeira and len(colunas_instituto) >= 2:
        saida_transposta: list[dict[str, object]] = []
        for linha_numero, (_, linha) in enumerate(quadro.iterrows(), start=1):
            candidato = limpar(linha.iloc[0])
            if not candidato or contem(candidato, NAO_CANDIDATOS + ("candidato",)):
                continue
            for indice in colunas_instituto:
                topo, detalhe = colunas[indice]
                rotulo = detalhe if "instituto" not in detalhe.casefold() else topo
                valor_original = limpar(linha.iloc[indice])
                valor = percentual(valor_original)
                if valor is None:
                    continue
                saida_transposta.append(
                    {
                        "ano": ano,
                        "uf": uf,
                        "pagina": titulo,
                        "url": url,
                        "tabela": numero,
                        "linha": indice,
                        "periodo_campo": rotulo,
                        "instituto": instituto_do_rotulo(rotulo),
                        "margem_erro_texto": "",
                        "margem_erro_pp": None,
                        "formato": formato,
                        "candidato_rotulo": candidato,
                        "percentual": valor,
                        "valor_original": valor_original,
                        "contexto": contexto,
                    }
                )
        return saida_transposta

    indice_periodo = next(
        (
            i
            for i, (a, b) in enumerate(colunas)
            if contem(f"{a} {b}", ("período", "periodo", "divulgação", "data"))
        ),
        None,
    )
    indice_instituto = next(
        (i for i, (a, b) in enumerate(colunas) if "instituto" in f"{a} {b}".casefold()),
        None,
    )
    indice_margem = next(
        (i for i, (a, b) in enumerate(colunas) if "margem" in f"{a} {b}".casefold()),
        None,
    )
    candidatos = []
    for i, (topo, detalhe) in enumerate(colunas):
        if i in {indice_periodo, indice_instituto, indice_margem}:
            continue
        nome = nome_candidato(topo, detalhe)
        if nome:
            candidatos.append((i, nome))
    if indice_periodo is None or indice_instituto is None or not candidatos:
        return []

    saida: list[dict[str, object]] = []
    for linha_numero, (_, linha) in enumerate(quadro.iterrows(), start=1):
        periodo = limpar(linha.iloc[indice_periodo])
        instituto = limpar(linha.iloc[indice_instituto])
        if not periodo or not instituto or periodo.casefold() in {"nan", "período da pesquisa"}:
            continue
        margem = limpar(linha.iloc[indice_margem]) if indice_margem is not None else ""
        for indice, candidato in candidatos:
            valor_original = limpar(linha.iloc[indice])
            valor = percentual(valor_original)
            if valor is None:
                continue
            saida.append(
                {
                    "ano": ano,
                    "uf": uf,
                    "pagina": titulo,
                    "url": url,
                    "tabela": numero,
                    "linha": linha_numero,
                    "periodo_campo": periodo,
                    "instituto": instituto,
                    "margem_erro_texto": margem,
                    "margem_erro_pp": percentual(margem),
                    "formato": formato,
                    "candidato_rotulo": candidato,
                    "percentual": valor,
                    "valor_original": valor_original,
                    "contexto": contexto,
                }
            )
    return saida


def coletar(anos: list[int], pausa: float = 0.8) -> tuple[pd.DataFrame, pd.DataFrame]:
    session = requests.Session()
    session.headers.update({"User-Agent": USER_AGENT, "Accept-Language": "pt-BR,pt;q=0.9"})
    valores: list[dict[str, object]] = []
    inventario: list[dict[str, object]] = []

    for ano in anos:
        for uf, estado in UFS.items():
            registro: dict[str, object] = {
                "ano": ano,
                "uf": uf,
                "estado": estado,
                "pagina": "",
                "url": "",
                "status": "",
                "tabelas_senado": 0,
                "linhas_pesquisa": 0,
                "observacoes_candidato": 0,
                "formatos": "",
                "erro": "",
            }
            try:
                titulo = titulo_pagina(uf, estado, ano)
                url = BASE + quote(titulo.replace(" ", "_"), safe="()_-")
                resposta = session.get(url, timeout=45)
                if resposta.status_code == 404:
                    registro["status"] = "pagina_nao_localizada"
                    registro["pagina"] = titulo
                    registro["url"] = url
                    inventario.append(registro)
                    continue
                resposta.raise_for_status()
                raiz = html.fromstring(resposta.content)
                candidatas = raiz.xpath(
                    '//table[contains(concat(" ", normalize-space(@class), " "), " wikitable ")]'
                )
                tabelas = [t for t in candidatas if eh_tabela_pesquisa_senado(t)]
                linhas: list[dict[str, object]] = []
                for numero, tabela in enumerate(tabelas, start=1):
                    linhas.extend(
                        extrair_tabela(
                            tabela,
                            ano=ano,
                            uf=uf,
                            titulo=titulo,
                            url=url,
                            numero=numero,
                        )
                    )
                valores.extend(linhas)
                registro.update(
                    {
                        "pagina": titulo,
                        "url": url,
                        "status": "com_pesquisas" if linhas else "sem_tabela_pesquisa_senado",
                        "tabelas_senado": len(tabelas),
                        "linhas_pesquisa": len(
                            {(x["tabela"], x["linha"]) for x in linhas}
                        ),
                        "observacoes_candidato": len(linhas),
                        "formatos": " | ".join(sorted({str(x["formato"]) for x in linhas})),
                    }
                )
            except Exception as exc:  # inventário precisa registrar a lacuna e continuar
                registro["status"] = "erro"
                registro["erro"] = f"{type(exc).__name__}: {exc}"
            inventario.append(registro)
            print(
                f"{ano} {uf}: {registro['status']} "
                f"({registro['linhas_pesquisa']} rodada(s))",
                flush=True,
            )
            time.sleep(pausa)

    return pd.DataFrame(valores), pd.DataFrame(inventario)


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--anos", nargs="+", type=int, default=[2018, 2010])
    parser.add_argument(
        "--saida",
        type=Path,
        default=Path("research/dados_senado_historico"),
    )
    args = parser.parse_args()

    valores, inventario = coletar(args.anos)
    if not valores.empty:
        chaves = ["ano", "uf", "tabela", "linha"]
        somas = valores.groupby(chaves)["percentual"].transform("sum")
        desconhecido = valores["formato"].eq("nao_identificado")
        valores.loc[desconhecido & somas.gt(100), "formato"] = "duas_mencoes_agregadas"
    args.saida.mkdir(parents=True, exist_ok=True)
    arquivo_valores = args.saida / "pesquisas_senado_wikipedia.csv"
    arquivo_inventario = args.saida / "inventario_wikipedia.csv"
    valores.to_csv(arquivo_valores, index=False)
    inventario.to_csv(arquivo_inventario, index=False)

    print("\nResumo por ano:")
    if not inventario.empty:
        resumo = inventario.groupby(["ano", "status"]).size().unstack(fill_value=0)
        print(resumo.to_string())
    print(f"\nValores: {arquivo_valores} ({len(valores)} observações candidato)")
    print(f"Inventário: {arquivo_inventario} ({len(inventario)} páginas)")


if __name__ == "__main__":
    main()
