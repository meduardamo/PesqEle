"""Estrutura o levantamento estadual de pesquisas para o Senado em 2010.

A página do Congresso em Foco é uma fonte secundária contemporânea que
reúne pesquisas dos 26 estados e do Distrito Federal. Ela serve como índice de
descoberta: percentuais e rótulos são preservados, mas questionário, amostra,
margem de erro e resultados devem ser validados nas fontes primárias antes da
calibração final.

Uso:
    python research/coletar_congresso_em_foco_2010.py
    python research/coletar_congresso_em_foco_2010.py --html pagina.html
"""

from __future__ import annotations

import argparse
import re
from pathlib import Path

import pandas as pd
import requests
from lxml import etree, html


URL = (
    "https://www.congressoemfoco.com.br/noticia/82698/"
    "a-disputa-para-o-senado-estado-por-estado"
)
USER_AGENT = "eixo-eleicoes-historico-senado/1.0 (pesquisa local auditavel)"

ESTADOS = {
    "acre": "AC",
    "alagoas": "AL",
    "amapá": "AP",
    "amazonas": "AM",
    "bahia": "BA",
    "ceará": "CE",
    "distrito federal": "DF",
    "espírito santo": "ES",
    "goiás": "GO",
    "maranhão": "MA",
    "mato grosso": "MT",
    "mato grosso do sul": "MS",
    "minas gerais": "MG",
    "pará": "PA",
    "paraíba": "PB",
    "paraná": "PR",
    "pernambuco": "PE",
    "piauí": "PI",
    "rio de janeiro": "RJ",
    "rio grande do norte": "RN",
    "rio grande do sul": "RS",
    "rondônia": "RO",
    "roraima": "RR",
    "santa catarina": "SC",
    "são paulo": "SP",
    "sergipe": "SE",
    "tocantins": "TO",
}

PADRAO_RODADA = re.compile(
    r"^(?P<instituto>.+?)\s+"
    r"(?P<periodo>\d{1,2}(?:-\d{1,2})?/\d{2})"
    r"(?:\s*-\s*|\s+)"
    r"(?P<resultados>.+)$"
)
PADRAO_RESULTADO = re.compile(
    r"(?:^|,\s+)(?P<candidato>.+?)\s+(?P<percentual>\d+(?:,\d+)?)%"
)


def carregar_html(arquivo: Path | None) -> bytes:
    if arquivo is not None:
        return arquivo.read_bytes()
    resposta = requests.get(
        URL,
        headers={"User-Agent": USER_AGENT, "Accept-Language": "pt-BR,pt;q=0.9"},
        timeout=45,
    )
    resposta.raise_for_status()
    return resposta.content


def blocos_artigo(conteudo: bytes) -> tuple[list[str], str]:
    raiz = html.fromstring(conteudo)
    artigos = raiz.xpath('//div[contains(@class, "html-content")]/p')
    if not artigos:
        raise ValueError("Conteúdo principal da notícia não localizado")

    fragmento = etree.tostring(artigos[0], encoding="unicode", method="html")
    fragmento = re.sub(r"<br\s*/?>", "\n", fragmento, flags=re.IGNORECASE)
    texto = html.fromstring(fragmento).text_content().replace("\xa0", " ")
    blocos = [
        re.sub(r"\s+", " ", bloco).strip()
        for bloco in re.split(r"\n\s*\n+", texto)
        if bloco.strip()
    ]
    publicado = raiz.xpath('string(//meta[@property="article:published_time"]/@content)')
    return blocos, publicado


def extrair(conteudo: bytes) -> tuple[pd.DataFrame, pd.DataFrame]:
    blocos, publicado = blocos_artigo(conteudo)
    observacoes: list[dict[str, object]] = []
    rodadas: list[dict[str, object]] = []
    uf_atual: str | None = None
    numero_rodada = 0

    for bloco in blocos:
        uf_cabecalho = ESTADOS.get(bloco.casefold())
        if uf_cabecalho:
            uf_atual = uf_cabecalho
            continue

        achou_rodada = PADRAO_RODADA.match(bloco)
        if uf_atual is None or achou_rodada is None:
            continue

        resultados = list(PADRAO_RESULTADO.finditer(achou_rodada["resultados"]))
        if not resultados:
            continue
        numero_rodada += 1
        percentuais = [
            float(resultado["percentual"].replace(",", "."))
            for resultado in resultados
        ]
        soma = sum(percentuais)
        formato = "duas_mencoes_agregadas" if soma > 100 else "nao_identificado"
        id_rodada = f"2010-{uf_atual}-{numero_rodada:03d}"

        rodadas.append(
            {
                "id_rodada": id_rodada,
                "ano": 2010,
                "uf": uf_atual,
                "instituto": achou_rodada["instituto"].strip(),
                "periodo_campo": achou_rodada["periodo"],
                "soma_percentuais_candidatos": round(soma, 2),
                "numero_candidatos": len(resultados),
                "formato_inferido": formato,
                "formato_confirmado": False,
                "fonte_primaria_confirmada": False,
                "publicado_em": publicado,
                "url": URL,
                "linha_original": bloco,
            }
        )
        for resultado, percentual in zip(resultados, percentuais, strict=True):
            candidato = resultado["candidato"].strip(" .,-")
            observacoes.append(
                {
                    "id_rodada": id_rodada,
                    "ano": 2010,
                    "uf": uf_atual,
                    "instituto": achou_rodada["instituto"].strip(),
                    "periodo_campo": achou_rodada["periodo"],
                    "candidato_rotulo": candidato,
                    "percentual": percentual,
                    "formato_inferido": formato,
                    "formato_confirmado": False,
                    "fonte_primaria_confirmada": False,
                    "publicado_em": publicado,
                    "url": URL,
                }
            )

    return pd.DataFrame(observacoes), pd.DataFrame(rodadas)


def validar_cobertura(observacoes: pd.DataFrame, rodadas: pd.DataFrame) -> None:
    esperadas = set(ESTADOS.values())
    encontradas = set(observacoes["uf"].unique()) if not observacoes.empty else set()
    faltantes = esperadas - encontradas
    if faltantes:
        raise ValueError(f"UFs sem observações: {', '.join(sorted(faltantes))}")
    if rodadas["id_rodada"].duplicated().any():
        raise ValueError("IDs de rodada duplicados")
    if not observacoes["id_rodada"].isin(rodadas["id_rodada"]).all():
        raise ValueError("Observação sem rodada correspondente")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--html", type=Path)
    parser.add_argument(
        "--saida",
        type=Path,
        default=Path("research/dados_senado_historico"),
    )
    args = parser.parse_args()

    observacoes, rodadas = extrair(carregar_html(args.html))
    validar_cobertura(observacoes, rodadas)
    args.saida.mkdir(parents=True, exist_ok=True)
    arquivo_observacoes = args.saida / "pesquisas_senado_2010_congresso_em_foco.csv"
    arquivo_rodadas = args.saida / "rodadas_senado_2010_congresso_em_foco.csv"
    observacoes.to_csv(arquivo_observacoes, index=False)
    rodadas.to_csv(arquivo_rodadas, index=False)

    por_formato = rodadas.groupby("formato_inferido").size().to_dict()
    print(f"UFs: {observacoes['uf'].nunique()}/27")
    print(f"Rodadas: {len(rodadas)}")
    print(f"Observações candidato: {len(observacoes)}")
    print(f"Formatos inferidos: {por_formato}")
    print(f"Observações: {arquivo_observacoes}")
    print(f"Rodadas: {arquivo_rodadas}")


if __name__ == "__main__":
    main()
