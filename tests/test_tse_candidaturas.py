# -*- coding: utf-8 -*-
"""Testes da consolidação de candidaturas (tse_candidaturas.py)."""

import pandas as pd
import pytest

from outros.tse_candidaturas import consolidar


@pytest.fixture
def base_csv():
    return pd.DataFrame([
        {"SQ_CANDIDATO": 101, "NM_URNA_CANDIDATO": "CANDIDATO UM", "SG_UF": "PR", "DS_CARGO": "GOVERNADOR"},
        {"SQ_CANDIDATO": 102, "NM_URNA_CANDIDATO": "CANDIDATO DOIS", "SG_UF": "GO", "DS_CARGO": "GOVERNADOR"},
        {"SQ_CANDIDATO": 103, "NM_URNA_CANDIDATO": "CANDIDATO TRES", "SG_UF": "RS", "DS_CARGO": "GOVERNADOR"},
        {"SQ_CANDIDATO": 104, "NM_URNA_CANDIDATO": "CANDIDATO QUATRO", "SG_UF": "SP", "DS_CARGO": "GOVERNADOR"},
    ])


@pytest.fixture
def df_api():
    return pd.DataFrame([
        {"sq_candidato": 101, "link_plano": None, "situacao": "Deferido", "foto_url": "http://foto/101.jpg"},
        {"sq_candidato": 102, "link_plano": "http://divulgacand/plano102.pdf", "situacao": "Deferido", "foto_url": "http://foto/102.jpg"},
        {"sq_candidato": 103, "link_plano": "", "situacao": "Deferido", "foto_url": ""},
        {"sq_candidato": 104, "link_plano": None, "situacao": "Deferido", "foto_url": ""},
    ])


def test_consolidar_fallback_link_manual(base_csv, df_api):
    # Candidato 101 tem link manual na planilha e API vazia -> mantém manual
    # Candidato 103 tem link manual na planilha e API string vazia -> mantém manual
    df_existente = pd.DataFrame([
        {"SQ_CANDIDATO": "101", "LINK_PLANO": "https://consultaunificadapje.tse.jus.br/doc101.pdf"},
        {"SQ_CANDIDATO": "103", "LINK_PLANO": "https://consultaunificadapje.tse.jus.br/doc103.pdf"},
    ])

    res = consolidar(df_api, base_csv, df_existente)

    p101 = res[res["SQ_CANDIDATO"] == 101].iloc[0]
    assert p101["LINK_PLANO"] == "https://consultaunificadapje.tse.jus.br/doc101.pdf"

    p103 = res[res["SQ_CANDIDATO"] == 103].iloc[0]
    assert p103["LINK_PLANO"] == "https://consultaunificadapje.tse.jus.br/doc103.pdf"


def test_consolidar_api_tem_prioridade(base_csv, df_api):
    # Candidato 102 tem link oficial na API e manual na planilha -> prevalece o da API
    df_existente = pd.DataFrame([
        {"SQ_CANDIDATO": "102", "LINK_PLANO": "https://consultaunificadapje.tse.jus.br/doc102_manual.pdf"},
    ])

    res = consolidar(df_api, base_csv, df_existente)

    p102 = res[res["SQ_CANDIDATO"] == 102].iloc[0]
    assert p102["LINK_PLANO"] == "http://divulgacand/plano102.pdf"


def test_consolidar_sem_link_em_nenhum(base_csv, df_api):
    # Candidato 104 não tem link na API nem na planilha -> LINK_PLANO fica vazio/nulo
    df_existente = pd.DataFrame([
        {"SQ_CANDIDATO": "101", "LINK_PLANO": "https://consultaunificadapje.tse.jus.br/doc101.pdf"},
    ])

    res = consolidar(df_api, base_csv, df_existente)

    p104 = res[res["SQ_CANDIDATO"] == 104].iloc[0]
    assert p104["LINK_PLANO"] in (None, "", "nan", float("nan")) or pd.isna(p104["LINK_PLANO"])


def test_consolidar_sem_existente(base_csv, df_api):
    res = consolidar(df_api, base_csv, None)
    p102 = res[res["SQ_CANDIDATO"] == 102].iloc[0]
    assert p102["LINK_PLANO"] == "http://divulgacand/plano102.pdf"

    p101 = res[res["SQ_CANDIDATO"] == 101].iloc[0]
    assert pd.isna(p101["LINK_PLANO"]) or p101["LINK_PLANO"] in (None, "", "None")
