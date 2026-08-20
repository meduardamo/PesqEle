import unittest

import pandas as pd

from compartilhado.pollingdata_scraper import (
    COLUNA_MODELO_AMOSTRAL,
    COLUNA_MODELO_HIBRIDO,
    adicionar_media_movel_13d_resultados_bi,
    adicionar_metricas_media_cenarios,
    agregar_resultados_bi_diario,
    calcular_agregadores_paralelos_resultados_bi,
    construir_resultados_bi,
    score_instituto,
    selecionar_cenario_principal,
)


def pesquisa(
    poll_id: str,
    data: str,
    percentual: float,
    classificacao: str,
    *,
    candidato: str = "Candidato",
    cenario: str = "1",
) -> dict:
    return {
        "poll_id": poll_id,
        "tipo": "candidato",
        "candidato": candidato,
        "partido": "P",
        "candidato_partido": f"{candidato} (P)",
        "scenario_label": cenario,
        "scenario_id": f"{poll_id}|{cenario}",
        "percentual": percentual,
        "ano": "2026",
        "uf": "BR",
        "cargo": "presidente",
        "turno": "t1",
        "disputa": "",
        "data_campo": data,
        "instituto": poll_id,
        "classificacao_instituto": classificacao,
        "registro_tse": f"BR-{poll_id}",
        "fonte_url": "",
        "horario_raspagem": "",
    }


def linhas_cenario(
    poll_id: str,
    data: str,
    valor_teste: float,
    classificacao: str = "A+",
    *,
    cenario: str = "1",
) -> list[dict]:
    candidatos = {
        "Lula": 40,
        "Flávio Bolsonaro": 32,
        "Ronaldo Caiado": 5,
        "Candidato teste": valor_teste,
    }
    return [
        pesquisa(
            poll_id,
            data,
            percentual,
            classificacao,
            candidato=candidato,
            cenario=cenario,
        )
        for candidato, percentual in candidatos.items()
    ]


def metadados_amostra(*itens: tuple[str, int, str]) -> pd.DataFrame:
    return pd.DataFrame([
        {
            "poll_id": poll_id,
            "scenario_id": f"{poll_id}|1",
            "amostra": amostra,
            "data_inicio_campo": data,
        }
        for poll_id, amostra, data in itens
    ])


def consolidar(rows: list[dict]) -> pd.DataFrame:
    df = adicionar_metricas_media_cenarios(pd.DataFrame(rows))
    df["percentual_base"] = df["percentual_media_cenarios"]
    df["origem_percentual_base"] = df["origem_percentual_media"]
    df["qtd_cenarios_considerados"] = 1
    return agregar_resultados_bi_diario(df)


class CalculosPollingDataTest(unittest.TestCase):
    def test_scores_institutos(self):
        self.assertEqual(score_instituto("A+"), 1.0)
        self.assertEqual(score_instituto("B"), 0.475)
        self.assertEqual(score_instituto("Ainda não foi avaliado"), 0.25)
        self.assertEqual(score_instituto(""), 0.25)
        self.assertEqual(score_instituto("classificação desconhecida"), 0.25)

    def test_media_cenarios_considera_apenas_cenarios_em_que_candidato_aparece(self):
        df = pd.DataFrame([
            pesquisa("p1", "2026-08-01", 40, "A+", candidato="A", cenario="1"),
            pesquisa("p1", "2026-08-01", 50, "A+", candidato="A", cenario="2"),
            pesquisa("p1", "2026-08-01", 35, "A+", candidato="B", cenario="1"),
        ])

        calculado = adicionar_metricas_media_cenarios(df)

        valores_a = calculado.loc[
            calculado["candidato"].eq("A"), "percentual_media_cenarios"
        ].unique()
        valores_b = calculado.loc[
            calculado["candidato"].eq("B"), "percentual_media_cenarios"
        ].unique()
        self.assertEqual(valores_a.tolist(), [45.0])
        self.assertEqual(valores_b.tolist(), [35.0])

    def test_media_diaria_pondera_pelo_score(self):
        diario = consolidar([
            pesquisa("a_mais", "2026-08-01", 50, "A+"),
            pesquisa("b_menos", "2026-08-01", 40, "B-"),
        ])

        self.assertEqual(len(diario), 1)
        self.assertAlmostEqual(diario.iloc[0]["peso_total_dia"], 1.4)
        self.assertAlmostEqual(diario.iloc[0]["percentual_base"], (50 + 40 * 0.4) / 1.4)

    def test_nao_avaliado_usa_mesmo_peso_padrao(self):
        diario = consolidar([
            pesquisa("sem_nota_1", "2026-08-01", 40, "Ainda não foi avaliado"),
            pesquisa("sem_nota_2", "2026-08-01", 20, "classificação desconhecida"),
        ])

        self.assertAlmostEqual(diario.iloc[0]["peso_total_dia"], 0.5)
        self.assertAlmostEqual(diario.iloc[0]["percentual_base"], 30.0)

    def test_media_movel_remove_pesquisa_ao_sair_da_janela_de_13_dias(self):
        diario = consolidar([
            pesquisa("antiga", "2026-08-01", 50, "A+"),
            pesquisa("nova", "2026-08-14", 30, "A+"),
        ])

        movel = adicionar_media_movel_13d_resultados_bi(diario)
        valor_13 = movel.loc[movel["data_campo"].eq("2026-08-13"), "media_movel_13d"].iloc[0]
        valor_14 = movel.loc[movel["data_campo"].eq("2026-08-14"), "media_movel_13d"].iloc[0]

        self.assertAlmostEqual(valor_13, 50.0)
        self.assertAlmostEqual(valor_14, 30.0)

    def test_cenario_principal_eh_o_mais_amplo(self):
        rows = [
            pesquisa("p1", "2026-08-01", 40, "A+", candidato="A", cenario="1"),
            pesquisa("p1", "2026-08-01", 35, "A+", candidato="B", cenario="1"),
            pesquisa("p1", "2026-08-01", 41, "A+", candidato="A", cenario="2"),
            pesquisa("p1", "2026-08-01", 34, "A+", candidato="B", cenario="2"),
            pesquisa("p1", "2026-08-01", 8, "A+", candidato="C", cenario="2"),
        ]

        principal = selecionar_cenario_principal(pd.DataFrame(rows))

        self.assertEqual(principal["scenario_label"].unique().tolist(), ["2"])
        self.assertEqual(set(principal["candidato"]), {"A", "B", "C"})

    def test_resultados_bi_usa_cenario_principal_sem_media_dos_cenarios(self):
        rows = [
            pesquisa("p1", "2026-08-01", 40, "A+", candidato="A", cenario="1"),
            pesquisa("p1", "2026-08-01", 35, "A+", candidato="B", cenario="1"),
            pesquisa("p1", "2026-08-01", 44, "A+", candidato="A", cenario="2"),
            pesquisa("p1", "2026-08-01", 31, "A+", candidato="B", cenario="2"),
            pesquisa("p1", "2026-08-01", 8, "A+", candidato="C", cenario="2"),
        ]

        calculado = construir_resultados_bi(pd.DataFrame(rows), metadados_amostra(("p1", 1000, "2026-08-01")))

        valores = calculado.set_index("candidato_partido")["percentual_base"].to_dict()
        self.assertEqual(valores["A (P)"], 44.0)
        self.assertEqual(valores["B (P)"], 31.0)
        self.assertEqual(valores["C (P)"], 8.0)
        self.assertTrue(calculado["cenario_usado_no_calculo"].eq("2").all())

    def test_cenario_principal_preserva_cada_confronto_de_segundo_turno(self):
        rows = [
            pesquisa("p1", "2026-08-01", 48, "A+", candidato="A", cenario="1"),
            pesquisa("p1", "2026-08-01", 44, "A+", candidato="B", cenario="1"),
            pesquisa("p1", "2026-08-01", 51, "A+", candidato="A", cenario="2"),
            pesquisa("p1", "2026-08-01", 39, "A+", candidato="C", cenario="2"),
        ]
        for row in rows[:2]:
            row["turno"] = "t2"
            row["disputa"] = "A x B"
        for row in rows[2:]:
            row["turno"] = "t2"
            row["disputa"] = "A x C"

        principal = selecionar_cenario_principal(pd.DataFrame(rows))

        self.assertEqual(set(principal["disputa"]), {"A x B", "A x C"})
        self.assertEqual(len(principal), 4)

    def test_modelo_amostral_aplica_raiz_amostra_e_meia_vida_30_dias(self):
        resultados = pd.DataFrame(
            linhas_cenario("antiga", "2026-01-01", 50)
            + linhas_cenario("nova", "2026-01-31", 30)
        )
        pesquisas = metadados_amostra(
            ("antiga", 400, "2026-01-01"),
            ("nova", 1600, "2026-01-31"),
        )

        calculado = calcular_agregadores_paralelos_resultados_bi(resultados, pesquisas)
        linha = calculado[
            calculado["candidato_partido"].eq("Candidato teste (P)")
            & calculado["data_campo"].eq("2026-01-31")
        ].iloc[0]

        # antiga: sqrt(400) * 0,5 = 10; nova: sqrt(1600) = 40
        self.assertAlmostEqual(linha[COLUNA_MODELO_AMOSTRAL], (50 * 10 + 30 * 40) / 50)

    def test_hibrido_multiplica_score_e_nao_avaliado_permanece_025(self):
        resultados = pd.DataFrame(
            linhas_cenario("alta", "2026-02-01", 50, "A+")
            + linhas_cenario("sem_nota", "2026-02-01", 30, "Ainda não foi avaliado")
        )
        pesquisas = metadados_amostra(
            ("alta", 400, "2026-02-01"),
            ("sem_nota", 400, "2026-02-01"),
        )

        calculado = calcular_agregadores_paralelos_resultados_bi(resultados, pesquisas)
        linha = calculado[
            calculado["candidato_partido"].eq("Candidato teste (P)")
            & calculado["data_campo"].eq("2026-02-01")
        ].iloc[0]

        self.assertAlmostEqual(linha[COLUNA_MODELO_HIBRIDO], (50 + 30 * 0.25) / 1.25)

    def test_pesquisa_so_entra_quando_campo_termina(self):
        resultados = pd.DataFrame(linhas_cenario("p1", "2026-02-05", 50))
        pesquisas = metadados_amostra(("p1", 1000, "2026-02-01"))

        calculado = calcular_agregadores_paralelos_resultados_bi(resultados, pesquisas)

        self.assertEqual(calculado["data_campo"].min(), "2026-02-05")
        self.assertEqual(calculado["data_campo"].max(), "2026-02-05")

    def test_modelo_amostral_exclui_pesquisa_sem_trio_obrigatorio(self):
        completas = linhas_cenario("completa", "2026-03-01", 40)
        incompletas = [
            row for row in linhas_cenario("incompleta", "2026-03-02", 10)
            if row["candidato"] != "Ronaldo Caiado"
        ]
        pesquisas = metadados_amostra(
            ("completa", 1000, "2026-03-01"),
            ("incompleta", 1000, "2026-03-02"),
        )

        calculado = calcular_agregadores_paralelos_resultados_bi(
            pd.DataFrame(completas + incompletas),
            pesquisas,
        )
        linha = calculado[
            calculado["candidato_partido"].eq("Candidato teste (P)")
            & calculado["data_campo"].eq("2026-03-02")
        ].iloc[0]

        self.assertAlmostEqual(linha[COLUNA_MODELO_AMOSTRAL], 40.0)
        self.assertNotAlmostEqual(linha[COLUNA_MODELO_HIBRIDO], 40.0)

    def test_presidencial_t1_exige_lula_e_flavio_no_cenario(self):
        completas = linhas_cenario("completa", "2026-03-01", 40)
        sem_flavio = [
            row for row in linhas_cenario("sem_flavio", "2026-03-02", 10)
            if row["candidato"] != "Flávio Bolsonaro"
        ]
        pesquisas = metadados_amostra(
            ("completa", 1000, "2026-03-01"),
            ("sem_flavio", 1000, "2026-03-02"),
        )

        calculado = calcular_agregadores_paralelos_resultados_bi(
            pd.DataFrame(completas + sem_flavio),
            pesquisas,
        )
        serie = calculado[calculado["candidato_partido"].eq("Candidato teste (P)")]

        # A pesquisa sem Flávio sai da série: ela para em 01/03, com 40.
        self.assertEqual(serie["data_campo"].max(), "2026-03-01")
        self.assertAlmostEqual(serie[COLUNA_MODELO_HIBRIDO].iloc[-1], 40.0)

    def test_cargo_sem_lula_e_flavio_nao_e_afetado(self):
        linhas = []
        for candidato, percentual in (("Fulano", 55), ("Beltrano", 45)):
            linha = pesquisa("gov1", "2026-03-01", percentual, "A+", candidato=candidato)
            linha["cargo"] = "governador"
            linha["uf"] = "SP"
            linhas.append(linha)
        pesquisas = metadados_amostra(("gov1", 1000, "2026-03-01"))

        calculado = calcular_agregadores_paralelos_resultados_bi(
            pd.DataFrame(linhas),
            pesquisas,
        )
        linha_gov = calculado[calculado["candidato_partido"].eq("Fulano (P)")].iloc[0]

        self.assertAlmostEqual(linha_gov[COLUNA_MODELO_HIBRIDO], 55.0)


if __name__ == "__main__":
    unittest.main()
