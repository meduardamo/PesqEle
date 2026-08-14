import unittest
from unittest.mock import patch

from compartilhado.relatorios_sheets_utils import CABECALHO_RELATORIOS
from relatorios import relatorios_extracao_segmentos as extracao


class FilaFalsa:
    def row_values(self, _linha):
        return CABECALHO_RELATORIOS


class ExtracaoSegmentosTest(unittest.TestCase):
    def test_preencher_fila_inclui_link_pollingdata(self):
        linhas = []
        pesquisa = {
            "numero_identificacao": "BR-01234/2026",
            "empresa_contratada": "Instituto Teste",
            "abrangencia": "BRASIL",
            "cargos": "Presidente",
            "data_divulgacao": "2026-08-12",
        }

        with (
            patch.object(extracao, "RELATORIOS_ID", "planilha-teste"),
            patch.object(extracao, "_sheets"),
            patch.object(extracao, "_aba", return_value=FilaFalsa()),
            patch.object(extracao, "_rel_records", return_value=[]),
            patch.object(
                extracao,
                "_append_rows_compacto",
                side_effect=lambda _ws, novas: linhas.extend(novas),
            ),
            patch.object(extracao, "_resetar_validacoes_relatorios"),
            patch.object(extracao, "_ultima_linha_com_registro", return_value=2),
        ):
            extracao._preencher_fila([pesquisa])

        self.assertEqual(len(linhas), 1)
        linha = dict(zip(CABECALHO_RELATORIOS, linhas[0]))
        self.assertEqual(
            linha["Link PollingData"],
            "https://flex.pollingdata.com.br/pdvoto/2026/presidente/br/"
            "t1_lula-flavio-sem-bolsonaros",
        )

    def test_link_presidencial_estadual_usa_uf_da_abrangencia(self):
        self.assertEqual(
            extracao._link_pollingdata_url("BR-08086/2026", "presidente", "ACRE"),
            "https://flex.pollingdata.com.br/pdvoto/2026/presidente/ac/t1",
        )


if __name__ == "__main__":
    unittest.main()
