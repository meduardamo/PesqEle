"""Contrato do cache Parquet publicado para os painéis.

O `values_to_df` daqui tem que continuar idêntico ao de `data_loaders.py` dos
painéis. Se divergirem, o caminho rápido (Parquet) e o fallback (Sheets)
passam a devolver DataFrames diferentes, e o painel muda de comportamento
conforme o Drive esteja no ar ou não. Estes testes fixam as duas regras que
não são óbvias: linha curta é preenchida, coluna inteiramente vazia some.
"""
import io
import unittest

import pandas as pd

from compartilhado.cache_parquet import nome_arquivo, values_to_df


class ValuesToDfTest(unittest.TestCase):
    def test_linha_curta_e_preenchida_com_vazio(self):
        # A outra linha precisa ter valor em `pct`: as duas regras se compõem, e
        # uma coluna que só recebeu preenchimento seria removida logo em seguida.
        df = values_to_df([["uf", "cargo", "pct"],
                           ["BR", "presidente"],
                           ["SP", "governador", "12,3"]])
        self.assertEqual(list(df.columns), ["uf", "cargo", "pct"])
        self.assertEqual(df.iloc[0]["pct"], "")

    def test_coluna_inteiramente_vazia_e_removida(self):
        # É por isso que o resultados_bi da T1 chega no painel com 16 colunas e
        # não com as 17 da aba: `disputa` só tem valor no 2º turno.
        df = values_to_df([["uf", "disputa"], ["BR", ""], ["SP", "  "]])
        self.assertEqual(list(df.columns), ["uf"])

    def test_coluna_sem_cabecalho_ganha_nome_posicional(self):
        df = values_to_df([["uf", ""], ["BR", "x"]])
        self.assertEqual(list(df.columns), ["uf", "col_2"])

    def test_linha_a_mais_e_cortada_no_tamanho_do_cabecalho(self):
        df = values_to_df([["uf"], ["BR", "sobra"]])
        self.assertEqual(list(df.columns), ["uf"])
        self.assertEqual(len(df), 1)

    def test_planilha_vazia_devolve_dataframe_vazio(self):
        self.assertTrue(values_to_df([]).empty)


class RoundTripTest(unittest.TestCase):
    def test_parquet_devolve_o_mesmo_dataframe(self):
        """O painel tem que receber do Parquet o que receberia do Sheets."""
        valores = [["uf", "cargo", "pct"],
                   ["BR", "presidente", "34,9"],
                   ["SP", "governador", ""]]
        original = values_to_df(valores)

        buffer = io.BytesIO()
        original.to_parquet(buffer, compression="zstd", index=False)
        devolvido = pd.read_parquet(io.BytesIO(buffer.getvalue()))

        self.assertTrue(original.equals(devolvido))
        self.assertEqual(list(original.dtypes), list(devolvido.dtypes))


class NomeArquivoTest(unittest.TestCase):
    def test_nome_e_deterministico(self):
        # O painel monta esse mesmo nome para achar o arquivo, sem índice.
        self.assertEqual(nome_arquivo("abc123", "resultados_bi"),
                         "abc123__resultados_bi.parquet")


if __name__ == "__main__":
    unittest.main()
