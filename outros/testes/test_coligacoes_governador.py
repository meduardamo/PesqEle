import unittest

from outros.coligacoes_governador import classify, split_parties


class ColigacoesGovernadorTest(unittest.TestCase):
    def test_expande_federacao_quando_tse_devolve_asteriscos(self):
        parties = split_parties(
            "**",
            "SOLIDARIEDADE",
            "FEDERAÇÃO RENOVAÇÃO SOLIDÁRIA(25-PRD/77-SOLIDARIEDADE)",
        )
        self.assertEqual(parties, ["PRD", "SOLIDARIEDADE"])
        self.assertEqual(classify(parties), "COLIGAÇÃO")

    def test_expande_federacao_dentro_de_coligacao(self):
        parties = split_parties(
            "12-PDT / FEDERAÇÃO BRASIL DA ESPERANÇA(13-PT/65-PC do B/43-PV)",
            "PT",
            "EXEMPLO",
        )
        self.assertEqual(parties, ["PDT", "PT", "PC do B", "PV"])

    def test_partido_isolado_quando_nao_ha_composicao(self):
        parties = split_parties("**", "UP", "UP")
        self.assertEqual(parties, ["UP"])
        self.assertEqual(classify(parties), "PARTIDO ISOLADO")


if __name__ == "__main__":
    unittest.main()
