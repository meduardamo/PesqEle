"""A gravação da aba não pode perder coluna que a aba já tem.

`gravar` apaga a aba e regrava tudo. Enquanto o conjunto de colunas saísse só
do `COLS` de quem está rodando, uma cópia antiga do código apagava, sem erro
nenhum, qualquer coluna criada depois dela: foi o que aconteceu em 02/09/2026
com as três colunas de etapa do tempo integral.
"""
import outros.processar_planos as pp


class AbaFalsa:
    def __init__(self, valores):
        self.valores = valores
        self.gravado = None

    def get_all_values(self):
        return self.valores

    def clear(self):
        self.valores = []

    def update(self, linhas, **kwargs):
        self.gravado = linhas
        self.valores = linhas


class PlanilhaFalsa:
    def __init__(self, aba):
        self.aba = aba

    def worksheet(self, nome):
        return self.aba


def _planilha_com_coluna_extra():
    cabecalho = ["sq_candidato", "tema", "nivel", "analisado_em",
                 "etapas_tempo_integral"]
    linhas = [
        cabecalho,
        ["1", "Tempo Integral", "Propõe ação", "01/09/2026 10:00", "Ensino Médio"],
        ["2", "Tempo Integral", "Define meta", "01/09/2026 11:00", "Educação Infantil"],
    ]
    return PlanilhaFalsa(AbaFalsa(linhas))


COLUNAS_CURTAS = ["sq_candidato", "tema", "nivel", "analisado_em"]


def test_coluna_que_so_existe_na_aba_sobrevive_a_gravacao():
    sh = _planilha_com_coluna_extra()
    pp.gravar(sh, "analise_planos", COLUNAS_CURTAS, ["sq_candidato", "tema"],
              [{"sq_candidato": "3", "tema": "Tempo Integral",
                "nivel": "Menciona vagamente", "analisado_em": "02/09/2026 09:00"}])
    gravado = sh.aba.gravado
    assert gravado[0][-1] == "etapas_tempo_integral"
    por_sq = {linha[0]: linha for linha in gravado[1:]}
    assert por_sq["1"][-1] == "Ensino Médio"
    assert por_sq["2"][-1] == "Educação Infantil"
    # O candidato novo entra sem etapa, que é o que se sabe dele: vazio, e não
    # o valor de outra linha.
    assert por_sq["3"][-1] == ""


def test_candidato_regravado_perde_a_etapa_antiga():
    """Reanálise troca o trecho, e a etapa vinha dele: não pode ser herdada."""
    sh = _planilha_com_coluna_extra()
    pp.gravar(sh, "analise_planos", COLUNAS_CURTAS, ["sq_candidato", "tema"],
              [{"sq_candidato": "1", "tema": "Tempo Integral",
                "nivel": "Define meta", "analisado_em": "02/09/2026 09:00"}])
    por_sq = {linha[0]: linha for linha in sh.aba.gravado[1:]}
    assert por_sq["1"][-1] == ""
    assert por_sq["2"][-1] == "Educação Infantil"
