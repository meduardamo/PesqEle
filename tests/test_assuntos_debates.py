"""Casos medidos nos debates já processados, presos como teste.

Cada caso aqui saiu de conferir uma fala real dos debates da Band de SP
(09/08/2026) e de MG (16/08/2026) contra a marcação que o dicionário produziu.
Metade são falsos positivos que o dicionário fazia e não faz mais; a outra
metade é marcação boa que precisa continuar saindo depois do ajuste, porque a
correção de um termo ambíguo é o tipo de mudança que apaga acerto sem avisar.

Uso:
    pytest tests/test_assuntos_debates.py
"""

import pytest

from outros.assuntos_debates import assuntos_da_fala, compilar, montar_termos

COMP_EIXO = compilar(montar_termos(por_tema=False))
COMP_TEMA = compilar(montar_termos(por_tema=True))


def marcar(fala):
    return (sorted(assuntos_da_fala(fala, COMP_EIXO, por_tema=False)),
            sorted(assuntos_da_fala(fala, COMP_TEMA, por_tema=True)))


# (fala, eixos, temas)
CASOS = [
    # ------------------------------------------------ o que passou a marcar
    # SP 0:23:50, 28 palavras sobre celular roubado, saía sem eixo nenhum: o
    # dicionário tinha 'roubo' e o debate fala no particípio.
    ("Me dói saber que um cidadão de bem tem o celular roubado",
     ["Criminalidade e violência"], ["Criminalidade e violência"]),
    # SP 0:24:27, videomonitoramento está na definição de 'Policiamento e
    # Efetivo' e não tinha termo nenhum.
    ("Observe a integração que existe hoje entre o Smart Sampa e o Muralha "
     "Paulista, e ali tem várias câmeras",
     ["Segurança pública"], ["Policiamento e Efetivo"]),

    # ------------------------------------------- o que deixou de marcar erra
    # 'seguranca publica' é o nome do eixo, não é resultado de crime: marca o
    # eixo e não escolhe tema.
    ("nós precisamos discutir a segurança pública deste estado",
     ["Segurança pública"], []),
    # MG: 'internet' solta marcava Tecnologia na Educação.
    ("Dá uma busca na internet", [], []),
    # SP: 'credito' solto marcava Ambiente de Negócios.
    ("as empresas estão tendo dificuldade com o problema do crédito", [], []),
    # MG 1:27:09: 'patrimonio' solto marcava Cultura.
    ("Um carro que é seu, que é do patrimônio do cidadão", [], []),

    # ------------------------------------- o que não pode ter sido derrubado
    ("vamos levar internet nas escolas de todo o estado",
     ["Educação"], ["Tecnologia na Educação"]),
    ("crédito rural para o produtor rural do estado",
     ["Economia e emprego"], ["Agropecuária"]),
    # MG 1:12:12, a única ocorrência de patrimônio que era cultura de verdade.
    ("a Serra do Curral é patrimônio dos mineiros, é tombada e deve ser preservada",
     ["Cultura, esporte e turismo"], ["Cultura"]),
    ("vamos ampliar o efetivo policial e o enfrentamento ao crime organizado",
     ["Segurança pública"],
     ["Enfrentamento ao Crime Organizado", "Policiamento e Efetivo"]),
    ("a dívida do estado com a União e o Propague",
     ["Finanças estaduais e dívida"], ["Finanças estaduais e dívida"]),
    # O caso que criou o TERMOS_AMBIGUOS, para não voltar.
    ("o investimento não avança", [], []),
]


@pytest.mark.parametrize("fala,eixos,temas", CASOS,
                         ids=[c[0][:45] for c in CASOS])
def test_marcacao(fala, eixos, temas):
    obtidos_eixo, obtidos_tema = marcar(fala)
    assert obtidos_eixo == eixos
    assert obtidos_tema == temas


def test_termo_so_eixo_nao_escolhe_tema():
    """'escola' diz Educação e não diz a etapa: eixo sim, tema não."""
    eixos, temas = marcar("a escola precisa de mais aluno na sala de aula")
    assert eixos == ["Educação"]
    assert temas == []
