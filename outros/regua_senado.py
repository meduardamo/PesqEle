"""Régua do Índice de competitividade eleitoral ao Senado, num lugar só.

Existe porque em 01/09/2026 a calibração e a publicação usavam réguas
diferentes com o mesmo nome: o script de calibração cortava as bandas em
3×ME / 0 / −ME / −2ME e o de publicação em ±ME√2 e ME√2 + k×ME. As taxas
publicadas por banda não eram, portanto, as taxas daquelas bandas.

Três regras que os dois lados obedecem:

1. A banda sai de `nota()`, que é esta função e nenhuma outra.
2. O ranking é dentro de uma rodada de pesquisa. Empilhar institutos do mesmo
   estado numa lista só inventa uma ordem que nenhuma pesquisa mediu.
3. Os dois lados rodam na mesma escala. `normalizar()` põe os percentuais para
   somar 100 entre as candidaturas medidas, que é como o histórico de 2010 e
   2018 foi montado. A média móvel de 2026 soma cerca de 76 por UF, porque
   branco, nulo e indeciso não são redistribuídos, e distância em pontos numa
   escala de 76 não é a mesma coisa que numa escala de 100.
"""
import math

# Erro absoluto médio das pesquisas de Senado contra o resultado, na escala
# comparável: 5,52 pontos em 2010 e 5,12 em 2018.
ME = 5.3
# O empate técnico é a margem da diferença entre dois percentuais, que é a
# margem multiplicada pela raiz de 2. As bandas seguintes contam margens inteiras.
EMPATE = ME * math.sqrt(2)

BANDA = {7: "Lidera isoladamente", 6: "Entre os 2 primeiros",
         5: "Empatado tecnicamente com o 2º", 4: "Até 1 margem de erro atrás",
         3: "Entre 1 e 2 margens de erro atrás", 2: "Entre 2 e 3 margens de erro atrás",
         1: "Mais de 3 margens de erro atrás"}

# Taxa de eleição observada em cada banda, medida com esta régua nas 203 rodadas
# de 2010 e 2018 (1.781 observações). Sai do `research/calibrar_senado.py`, que
# grava `research/dados_senado_historico/taxas_por_banda.csv`.
#
# As bandas 3, 2 e 1 publicam o mesmo número porque as três taxas medidas ficaram
# em 3%, 3% e 2%, sem ordem entre si. Juntas são 929 casos e 2%.
TAXA = {7: "77%", 6: "53%", 5: "28%", 4: "13%", 3: "2%", 2: "2%", 1: "2%"}

# Faixa da classe, no mesmo desenho das outras abas do Radar.
def classe(nota):
    return "Alta" if nota >= 6 else "Média" if nota >= 4 else "Baixa"


def normalizar(valores):
    """Percentuais na escala comparável: somam 100 entre as candidaturas medidas.

    `valores` é a lista de percentuais de um estado ou de uma rodada, já sem os
    ausentes. Devolve a lista na mesma ordem.
    """
    total = sum(valores)
    if total <= 0:
        return list(valores)
    return [v * 100.0 / total for v in valores]


def nota(posicao, percentual, segundo):
    """Banda de quem está na `posicao` (0 é o 1º colocado), na escala comparável.

    2026 tem duas vagas por estado, então a linha de corte é o 2º colocado. Quem
    já está em 1º ou 2º é medido contra o 3º, que é quem tira a cadeira, mas a
    banda de 7 contra 6 depende da vantagem sobre o 2º.
    """
    if posicao == 0:
        return 7 if (percentual - segundo) > EMPATE else 6
    if posicao == 1:
        return 6
    atras = segundo - percentual
    if atras <= EMPATE:
        return 5
    if atras <= EMPATE + ME:
        return 4
    if atras <= EMPATE + 2 * ME:
        return 3
    if atras <= EMPATE + 3 * ME:
        return 2
    return 1


def notas_da_rodada(percentuais):
    """Bandas de uma rodada inteira, a partir dos percentuais na ordem em que vêm.

    Normaliza, ordena, aplica `nota()` e devolve as bandas na ordem de entrada.
    """
    escala = normalizar(percentuais)
    ordem = sorted(range(len(escala)), key=lambda i: -escala[i])
    segundo = escala[ordem[1]] if len(ordem) > 1 else 0.0
    saida = [None] * len(escala)
    for posicao, i in enumerate(ordem):
        saida[i] = nota(posicao, escala[i], segundo)
    return saida
