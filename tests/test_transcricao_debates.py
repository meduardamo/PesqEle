"""Falhas de parse e de nome de falante que já custaram uma transcrição.

Todos os casos saíram da retranscrição do debate de MG de 16/08/2026, que
terminou verde no Actions e devolveu uma transcrição com 20 minutos faltando.

Uso:
    pytest tests/test_transcricao_debates.py
"""

from outros.transcricao_debates import (em_segundos, parsear,
                                        unificar_falantes)


def test_bloco_colado_numa_linha_vira_varios_turnos():
    """O bloco 1 do MG voltou sem quebra de linha nenhuma.

    O regex casava o primeiro turno e os outros 10 minutos iam para dentro
    daquela fala: uma célula de 1.891 palavras com "[00:04] MATEUS SIMÕES:"
    escrito no meio, atribuída inteira a um candidato só.
    """
    texto = ("[00:00] MATEUS SIMÕES: renegociação, porque de fato,"
             "[00:01] MATEUS SIMÕES: eh o acordo do Propague,"
             "[00:04] ALEXANDRE KALIL: não é isso que eu disse")
    linhas, ignoradas, _ = parsear(texto, 0, None)
    assert len(linhas) == 3
    assert ignoradas == 0
    assert [r["falante"] for r in linhas] == [
        "MATEUS SIMÕES", "MATEUS SIMÕES", "ALEXANDRE KALIL"]
    assert linhas[0]["fala"] == "renegociação, porque de fato,"


def test_timestamp_citado_dentro_da_fala_nao_parte_o_turno():
    """A guarda do teste acima não pode virar troca de turno onde não há."""
    linhas, _, _ = parsear(
        "[00:10] KALIL: olha o que ele disse no minuto [00:15] daquele vídeo",
        0, None)
    assert len(linhas) == 1
    assert "[00:15]" in linhas[0]["fala"]


def test_formato_normal_continua_igual():
    linhas, ignoradas, _ = parsear(
        "[00:00] A: um\n[00:05] B: dois\n[01:00] A: três", 0, None)
    assert len(linhas) == 3
    assert ignoradas == 0


def test_sobreposicao_sai_pelo_limite():
    linhas, _, fora = parsear("[00:00] A: um\n[10:30] A: dois", 0, 600)
    assert len(linhas) == 1
    assert fora == 1


def test_offset_soma_no_tempo_absoluto():
    """--inicio 00:50:00 tem que devolver timestamp que casa com o vídeo."""
    linhas, _, _ = parsear("[00:00] A: um", 3000, None)
    assert linhas[0]["segundos"] == 3000
    assert linhas[0]["tempo"] == "00:50:00"


def test_em_segundos():
    assert em_segundos("00:50:00") == 3000
    assert em_segundos("50:00") == 3000
    assert em_segundos("3000") == 3000
    assert em_segundos(None) == 0
    assert em_segundos("") == 0


def test_falante_com_e_sem_acento_vira_um_so():
    """No MG saíram 'LUCAS CATTA PRÊTA' (59 turnos) e 'PRETA' (5)."""
    linhas = [
        {"falante": "LUCAS CATTA PRÊTA", "fala": "a " * 50},
        {"falante": "LUCAS CATTA PRETA", "fala": "b " * 5},
        {"falante": "ALEXANDRE KALIL", "fala": "c " * 30},
    ]
    unificar_falantes(linhas)
    assert {r["falante"] for r in linhas} == {"LUCAS CATTA PRÊTA",
                                             "ALEXANDRE KALIL"}


def test_falante_vence_por_palavra_e_nao_por_turno():
    """Variante que só aparece em aparte não pode virar o nome canônico."""
    linhas = [
        {"falante": "MATEUS SIMOES", "fala": "x"},
        {"falante": "MATEUS SIMOES", "fala": "y"},
        {"falante": "MATEUS SIMÕES", "fala": "z " * 40},
    ]
    unificar_falantes(linhas)
    assert {r["falante"] for r in linhas} == {"MATEUS SIMÕES"}
