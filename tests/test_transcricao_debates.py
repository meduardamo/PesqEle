"""Falhas de parse e de nome de falante que já custaram uma transcrição.

Todos os casos saíram da retranscrição do debate de MG de 16/08/2026, que
terminou verde no Actions e devolveu uma transcrição com 20 minutos faltando.

Uso:
    pytest tests/test_transcricao_debates.py
"""

from outros.transcricao_debates import (aviso_actions, em_segundos, parsear,
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


def test_centesimo_no_timestamp_nao_descarta_a_linha():
    """O bloco 10 do SP voltou como "[01:56.00] FERNANDO HADDAD: ...".

    Eram 96 linhas, 2.033 palavras, todas descartadas por causa do ".00", e o
    log só dizia 'ritmo de fala baixo'. Dez minutos de debate.
    """
    linhas, ignoradas, _ = parsear(
        "[01:56.00] FERNANDO HADDAD: Bom, eu só vou recuperar aqui parte da fala.",
        0, None)
    assert ignoradas == 0
    assert linhas[0]["segundos"] == 116
    assert linhas[0]["falante"] == "FERNANDO HADDAD"


def test_formato_com_travessao_e_fala_na_linha_seguinte():
    """O bloco 11 do MG voltou em "02:08 - NOME:" com a fala embaixo, entre aspas.

    Eram 86 linhas e 3.718 palavras que não entraram na transcrição.
    """
    texto = ('02:08 - MATEUS SIMÕES:\n'
             '"Eu tenho a tranquilidade de ter um estado que saiu sob o meu comando"\n'
             '\n'
             '03:06 - LUCAS CATTA PRÊTA: "Exato, Mateus. Agora para fechar."')
    linhas, ignoradas, _ = parsear(texto, 0, None)
    assert len(linhas) == 2
    assert ignoradas == 0
    assert linhas[0]["segundos"] == 128
    assert linhas[0]["falante"] == "MATEUS SIMÕES"
    assert linhas[0]["fala"].startswith("Eu tenho a tranquilidade")
    assert '"' not in linhas[0]["fala"]
    assert linhas[1]["falante"] == "LUCAS CATTA PRÊTA"


def test_timestamp_citado_dentro_da_fala_nao_parte_o_turno():
    """A guarda do teste acima não pode virar troca de turno onde não há."""
    linhas, _, _ = parsear(
        "[00:10] KALIL: olha o que ele disse no minuto [00:15] daquele vídeo",
        0, None)
    assert len(linhas) == 1
    assert "[00:15]" in linhas[0]["fala"]


def test_tempo_solto_perto_do_marcador_nao_rouba_a_fala_seguinte():
    """Sabatina do PontoPoder de 20/08: um turno inteiro foi para o falante errado.

    O modelo pôs um tempo solto no meio da fala do Ciro e o marcador do
    jornalista veio 29 caracteres depois. O nome do falante podia conter
    colchete, então casou de "[06:27]" até os dois pontos de "[06:28]", a
    quebra entrou no lugar errado e a pergunta do jornalista saiu com falante
    "NO ENSINO MÉDIO DO CEARÁ. [06".
    """
    linhas, _, _ = parsear(
        "[06:20] CIRO GOMES: a grande novidade [06:27] no ensino médio do "
        "Ceará. [06:28] JORNALISTA: Candidato, a chapa do senhor não tem "
        "mulheres.", 0, None)
    assert [l["falante"] for l in linhas] == ["CIRO GOMES", "JORNALISTA"]
    assert linhas[0]["fala"].endswith("no ensino médio do Ceará.")
    assert linhas[1]["fala"].startswith("Candidato,")


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


def test_aviso_actions_vira_anotacao_no_actions(capsys, monkeypatch):
    """Fora do Actions é só log; dentro, sai a anotação que aparece no run."""
    monkeypatch.delenv("GITHUB_ACTIONS", raising=False)
    aviso_actions("resumo automático falhou")
    assert "::warning::" not in capsys.readouterr().out

    monkeypatch.setenv("GITHUB_ACTIONS", "true")
    aviso_actions("resumo automático falhou")
    assert "::warning::resumo automático falhou" in capsys.readouterr().out


def test_aviso_actions_nao_quebra_a_anotacao_em_varias_linhas():
    """Quebra de linha corta a anotação e o resto vira log solto."""
    import io, contextlib, os
    os.environ["GITHUB_ACTIONS"] = "true"
    buf = io.StringIO()
    try:
        with contextlib.redirect_stdout(buf):
            aviso_actions("Traceback\nlinha 2\nlinha 3")
    finally:
        os.environ.pop("GITHUB_ACTIONS", None)
    anotacao = [l for l in buf.getvalue().splitlines() if l.startswith("::warning::")]
    assert len(anotacao) == 1
    assert "linha 3" in anotacao[0]
