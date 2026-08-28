"""Classificação temática do clipping do Instagram.

O caso que motivou o arquivo: até 28/08/2026 o casamento era substring pura, e
"rio" (Meio Ambiente) casava dentro de comentáRIOs e pRIOridade. No clipping de
28/08 isso pôs 155 dos 571 posts em Meio Ambiente, 89 só por essa substring.
Cada teste aqui é uma armadilha real vista na planilha, não caso inventado.
"""

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "outros"))

from instagram_relatorio import mapear_tema_principal  # noqa: E402


def tema(tags="", legenda=""):
    return mapear_tema_principal(tags, legenda)


# ── Substring que não é a palavra ────────────────────────────────────────────

def test_rio_dentro_de_outra_palavra_nao_e_meio_ambiente():
    for legenda in ("Obrigado pelos comentários de incentivo!",
                    "Saúde é prioridade no nosso governo",
                    "Estamos no interior do estado",
                    "Feliz aniversário, companheiro",
                    "Reunião com o secretário e o ministério"):
        assert tema("Campanha eleitoral", legenda) != "Meio Ambiente", legenda


def test_saudade_nao_e_saude():
    assert tema("Campanha eleitoral", "Que saudade de vocês, meu povo") != "Saúde"


def test_escolha_nao_e_educacao():
    assert tema("Campanha eleitoral", "A escolha é do povo") != "Educação"


def test_redes_sociais_nao_e_assistencia_social():
    assert tema("Propaganda eleitoral",
                "Me segue nas redes sociais") != "Assistência Social"


def test_solidariedade_partido_nao_e_assistencia_social():
    assert tema("Campanha eleitoral",
                "Clécio Luís, do Solidariedade") != "Assistência Social"


def test_discriminacao_nao_e_seguranca():
    assert tema("Direitos humanos",
                "Não aceitamos discriminação") != "Segurança Pública"


def test_hospitalidade_nao_e_saude():
    assert tema("Campanha eleitoral",
                "A hospitalidade do povo daqui") != "Saúde"


# ── Nome de lugar não é assunto ──────────────────────────────────────────────

def test_toponimo_com_rio_ou_floresta_nao_e_meio_ambiente():
    for legenda in ("Comício em Nísia Floresta",
                    "Carreata em Mãe do Rio",
                    "Caminhada em Rio Branco",
                    "Agenda no Rio Grande do Norte"):
        assert tema("Campanha eleitoral", legenda) != "Meio Ambiente", legenda


def test_floresta_sozinha_continua_valendo():
    assert tema("Meio ambiente", "Vamos preservar a floresta") == "Meio Ambiente"


# ── Hashtag em CamelCase ─────────────────────────────────────────────────────

def test_hashtag_camelcase_vira_palavra():
    assert tema("", "#PreservacaoAmbiental #Floresta") == "Meio Ambiente"


# ── O assunto ganha da campanha ──────────────────────────────────────────────

def test_pauta_ganha_do_residual():
    assert tema("Propaganda eleitoral Segurança pública",
                "Concurso para 5 mil policiais militares") == "Segurança Pública"
    assert tema("Campanha eleitoral Saúde pública",
                "Vamos entregar o hospital") == "Saúde"


def test_post_so_de_campanha_fica_em_atos_de_campanha():
    assert tema("Campanha eleitoral Carreata",
                "Hoje tem carreata, bora pra rua!") == "Atos de Campanha e Propaganda"


def test_tag_do_gemini_pesa_mais_que_a_legenda():
    # A legenda cita trabalho (termo fraco); a tag diz do que o post é.
    assert tema("Educação em tempo integral",
                "Mais uma manhã de trabalho") == "Educação"


# ── Alianças ─────────────────────────────────────────────────────────────────

def test_apoio_da_rua_nao_e_alianca():
    assert tema("Campanha eleitoral",
                "Que apoio lindo do povo na caminhada") != "Alianças e Apoios Políticos"


def test_apoio_politico_e_alianca():
    assert tema("Apoio político Coligação",
                "Anunciamos o apoio do partido") == "Alianças e Apoios Políticos"


# ── Tema estreito ────────────────────────────────────────────────────────────

def test_causa_animal_ganha_de_seguranca():
    assert tema("Causa animal / Proteção animal Maus-tratos aos animais "
                "Segurança pública Delegacia especializada",
                "Delegacia de proteção animal") == "Causa Animal"


# ── Sem sinal nenhum ─────────────────────────────────────────────────────────

def test_sem_termo_cai_em_outros_assuntos():
    assert tema("", "") == "Outros Assuntos"
    assert tema("Bastidores", "Bom dia!") == "Outros Assuntos"


def test_detalhar_devolve_a_evidencia():
    resultado = mapear_tema_principal("Saúde pública", "Novo hospital", detalhar=True)
    tema_achado, pontos, termos = resultado
    assert tema_achado == "Saúde"
    assert pontos > 0
    assert "hospital" in termos
