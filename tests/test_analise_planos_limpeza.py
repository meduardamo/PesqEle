from outros.analise_planos import (
    _limpa, conferir_etapas_tempo_integral, limpar_ruido_citacao)


def test_tira_numero_editorial_sem_tirar_meta():
    assert limpar_ruido_citacao("25-Lançamento de programa;") == "Lançamento de programa;"
    assert limpar_ruido_citacao("3.2 - Ampliar a rede.") == "Ampliar a rede."
    assert limpar_ruido_citacao("Reduzir em 30% até 2030.") == "Reduzir em 30% até 2030."
    assert limpar_ruido_citacao("2030 - meta de erradicação") == "2030 - meta de erradicação"


def test_troca_bullets_por_pontuacao():
    trecho = "HUBS REGIONAIS DE ALIMENTOS • Assistência técnica. • Venda para escolas."
    assert limpar_ruido_citacao(trecho) == (
        "Hubs Regionais de Alimentos; Assistência técnica. Venda para escolas."
    )


def test_corrige_hifen_separado_por_quebra():
    assert limpar_ruido_citacao("programas de pós- graduação") == "programas de pós-graduação"


def test_baixa_so_bloco_inicial_em_caixa_alta_e_preserva_siglas():
    trecho = "O HOSPITAL DA MULHER REFERÊNCIA NACIONAL EM SAÚDE INTEGRAL [...] Implantação"
    assert limpar_ruido_citacao(trecho) == (
        "O Hospital da Mulher Referência Nacional em Saúde Integral [...] Implantação"
    )
    assert limpar_ruido_citacao("O SUS DE GOIÁS SERÁ REFORÇADO com a SES") == (
        "O SUS de Goiás Será Reforçado com a SES"
    )


def test_limpa_corta_depois_de_remover_ruido():
    assert _limpa("69- Implementação da educação inclusiva;", ruido_citacao=True) == (
        "Implementação da educação inclusiva;"
    )


def test_etapas_explicitamente_ligadas_ao_tempo_integral():
    evidencia = "Ampliar o ensino fundamental e o ensino médio em tempo integral."
    r = conferir_etapas_tempo_integral(
        ["Ensino Fundamental", "Ensino Médio"], evidencia, "explícita",
        evidencia, "Propõe ação")
    assert r["etapas_tempo_integral"] == "Ensino Fundamental | Ensino Médio"
    assert r["etapas_inferidas_tempo_integral"] == ""


def test_rede_estadual_e_ensino_medio_inferido():
    evidencia = "Expandir as escolas estaduais de tempo integral."
    r = conferir_etapas_tempo_integral(
        ["Ensino Médio"], evidencia, "inferida", evidencia, "Propõe ação")
    assert r["etapas_tempo_integral"] == "Ensino Médio"
    assert r["etapas_inferidas_tempo_integral"] == "Ensino Médio"


def test_escola_sem_etapa_fica_nao_especificada():
    evidencia = "Expandir as escolas de tempo integral."
    r = conferir_etapas_tempo_integral(
        ["Ensino Médio"], evidencia, "inferida", evidencia, "Propõe ação")
    assert r["etapas_tempo_integral"] == "Etapa não especificada"
    assert r["etapas_inferidas_tempo_integral"] == ""


def test_tempo_integral_ausente_nao_se_aplica():
    r = conferir_etapas_tempo_integral(
        ["Ensino Médio"], "", "inferida", "", "Não menciona")
    assert r["etapas_tempo_integral"] == "Não se aplica"
