import os
import re
import time
import json
import hashlib
import unicodedata
from datetime import datetime
import zoneinfo
import pandas as pd
import gspread
from gspread import Cell
from gspread.utils import rowcol_to_a1
from google.oauth2.service_account import Credentials

from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from compartilhado.relatorios_sheets_utils import (autorizar_com_retry as _autorizar,
                                                   reescrever_aba)

try:
    from webdriver_manager.chrome import ChromeDriverManager
    _HAS_WDM = True
except Exception:
    _HAS_WDM = False


UFS = [
    "ac", "al", "am", "ap", "ba", "ce", "df", "es", "go",
    "ma", "mg", "ms", "mt", "pa", "pb", "pe", "pi", "pr",
    "rj", "rn", "ro", "rr", "rs", "sc", "se", "sp", "to"
]

PRESIDENTE_URLS_DEFAULT = [
    "https://www.pollingdata.com.br/2026/presidente/br/2026_presidente_br_t1_lula-flavio-sem-bolsonaros.html",
]

PRESIDENTE_T2_URLS_DEFAULT = [
    "https://www.pollingdata.com.br/2026/presidente/br/t2_flavio-lula/",
    "https://www.pollingdata.com.br/2026/presidente/br/t2_lula-caiado/",
    "https://www.pollingdata.com.br/2026/presidente/br/t2_lula-zema/",
    "https://www.pollingdata.com.br/2026/presidente/br/t2_lula-michelle/",
    "https://www.pollingdata.com.br/2026/presidente/br/t2_joaquim-lula/",
]

WAIT_CSS = "div#dados-das-pesquisas"
FORCAR_LOCALE_PLANILHA = True
LOCALE_PLANILHA = "en_US"
# Campo canônico de procedência. `origem` substitui a antiga coluna técnica
# `conferida` nas matrizes PollingData.
ORIGEM_POLLINGDATA = "PollingData (raspagem)"
ORIGEM_POLLING_MANUAL = "polling_manual"
ORIGEM_PDF_RELATORIO = "PDF (relatório do instituto)"

# Nota de confiabilidade de cada instituto, na escala do Ranking de Institutos
# de Pesquisa do Pindograma (A, B+, B, B-, C, D), calculado sobre pesquisas de
# 2012 a 2022 com método inspirado no FiveThirtyEight e atualizado em
# 24/09/2024. Fonte: https://pindograma.com.br/ranking.html, dados públicos em
# github.com/pindograma/pindograma.github.io (assets/ranking_data_4.js).
#
# Trocamos o ranking do PollsterGraph pelo do Pindograma em 01/09/2026 por três
# motivos: o PollsterGraph é desenvolvido pela AtlasIntel, que é um dos
# institutos avaliados e aparece em A+ nele; o Pindograma avalia sobre muito
# mais pesquisas por instituto (Datafolha em 190 contra 29); e publica a base
# aberta, o que deixa a nota auditável. O Pindograma fornece os dados que o
# PollsterGraph agrega, mas os dois ranqueamentos são metodologias distintas.
#
# Instituto que o Pindograma não avalia fica como "Ainda não foi avaliado" em
# vez de herdar a nota antiga: misturar duas escalas daria peso sem sentido.
#
# 02/09/2026: a tabela passou a ser a base inteira, gerada de ranking_data_4.js,
# e não mais uma transcrição manual. A versão anterior tinha 68 dos 196
# institutos publicados, então 134 caíam em "Ainda não foi avaliado" com a nota
# existindo, entre eles Ideia Big Data (B+), FSB Pesquisa (B), PoderData (B) e
# Sensus (B-). Nenhuma das 68 notas transcritas estava errada; o que faltava era
# o resto. Onde o nome que a matriz usa é mais curto que o do Pindograma
# (Amostragem, Methodus, Ipec), a chave continua sendo o nome da matriz e o nome
# publicado entrou como alias, para não renomear o que já está gravado.
#
# "Sensor Pesquisas" ficou de fora de propósito: aparece duas vezes na base, com
# CNPJs diferentes e notas diferentes (B- e D). Pelo nome não dá para saber qual
# das duas empresas assina a pesquisa, e escolher uma seria inventar.
CLASSIFICACAO_INSTITUTOS = {
    '6 Sigma': 'B',
    'ABC Dados': 'B-',
    'Access': 'C',
    'Acertar': 'B-',
    'Action Marketing': 'B+',
    'Advise': 'D',
    'Agili Pesquisas': 'B',
    'Agora Pesquisa (BA)': 'B',
    'Agorasei': 'B',
    'Alternativa Dados': 'B-',
    'American Analytics': 'B',
    'Amostragem': 'A',
    'AR7 Pesquisa': 'C',
    'AtlasIntel': 'A',
    'Avaliar Pesquisa': 'B',
    'Badra Comunicação': 'B+',
    'BMO': 'B',
    'BR Marketing': 'D',
    'Brand Consultoria': 'B',
    'Brasil Dados': 'B',
    'Brasmarket': 'D',
    'Camminus Marketing': 'B+',
    'Captavox': 'B+',
    'Census': 'B-',
    'Certifica Consultoria': 'B',
    'Certus': 'B',
    'Colectta': 'B',
    'Compet': 'B',
    'Comunicare': 'B-',
    'Conpar': 'B+',
    'Consult Pesquisa': 'B',
    'Consulte Inteligência (PI)': 'B+',
    'Cooperfil': 'C',
    'Correio do Povo Tocantinense': 'B',
    'CP2 Pesquisa': 'B',
    'CTAS Tecnologia': 'B',
    'Dados Pesquisa (GO)': 'B',
    'Data AZ': 'B-',
    'Data Fato': 'B-',
    'Data M': 'B-',
    'Dataconsulte': 'C',
    'Datafolha': 'B+',
    'Dataform': 'B',
    'Datamax': 'B',
    'Datamédia': 'C',
    'Dataqualy': 'B',
    'Datasonda': 'B',
    'Datavox (PB)': 'B-',
    'Delta Agência de Pesquisa': 'B',
    'Desttaq': 'B-',
    'DGABC Pesquisas': 'B-',
    'Diagnóstico Pesquisas': 'B',
    'Directa Pesquisa': 'D',
    'DMP': 'B-',
    'Doxa': 'B',
    'Econométrica': 'B',
    'Eficaz Pesquisas': 'B+',
    'Empresa de Pesquisas Técnicas': 'B',
    'Enquet Pesquisas': 'B',
    'EPP': 'A',
    'Equação Pesquisas': 'B',
    'Escutec': 'B',
    'Estimativa': 'D',
    'Exata OP': 'B-',
    'Exata Pesquisa (MA)': 'B+',
    'Exata.GO': 'D',
    'Exatus': 'B',
    'F5 Atualiza Dados': 'B+',
    'Falpe Pesquisas': 'D',
    'Fernandes Consultoria': 'B-',
    'FLS Pesquisa': 'B',
    'Folha Capital': 'B',
    'Fortiori': 'B-',
    'FSB Pesquisa': 'B',
    'Futura': 'A',
    'Gazeta Dados': 'B+',
    'GERP': 'B-',
    'Goiás Pesquisas': 'B',
    'Govnet/Instituto Opinião (SP)': 'B+',
    'GPP': 'B',
    'Grupo M': 'B-',
    'IABR': 'B',
    'IBPS': 'B',
    'Ideia Big Data': 'B+',
    'IMAPE': 'B-',
    'Incope': 'B+',
    'Index Pesquisas': 'B-',
    'Infornews': 'B',
    'INOP': 'D',
    'INOPE': 'B+',
    'INOR': 'C',
    'Inove Consultoria': 'B+',
    'Instituto Credibilidade': 'C',
    'Instituto Datailha': 'B-',
    'Instituto Datasensus': 'B+',
    'Instituto de Pesquisa Resultado (MS)': 'B',
    'Instituto Estimativa': 'B-',
    'Instituto Exatta (PE)': 'B-',
    'Instituto França': 'B-',
    'Instituto Gauss': 'C',
    'Instituto Haverroth': 'B',
    'Instituto Jales': 'B-',
    'Instituto Múltipla': 'B',
    'Instituto Opinião (PR)': 'B',
    'Instituto Podium': 'B-',
    'Instituto Qualitativa': 'B',
    'Instituto Ranking': 'B',
    'Instituto Realidade': 'D',
    'Instituto Seta': 'B',
    'Instituto Skala': 'B',
    'Instituto Stylo': 'B',
    'Instituto Vision Survey': 'B-',
    'Instituto VOPE': 'C',
    'Inteligence Serviços': 'D',
    'IPAT': 'B+',
    'IPC': 'B',
    'Ipec': 'A',
    'IPEMS': 'B+',
    'Ipespe': 'B',
    'IPMN': 'B-',
    'IPPEC (PR)': 'B',
    'IPPI': 'B',
    'IRG Consultoria': 'B+',
    'Item Pesquisas': 'B+',
    'J. J. Coelho': 'C',
    'Jornal Correio Continental': 'D',
    'Jornal Girassol': 'B',
    'Jornal O Coletivo': 'B-',
    'Jornal O+Positivo': 'B',
    'Jornal Sudoeste (BA)': 'D',
    'London Pesquisas': 'D',
    'M S Passos Comunicação': 'B',
    'Malujoa Comunicações': 'D',
    'Mapa Marketing': 'A',
    'Mark Instituto de Pesquisa': 'B-',
    'MAS Opinião': 'B+',
    'MBO': 'B-',
    'MDA': 'B+',
    'Merita Comunicações': 'C',
    'Methodus': 'B+',
    'MT Dados': 'B',
    'Multidados': 'C',
    'Naipes Marketing': 'B-',
    'Novo Ibrape': 'B+',
    'Opinar': 'B',
    'Opinião Brasil (DF)': 'B-',
    'Opinião Pesquisas (PB)': 'B+',
    'Painel Brasil': 'B',
    'Paraná Pesquisas': 'B+',
    'Perfil Pesquisas Técnicas': 'B',
    'Perspectiva Mercado e Opinião': 'B',
    'PoderData': 'B',
    'Polo Pesquisas': 'B+',
    'Pontual Pesquisas': 'B-',
    'Pop Pesquisas': 'B-',
    'Porto News': 'B-',
    'Potencial': 'B',
    'Prever Pesquisas': 'B+',
    'Promídia': 'B-',
    'Quaest': 'B',
    'Quallity Serviços Inteligente': 'D',
    'R M Mariath': 'B-',
    'Radar Inteligência': 'B+',
    'Real Dados': 'A',
    'Real Time Big Data': 'B',
    'RF Consultoria': 'C',
    'RPP Logística': 'B-',
    'Rádio Maracu': 'C',
    'Sebram': 'D',
    'Seculus': 'C',
    'Sensatus Pesquisa': 'B',
    'Sensus': 'B-',
    'Serpes': 'B+',
    'Sinfor': 'B+',
    'Solução Treinamento': 'B+',
    'Studio Pesquisas': 'B',
    'Surgiu Pesquisas': 'B',
    'TDL Pesquisa': 'D',
    'Tecnodados': 'B',
    'Tendência Pesquisa (SC)': 'B',
    'TS2 Soluções': 'B-',
    'UP Pesquisas': 'B-',
    'Vale Consultoria': 'B-',
    'Veiga Consultoria e Pesquisa': 'B',
    'Ver Pesquisa': 'A',
    'Veritá': 'B',
    'Vetor Assessoria': 'B+',
    'Visão Pesquisas': 'D',
    'Voga Pesquisas': 'C',
    'Voice Pesquisas (MT)': 'B',
    'Vox Opinião Pública (SP)': 'B-',
    'Vox Populi': 'B',
    'W J Mendes': 'B',
    'Zaytec Brasil': 'B',
    'Ágora Pesquisa (RJ)': 'A',
}

# Índice contínuo de desempenho publicado pelo Pindograma (`pred_pm_adj`).
# Menor é melhor. A letra do Pindograma comprime 75 institutos na faixa B; a
# O painel usa o mesmo índice, sem alterar a ordem, e o divide em dez grupos de
# tamanho semelhante para comunicar melhor as diferenças internas.
INDICE_PINDOGRAMA = {
    '6 Sigma': 0.390,
    'ABC Dados': 0.941,
    'Access': 1.264,
    'Acertar': 0.855,
    'Action Marketing': -0.322,
    'Advise': 1.779,
    'Agili Pesquisas': 0.485,
    'Agora Pesquisa (BA)': 0.124,
    'Agorasei': 0.216,
    'Alternativa Dados': 0.820,
    'American Analytics': 0.065,
    'Amostragem': -1.022,
    'AR7 Pesquisa': 1.080,
    'AtlasIntel': -0.576,
    'Avaliar Pesquisa': 0.255,
    'Badra Comunicação': 0.000,
    'BMO': 0.267,
    'BR Marketing': 2.066,
    'Brand Consultoria': 0.229,
    'Brasil Dados': 0.223,
    'Brasmarket': 2.001,
    'Camminus Marketing': -0.298,
    'Captavox': -0.153,
    'Census': 0.627,
    'Certifica Consultoria': 0.305,
    'Certus': 0.314,
    'Colectta': 0.399,
    'Compet': 0.398,
    'Comunicare': 0.658,
    'Conpar': -0.161,
    'Consult Pesquisa': 0.124,
    'Consulte Inteligência (PI)': -0.344,
    'Cooperfil': 1.262,
    'Correio do Povo Tocantinense': 0.196,
    'CP2 Pesquisa': 0.225,
    'CTAS Tecnologia': 0.001,
    'Dados Pesquisa (GO)': 0.230,
    'Data AZ': 0.713,
    'Data Fato': 0.600,
    'Data M': 0.630,
    'Dataconsulte': 1.401,
    'Datafolha': -0.395,
    'Dataform': 0.100,
    'Datamax': 0.463,
    'Datamédia': 1.394,
    'Dataqualy': 0.342,
    'Datasonda': 0.403,
    'Datavox (PB)': 0.674,
    'Delta Agência de Pesquisa': 0.271,
    'Desttaq': 0.815,
    'DGABC Pesquisas': 0.575,
    'Diagnóstico Pesquisas': 0.197,
    'Directa Pesquisa': 1.615,
    'DMP': 0.553,
    'Doxa': 0.212,
    'Econométrica': 0.252,
    'Eficaz Pesquisas': -0.049,
    'Empresa de Pesquisas Técnicas': 0.301,
    'Enquet Pesquisas': 0.475,
    'EPP': -0.589,
    'Equação Pesquisas': 0.124,
    'Escutec': 0.042,
    'Estimativa': 1.610,
    'Exata OP': 0.649,
    'Exata Pesquisa (MA)': -0.048,
    'Exata.GO': 1.624,
    'Exatus': 0.416,
    'F5 Atualiza Dados': -0.338,
    'Falpe Pesquisas': 1.995,
    'Fernandes Consultoria': 0.871,
    'FLS Pesquisa': 0.205,
    'Folha Capital': 0.343,
    'Fortiori': 0.501,
    'FSB Pesquisa': 0.015,
    'Futura': -0.752,
    'Gazeta Dados': -0.429,
    'GERP': 0.844,
    'Goiás Pesquisas': 0.261,
    'Govnet/Instituto Opinião (SP)': -0.352,
    'GPP': 0.170,
    'Grupo M': 0.559,
    'IABR': 0.043,
    'IBPS': 0.172,
    'Ideia Big Data': -0.159,
    'IMAPE': 0.970,
    'Incope': -0.366,
    'Index Pesquisas': 0.516,
    'Infornews': 0.185,
    'INOP': 2.066,
    'INOPE': -0.020,
    'INOR': 1.080,
    'Inove Consultoria': -0.157,
    'Instituto Credibilidade': 1.076,
    'Instituto Datailha': 0.769,
    'Instituto Datasensus': -0.031,
    'Instituto de Pesquisa Resultado (MS)': 0.102,
    'Instituto Estimativa': 0.920,
    'Instituto Exatta (PE)': 0.580,
    'Instituto França': 0.588,
    'Instituto Gauss': 1.146,
    'Instituto Haverroth': 0.363,
    'Instituto Jales': 0.553,
    'Instituto Múltipla': 0.412,
    'Instituto Opinião (PR)': 0.313,
    'Instituto Podium': 0.778,
    'Instituto Qualitativa': 0.324,
    'Instituto Ranking': 0.093,
    'Instituto Realidade': 2.322,
    'Instituto Seta': 0.237,
    'Instituto Skala': 0.408,
    'Instituto Stylo': 0.136,
    'Instituto Vision Survey': 0.580,
    'Instituto VOPE': 1.240,
    'Inteligence Serviços': 1.644,
    'IPAT': -0.070,
    'IPC': 0.332,
    'Ipec': -0.606,
    'IPEMS': -0.473,
    'Ipespe': 0.173,
    'IPMN': 0.910,
    'IPPEC (PR)': 0.084,
    'IPPI': 0.334,
    'IRG Consultoria': -0.382,
    'Item Pesquisas': -0.242,
    'J. J. Coelho': 1.039,
    'Jornal Correio Continental': 1.658,
    'Jornal Girassol': 0.004,
    'Jornal O Coletivo': 0.996,
    'Jornal O+Positivo': 0.305,
    'Jornal Sudoeste (BA)': 2.269,
    'London Pesquisas': 1.731,
    'M S Passos Comunicação': 0.352,
    'Malujoa Comunicações': 2.326,
    'Mapa Marketing': -0.581,
    'Mark Instituto de Pesquisa': 0.671,
    'MAS Opinião': -0.372,
    'MBO': 0.757,
    'MDA': -0.425,
    'Merita Comunicações': 1.365,
    'Methodus': -0.308,
    'MT Dados': 0.162,
    'Multidados': 1.146,
    'Naipes Marketing': 0.531,
    'Novo Ibrape': -0.048,
    'Opinar': 0.093,
    'Opinião Brasil (DF)': 0.679,
    'Opinião Pesquisas (PB)': -0.192,
    'Painel Brasil': 0.232,
    'Paraná Pesquisas': -0.421,
    'Perfil Pesquisas Técnicas': 0.088,
    'Perspectiva Mercado e Opinião': 0.277,
    'PoderData': 0.367,
    'Polo Pesquisas': -0.278,
    'Pontual Pesquisas': 0.638,
    'Pop Pesquisas': 0.868,
    'Porto News': 0.886,
    'Potencial': 0.026,
    'Prever Pesquisas': -0.164,
    'Promídia': 0.826,
    'Quaest': 0.013,
    'Quallity Serviços Inteligente': 1.969,
    'R M Mariath': 0.738,
    'Radar Inteligência': -0.324,
    'Real Dados': -0.735,
    'Real Time Big Data': 0.066,
    'RF Consultoria': 1.381,
    'RPP Logística': 0.930,
    'Rádio Maracu': 1.104,
    'Sebram': 2.278,
    'Seculus': 1.055,
    'Sensatus Pesquisa': 0.277,
    'Sensus': 0.961,
    'Serpes': -0.056,
    'Sinfor': -0.037,
    'Solução Treinamento': -0.203,
    'Studio Pesquisas': 0.049,
    'Surgiu Pesquisas': 0.444,
    'TDL Pesquisa': 1.546,
    'Tecnodados': 0.313,
    'Tendência Pesquisa (SC)': 0.410,
    'TS2 Soluções': 0.864,
    'UP Pesquisas': 0.762,
    'Vale Consultoria': 0.665,
    'Veiga Consultoria e Pesquisa': 0.133,
    'Ver Pesquisa': -0.666,
    'Veritá': 0.441,
    'Vetor Assessoria': -0.042,
    'Visão Pesquisas': 1.877,
    'Voga Pesquisas': 1.061,
    'Voice Pesquisas (MT)': 0.031,
    'Vox Opinião Pública (SP)': 0.562,
    'Vox Populi': 0.067,
    'W J Mendes': 0.467,
    'Zaytec Brasil': 0.313,
    'Ágora Pesquisa (RJ)': -0.532,
}

ORDEM_CLASSIFICACOES = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D"]


def _montar_classificacoes_por_indice() -> tuple[dict[str, str], dict[str, int]]:
    ordenados = sorted(INDICE_PINDOGRAMA, key=INDICE_PINDOGRAMA.get)
    total = len(ordenados)
    faixas = {}
    posicoes = {}
    for i, nome in enumerate(ordenados):
        faixas[nome] = ORDEM_CLASSIFICACOES[min(9, int(i * 10 / total))]
        posicoes[nome] = i + 1
    return faixas, posicoes


CLASSIFICACAO_POR_INDICE, POSICAO_PINDOGRAMA = _montar_classificacoes_por_indice()


METODOLOGIA_INSTITUTOS = {
    "Datafolha": "Pesquisa quantitativa, por amostragem, com aplicação de questionário estruturado e abordagem presencial em pontos de fluxo. Universo: população brasileira com 16 anos ou mais.",
    "AtlasIntel": "Pesquisa quantitativa, com coleta online via questionário estruturado e pós-estratificação da amostra conforme o perfil do eleitorado nacional.",
    "Jornal Girassol": "Pesquisa quantitativa, com aplicação de questionário estruturado em entrevistas presenciais junto ao eleitorado, realizadas em domicílios e estabelecimentos comerciais na zona urbana. Uma entrevista por domicílio ou estabelecimento.",
    "Gazeta Dados": "Pesquisa quantitativa, com aplicação de questionário estruturado para aferição de intenção de voto e avaliação de governo.",
    "MDA": "Pesquisa quantitativa, com entrevistas presenciais realizadas em domicílios e, complementarmente, em pontos de fluxo.",
    "Badra Comunicação": "Pesquisa quantitativa, por amostragem por cotas, com seleção de entrevistados proporcional ao perfil do universo pesquisado.",
    "Ibope": "Pesquisa quantitativa, com entrevistas telefônicas e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado.",
    "DMP": "Pesquisa quantitativa, com entrevistas presenciais e aplicação de questionário estruturado, por amostragem probabilística simples. Universo: eleitorado das zonas eleitorais do município.",
    "Paraná Pesquisas": "Pesquisa quantitativa, com aplicação de questionário em formato espontâneo (sem apresentação de nomes) e estimulado (com apresentação de nomes).",
    "Real Time Big Data": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Grupo M": "Pesquisa científica estruturada, com foco em modelagem de sistemas de informação.",
    "Futura": "Pesquisa quantitativa, com foco em intenção de voto e avaliação de governo.",
    "Instituto Amostragem": "Pesquisa qualitativa, realizada por meio de grupos de discussão (focus group), entrevistas semiestruturadas e entrevistas em profundidade.",
    "Instituto Gasparetto de Pesquisas": "Pesquisa quantitativa, com entrevistas presenciais e aplicação de questionário estruturado, por amostragem aleatória estratificada, com base em dados do IBGE/TSE.",
    "Serpes": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Perfil Pesquisas Técnicas (RN)": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Voice Pesquisas (MT)": "Pesquisa quantitativa, com entrevistas telefônicas (fixo e celular) e aplicação de questionário estruturado, por amostragem probabilística do eleitorado.",
    "Govnet/Instituto Opinião (SP)": "Pesquisa quantitativa, com foco em intenção de voto e opinião pública junto a uma amostra representativa da população.",
    "Instituto Econométrica": "Pesquisa quantitativa, com entrevistas presenciais face a face e aplicação de questionário estruturado com questões abertas e fechadas, conduzida por entrevistadores treinados e identificados.",
    "Instituto de Pesquisa Resultado (MS)": "Pesquisa mista, com abordagens qualitativa e quantitativa, utilizando entrevistas telefônicas (CATI) e coleta online. Foco em intenção de voto, imagem política, tendências de mercado e satisfação de clientes.",
    "6 Sigma": "Pesquisa quantitativa, com entrevistas presenciais por abordagem aleatória e aplicação de questionário estruturado junto à amostra definida.",
    "Prever Pesquisas": "Pesquisa quantitativa de campo, com foco em intenção de voto e opinião pública.",
    "Ágora Pesquisa (RJ)": "Pesquisa quantitativa, de natureza exploratório-descritiva, operacionalizada por meio da técnica survey com aplicação de questionário estruturado. A coleta de dados será realizada exclusivamente por meio de entrevistas pessoais domiciliares, utilizando dispositivos eletrônicos (tablets). O universo é constituído pelos eleitores com 16 anos ou mais, regularmente inscritos no cadastro eleitoral e residentes no estado do Rio de Janeiro.",
    "EPP": "Pesquisa quantitativa, por amostragem por cotas de gênero, faixa etária, grau de instrução, renda familiar, zona (urbana e rural), microrregiões e municípios.",
    "Exata Pesquisa (MA)": "Pesquisa quantitativa, com entrevistas domiciliares presenciais e aplicação de questionário estruturado, conduzida por profissionais treinados. Universo: população eleitora com 16 anos ou mais.",
    "Dataqualy": "Pesquisa mista, com abordagens qualitativa (grupos focais, entrevistas em profundidade, etnografia) e quantitativa (amostragem).",
    "Painel Brasil": "Pesquisa mista, com abordagens qualitativa e quantitativa, utilizando diversas técnicas de coleta.",
    "Brand Consultoria": "Pesquisa quantitativa, com entrevistas telefônicas (CATI) e aplicação de questionário estruturado com alternativas randomizadas, junto a uma amostra representativa do eleitorado.",
    "Vox Populi": "Pesquisa mista, com abordagem qualitativa para análise de motivações e comportamentos e abordagem quantitativa com aplicação de questionário estruturado junto a uma amostra representativa do público-alvo.",
    "IPEMS": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Real Dados": "Pesquisa quantitativa, com foco em coleta e análise de dados sobre comportamento do consumidor.",
    "Instituto Datailha": "Pesquisa quantitativa, com entrevistas presenciais e aplicação de questionário estruturado para aferição da opinião do eleitorado.",
    "Tendência Pesquisa (SC)": "Pesquisa quantitativa, com entrevistas presenciais do tipo intercept em pontos de fluxo, por amostragem aleatória simples com controle de cotas (sexo, faixa etária, grau de instrução e nível econômico).",
    "Certifica Consultoria": "Pesquisa mista, com abordagens qualitativa e quantitativa combinadas.",
    "IPAT": "Pesquisa quantitativa, com foco em opinião pública, hábitos e mercado.",
    "Dados Pesquisa (GO)": "Pesquisa quantitativa, com coleta de dados primários, aplicada a estudos eleitorais, de opinião pública e de mercado.",
    "Agora Pesquisa (BA)": "Pesquisa quantitativa, com foco em intenção de voto e avaliação de governo.",
    "Pontual Pesquisas (AM)": "Pesquisa quantitativa, com entrevistas presenciais.",
    "Instituto Haverroth": "Pesquisa mista, com abordagens qualitativa e quantitativa, voltada à identificação do perfil, expectativas e tendências de eleitores e consumidores.",
    "IRG Consultoria": "Pesquisa quantitativa, com entrevistas telefônicas individuais e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado.",
    "Fortiori": "Pesquisa mista, com abordagem quantitativa para análise de dados e qualitativa para compreensão de comportamentos e atitudes.",
    "Colectta Consultoria": "Pesquisa quantitativa, com entrevistas presenciais por abordagem aleatória e aplicação de questionário estruturado.",
    "Consult Pesquisa (RN)": "Pesquisa quantitativa, por amostragem probabilística com sorteio em múltiplos estágios.",
    "Agorasei Pesquisa": "Pesquisa quantitativa de campo, com foco em intenção de voto e avaliação de governo.",
    "Opinião Pesquisas (PB)": "Pesquisa quantitativa, por amostragem probabilística ou por cotas, visando a representatividade da população estudada.",
    "Ipespe": "Pesquisa baseada em análise de dados secundários, com foco em histórico eleitoral e comportamento do eleitor.",
    "Quaest": "Pesquisa quantitativa, com aplicação de questionário estruturado junto a uma amostra representativa, com análise estatística dos dados.",
    "W J Mendes": "Pesquisa quantitativa, com uso de métodos estatísticos e análise de dados numéricos para identificação de padrões.",
    "Instituto Qualitativa": "Pesquisa qualitativa, com foco na compreensão de motivações, sentimentos e opiniões sobre fenômenos sociais, políticos ou de mercado.",
    "AR7 Pesquisa": "Pesquisa quantitativa, com foco em opinião pública.",
    "Veritá": "Pesquisa quantitativa, com entrevistas e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado estadual.",
    "Instituto Methodus": "Pesquisa mista, com abordagens qualitativa e quantitativa, presenciais e digitais, com foco em inteligência estratégica.",
    "Doxa": "Pesquisa quantitativa, por amostragem, com aplicação de questionário estruturado e abordagem presencial junto a uma amostra representativa do eleitorado, com base em dados do TSE, IBGE e FAPESPA.",
    "Instituto Seta": "Pesquisa quantitativa, com entrevistas presenciais e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado.",
    "Ranking Pesquisa": "Pesquisa quantitativa, com entrevistas domiciliares presenciais e aplicação de questionário estruturado junto a uma amostra representativa da população. Universo: eleitorado do estado do Rio Grande do Norte. Foco em avaliação do cenário político e administrativo.",
    "Potencial": "Pesquisa quantitativa, com coleta de dados e uso de tecnologia aplicados à inteligência de mercado.",
    "F5 Atualiza Dados": "Pesquisa quantitativa, com entrevistas telefônicas (CATI) e aplicação de questionário estruturado.",
    "MBO": "Pesquisa quantitativa, com aplicação de questionário em formato espontâneo (sem apresentação de nomes) e estimulado (com apresentação de nomes dos pré-candidatos).",
    "Comunicare": "Pesquisa quantitativa, com foco em opinião pública e preferências do eleitorado e da sociedade civil.",
    "Naipes Marketing": "Pesquisa quantitativa de mercado, com uso de tecnologia e inteligência de dados.",
    "Instituto Exatta (PE)": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Escutec": "Pesquisa quantitativa, com aplicação de questionário estruturado.",
    "Instituto França": "Pesquisa quantitativa, com entrevistas presenciais individuais e aplicação de questionário estruturado junto a uma amostra representativa do eleitorado.",
    "Ibrape": "Pesquisa quantitativa, com metodologia científica validada.",
    "Instituto Datasensus": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "INOP": "Pesquisa qualitativa, com metodologia estruturada e em conformidade legal.",
    "Instituto Vope": "Pesquisa quantitativa, com entrevistas presenciais diretas junto ao eleitorado.",
    "Vox Opinião Pública (SP)": "Pesquisa quantitativa, com entrevistas presenciais domiciliares.",
    "Seculus Consultoria": "Pesquisa quantitativa de campo, com aplicação de questionário estruturado, por amostragem probabilística ou por cotas, visando representatividade geográfica e demográfica.",
    "Datavox (PB)": "Pesquisa quantitativa, com foco em opinião pública e intenção de voto.",
    "Multidados": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Voga Pesquisas": "Pesquisa mista, com abordagens qualitativa, quantitativa e de monitoramento.",
    "Studio Pesquisas": "Pesquisa mista, com abordagens qualitativa e quantitativa.",
    "Census Pesquisas": "Pesquisa quantitativa, com entrevistas domiciliares presenciais e aplicação de questionário estruturado com perguntas espontâneas e estimuladas (alternativas em ordem alfabética ou randomizadas), por amostragem estratificada em 2 estágios com abordagem aleatória simples (gênero, faixa etária, nível de instrução e renda).",
    "Zaytec Brasil": "Pesquisa mista, com abordagens qualitativa, quantitativa, análise de dados secundários e combinação quali-quanti.",
    "Access": "Pesquisa baseada em análise de dados secundários e revisão bibliográfica.",
    "Brasmarket": "Pesquisa quantitativa, com foco em opinião pública e intenção de voto.",
    "FSB Pesquisa": "Pesquisa quantitativa, com entrevistas telefônicas e aplicação de questionário estruturado para aferição de intenção de voto.",
    "GERP": "Pesquisa quantitativa, com aplicação de questionário em formato espontâneo (sem apresentação de lista de candidatos).",
    "Ideia Big Data": "Pesquisa mista, com abordagens qualitativa, quantitativa e análise combinada quali-quanti.",
    "Ipec (antigo Ibope)": "Pesquisa quantitativa, com entrevistas presenciais face a face e aplicação de questionário estruturado, por amostragem com base em dados do IBGE para representatividade geográfica e socioeconômica. Amostra típica: 2.000 entrevistas em levantamentos nacionais.",
    "Sensus": "Pesquisa quantitativa, com entrevistas presenciais face a face.",
    "121 Labs": "Pesquisa quantitativa, com foco em opinião pública e mercado, com análise de dados.",
    "Agili Pesquisas": "Pesquisa quantitativa, com foco na captação de percepções e opiniões do público-alvo.",
    "Alexandre Sócrates Naviskas": "Pesquisa quantitativa, com foco em pesquisas eleitorais e de opinião pública.",
    "American Analytics": "Pesquisa quantitativa, com coleta, organização e análise de dados para suporte à tomada de decisão estratégica.",
    "Datasonda": "Pesquisa quantitativa, por amostragem por cotas representativas do eleitorado (gênero, faixa etária, escolaridade, renda e bairros).",
    "Delta Agência de Pesquisa": "Pesquisa quantitativa, por amostragem por cotas, com entrevistas presenciais face a face e aplicação de questionário estruturado por entrevistadores treinados.",
}


# Aliases de instituto: nomes alternativos que devem ser tratados como o
# instituto canônico (ex.: pesquisas em parceria, grafias variantes). A chave
# é o nome alternativo, o valor é o nome canônico que deve aparecer na planilha
# e ser usado para classificação/metodologia.
ALIASES_INSTITUTO = {
    # AtlasIntel é o instituto; eventuais veículos/parceiros não entram no
    # nome canônico publicado.
    "AtlasIntel/Bloomberg": "AtlasIntel",
    "AtlasIntel/Meio Norte": "AtlasIntel",
    "Atlas Intel/Bloomberg": "AtlasIntel",
    "Atlas Intel/Meio Norte": "AtlasIntel",
    # Quaest em parcerias
    "Genial/Quaest": "Quaest",
    "Quaest/Genial": "Quaest",
    "Genial Quaest": "Quaest",
    # O Ibope virou Ipec em 2021. A matriz recebe "IPEC" da extração e a nota
    # estava guardada em "Ibope", então as 9 pesquisas caíam em "Ainda não foi
    # avaliado" com a nota existindo.
    "IPEC": "Ipec",
    "Ibope": "Ipec",
    "IBOPE": "Ipec",
    "Ipec (antigo Ibope)": "Ipec",
    "Ipec/Globo": "Ipec",
    # GERP — METODOLOGIA_INSTITUTOS já usa caixa alta
    "Gerp": "GERP",
    "gerp": "GERP",
    "Grupo GERP": "GERP",
    "Grupo Gerp": "GERP",
    # Datamax — maioria dos registros sem espaço
    "Data Max": "Datamax",
    "DataMax": "Datamax",
    "data max": "Datamax",
    # Nome legal usado em alguns registros do PesqEle; corresponde à marca Ideia.
    "Mídia Inteligência em Pesquisa": "Ideia Inteligência",
    "Midia Inteligencia em Pesquisa": "Ideia Inteligência",
    "MDA Pesquisas": "MDA",
    "Nexus/BTG Pactual": "Nexus",
    "Nexus / BTG Pactual": "Nexus",
    # Grafias históricas já presentes nas matrizes.
    "IPESPE": "Ipespe",
    "Ipems": "IPEMS",
    "Instituto Opinião PI": "Instituto Opinião (PI)",
    # ECM — nome legal do TSE corresponde à marca ECM Pesquisas.
    "ECM - EDICAO, COMUNICACAO & MARKETING LTDA / ECM-PESQUISAS": "ECM Pesquisas",
    "ECM - EDIÇÃO, COMUNICAÇÃO & MARKETING LTDA / ECM-PESQUISAS": "ECM Pesquisas",
    "ECM-PESQUISAS": "ECM Pesquisas",
    "ECM-Pesquisas": "ECM Pesquisas",
    # IMAPE — nome legal do TSE.
    "IMAPE - INST. MAJORITARIO DE PESQUISAS E ESTATISTICAS": "IMAPE",
    # Nomes como o Pindograma publica, apontando para a grafia que a matriz já
    # usa. Sem eles, a nota existe na tabela e não é encontrada.
    #
    # Os cinco abaixo saíram de CLASSIFICACAO_SEM_FONTE em 02/09/2026, depois de
    # cruzar UF, fonte e registro TSE das pesquisas gravadas com a base. Nos
    # quatro primeiros a nota que já valia é igual à da base, então o peso não
    # muda; o alias só liga o nome ao instituto certo.
    #   Instituto Ranking Brasil Inteligência: as 4 pesquisas (MS, senador) são
    #     publicadas em rankingpesquisa.com.br, que é a entrada "Ranking
    #     Pesquisa" da base.
    #   Delta Pesquisas: 30 pesquisas no AC, nome e nota (B) da Delta Agência.
    #   Direct Pesquisas: 13 em GO, nome e nota (D) da Directa Pesquisa.
    #   DMP Pesquisa: 3 no AM, nome e nota (B-) da DMP.
    #   Opinião: 8 pesquisas, todas do DF, e a única entrada de "Opinião" no DF
    #     é a Opinião Brasil (DF). Este é o único que muda de peso, de B+ (0,55)
    #     para B- (0,40). É o que a UF sustenta; para certeza seria preciso bater
    #     o CNPJ do registro no PesqEle contra o que a base publica.
    "Instituto Ranking Brasil Inteligência": "Instituto Ranking",
    "Delta Pesquisas": "Delta Agência de Pesquisa",
    "Direct Pesquisas": "Directa Pesquisa",
    "DMP Pesquisa": "DMP",
    "Opinião": "Opinião Brasil (DF)",
    "Agorasei Pesquisa": "Agorasei",
    "Census Pesquisas": "Census",
    "Colectta Consultoria": "Colectta",
    "Consult Pesquisa (RN)": "Consult Pesquisa",
    "Exata GO": "Exata.GO",
    "Exata Op (DF)": "Exata OP",
    "Exatus Consultoria (RN)": "Exatus",
    "Ibrape": "Novo Ibrape",
    "Instituto Amostragem": "Amostragem",
    "Instituto Econométrica": "Econométrica",
    "Instituto Methodus": "Methodus",
    "Instituto Visão (PR)": "Visão Pesquisas",
    "Instituto Vope": "Instituto VOPE",
    "IPPI Pesquisas e Consultoria": "IPPI",
    "Jornal Stylo": "Instituto Stylo",
    "Múltipla Pesquisa (PE)": "Instituto Múltipla",
    "Opinar Pesquisas": "Opinar",
    "Perfil Pesquisas": "Perfil Pesquisas Técnicas",
    "Perfil Pesquisas Técnicas (RN)": "Perfil Pesquisas Técnicas",
    "Pontual Pesquisas (AM)": "Pontual Pesquisas",
    "Ranking Pesquisa": "Instituto Ranking",
    "Seculus Consultoria": "Seculus",
    # Veritá — grafias sem acento e com prefixo.
    "Verita": "Veritá",
    "VERITA": "Veritá",
    "VERITÁ": "Veritá",
    "verita": "Veritá",
    "veritá": "Veritá",
    "Instituto Veritá": "Veritá",
    "Instituto Verita": "Veritá",
    "Veritá Pesquisas": "Veritá",
    "Verita Pesquisas": "Veritá",
}


def normalizar_instituto(nome) -> str:
    """Resolve um nome de instituto via tabela de aliases. Se não houver alias,
    retorna o nome original (com espaços normalizados)."""
    nome_norm = _norm_ws(nome)
    # Mantém AtlasIntel como nome único, mesmo quando o material menciona o
    # veículo parceiro depois de uma barra.
    if re.fullmatch(r"atlas\s*intel(?:\s*/\s*.+)?", nome_norm, flags=re.IGNORECASE):
        return "AtlasIntel"
    if re.fullmatch(r"nexus(?:\s*/\s*btg(?:\s+pactual)?)?", nome_norm, flags=re.IGNORECASE):
        return "Nexus"
    if re.fullmatch(r"(?:grupo\s+)?gerp", nome_norm, flags=re.IGNORECASE):
        return "GERP"
    return ALIASES_INSTITUTO.get(nome_norm, nome_norm)


# A Indexa não está na base do
# Pindograma, e a entrada "Index Pesquisas" é outra empresa (institutoindex.com.br,
# CNPJ próprio), não a Indexa Pesquisas das pesquisas nacionais da CNN e da
# CartaCapital. Instituto sem nota publicada vale "Ainda não foi avaliado" (0,25),
# não uma classificação atribuída por aproximação de nome.
#
# Notas que estavam na transcrição manual antiga e que não foram encontradas na
# base publicada do Pindograma, seja porque o nome não existe lá, seja porque há
# mais de um candidato e o nome não desempata ("Opinião" bate com sete). Ficam
# separadas para não se passarem por nota do Pindograma, e continuam valendo
# para não derrubar para 0,25 quem já tinha peso. Precisam de conferência: ou
# viram alias para o nome certo da base, ou saem.
CLASSIFICACAO_SEM_FONTE = {
    # Sobrou um. As 3 pesquisas são de MT, governador e senador. O único
    # candidato na base é "Index Pesquisas", que tem a mesma nota B-, mas o nome
    # é bem diferente e o erro médio daquela entrada é 6,0. Como a nota bate, na
    # prática nada muda; fica aqui marcado até alguém bater o CNPJ.
    'Data Index': 'B-',
}


OVERRIDES_CLASSIFICACAO = {}


def classificar_instituto(nome):
    canonico = normalizar_instituto(nome)
    if canonico in CLASSIFICACAO_POR_INDICE:
        return CLASSIFICACAO_POR_INDICE[canonico]
    return CLASSIFICACAO_SEM_FONTE.get(canonico, "Ainda não foi avaliado")


# Pesos graduais das dez faixas de classificação formadas a partir do índice
# contínuo do Pindograma. Instituto sem índice mantém o peso conservador 0,25.
SCORE_INSTITUTO = {
    "A+": 0.85,
    "A":  0.76,
    "A-": 0.67,
    "B+": 0.58,
    "B":  0.49,
    "B-": 0.40,
    "C+": 0.31,
    "C":  0.22,
    "C-": 0.135,
    "D":  0.05,
    "Ainda não foi avaliado": 0.25,
}

MEIA_VIDA_AGREGADORES_DIAS = 30
COLUNA_MODELO_AMOSTRAL = "media_amostral_30d"
COLUNA_MODELO_HIBRIDO = "media_hibrida_30d"


def score_instituto(classificacao) -> float:
    """Retorna o peso (0–1) de uma classificação de instituto.

    Classificações desconhecidas (ou vazias) caem no mesmo score de
    "Ainda não foi avaliado" (0,25) — valor conservador da nota metodológica.
    """
    return SCORE_INSTITUTO.get(_norm_ws(classificacao), 0.25)


# METODOLOGIA_INSTITUTOS foi escrito com a grafia que cada texto tinha na época,
# e parte dela é a grafia longa ("Instituto Amostragem", "Ipec (antigo Ibope)").
# Como essas grafias viraram alias, procurar direto pela chave normalizada
# deixava 15 textos inalcançáveis. O índice abaixo resolve cada chave uma vez, na
# importação, e é por ele que a busca passa.
# O índice é montado na primeira chamada, não na importação: normalizar_instituto
# depende de _norm_ws, que é definido mais abaixo neste arquivo. Montar aqui em
# cima quebrava o import inteiro do módulo com NameError.
_METODOLOGIA_POR_CANONICO: dict[str, str] | None = None


def obter_metodologia(nome):
    global _METODOLOGIA_POR_CANONICO
    if _METODOLOGIA_POR_CANONICO is None:
        indice: dict[str, str] = {}
        for _nome_metod, _texto_metod in METODOLOGIA_INSTITUTOS.items():
            indice.setdefault(normalizar_instituto(_nome_metod), _texto_metod)
        _METODOLOGIA_POR_CANONICO = indice
    return _METODOLOGIA_POR_CANONICO.get(normalizar_instituto(nome), "")


def env_bool(name: str, default: bool = False) -> bool:
    v = (os.getenv(name, "") or "").strip().lower()
    if v == "":
        return default
    return v in ("1", "true", "t", "yes", "y", "sim", "on")


def _norm_ws(s) -> str:
    try:
        if pd.isna(s):
            s = ""
    except Exception:
        pass
    return re.sub(r"\s+", " ", str(s)).strip()


def _strip_html(s: str) -> str:
    return re.sub(r"<[^>]+>", "", str(s or "")).strip()


def _slug(s: str) -> str:
    s = _norm_ws(s).lower()
    s = re.sub(r"[^a-z0-9]+", "-", s).strip("-")
    return s


def _sha1_short(s: str, n=10) -> str:
    return hashlib.sha1(str(s).encode("utf-8", errors="ignore")).hexdigest()[:n]


def normalizar_data_campo_segura(valor) -> str:
    """
    Normaliza datas aceitando apenas estes formatos de entrada:
    - YYYY-MM-DD
    - M/D/YYYY ou MM/DD/YYYY

    Importante: não interpreta datas como DD/MM/YYYY.
    Ex.: 2/3/2026 -> 2026-02-03
    """
    s = _norm_ws(valor)
    if not s:
        return ""

    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return s

    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
        try:
            return datetime.strptime(s, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return s

    return s


def extrair_ultima_data(s: str) -> str:
    datas = re.findall(r"\d{4}-\d{2}-\d{2}", str(s))
    return datas[-1] if datas else _norm_ws(s)


def extrair_primeira_data(s: str) -> str:
    """Extrai o início do campo quando o PollingData fornece uma faixa."""
    datas = re.findall(r"\d{4}-\d{2}-\d{2}", str(s))
    return datas[0] if datas else _norm_ws(s)


def parse_url_meta(url: str):
    u = url.strip()

    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>governador)/(?P<uf>[a-z]{2})/.*?_t(?P<turno>\d)\.html",
        u, re.I
    )
    if m:
        return {
            "ano": int(m.group("ano")),
            "cargo": "governador",
            "uf": m.group("uf").upper(),
            "turno": f"t{m.group('turno')}"
        }

    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>presidente)/(?P<uf>[a-z]{2})/(?:[^/]*_)?(?P<turno>t\d)(?:_(?P<slug>[^/.]+))?(?:\.html)?/?$",
        u, re.I
    )
    if m:
        turno = m.group("turno")
        slug = m.group("slug") or ""
        # disputa só distingue confrontos no 2º turno; no 1º turno fica vazia
        disputa = f"{turno.lower()}_{slug}" if (slug and turno.lower() == "t2") else ""
        return {
            "ano": int(m.group("ano")),
            "cargo": "presidente",
            "uf": m.group("uf").upper(),
            "turno": turno.lower(),
            "disputa": disputa,
        }

    m = re.search(
        r"/(?P<ano>\d{4})/(?P<cargo>senador)/(?P<uf>[a-z]{2})/(?:.*?_(?P<turno>t\d)\.html|(?P<turno2>t\d)/?$)",
        u, re.I
    )
    if m:
        turno = m.group("turno") or m.group("turno2")
        return {
            "ano": int(m.group("ano")),
            "cargo": "senador",
            "uf": m.group("uf").upper(),
            "turno": turno.lower()
        }

    return {"ano": None, "cargo": None, "uf": None, "turno": None, "disputa": None}


def parsear_pesquisa(texto):
    nome, id_pesquisa, data = "", "", ""

    for linha in [l.strip() for l in str(texto).strip().split("\n") if l.strip()]:
        if re.match(r"^\(\d+\)$", linha):
            continue

        m = re.search(r"(\d{4}-\d{2}-\d{2})", linha)
        if m:
            data = m.group(1)
            antes = linha[:linha.index(data)].strip()
            if antes:
                id_pesquisa = antes
        else:
            nome = re.sub(r"\s*\(\d+\)\s*$", "", linha).strip()

    return _norm_ws(nome), _norm_ws(id_pesquisa), _norm_ws(data)


def parsear_pct(valor):
    v = str(valor).strip()
    if not v or v in ("-", "NaN%", "nan%", "NaN", "nan", "NA", ""):
        return None

    try:
        return float(v.replace("%", "").replace(",", ".").strip())
    except Exception:
        return None


def normalizar_percentual_planilha(valor):
    v = str(valor).strip().lstrip("'")
    if not v or v in ("-", "NaN%", "nan%", "NaN", "nan", "NA", ""):
        return None
    try:
        n = float(v.replace("%", "").replace(",", ".").strip())
        if abs(n) >= 1000:
            n = n / 1000.0
        return round(n, 1)
    except Exception:
        return None


# Sigla canonica, sempre em maiuscula. O PollingData publica a mesma legenda com
# grafias diferentes conforme o instituto, e ate aqui o partido entrava cru na
# matriz: em 21/07/2026 a T1 tinha "UB" e "UNIAO", "POD" e "PODE", "DEMOCRATA" e
# "DEM", "PC" e "PCO", "PARTIDO MISSÃO" e "MISSAO" para as mesmas pessoas. Isso
# fatia a serie do candidato em duas no resultados_bi e polui o levantamento de
# divergencia de partido, que serve pra achar erro de cadastro de verdade.
PARTIDOS_CANONICOS = {
    "UB": "UNIAO", "UNIÃO": "UNIAO", "UNIAO BRASIL": "UNIAO", "UNIÃO BRASIL": "UNIAO",
    "POD": "PODE", "PODEMOS": "PODE",
    "DEMOCRATA": "DEM", "DEMOCRATAS": "DEM",
    "PC": "PCO",
    "PARTIDO MISSÃO": "MISSAO", "PARTIDO MISSAO": "MISSAO", "MISSÃO": "MISSAO",
}


def normalizar_sigla_partido(valor: str) -> str:
    """Sigla em maiuscula, resolvendo as grafias que o PollingData mistura.

    Sigla desconhecida passa direto, so em maiuscula: e melhor entrar com o nome
    que veio do que ser descartada em silencio.
    """
    sigla = _norm_ws(valor).upper()
    return PARTIDOS_CANONICOS.get(sigla, sigla)


def parsear_candidato_partido(col_header: str):
    col_clean = _strip_html(str(col_header).replace("<br>", " "))
    m = re.match(r"^(.+?)\s*\(([^)]+)\)\s*$", col_clean.strip())
    if m:
        return _norm_ws(m.group(1)), normalizar_sigla_partido(m.group(2))
    return _norm_ws(col_clean), ""


def inferir_confianca(s):
    m = re.search(r"(\d{2,3})\s*%\s*\)", str(s or ""))
    if m:
        try:
            return int(m.group(1))
        except Exception:
            return None
    return None


def inferir_margem_erro(s):
    m = re.search(r"(\d+(?:\.\d+)?)\s*%", str(s or "").replace(",", "."))
    if m:
        try:
            return float(m.group(1))
        except Exception:
            return None
    return None


def gerar_poll_id(uf, instituto, id_pesquisa, data_campo, cargo, turno, raw_block_hash, disputa=""):
    uf = uf.upper()
    data_campo = _norm_ws(data_campo)
    instituto_slug = _slug(instituto)

    # Para t2, inclui a disputa na chave para diferenciar o mesmo poll
    # publicado em múltiplos matchups (ex.: AtlasIntel em t2_lula-zema e t2_flavio-lula)
    turno_key = disputa if disputa else turno

    if id_pesquisa and id_pesquisa.lower() not in ("sem registro", "sem_registro", "semregistro", "nan", ""):
        return f"{uf}|{cargo}|{turno_key}|{id_pesquisa}|{data_campo}"

    return f"{uf}|{cargo}|{turno_key}|{instituto_slug}|{data_campo}"


def gerar_scenario_id(poll_id, scenario_label):
    return f"{poll_id}|{_norm_ws(scenario_label)}"


def eh_cenario_media(scenario_label) -> bool:
    s = _norm_ws(scenario_label).lower()
    return bool(re.match(r"^m[eé]dia\b", s))


def obter_spreadsheet_id():
    spreadsheet_id = (os.getenv("SPREADSHEET_ID_POLLINGDATA", "") or "").strip()
    if spreadsheet_id:
        return spreadsheet_id
    raise RuntimeError("SPREADSHEET_ID_POLLINGDATA não definido.")


def obter_spreadsheet_id_t2():
    sid = (os.getenv("SPREADSHEET_ID_POLLINGDATA_T2", "") or "").strip()
    if sid:
        return sid
    raise RuntimeError("SPREADSHEET_ID_POLLINGDATA_T2 não definido.")


def urls_presidente_2026_t1(ufs):
    return [
        f"https://www.pollingdata.com.br/2026/presidente/{uf}/2026_presidente_{uf}_t1.html"
        for uf in ufs
    ]


def urls_governador_2026_t1(ufs):
    return [
        f"https://www.pollingdata.com.br/2026/governador/{uf}/2026_governador_{uf}_t1.html"
        for uf in ufs
    ]


def urls_senado_2026_t1(ufs):
    return [
        f"https://www.pollingdata.com.br/2026/senador/{uf}/2026_senador_{uf}_t1.html"
        for uf in ufs
    ]


def montar_urls(incluir_governador: bool, incluir_senado: bool, incluir_presidente: bool, incluir_presidente_t2: bool = True):
    urls = []

    if incluir_governador:
        urls += urls_governador_2026_t1(UFS)

    if incluir_senado:
        urls += urls_senado_2026_t1(UFS)

    if incluir_presidente:
        urls += list(PRESIDENTE_URLS_DEFAULT)
        urls += urls_presidente_2026_t1(UFS)

    if incluir_presidente_t2:
        urls += list(PRESIDENTE_T2_URLS_DEFAULT)

    return urls


def criar_driver():
    options = webdriver.ChromeOptions()
    options.add_argument("--headless=new")
    options.add_argument("--no-sandbox")
    options.add_argument("--disable-dev-shm-usage")
    options.add_argument("--window-size=1920,1080")
    options.add_argument("--disable-blink-features=AutomationControlled")
    options.add_experimental_option("excludeSwitches", ["enable-automation"])
    options.add_experimental_option("useAutomationExtension", False)

    if _HAS_WDM:
        service = Service(ChromeDriverManager().install())
    else:
        service = Service()

    driver = webdriver.Chrome(service=service, options=options)
    driver.execute_script("Object.defineProperty(navigator, 'webdriver', {get: () => undefined})")
    return driver


def esperar_tabela(driver, timeout=40):
    # Tenta no frame principal primeiro
    try:
        WebDriverWait(driver, min(timeout, 10)).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
        )
        time.sleep(2)
        return
    except Exception:
        pass

    # Procura a tabela dentro de iframes
    for iframe in driver.find_elements(By.TAG_NAME, "iframe"):
        try:
            driver.switch_to.frame(iframe)
            WebDriverWait(driver, min(timeout, 8)).until(
                EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
            )
            time.sleep(2)
            return  # permanece no iframe para o scraping
        except Exception:
            driver.switch_to.default_content()

    # Fallback: espera com timeout completo no frame principal
    WebDriverWait(driver, timeout).until(
        EC.presence_of_element_located((By.CSS_SELECTOR, WAIT_CSS))
    )
    time.sleep(2)


def coletar_headers_visiveis(driver):
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    headers = []

    for sel in ["thead th", "table th", ".rt-thead .rt-th"]:
        try:
            els = secao.find_elements(By.CSS_SELECTOR, sel)
            for el in els:
                txt = _norm_ws(el.text)
                if txt and txt.lower() not in [h.lower() for h in headers]:
                    headers.append(txt)
        except Exception:
            pass

    return headers


def decidir_layout(driver):
    headers = coletar_headers_visiveis(driver)
    headers_txt = " | ".join([h.lower() for h in headers])

    tem_cnpj_instituto = "cnpj instituto" in headers_txt
    tem_cnpj_contratante = "cnpj contratante" in headers_txt

    if tem_cnpj_instituto and tem_cnpj_contratante:
        return "novo"

    return "antigo"


def detectar_layout_novo_json(driver) -> bool:
    try:
        scripts = driver.find_elements(
            By.CSS_SELECTOR,
            "script[type='application/json'][data-for='tab_pesquisas']"
        )
        return len(scripts) > 0
    except Exception:
        return False


def scrape_novo_layout(driver, url, horario_raspagem, meta):
    ano = meta["ano"]
    cargo = meta["cargo"]
    uf = meta["uf"]
    turno = meta["turno"]
    disputa = meta.get("disputa") or ""

    try:
        el = driver.find_element(
            By.CSS_SELECTOR,
            "script[type='application/json'][data-for='tab_pesquisas']"
        )
        raw = el.get_attribute("innerHTML") or el.text
        data_json = json.loads(raw)
        d = data_json["x"]["tag"]["attribs"]["data"]
    except Exception as e:
        print(f"  [-] erro ao extrair JSON do novo: {e}")
        return None, None

    institutos = d.get("Instituto", [])
    urls_fonte = d.get("url", [])
    modos = d.get("Modo", [])
    entrevistas = d.get("Entrevistas", [])
    registros = d.get("registro", [])
    erros = d.get("erro", [])
    ranges = d.get("range", [])
    cenarios_lst = d.get("cenarios", [])

    pesquisas_rows = []
    resultados_rows = []

    for i in range(len(institutos)):
        instituto = normalizar_instituto(institutos[i])
        link_fonte = _norm_ws(urls_fonte[i]) if i < len(urls_fonte) else ""
        modo = _norm_ws(_strip_html(modos[i])) if i < len(modos) else ""
        amostra_raw = entrevistas[i] if i < len(entrevistas) else None
        registro = _norm_ws(registros[i]) if i < len(registros) else ""
        erro_conf = _strip_html(erros[i]) if i < len(erros) else ""
        faixa_campo = ranges[i] if i < len(ranges) else ""
        data_inicio_campo = extrair_primeira_data(faixa_campo)
        data_campo = extrair_ultima_data(faixa_campo)

        registro_norm = "" if registro.lower().startswith("sem") else registro

        amostra = None
        try:
            amostra = int(float(str(amostra_raw)))
        except Exception:
            pass

        margem = inferir_margem_erro(erro_conf)
        confianca = inferir_confianca(erro_conf)
        classificacao = classificar_instituto(instituto)
        metodologia = obter_metodologia(instituto)

        block_hash = _sha1_short(f"{instituto}|{registro}|{data_campo}", 10)
        poll_id = gerar_poll_id(uf, instituto, registro_norm, data_campo, cargo, turno, block_hash, disputa=disputa)

        cenarios_dict = cenarios_lst[i] if i < len(cenarios_lst) else {}
        cenario_nums = cenarios_dict.get("cenario", [1])

        for c_idx, c_num in enumerate(cenario_nums):
            scenario_label = str(c_num)
            scenario_id = gerar_scenario_id(poll_id, scenario_label)

            pesquisas_rows.append({
                "scenario_id": scenario_id,
                "poll_id": poll_id,
                "ano": ano,
                "uf": uf,
                "cargo": cargo,
                "turno": turno,
                "disputa": disputa,
                "instituto": instituto,
                "classificacao_instituto": classificacao,
                "registro_tse": registro,
                "data_inicio_campo": data_inicio_campo,
                "data_campo": data_campo,
                "modo": modo,
                "amostra": amostra,
                "margem_erro": margem,
                "confianca": confianca,
                "scenario_label": scenario_label,
                "fonte_url": url,
                "fonte_url_original": link_fonte,
                "horario_raspagem": horario_raspagem,
                "origem": ORIGEM_POLLINGDATA,
                "metodologia": metodologia,
            })

            for col_key, col_vals in cenarios_dict.items():
                if col_key == "cenario":
                    continue

                val = col_vals[c_idx] if c_idx < len(col_vals) else None
                pct = parsear_pct(val)
                if pct is None:
                    continue

                col_clean = _strip_html(col_key.replace("<br>", " "))
                if col_clean.lower() in ("não válido", "nao valido", "não valido"):
                    candidato = "Não válido"
                    partido = ""
                    tipo = "nao_valido"
                    candidato_partido = "Não válido"
                else:
                    candidato, partido = parsear_candidato_partido(col_key)
                    tipo = "candidato"
                    candidato_partido = f"{candidato} ({partido})" if partido else candidato

                resultados_rows.append({
                    "scenario_id": scenario_id,
                    "poll_id": poll_id,
                    "ano": ano,
                    "uf": uf,
                    "cargo": cargo,
                    "turno": turno,
                    "disputa": disputa,
                    "data_campo": data_campo,
                    "instituto": instituto,
                    "classificacao_instituto": classificacao,
                    "registro_tse": registro,
                    "scenario_label": scenario_label,
                    "candidato": candidato,
                    "partido": partido,
                    "candidato_partido": candidato_partido,
                    "tipo": tipo,
                    "percentual": pct,
                    "fonte_url": url,
                    "horario_raspagem": horario_raspagem,
                })

    df_p = pd.DataFrame(pesquisas_rows)
    df_r = pd.DataFrame(resultados_rows)

    print(f"  [novo] {len(df_p)} cenários | {len(df_r)} resultados")
    return df_p, df_r


def expandir_todos_antigo(driver, secao, max_clicks=120):
    i = 0

    while True:
        btns = secao.find_elements(By.CSS_SELECTOR, "button.rt-expander-button")
        fechados = [b for b in btns if (b.get_attribute("aria-expanded") or "").lower() == "false"]

        if not fechados:
            break

        driver.execute_script("arguments[0].click();", fechados[0])
        time.sleep(0.8)

        i += 1
        if i >= max_clicks:
            break


def extrair_link_fonte_do_grupo(group) -> str:
    for sel in [
        "table#tab_instituto a[href]",
        "div.rt-td-inner table a[href]",
        "div[id^='tab_'] a[href]",
        ".rt-expandable-content a[href]",
    ]:
        try:
            el = group.find_element(By.CSS_SELECTOR, sel)
            href = (el.get_attribute("href") or "").strip()
            if href and href.startswith("http"):
                return href
        except Exception:
            continue

    return ""


def extrair_tabela_react(secao):
    headers = []

    for el in secao.find_elements(By.CSS_SELECTOR, "div.rt-thead .rt-th"):
        inner = el.find_elements(By.CSS_SELECTOR, ".rt-text-content, .rt-sort-header")
        text = (inner[0].text.strip() if inner else el.text.strip()).replace("\n", " ").strip()
        if text:
            headers.append(text)

    rows_data = []
    row_group_idx = []
    links_por_grupo = []

    for g_idx, group in enumerate(secao.find_elements(By.CSS_SELECTOR, "div.rt-tbody div.rt-tr-group")):
        links_por_grupo.append(extrair_link_fonte_do_grupo(group))

        for row in group.find_elements(By.CSS_SELECTOR, "div.rt-tr"):
            cells = row.find_elements(By.CSS_SELECTOR, "div.rt-td")
            if not cells:
                continue

            vals = [c.text.strip() for c in cells]
            if any(vals):
                rows_data.append(vals)
                row_group_idx.append(g_idx)

    if not rows_data:
        return None, []

    n_cols = max(len(r) for r in rows_data)
    if len(headers) < n_cols:
        headers += [f"Col_{i}" for i in range(len(headers), n_cols)]

    headers = headers[:n_cols]

    for r in rows_data:
        while len(r) < n_cols:
            r.append("")

    df = pd.DataFrame(rows_data, columns=headers)
    df["_link_fonte"] = [links_por_grupo[g] for g in row_group_idx]

    return df, links_por_grupo


def scrape_antigo_layout(driver, url, horario_raspagem, meta):
    ano = meta["ano"]
    cargo = meta["cargo"]
    uf = meta["uf"]
    turno = meta["turno"]
    disputa = meta.get("disputa") or ""

    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)
    expandir_todos_antigo(driver, secao)
    time.sleep(2)
    secao = driver.find_element(By.CSS_SELECTOR, WAIT_CSS)

    df_raw, _ = extrair_tabela_react(secao)
    if df_raw is None or df_raw.empty:
        print("  [-] sem tabela no antigo")
        return None, None

    col_pesquisa = df_raw.columns.tolist()[0]
    df_raw[col_pesquisa] = df_raw[col_pesquisa].replace("", pd.NA).ffill()
    df_raw["_link_fonte"] = df_raw["_link_fonte"].replace("", pd.NA).ffill().fillna("")

    parsed = df_raw[col_pesquisa].apply(parsear_pesquisa)
    df_raw["instituto"] = parsed.apply(lambda x: x[0])
    df_raw["registro_tse"] = parsed.apply(lambda x: x[1])
    df_raw["data_inicio_campo"] = parsed.apply(lambda x: extrair_primeira_data(x[2]))
    df_raw["data_campo"] = parsed.apply(lambda x: extrair_ultima_data(x[2]))
    df_raw["_block_hash"] = df_raw[col_pesquisa].apply(lambda x: _sha1_short(_norm_ws(x), 10))
    df_raw = df_raw.drop(columns=[col_pesquisa])

    if "Cenários" not in df_raw.columns:
        df_raw["Cenários"] = ""

    cols_meta = [c for c in df_raw.columns if c in {"Modo Pesquisa", "Entrevistas", "Erro (Confiança)", "Cenários"}]
    cols_meta += ["instituto", "registro_tse", "data_inicio_campo", "data_campo", "_block_hash", "_link_fonte"]
    cols_meta = [c for c in cols_meta if c in df_raw.columns]

    cols_cand = [c for c in df_raw.columns if c not in cols_meta]
    cols_cand = [
        c for c in cols_cand
        if re.search(r"\([A-Za-z]{2,}\)", str(c)) or str(c).lower().strip() in ("não válido", "nao valido")
    ]

    pesquisas_rows = []
    resultados_rows = []

    for _, row in df_raw.iterrows():
        instituto = normalizar_instituto(row.get("instituto", ""))
        registro_tse = _norm_ws(row.get("registro_tse", ""))
        data_inicio_campo = _norm_ws(row.get("data_inicio_campo", ""))
        data_campo = _norm_ws(row.get("data_campo", ""))
        modo = _norm_ws(row.get("Modo Pesquisa", ""))
        entrevistas_raw = _norm_ws(row.get("Entrevistas", ""))
        erro_conf = _norm_ws(row.get("Erro (Confiança)", ""))
        scenario_label = _norm_ws(row.get("Cenários", "")) or "NA"
        block_hash = _norm_ws(row.get("_block_hash", ""))
        link_fonte_original = _norm_ws(row.get("_link_fonte", ""))
        classificacao = classificar_instituto(instituto)
        metodologia = obter_metodologia(instituto)

        poll_id = gerar_poll_id(uf, instituto, registro_tse, data_campo, cargo, turno, block_hash, disputa=disputa)
        scenario_id = gerar_scenario_id(poll_id, scenario_label)

        amostra = None
        try:
            if entrevistas_raw and str(entrevistas_raw).strip().isdigit():
                amostra = int(entrevistas_raw)
        except Exception:
            pass

        pesquisas_rows.append({
            "scenario_id": scenario_id,
            "poll_id": poll_id,
            "ano": ano,
            "uf": uf,
            "cargo": cargo,
            "turno": turno,
            "disputa": disputa,
            "instituto": instituto,
            "classificacao_instituto": classificacao,
            "registro_tse": registro_tse,
            "data_inicio_campo": data_inicio_campo,
            "data_campo": data_campo,
            "modo": modo,
            "amostra": amostra,
            "margem_erro": inferir_margem_erro(erro_conf),
            "confianca": inferir_confianca(erro_conf),
            "scenario_label": scenario_label,
            "fonte_url": url,
            "fonte_url_original": link_fonte_original,
            "horario_raspagem": horario_raspagem,
            "origem": ORIGEM_POLLINGDATA,
            "metodologia": metodologia,
        })

        for col in cols_cand:
            pct = parsear_pct(row.get(col, ""))
            if pct is None:
                continue

            colname = _norm_ws(col)
            if colname.lower() in ("não válido", "nao valido"):
                candidato, partido, tipo = "Não válido", "", "nao_valido"
                candidato_partido = "Não válido"
            else:
                candidato, partido = parsear_candidato_partido(colname)
                tipo = "candidato"
                candidato_partido = f"{candidato} ({partido})" if partido else candidato

            resultados_rows.append({
                "scenario_id": scenario_id,
                "poll_id": poll_id,
                "ano": ano,
                "uf": uf,
                "cargo": cargo,
                "turno": turno,
                "disputa": disputa,
                "data_campo": data_campo,
                "instituto": instituto,
                "classificacao_instituto": classificacao,
                "registro_tse": registro_tse,
                "scenario_label": scenario_label,
                "candidato": candidato,
                "partido": partido,
                "candidato_partido": candidato_partido,
                "tipo": tipo,
                "percentual": pct,
                "fonte_url": url,
                "horario_raspagem": horario_raspagem,
            })

    df_p = pd.DataFrame(pesquisas_rows)
    df_r = pd.DataFrame(resultados_rows)

    print(f"  [antigo] {len(df_p)} cenários | {len(df_r)} resultados")
    return df_p, df_r


def scrape_url(driver, url, horario_raspagem):
    meta = parse_url_meta(url)

    if not meta["cargo"]:
        print(f"[-] URL não reconhecida: {url}")
        return None, None

    print(f"[+] {meta['cargo'].upper()} {meta['uf']} {meta['turno']} -> {url}")

    driver.switch_to.default_content()
    driver.get(url)
    esperar_tabela(driver)  # pode trocar para iframe internamente

    try:
        layout = decidir_layout(driver)

        if layout == "novo":
            if not detectar_layout_novo_json(driver):
                print("  [-] headers indicaram novo, mas o JSON do novo não apareceu")
                return None, None
            return scrape_novo_layout(driver, url, horario_raspagem, meta)

        return scrape_antigo_layout(driver, url, horario_raspagem, meta)
    finally:
        driver.switch_to.default_content()


def gs_client_from_env():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não definido.")

    creds_dict = json.loads(raw)
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    credentials = Credentials.from_service_account_info(creds_dict, scopes=scopes)
    return _autorizar(credentials)


def garantir_aba(sh, nome, rows=50000, cols=25):
    try:
        return sh.worksheet(nome)
    except gspread.exceptions.WorksheetNotFound:
        return sh.add_worksheet(title=nome, rows=rows, cols=cols)


def _aba_vazia(values):
    if not values:
        return True

    if len(values) == 1 and all(str(x).strip() == "" for x in values[0]):
        return True

    return False


def reordenar_metodologia_para_ultima_coluna(df: pd.DataFrame) -> pd.DataFrame:
    cols_final = [
        "percentual_media_cenarios",
        "origem_percentual_media",
        "metodologia",
        "origem",
    ]
    cols = [c for c in df.columns if c not in cols_final]
    cols += [c for c in cols_final if c in df.columns]
    return df[cols]


def adicionar_posicao_pesquisa(df: pd.DataFrame) -> pd.DataFrame:
    """
    Mantém compatibilidade com o fluxo atual, mas calcula a posição usando
    a média dos cenários por pesquisa.
    """
    if df.empty:
        return df

    return adicionar_metricas_media_cenarios(df)


def adicionar_metricas_media_cenarios(df: pd.DataFrame) -> pd.DataFrame:
    """
    Calcula um percentual de referência por candidato dentro de cada poll_id.

    Regra:
    - se existirem cenários individuais para o candidato, usa a média
      calculada a partir desses cenários em que ele aparece;
    - só usa o cenário de média já salvo como fallback quando não houver
      cenários individuais disponíveis;
    - a ausência de um candidato em outro cenário não entra como zero no
      cálculo dessa média de referência.

    A posição passa a ser calculada por poll_id com base nesse percentual
    de referência, e não mais por scenario_id.
    """
    if df.empty:
        return df

    df = df.copy()
    cols_auxiliares = [
        "percentual_media_cenarios",
        "origem_percentual_media",
        "posicao_pesquisa",
        "percentual_media_existente",
        "percentual_media_calculada",
        "eh_cenario_media",
    ]
    cols_para_remover = [c for c in cols_auxiliares if c in df.columns]
    if cols_para_remover:
        df = df.drop(columns=cols_para_remover)

    df["scenario_label"] = df["scenario_label"].fillna("").astype(str)
    df["eh_cenario_media"] = df["scenario_label"].apply(eh_cenario_media)

    chaves = ["poll_id", "tipo", "candidato"]
    df_media = (
        df[df["eh_cenario_media"]]
        .groupby(chaves, dropna=False)["percentual"]
        .mean()
        .reset_index(name="percentual_media_existente")
    )
    df_cenarios = (
        df[~df["eh_cenario_media"]]
        .groupby(chaves, dropna=False)["percentual"]
        .mean()
        .reset_index(name="percentual_media_calculada")
    )

    df_ref = (
        df[chaves]
        .drop_duplicates()
        .merge(df_media, on=chaves, how="left")
        .merge(df_cenarios, on=chaves, how="left")
    )
    df_ref["percentual_media_cenarios"] = df_ref["percentual_media_calculada"].combine_first(
        df_ref["percentual_media_existente"]
    )
    df_ref["origem_percentual_media"] = df_ref["percentual_media_calculada"].apply(
        lambda x: "media_calculada_no_codigo" if pd.notna(x) else ""
    )
    df_ref.loc[
        df_ref["origem_percentual_media"].eq("") & df_ref["percentual_media_existente"].notna(),
        "origem_percentual_media"
    ] = "cenario_media_existente"
    df_ref["posicao_pesquisa"] = (
        df_ref.groupby("poll_id")["percentual_media_cenarios"]
        .rank(method="min", ascending=False)
        .astype("Int64")
    )

    df = df.merge(
        df_ref[chaves + ["percentual_media_cenarios", "origem_percentual_media", "posicao_pesquisa"]],
        on=chaves,
        how="left",
    )

    return df.drop(columns=["eh_cenario_media"])


def preencher_posicao_pesquisa_na_aba(aba):
    """
    Lê a aba 'resultados', recalcula a média dos cenários e a posição
    consolidada por pesquisa, e atualiza apenas as células necessárias.
    """
    print("  [posicao] recalculando média dos cenários e posição consolidada...")
    values = aba.get_all_values()

    if _aba_vazia(values) or len(values) < 2:
        print("  [posicao] aba vazia ou sem dados, nada a preencher")
        return

    header = values[0]

    if "posicao_pesquisa" not in header:
        print("  [posicao] coluna posicao_pesquisa ainda não existe na aba, será criada no próximo insert")
        return

    col_idx_pos = header.index("posicao_pesquisa")

    if "percentual_media_cenarios" not in header:
        print("  [posicao] coluna percentual_media_cenarios ainda não existe na aba, será criada no próximo insert")
        return

    col_idx_pct_media = header.index("percentual_media_cenarios")

    if "poll_id" not in header or "percentual" not in header:
        print("  [posicao] colunas poll_id ou percentual ausentes, impossível recalcular")
        return

    if "tipo" not in header or "candidato" not in header:
        print("  [posicao] colunas tipo ou candidato ausentes, impossível recalcular")
        return

    col_idx_poll = header.index("poll_id")
    col_idx_pct = header.index("percentual")
    col_idx_tipo = header.index("tipo")
    col_idx_candidato = header.index("candidato")
    col_idx_scenario_label = header.index("scenario_label") if "scenario_label" in header else None
    col_idx_origem = header.index("origem_percentual_media") if "origem_percentual_media" in header else None

    rows = values[1:]
    registros = []
    for i, row in enumerate(rows):
        def safe_get(r, idx):
            return r[idx] if idx < len(r) else ""

        poll_id = safe_get(row, col_idx_poll).strip()
        pct_raw = safe_get(row, col_idx_pct).strip()
        pos_atual = safe_get(row, col_idx_pos).strip()
        pct_media_atual = safe_get(row, col_idx_pct_media).strip()
        origem_atual = safe_get(row, col_idx_origem).strip() if col_idx_origem is not None else ""

        pct = parsear_pct(pct_raw)

        registros.append({
            "row_num": i + 2,
            "poll_id": poll_id,
            "percentual": pct,
            "tipo": safe_get(row, col_idx_tipo).strip(),
            "candidato": safe_get(row, col_idx_candidato).strip(),
            "scenario_label": safe_get(row, col_idx_scenario_label).strip() if col_idx_scenario_label is not None else "",
            "posicao_atual": pos_atual,
            "percentual_media_atual": parsear_pct(pct_media_atual),
            "origem_percentual_media_atual": origem_atual,
        })

    df = pd.DataFrame(registros)

    df_valido = df[df["poll_id"].ne("") & df["percentual"].notna()].copy()

    if df_valido.empty:
        print("  [posicao] nenhuma linha com dados válidos para recalcular")
        return

    df_valido = adicionar_metricas_media_cenarios(df_valido)

    def col_idx_to_letter(idx):
        result = ""
        while idx >= 0:
            result = chr(idx % 26 + ord("A")) + result
            idx = idx // 26 - 1
        return result

    col_letter_pos = col_idx_to_letter(col_idx_pos)
    col_letter_pct_media = col_idx_to_letter(col_idx_pct_media)
    col_letter_origem = col_idx_to_letter(col_idx_origem) if col_idx_origem is not None else None

    cell_updates = []
    for _, row in df_valido.iterrows():
        posicao_nova = row["posicao_pesquisa"]
        pct_media_novo = row["percentual_media_cenarios"]

        if pd.notna(posicao_nova):
            posicao_nova_str = str(int(posicao_nova))
            if row["posicao_atual"] != posicao_nova_str:
                cell_updates.append({
                    "range": f"{col_letter_pos}{row['row_num']}",
                    "values": [[posicao_nova_str]]
                })

        if pd.notna(pct_media_novo):
            pct_media_atual = row["percentual_media_atual"]
            if pd.isna(pct_media_atual) or abs(float(pct_media_atual) - float(pct_media_novo)) > 1e-9:
                cell_updates.append({
                    "range": f"{col_letter_pct_media}{row['row_num']}",
                    "values": [[str(pct_media_novo)]]
                })

        if col_letter_origem is not None:
            origem = row.get("origem_percentual_media", "") or ""
            if row.get("origem_percentual_media_atual", "") != origem:
                cell_updates.append({
                    "range": f"{col_letter_origem}{row['row_num']}",
                    "values": [[origem]]
                })

    if cell_updates:
        aba.batch_update(cell_updates)
        print(f"  [posicao] {len(cell_updates)} células atualizadas com média dos cenários e posição")
    else:
        print("  [posicao] planilha já estava consistente com a média dos cenários")


def carregar_df_da_aba(aba) -> pd.DataFrame:
    values = aba.get_all_values()
    if _aba_vazia(values) or len(values) < 2:
        return pd.DataFrame()

    header = values[0]
    rows = values[1:]
    rows_norm = []
    for row in rows:
        if len(row) < len(header):
            row = row + [""] * (len(header) - len(row))
        rows_norm.append(row[:len(header)])

    return pd.DataFrame(rows_norm, columns=header)


def sobrescrever_aba(aba, df: pd.DataFrame):
    df = reordenar_metodologia_para_ultima_coluna(df)
    n_cols_schema = max(1, len(df.columns))
    n_rows_desejado = max(2, len(df) + 1)  # +1 do header, mínimo 2 para não rejeitar

    def _redimensionar():
        try:
            if aba.col_count != n_cols_schema or aba.row_count > max(n_rows_desejado, 100):
                aba.resize(rows=n_rows_desejado, cols=n_cols_schema)
        except Exception as e:
            print(f"  [rewrite] não foi possível redimensionar '{aba.title}': {e}")

    if df.empty:
        reescrever_aba(aba, [df.columns.tolist()], "rewrite", antes=_redimensionar)
        print(f"  [rewrite] aba '{aba.title}' limpa e mantido apenas header")
        return

    # Mantém números como números no payload da API. Converter o DataFrame inteiro
    # para texto faz com que decimais possam perder o separador em regravações.
    df_export = df.astype(object).where(pd.notna(df), "")
    reescrever_aba(aba, [df.columns.tolist()] + df_export.values.tolist(),
                   "rewrite", antes=_redimensionar, value_input_option="RAW")
    print(f"  [rewrite] aba '{aba.title}' regravada com {len(df)} linhas × {n_cols_schema} colunas")


def corrigir_coluna_numerica_na_aba(aba, nome_coluna: str, padrao: str = "0.0"):
    values_raw = aba.get_all_values(value_render_option="UNFORMATTED_VALUE")
    if _aba_vazia(values_raw) or len(values_raw) < 2:
        return

    header = values_raw[0]
    if nome_coluna not in header:
        return

    col_idx = header.index(nome_coluna) + 1
    updates = []

    for row_idx, row in enumerate(values_raw[1:], start=2):
        atual_raw = row[col_idx - 1] if len(row) >= col_idx else ""
        novo = normalizar_percentual_planilha(atual_raw)
        if novo is None:
            continue

        atual_raw_txt = str(atual_raw).strip()
        veio_como_texto = isinstance(atual_raw, str)
        exige_forcar_numero = veio_como_texto and (
            atual_raw_txt.startswith("'")
            or "%" in atual_raw_txt
            or "," in atual_raw_txt
            or "." in atual_raw_txt
        )

        atual_f = None
        if isinstance(atual_raw, (int, float)):
            atual_f = float(atual_raw)
        else:
            atual_txt = str(atual_raw).strip().lstrip("'")
            try:
                atual_f = float(atual_txt.replace("%", "").replace(",", "."))
            except Exception:
                atual_f = None

        if veio_como_texto and atual_f is not None:
            exige_forcar_numero = True

        if (
            atual_f is not None
            and round(atual_f, 1) == novo
            and abs(atual_f) < 1000
            and not exige_forcar_numero
        ):
            continue

        updates.append(Cell(row=row_idx, col=col_idx, value=float(novo)))

    if updates:
        aba.update_cells(updates, value_input_option="RAW")
        print(f"  [fmt] aba '{aba.title}': {len(updates)} célula(s) corrigidas na coluna '{nome_coluna}'")

    n_linhas = len(values_raw)
    col_letra = rowcol_to_a1(1, col_idx)[:-1]
    intervalo = f"{col_letra}2:{col_letra}{n_linhas}"
    aba.format(intervalo, {"numberFormat": {"type": "NUMBER", "pattern": padrao}})


def adicionar_media_movel_13d_resultados_bi(df: pd.DataFrame) -> pd.DataFrame:
    """
    Expande cada série para o grão diário e calcula uma média móvel de 13 dias
    por candidato em cada combinação de ano/cargo/uf/turno/disputa/tipo/candidato_partido.
    disputa distingue os confrontos do 2º turno (ex.: Lula x Bolsonaro vs.
    Lula x Tarcísio); no 1º turno vem vazia e não afeta o agrupamento.

    A média móvel é **ponderada pelo score do instituto**: para cada janela de
    13 dias, mm = Σ(pct_dia × peso_total_dia) / Σ(peso_total_dia). Quando
    peso_total_dia não está disponível (ex.: dados antigos), cai para média
    aritmética simples para preservar compatibilidade.

    Nos dias sem pesquisa, percentual_base e demais métricas diárias permanecem
    vazios, mas media_movel_13d continua preenchida para permitir linha contínua
    no BI.
    """
    if df.empty:
        df = df.copy()
        df["media_movel_13d"] = None
        return df

    df = df.copy()
    df["media_movel_13d"] = None

    if "data_campo" not in df.columns or "percentual_base" not in df.columns:
        return df

    if "peso_total_dia" not in df.columns:
        df["peso_total_dia"] = pd.NA

    for col in ["ano", "cargo", "uf", "turno", "disputa", "tipo", "candidato", "partido", "candidato_partido"]:
        if col not in df.columns:
            df[col] = ""

    df["candidato_partido"] = (
        df["candidato_partido"]
        .fillna("")
        .astype(str)
        .str.strip()
    )
    vazio_candidato_partido = df["candidato_partido"].eq("")
    if vazio_candidato_partido.any():
        candidato = df["candidato"].fillna("").astype(str).str.strip()
        partido = df["partido"].fillna("").astype(str).str.strip()
        composto = candidato.where(partido.eq(""), candidato + " (" + partido + ")")
        df.loc[vazio_candidato_partido, "candidato_partido"] = composto.loc[vazio_candidato_partido]

    for col in ["ano", "cargo", "uf", "turno", "disputa", "tipo", "candidato_partido", "data_campo"]:
        df[col] = df[col].fillna("").astype(str).str.strip()

    df["_data_campo_dt"] = pd.to_datetime(df["data_campo"], errors="coerce")
    df["_percentual_base_num"] = pd.to_numeric(
        df["percentual_base"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )
    df["_peso_total_dia_num"] = pd.to_numeric(
        df["peso_total_dia"].astype(str).str.replace(",", ".", regex=False),
        errors="coerce",
    )

    # disputa junto de turno: no 2º turno cada confronto (Lula x Bolsonaro,
    # Lula x Tarcísio etc.) é uma série própria, senão a janela de 13 dias
    # mistura dias de confrontos diferentes na mesma média móvel.
    chaves_escopo = ["ano", "cargo", "uf", "turno", "disputa", "tipo"]
    chaves_serie = chaves_escopo + ["candidato_partido"]
    colunas_dimensao = ["ano", "uf", "cargo", "turno", "disputa", "tipo", "candidato", "partido", "candidato_partido"]

    df_com_serie = df[
        df["_data_campo_dt"].notna()
        & df["candidato_partido"].astype(str).str.strip().ne("")
    ].copy()
    df_sem_serie = df[~df.index.isin(df_com_serie.index)].copy()

    if df_com_serie.empty:
        return df.drop(columns=["_data_campo_dt", "_percentual_base_num"], errors="ignore")

    datas_finais_por_escopo = (
        df_com_serie.groupby(chaves_escopo, dropna=False)["_data_campo_dt"]
        .max()
        .to_dict()
    )

    partes_expandidas = []

    for chave_serie, grupo in df_com_serie.groupby(chaves_serie, dropna=False):
        grupo = grupo.sort_values("_data_campo_dt").copy()
        datas_validas = grupo["_data_campo_dt"].dropna().sort_values()
        if datas_validas.empty:
            partes_expandidas.append(grupo)
            continue

        chave_escopo = chave_serie[:len(chaves_escopo)] if isinstance(chave_serie, tuple) else (chave_serie,)
        data_final_escopo = datas_finais_por_escopo.get(chave_escopo, datas_validas.iloc[-1])
        faixa_datas = pd.date_range(datas_validas.iloc[0], data_final_escopo, freq="D")
        base_datas = pd.DataFrame({"_data_campo_dt": faixa_datas})

        # Ponderação por score do instituto: na janela rolling de 13 dias,
        # mm = Σ(pct_dia × peso_dia) / Σ(peso_dia). Se peso_total_dia não
        # estiver disponível para uma linha (NaN ou 0), assumimos peso 1
        # — fallback para média aritmética simples, preservando compatibilidade
        # com séries antigas que ainda não tinham peso_total_dia.
        grupo_diario = (
            grupo.groupby("_data_campo_dt")
            .agg(
                _pct=("_percentual_base_num", "mean"),
                _peso=("_peso_total_dia_num", "sum"),
            )
        )
        peso_vazio = grupo_diario["_peso"].isna() | grupo_diario["_peso"].eq(0)
        grupo_diario.loc[peso_vazio & grupo_diario["_pct"].notna(), "_peso"] = 1.0

        num = (grupo_diario["_pct"] * grupo_diario["_peso"]).reindex(faixa_datas, fill_value=0.0)
        den = grupo_diario["_peso"].reindex(faixa_datas, fill_value=0.0)
        num.index = pd.to_datetime(num.index, utc=True).tz_localize(None)
        den.index = pd.to_datetime(den.index, utc=True).tz_localize(None)

        roll_num = num.rolling(window="13D", min_periods=1).sum()
        roll_den = den.rolling(window="13D", min_periods=1).sum()
        mm_diaria = roll_num / roll_den.where(roll_den > 0, other=pd.NA)

        grupo_merge = (
            grupo.drop(columns=["media_movel_13d"], errors="ignore")
            .drop_duplicates(subset=["_data_campo_dt"], keep="first")
            .copy()
        )

        expandido = base_datas.merge(grupo_merge, on="_data_campo_dt", how="left")

        referencia = grupo.iloc[0]
        for col in colunas_dimensao:
            expandido[col] = expandido[col].fillna(referencia.get(col, ""))

        expandido["data_campo"] = expandido["_data_campo_dt"].dt.strftime("%Y-%m-%d")
        expandido["media_movel_13d"] = mm_diaria.to_numpy()
        partes_expandidas.append(expandido)

    frames_concat = [parte for parte in partes_expandidas if parte is not None and not parte.empty]
    if not df_sem_serie.empty:
        frames_concat.append(df_sem_serie)
    df_expandido = pd.concat(frames_concat, ignore_index=True, sort=False) if frames_concat else df.copy()
    if not df_expandido.empty:
        df_expandido = df_expandido.convert_dtypes(convert_string=False)
    return df_expandido.drop(
        columns=["_data_campo_dt", "_percentual_base_num", "_peso_total_dia_num"],
        errors="ignore",
    )


def deduplicar_resultados_bi_preferindo_cenario_media(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicatas lógicas no BI quando a mesma pesquisa aparece com
    poll_ids diferentes ao longo do tempo.

    Regra de preferência:
    - prioriza linhas com origem_percentual_base = cenario_media_existente;
    - depois media_calculada_no_codigo;
    - por fim qualquer outra origem.

    Para pesquisas sem registro válido, a chave lógica usa instituto + data +
    escopo eleitoral, evitando que mudanças no block_hash gerem duplicatas no BI.
    """
    if df.empty:
        return df

    df = df.copy()

    def registro_valido(valor: str) -> bool:
        s = _norm_ws(valor).lower()
        return s not in ("", "sem registro", "sem_registro", "semregistro", "nan")

    def chave_pesquisa_logica(row) -> str:
        # Inclui disputa: no 2º turno, o mesmo registro/pesquisa pode trazer
        # vários confrontos hipotéticos (ex.: Lula x Bolsonaro, Lula x Tarcísio).
        # Sem isso, o dedup trata os candidatos comuns como duplicata e apaga
        # o cenário do outro confronto.
        registro = _norm_ws(row.get("registro_tse", ""))
        if registro_valido(registro):
            return "|".join([
                "registro",
                registro,
                _norm_ws(row.get("ano", "")),
                _norm_ws(row.get("uf", "")),
                _norm_ws(row.get("cargo", "")),
                _norm_ws(row.get("turno", "")),
                _norm_ws(row.get("disputa", "")),
            ])

        return "|".join([
            "sem_registro",
            _slug(row.get("instituto", "")),
            _norm_ws(row.get("data_campo", "")),
            _norm_ws(row.get("ano", "")),
            _norm_ws(row.get("uf", "")),
            _norm_ws(row.get("cargo", "")),
            _norm_ws(row.get("turno", "")),
            _norm_ws(row.get("disputa", "")),
        ])

    prioridade_origem = {
        "cenario_media_existente": 0,
        "media_calculada_no_codigo": 1,
    }

    df["_chave_pesquisa_logica"] = df.apply(chave_pesquisa_logica, axis=1)
    df["_chave_bi_dedup"] = (
        df["_chave_pesquisa_logica"].astype(str)
        + "|" + df["tipo"].fillna("").astype(str).str.strip()
        + "|" + df["candidato_partido"].fillna("").astype(str).str.strip()
    )
    df["_prioridade_origem_bi"] = (
        df["origem_percentual_base"]
        .fillna("")
        .astype(str)
        .map(lambda x: prioridade_origem.get(x, 9))
    )

    df = (
        df.sort_values(
            by=["_chave_bi_dedup", "_prioridade_origem_bi", "poll_id"],
            ascending=[True, True, True],
            na_position="last",
        )
        .drop_duplicates(subset=["_chave_bi_dedup"], keep="first")
        .copy()
    )

    return df.drop(
        columns=["_chave_pesquisa_logica", "_chave_bi_dedup", "_prioridade_origem_bi"],
        errors="ignore",
    )


def agregar_resultados_bi_diario(df: pd.DataFrame) -> pd.DataFrame:
    """
    Agrega a base de BI no grão diário antes do cálculo da média móvel.

    A média diária por candidato é **ponderada pelo score do instituto**
    (ver SCORE_INSTITUTO). Cada pesquisa contribui com uma observação
    (já consolidada por cenário em adicionar_metricas_media_cenarios)
    multiplicada pelo seu peso. Também devolve peso_total_dia
    (Σ w_i no dia), usado depois pela média móvel ponderada.
    """
    if df.empty:
        return df

    df = df.copy()
    prioridade_origem = {
        "cenario_media_existente": 0,
        "media_calculada_no_codigo": 1,
    }

    for col in [
        "ano", "uf", "cargo", "turno", "disputa", "data_campo", "tipo", "candidato",
        "partido", "candidato_partido", "instituto", "classificacao_instituto",
        "registro_tse", "origem_percentual_base", "cenario_usado_no_calculo",
        "fonte_url", "poll_id", "horario_raspagem"
    ]:
        if col not in df.columns:
            df[col] = ""

    df["_prioridade_origem_bi"] = (
        df["origem_percentual_base"]
        .fillna("")
        .astype(str)
        .map(lambda x: prioridade_origem.get(x, 9))
    )

    df["_peso"] = df["classificacao_instituto"].apply(score_instituto).astype(float)
    df["_pct_num"] = pd.to_numeric(df["percentual_base"], errors="coerce")
    df["_pct_x_peso"] = df["_pct_num"] * df["_peso"]
    # Linhas sem percentual válido não devem entrar no denominador
    df.loc[df["_pct_num"].isna(), "_peso"] = 0.0
    df["_pct_x_peso"] = df["_pct_x_peso"].fillna(0.0)

    # disputa entra na dimensão: no 2º turno, cada confronto (ex.: Lula x
    # Bolsonaro vs. Lula x Tarcísio) é uma série própria, não pode ser
    # agregado junto só porque o candidato e o dia são os mesmos.
    dims = [
        "ano", "uf", "cargo", "turno", "disputa", "data_campo", "tipo",
        "candidato", "partido", "candidato_partido"
    ]

    def juntar_unicos(series: pd.Series) -> str:
        valores = sorted({_norm_ws(v) for v in series if _norm_ws(v)})
        return " | ".join(valores)

    df = df.sort_values(
        by=dims + ["_prioridade_origem_bi", "poll_id"],
        ascending=[True] * len(dims) + [True, True],
        na_position="last",
    )

    df_diario = (
        df.groupby(dims, dropna=False)
        .agg(
            _peso_total=("_peso", "sum"),
            _pct_x_peso_sum=("_pct_x_peso", "sum"),
            qtd_pesquisas_dia=("poll_id", "nunique"),
            qtd_cenarios_considerados=("qtd_cenarios_considerados", "sum"),
            origem_percentual_base=("origem_percentual_base", "first"),
            cenario_usado_no_calculo=("cenario_usado_no_calculo", "first"),
            institutos_no_dia=("instituto", juntar_unicos),
            classificacoes_instituto_no_dia=("classificacao_instituto", juntar_unicos),
            registros_tse_no_dia=("registro_tse", juntar_unicos),
            fontes_no_dia=("fonte_url", juntar_unicos),
            poll_ids_agregados=("poll_id", juntar_unicos),
            horario_raspagem=("horario_raspagem", "max"),
        )
        .reset_index()
    )

    peso_total = df_diario["_peso_total"].astype(float)
    df_diario["percentual_base"] = (
        df_diario["_pct_x_peso_sum"] / peso_total.where(peso_total > 0, other=pd.NA)
    )
    df_diario["peso_total_dia"] = peso_total.round(4)
    df_diario["score_medio_dia"] = (
        peso_total / df_diario["qtd_pesquisas_dia"].replace(0, pd.NA)
    ).round(4)

    return df_diario.drop(columns=["_peso_total", "_pct_x_peso_sum"])


def selecionar_cenario_principal(df_resultados: pd.DataFrame) -> pd.DataFrame:
    """Seleciona um cenário por pesquisa para os agregadores de controle.

    Para evitar que uma mesma rodada acumule peso, o modelo usa apenas o
    cenário principal/mais amplo. Aqui, "mais amplo" é o cenário com o maior
    número de candidatos. Empates são resolvidos de forma determinística pelo
    rótulo e pelo id do cenário.
    """
    if df_resultados is None or df_resultados.empty:
        return pd.DataFrame()

    df = df_resultados.copy()
    for col in ["poll_id", "scenario_id", "scenario_label", "tipo", "candidato", "turno", "disputa"]:
        if col not in df.columns:
            df[col] = ""

    df = df[
        df["poll_id"].astype(str).str.strip().ne("")
        & df["tipo"].astype(str).str.strip().str.lower().eq("candidato")
        & ~df["scenario_label"].apply(eh_cenario_media)
    ].copy()
    if df.empty:
        return df

    scenario_id_vazio = df["scenario_id"].astype(str).str.strip().eq("")
    df.loc[scenario_id_vazio, "scenario_id"] = (
        df.loc[scenario_id_vazio, "poll_id"].astype(str)
        + "|cenario|"
        + df.loc[scenario_id_vazio, "scenario_label"].astype(str)
    )

    chaves_escopo = ["poll_id", "turno", "disputa"]
    contagens = (
        df.groupby(chaves_escopo + ["scenario_id", "scenario_label"], dropna=False)["candidato"]
        .nunique()
        .reset_index(name="_qtd_candidatos")
    )
    contagens["_cenario_ordem_num"] = pd.to_numeric(
        contagens["scenario_label"], errors="coerce"
    )
    contagens["_cenario_ordem_txt"] = contagens["scenario_label"].fillna("").astype(str)
    escolhidos = (
        contagens.sort_values(
            chaves_escopo + ["_qtd_candidatos", "_cenario_ordem_num", "_cenario_ordem_txt", "scenario_id"],
            ascending=[True] * len(chaves_escopo) + [False, True, True, True],
            na_position="last",
        )
        .drop_duplicates(chaves_escopo, keep="first")[chaves_escopo + ["scenario_id"]]
    )
    return df.merge(escolhidos, on=chaves_escopo + ["scenario_id"], how="inner")


def _normalizar_candidato_agregador(valor) -> str:
    txt = unicodedata.normalize("NFKD", _norm_ws(valor))
    return "".join(ch for ch in txt if not unicodedata.combining(ch)).lower()


def _polls_com_todos_os_nomes(df: pd.DataFrame, nomes) -> list:
    """poll_ids cujo cenário selecionado testa todos os nomes informados."""
    ids = []
    for poll_id, grupo in df.groupby("poll_id", dropna=False):
        presentes = " | ".join(grupo["candidato"].map(_normalizar_candidato_agregador))
        if all(nome in presentes for nome in nomes):
            ids.append(poll_id)
    return ids


def _anexar_metadados_pesquisa(
    df_resultados: pd.DataFrame,
    df_pesquisas: pd.DataFrame | None,
) -> pd.DataFrame:
    """Anexa amostra e início do campo às linhas de resultado."""
    df = df_resultados.copy()
    for col in ["amostra", "data_inicio_campo"]:
        if col not in df.columns:
            df[col] = pd.NA

    if df_pesquisas is None or df_pesquisas.empty:
        return df

    meta = df_pesquisas.copy()
    for col in ["scenario_id", "poll_id", "amostra", "data_inicio_campo"]:
        if col not in meta.columns:
            meta[col] = pd.NA

    if "scenario_id" in df.columns:
        por_cenario = (
            meta[["scenario_id", "amostra", "data_inicio_campo"]]
            .dropna(subset=["scenario_id"])
            .drop_duplicates("scenario_id", keep="first")
            .rename(columns={
                "amostra": "_amostra_meta",
                "data_inicio_campo": "_data_inicio_meta",
            })
        )
        df = df.merge(por_cenario, on="scenario_id", how="left")
        df["amostra"] = df["amostra"].where(df["amostra"].notna(), df["_amostra_meta"])
        df["data_inicio_campo"] = df["data_inicio_campo"].where(
            df["data_inicio_campo"].notna(), df["_data_inicio_meta"]
        )
        df = df.drop(columns=["_amostra_meta", "_data_inicio_meta"])

    faltam_meta = df["amostra"].isna() | df["data_inicio_campo"].isna()
    if faltam_meta.any() and "poll_id" in df.columns:
        por_poll = (
            meta[["poll_id", "amostra", "data_inicio_campo"]]
            .dropna(subset=["poll_id"])
            .drop_duplicates("poll_id", keep="first")
            .rename(columns={
                "amostra": "_amostra_poll",
                "data_inicio_campo": "_data_inicio_poll",
            })
        )
        df = df.merge(por_poll, on="poll_id", how="left")
        df["amostra"] = df["amostra"].where(df["amostra"].notna(), df["_amostra_poll"])
        df["data_inicio_campo"] = df["data_inicio_campo"].where(
            df["data_inicio_campo"].notna(), df["_data_inicio_poll"]
        )
        df = df.drop(columns=["_amostra_poll", "_data_inicio_poll"])

    return df


def _calcular_serie_agregada_30d(
    df: pd.DataFrame,
    coluna_saida: str,
    *,
    ponderar_instituto: bool,
    datas_finais_escopo: dict | None = None,
) -> pd.DataFrame:
    """Calcula a média diária com amostra e decaimento de meia-vida 30 dias."""
    chaves_escopo = ["ano", "uf", "cargo", "turno", "disputa", "tipo"]
    chaves_serie = chaves_escopo + ["candidato_partido"]
    chaves_saida = chaves_serie + ["data_campo"]
    if df.empty:
        return pd.DataFrame(columns=chaves_saida + [coluna_saida])

    datas_finais = datas_finais_escopo or (
        df.groupby(chaves_escopo, dropna=False)["_data_disponivel"].max().to_dict()
    )
    linhas = []
    for chave, grupo in df.groupby(chaves_serie, dropna=False):
        grupo = grupo.sort_values("_data_disponivel").copy()
        if grupo.empty:
            continue
        chave_escopo = chave[:len(chaves_escopo)]
        data_final = datas_finais.get(chave_escopo, grupo["_data_disponivel"].max())
        for data_ref in pd.date_range(grupo["_data_disponivel"].min(), data_final, freq="D"):
            disponiveis = grupo[grupo["_data_disponivel"].le(data_ref)].copy()
            idade = (data_ref - disponiveis["_data_peso"]).dt.days.clip(lower=0)
            peso = disponiveis["_amostra_num"].pow(0.5) * (
                2.0 ** (-idade / MEIA_VIDA_AGREGADORES_DIAS)
            )
            if ponderar_instituto:
                peso = peso * disponiveis["_score_instituto"]
            denominador = peso.sum()
            if denominador <= 0:
                continue
            valor = (disponiveis["_percentual_num"] * peso).sum() / denominador
            linha = dict(zip(chaves_serie, chave))
            linha["data_campo"] = data_ref.strftime("%Y-%m-%d")
            linha[coluna_saida] = float(valor)
            linhas.append(linha)

    return pd.DataFrame(linhas, columns=chaves_saida + [coluna_saida])


def calcular_agregadores_paralelos_resultados_bi(
    df_resultados: pd.DataFrame,
    df_pesquisas: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """Gera os modelos amostral e híbrido sem substituir a série oficial.

    Modelo amostral: cenário mais amplo, pesquisas presidenciais nacionais de 1º
    turno que testam Lula, Flávio Bolsonaro e Ronaldo Caiado simultaneamente,
    peso sqrt(amostra) e meia-vida de 30 dias.

    Híbrido: mesmo cenário principal e componentes de amostra/recência, com o
    score de qualidade do instituto multiplicando o peso. Instituto não
    avaliado continua recebendo 0,25, sem exceções por nome.
    """
    principal = selecionar_cenario_principal(df_resultados)
    if principal.empty:
        return pd.DataFrame()
    principal = _anexar_metadados_pesquisa(principal, df_pesquisas)

    for col in [
        "ano", "uf", "cargo", "turno", "disputa", "tipo", "candidato",
        "partido", "candidato_partido", "classificacao_instituto",
        "registro_tse", "data_campo",
    ]:
        if col not in principal.columns:
            principal[col] = ""
    vazio_cp = principal["candidato_partido"].fillna("").astype(str).str.strip().eq("")
    principal.loc[vazio_cp, "candidato_partido"] = principal.loc[vazio_cp, "candidato"].astype(str)

    principal["_percentual_num"] = pd.to_numeric(principal.get("percentual"), errors="coerce")
    principal["_amostra_num"] = pd.to_numeric(principal["amostra"], errors="coerce")
    inicio = pd.to_datetime(principal["data_inicio_campo"], errors="coerce")
    fim = pd.to_datetime(principal["data_campo"], errors="coerce")
    # A pesquisa só passa a compor a série quando o campo termina, evitando
    # vazamento retrospectivo. A idade do peso, porém, é contada desde o início
    # do campo, conforme a definição do modelo amostral.
    principal["_data_peso"] = inicio.fillna(fim)
    principal["_data_disponivel"] = fim
    principal["_score_instituto"] = principal["classificacao_instituto"].apply(score_instituto)
    principal = principal[
        principal["_percentual_num"].notna()
        & principal["_amostra_num"].gt(0)
        & principal["_data_peso"].notna()
        & principal["_data_disponivel"].notna()
    ].copy()
    if principal.empty:
        return pd.DataFrame()

    # Uma linha por pesquisa/candidato: candidato ausente não entra no seu
    # denominador, em vez de ser imputado como zero.
    principal = principal.sort_values(["poll_id", "scenario_id", "candidato_partido"])
    principal = principal.drop_duplicates(["poll_id", "candidato_partido"], keep="first")

    # Presidencial de 1º turno só entra quando o cenário escolhido testa Lula e
    # Flávio juntos. Sem isso a série mistura disputas diferentes na mesma linha
    # (cenário com Michelle no lugar do Flávio, por exemplo) e o percentual de
    # cada candidato vira média de confrontos que não são o mesmo confronto.
    presidencial_t1 = (
        principal["cargo"].astype(str).str.lower().str.contains("president", na=False)
        & principal["turno"].astype(str).str.strip().str.lower().eq("t1")
    )
    if presidencial_t1.any():
        polls_ok = _polls_com_todos_os_nomes(principal[presidencial_t1], ("lula", "flavio"))
        descartar = presidencial_t1 & ~principal["poll_id"].isin(polls_ok)
        if descartar.any():
            n_fora = principal.loc[descartar, "poll_id"].nunique()
            print(f"  [agregador] {n_fora} pesquisa(s) presidenciais de 1o turno fora da serie: cenario sem Lula e Flavio juntos")
            principal = principal[~descartar].copy()
        if principal.empty:
            return pd.DataFrame()

    chaves_escopo = ["ano", "uf", "cargo", "turno", "disputa", "tipo"]
    datas_finais_escopo = (
        principal.groupby(chaves_escopo, dropna=False)["_data_disponivel"].max().to_dict()
    )

    hibrido = _calcular_serie_agregada_30d(
        principal,
        COLUNA_MODELO_HIBRIDO,
        ponderar_instituto=True,
        datas_finais_escopo=datas_finais_escopo,
    )

    scope_abc = (
        principal["uf"].astype(str).str.strip().str.upper().eq("BR")
        & principal["cargo"].astype(str).str.lower().str.contains("president", na=False)
        & principal["turno"].astype(str).str.strip().str.lower().eq("t1")
        & principal["registro_tse"].fillna("").astype(str).str.strip().ne("")
        & ~principal["registro_tse"].fillna("").astype(str).str.lower().str.startswith("sem")
        & principal["_data_disponivel"].ge(pd.Timestamp("2026-01-01"))
    )
    abc_base = principal[scope_abc].copy()
    poll_ids_abc = _polls_com_todos_os_nomes(abc_base, ("lula", "flavio", "caiado"))
    abc_base = abc_base[abc_base["poll_id"].isin(poll_ids_abc)]
    abc = _calcular_serie_agregada_30d(
        abc_base,
        COLUNA_MODELO_AMOSTRAL,
        ponderar_instituto=False,
        datas_finais_escopo=datas_finais_escopo,
    )

    chaves = ["ano", "uf", "cargo", "turno", "disputa", "tipo", "candidato_partido", "data_campo"]
    if abc.empty:
        return hibrido
    if hibrido.empty:
        return abc
    return hibrido.merge(abc, on=chaves, how="outer")


def construir_resultados_bi(
    df_resultados: pd.DataFrame,
    df_pesquisas: pd.DataFrame | None = None,
) -> pd.DataFrame:
    """
    Gera uma base consolidada para BI no grão diário por candidato_partido.
    Primeiro seleciona o cenário principal (o mais amplo) de cada pesquisa;
    depois agrega por dia e só então calcula as médias móveis.
    """
    # Apenas colunas consumidas pelo painel Streamlit. Para auditoria detalhada
    # (poll_ids, registros TSE, fontes, peso de cada dia, origem do cálculo,
    # candidato/partido separados, ano) consulte a aba `resultados`, que mantém
    # o grão por scenario_id.
    cols = [
        "uf", "cargo", "turno", "disputa", "data_campo",
        "candidato_partido", "tipo",
        "percentual_base", "media_movel_13d",
        COLUNA_MODELO_AMOSTRAL, COLUNA_MODELO_HIBRIDO,
        "qtd_pesquisas_dia",
        "cenario_usado_no_calculo",
        "eh_lider", "eh_segundo",
        "institutos_no_dia", "classificacoes_instituto_no_dia",
    ]

    if df_resultados is None or df_resultados.empty:
        return pd.DataFrame(columns=cols)

    df = df_resultados.copy()

    # disputa só distingue confrontos no 2º turno. No 1º turno deve ser vazia:
    # valores espúrios (números, slugs herdados) fatiam a série de cada candidato
    # e quebram a média móvel de 13 dias. Normaliza mantendo disputa só em t2.
    if "disputa" not in df.columns:
        df["disputa"] = ""
    _eh_t2 = (
        df["turno"].astype(str).str.strip().str.lower().eq("t2")
        if "turno" in df.columns else False
    )
    df["disputa"] = df["disputa"].where(_eh_t2, "")

    if "percentual" in df.columns:
        df["percentual"] = df["percentual"].apply(parsear_pct)
    else:
        df["percentual"] = None

    if "data_campo" not in df.columns:
        df["data_campo"] = ""
    else:
        df["data_campo"] = df["data_campo"].apply(normalizar_data_campo_segura)

    for col in ["poll_id", "tipo", "candidato", "scenario_label"]:
        if col not in df.columns:
            df[col] = ""

    df = df[df["poll_id"].astype(str).str.strip().ne("") & df["percentual"].notna()].copy()
    if df.empty:
        return pd.DataFrame(columns=cols)

    df_base = selecionar_cenario_principal(df)
    if df_base.empty:
        return pd.DataFrame(columns=cols)
    df_base = df_base.copy()
    df_base["origem_percentual_base"] = "cenario_principal"
    df_base["percentual_base"] = pd.to_numeric(df_base["percentual"], errors="coerce")
    df_base["qtd_cenarios_considerados"] = 1
    df_base["cenario_usado_no_calculo"] = df_base["scenario_label"].fillna("").astype(str)

    df_candidatos = df_base[df_base["tipo"].astype(str).str.lower().eq("candidato")].copy()
    df_candidatos = deduplicar_resultados_bi_preferindo_cenario_media(df_candidatos)
    df_candidatos = agregar_resultados_bi_diario(df_candidatos)

    # disputa entra na chave de ranking: senão, candidatos de confrontos
    # diferentes do 2º turno (Lula x Bolsonaro, Lula x Tarcísio) competem
    # juntos pelo 1º/2º lugar em vez de dentro do próprio confronto.
    chaves_posicao = ["ano", "uf", "cargo", "turno", "disputa", "data_campo"]
    df_candidatos["posicao_candidato"] = (
        df_candidatos.groupby(chaves_posicao)["percentual_base"]
        .rank(method="min", ascending=False)
        .astype("Int64")
    )
    df_candidatos["eh_lider"] = df_candidatos["posicao_candidato"].eq(1)
    df_candidatos["eh_segundo"] = df_candidatos["posicao_candidato"].eq(2)
    df_candidatos = adicionar_media_movel_13d_resultados_bi(df_candidatos)

    df_paralelos = calcular_agregadores_paralelos_resultados_bi(df, df_pesquisas)
    if not df_paralelos.empty:
        chaves_paralelos = [
            "ano", "uf", "cargo", "turno", "disputa", "tipo",
            "candidato_partido", "data_campo",
        ]
        df_candidatos = df_candidatos.merge(
            df_paralelos,
            on=chaves_paralelos,
            how="left",
        )

    for col in cols:
        if col not in df_candidatos.columns:
            df_candidatos[col] = ""

    df_candidatos = df_candidatos[cols].copy()
    # Ordenação por percentual_base desc é equivalente a ordenar por posicao_candidato asc
    # (líder vem primeiro), e permite remover posicao_candidato do schema exportado.
    df_candidatos = df_candidatos.sort_values(
        by=["cargo", "uf", "turno", "disputa", "data_campo", "percentual_base", "candidato_partido"],
        ascending=[True, True, True, True, False, False, True],
        na_position="last",
    ).reset_index(drop=True)

    df_candidatos["eh_lider"] = df_candidatos["eh_lider"].map({True: "TRUE", False: "FALSE"}).fillna("")
    df_candidatos["eh_segundo"] = df_candidatos["eh_segundo"].map({True: "TRUE", False: "FALSE"}).fillna("")

    return df_candidatos


def dedup_e_salvar(aba, df: pd.DataFrame, key_col: str):
    if key_col not in df.columns:
        raise RuntimeError(f"df não tem coluna de chave: '{key_col}'")

    df = reordenar_metodologia_para_ultima_coluna(df)

    antes = len(df)
    df = df.drop_duplicates(subset=[key_col], keep="first").reset_index(drop=True)

    if len(df) < antes:
        print(f"  [dedup interno] removidas {antes - len(df)} duplicatas na coleta")

    values = aba.get_all_values()

    if _aba_vazia(values):
        df_export = df.astype(object).where(pd.notna(df), "")
        reescrever_aba(aba, [df.columns.tolist()] + df_export.values.tolist(),
                       "aba vazia", value_input_option="RAW")
        print(f"  [aba vazia] {len(df)} linhas gravadas")
        return len(df), 0

    header = values[0]
    if key_col not in header:
        print(f"  [aviso] chave '{key_col}' ausente no header. Reescrevendo aba.")
        df_export = df.astype(object).where(pd.notna(df), "")
        reescrever_aba(aba, [df.columns.tolist()] + df_export.values.tolist(),
                       "header ausente", value_input_option="RAW")
        return len(df), 0

    colunas_novas = [c for c in df.columns if c not in header]
    # Campos técnicos ao final; `origem` fica por último para que toda coluna
    # nova de procedência seja adicionada sem deslocar os campos operacionais.
    cols_final = ["percentual_media_cenarios", "origem_percentual_media", "metodologia", "origem"]
    header_base = [c for c in header if c not in cols_final]
    novas_base = [c for c in colunas_novas if c not in cols_final]
    header_final = header_base + novas_base
    header_final += [c for c in cols_final if c in df.columns or c in header]

    if header_final != header:
        aba.update([header_final], range_name="A1")
        if colunas_novas:
            print(f"  [schema] {len(colunas_novas)} coluna(s) nova(s): {colunas_novas}")
        else:
            print(f"  [schema] colunas finais reposicionadas: {[c for c in cols_final if c in header_final]}")

    idx_key = header_final.index(key_col)
    values = aba.get_all_values()
    existing = {row[idx_key] for row in values[1:] if len(row) > idx_key and row[idx_key].strip()}

    df_add = df[~df[key_col].astype(str).isin(existing)].reset_index(drop=True)

    if df_add.empty:
        print(f"  [sem novidades] {len(existing)} já existiam")
        return 0, len(existing)

    df_add = df_add.reindex(columns=header_final, fill_value="")
    # Não serializar números como strings: 45.9 deve chegar como número 45.9,
    # nunca como texto que uma planilha possa reinterpretar como 459.
    df_export = df_add.astype(object).where(pd.notna(df_add), "")
    aba.insert_rows(df_export.values.tolist(), row=2, value_input_option="RAW")

    print(f"  [insert] {len(df_add)} nova(s) | {len(existing)} já existiam")
    return len(df_add), len(existing)


def _norm_key_text(s) -> str:
    txt = _norm_ws(s)
    if not txt:
        return ""
    txt = unicodedata.normalize("NFKD", txt)
    txt = "".join(ch for ch in txt if not unicodedata.combining(ch))
    txt = txt.lower()
    txt = re.sub(r"[^a-z0-9]+", " ", txt)
    return re.sub(r"\s+", " ", txt).strip()


def _registro_tse_valido(valor) -> bool:
    s = _norm_ws(valor).lower()
    return s not in ("", "sem registro", "sem_registro", "semregistro", "nan", "none")


def _tipo_origem(origem_valor) -> str:
    """Classifica as procedências usadas nas matrizes, tolerando rótulos antigos."""
    origem = _norm_key_text(origem_valor)
    if "manual" in origem or "streamlit" in origem:
        return "manual"
    if "pdf" in origem and "relatorio" in origem:
        return "pdf"
    if "pollingdata" in origem:
        return "pollingdata"
    return ""


def _eh_linha_manual(origem_valor) -> bool:
    return _tipo_origem(origem_valor) == "manual"


def _eh_linha_oficial_pollingdata(origem_valor) -> bool:
    """Somente o coletor do PollingData substitui entrada manual equivalente.

    PDFs de relatório têm procedência própria e nunca acionam essa reconciliação.
    """
    return _tipo_origem(origem_valor) == "pollingdata"


def _origem_migrada(origem_valor, conferida_legada="") -> str:
    """Preserva a origem já existente ou traduz a marca legada `conferida`."""
    origem = _norm_ws(origem_valor)
    if origem:
        return origem

    legado = _norm_ws(conferida_legada).lower()
    if legado == "manual_streamlit" or "manual" in legado:
        return ORIGEM_POLLING_MANUAL
    if "pdf" in legado or "relatorio" in _norm_key_text(legado):
        return ORIGEM_PDF_RELATORIO
    # Linhas antigas sem marca eram produzidas pelo scraper PollingData.
    return ORIGEM_POLLINGDATA


def _parse_data_yyyy_mm_dd(valor):
    s = normalizar_data_campo_segura(valor)
    if not s:
        return None
    try:
        return datetime.strptime(s, "%Y-%m-%d").date()
    except Exception:
        return None


def _assinatura_poll(df_resultados: pd.DataFrame, poll_id: str) -> dict:
    if df_resultados is None or df_resultados.empty or not poll_id:
        return {}

    df = df_resultados.copy()
    for col in ["poll_id", "tipo", "candidato_partido", "candidato", "percentual"]:
        if col not in df.columns:
            df[col] = ""

    recorte = df[df["poll_id"].astype(str).str.strip().eq(_norm_ws(poll_id))].copy()
    if recorte.empty:
        return {}

    recorte["_tipo_key"] = recorte["tipo"].apply(_norm_key_text)
    recorte["_cand_key"] = recorte["candidato_partido"].apply(_norm_key_text)
    recorte["_cand_key"] = recorte["_cand_key"].mask(
        recorte["_cand_key"].eq(""),
        recorte["candidato"].apply(_norm_key_text),
    )
    recorte["_item_key"] = recorte["_tipo_key"] + "|" + recorte["_cand_key"]
    recorte["_pct"] = recorte["percentual"].apply(parsear_pct)
    recorte = recorte[recorte["_item_key"].str.strip().ne("") & recorte["_pct"].notna()].copy()
    if recorte.empty:
        return {}

    medias = recorte.groupby("_item_key", dropna=False)["_pct"].mean()
    return {k: float(v) for k, v in medias.items()}


def _avaliar_similaridade_polls(assinatura_manual: dict, assinatura_oficial: dict) -> tuple[int, float | None]:
    if not assinatura_manual or not assinatura_oficial:
        return 0, None

    itens_comuns = sorted(set(assinatura_manual.keys()) & set(assinatura_oficial.keys()))
    if not itens_comuns:
        return 0, None

    erros = [abs(assinatura_manual[k] - assinatura_oficial[k]) for k in itens_comuns]
    mae = sum(erros) / len(erros)
    return len(itens_comuns), float(mae)


def reconciliar_manuais_com_oficiais(
    df_p_existente: pd.DataFrame,
    df_r_existente: pd.DataFrame,
    df_p_novo: pd.DataFrame,
    df_r_novo: pd.DataFrame,
    janela_dias_fallback: int = 3,
    mae_max_fallback: float = 1.5,
):
    """
    Remove entradas manuais já cobertas por entrada oficial recém-coletada.
    Regras:
    1) Match forte: mesmo registro_tse + escopo (uf/cargo/turno);
    2) Fallback: mesmo instituto + escopo + data próxima + resultados parecidos.
    """
    if df_p_existente is None:
        df_p_existente = pd.DataFrame()
    if df_r_existente is None:
        df_r_existente = pd.DataFrame()
    if df_p_novo is None:
        df_p_novo = pd.DataFrame()
    if df_r_novo is None:
        df_r_novo = pd.DataFrame()

    if df_p_existente.empty or df_p_novo.empty:
        return df_p_existente, df_r_existente, pd.DataFrame()

    p_exist = df_p_existente.copy()
    p_novo = df_p_novo.copy()

    for df in [p_exist, p_novo]:
        for col in ["poll_id", "uf", "cargo", "turno", "instituto", "registro_tse", "data_campo", "origem"]:
            if col not in df.columns:
                df[col] = ""

    p_exist["_is_manual"] = p_exist["origem"].apply(_eh_linha_manual)
    p_novo["_is_manual"] = p_novo["origem"].apply(_eh_linha_manual)
    p_novo["_is_official"] = p_novo["origem"].apply(_eh_linha_oficial_pollingdata)

    p_manual = (
        p_exist[p_exist["_is_manual"]]
        .sort_values(by=["poll_id", "scenario_id"] if "scenario_id" in p_exist.columns else ["poll_id"])
        .drop_duplicates(subset=["poll_id"], keep="first")
        .copy()
    )
    p_oficial = (
        p_novo[p_novo["_is_official"]]
        .sort_values(by=["poll_id", "scenario_id"] if "scenario_id" in p_novo.columns else ["poll_id"])
        .drop_duplicates(subset=["poll_id"], keep="first")
        .copy()
    )

    if p_manual.empty or p_oficial.empty:
        return df_p_existente, df_r_existente, pd.DataFrame()

    for df in [p_manual, p_oficial]:
        df["_uf_key"] = df["uf"].apply(_norm_key_text)
        df["_cargo_key"] = df["cargo"].apply(_norm_key_text)
        df["_turno_key"] = df["turno"].apply(_norm_key_text)
        df["_inst_key"] = df["instituto"].apply(_norm_key_text)
        df["_registro_key"] = df["registro_tse"].apply(lambda x: _norm_ws(x).upper())
        df["_registro_ok"] = df["registro_tse"].apply(_registro_tse_valido)
        df["_data_dt"] = df["data_campo"].apply(_parse_data_yyyy_mm_dd)

    remover_poll_ids = set()
    detalhes = []
    cache_assin_manual = {}
    cache_assin_oficial = {}

    def assinatura_manual(pid: str) -> dict:
        if pid not in cache_assin_manual:
            cache_assin_manual[pid] = _assinatura_poll(df_r_existente, pid)
        return cache_assin_manual[pid]

    def assinatura_oficial(pid: str) -> dict:
        if pid not in cache_assin_oficial:
            cache_assin_oficial[pid] = _assinatura_poll(df_r_novo, pid)
        return cache_assin_oficial[pid]

    for _, off in p_oficial.iterrows():
        off_poll_id = _norm_ws(off.get("poll_id", ""))
        if not off_poll_id:
            continue

        base_escopo = (
            p_manual["_uf_key"].eq(off["_uf_key"])
            & p_manual["_cargo_key"].eq(off["_cargo_key"])
            & p_manual["_turno_key"].eq(off["_turno_key"])
        )
        cand = p_manual[base_escopo].copy()
        if cand.empty:
            continue

        # Regra 1: match forte por registro TSE
        if bool(off.get("_registro_ok")):
            cand_reg = cand[
                cand["_registro_ok"] & cand["_registro_key"].eq(off["_registro_key"])
            ].copy()
            if not cand_reg.empty:
                for _, man in cand_reg.iterrows():
                    man_poll_id = _norm_ws(man.get("poll_id", ""))
                    if not man_poll_id or man_poll_id in remover_poll_ids:
                        continue
                    remover_poll_ids.add(man_poll_id)
                    detalhes.append({
                        "poll_id_manual_removido": man_poll_id,
                        "poll_id_oficial": off_poll_id,
                        "regra_match": "registro_tse_igual",
                        "itens_em_comum": "",
                        "mae_percentual": "",
                        "dif_dias_data_campo": "",
                    })
                continue

        # Regra 2: fallback por instituto + data próxima + similaridade
        cand_fb = cand[cand["_inst_key"].eq(off["_inst_key"])].copy()
        if cand_fb.empty:
            continue

        data_off = off.get("_data_dt")
        if data_off is not None:
            cand_fb["_dif_dias"] = cand_fb["_data_dt"].apply(
                lambda d: abs((d - data_off).days) if d is not None else None
            )
            cand_fb = cand_fb[
                cand_fb["_dif_dias"].notna() & cand_fb["_dif_dias"].le(janela_dias_fallback)
            ].copy()
        else:
            cand_fb["_dif_dias"] = None

        if cand_fb.empty:
            continue

        assinatura_off = assinatura_oficial(off_poll_id)
        melhor = None
        for _, man in cand_fb.iterrows():
            man_poll_id = _norm_ws(man.get("poll_id", ""))
            if not man_poll_id or man_poll_id in remover_poll_ids:
                continue
            assinatura_man = assinatura_manual(man_poll_id)
            itens_comuns, mae = _avaliar_similaridade_polls(assinatura_man, assinatura_off)
            if itens_comuns == 0 or mae is None:
                continue
            if mae > mae_max_fallback:
                continue

            cand_item = {
                "man_poll_id": man_poll_id,
                "itens_comuns": itens_comuns,
                "mae": mae,
                "dif_dias": man.get("_dif_dias"),
            }
            if melhor is None:
                melhor = cand_item
            else:
                # prioriza menor erro; empate: mais itens comuns
                if (cand_item["mae"] < melhor["mae"]) or (
                    cand_item["mae"] == melhor["mae"] and cand_item["itens_comuns"] > melhor["itens_comuns"]
                ):
                    melhor = cand_item

        if melhor is not None:
            remover_poll_ids.add(melhor["man_poll_id"])
            detalhes.append({
                "poll_id_manual_removido": melhor["man_poll_id"],
                "poll_id_oficial": off_poll_id,
                "regra_match": "fallback_instituto_data_similaridade",
                "itens_em_comum": melhor["itens_comuns"],
                "mae_percentual": round(melhor["mae"], 3),
                "dif_dias_data_campo": int(melhor["dif_dias"]) if pd.notna(melhor["dif_dias"]) else "",
            })

    if not remover_poll_ids:
        return df_p_existente, df_r_existente, pd.DataFrame()

    p_limpo = df_p_existente[~df_p_existente["poll_id"].astype(str).str.strip().isin(remover_poll_ids)].copy()
    r_limpo = df_r_existente[~df_r_existente["poll_id"].astype(str).str.strip().isin(remover_poll_ids)].copy()
    df_det = pd.DataFrame(detalhes).drop_duplicates().reset_index(drop=True)
    return p_limpo, r_limpo, df_det


def append_log_resultados_manual(gc, spreadsheet_id: str, df_log: pd.DataFrame, aba_nome: str = "resultados_manual") -> int:
    """
    Registra histórico manual em aba append-only.
    Não interfere nas abas operacionais (`pesquisas`, `resultados`, `resultados_bi`).
    """
    if df_log is None or df_log.empty:
        return 0

    sh = gc.open_by_key(spreadsheet_id)
    aba = garantir_aba(sh, aba_nome, rows=50000, cols=50)

    values = aba.get_all_values()
    header_novo = df_log.columns.tolist()

    if _aba_vazia(values):
        reescrever_aba(aba, [header_novo], aba_nome)
        header_final = header_novo
    else:
        header_atual = values[0]
        cols_novas = [c for c in header_novo if c not in header_atual]
        header_final = header_atual + cols_novas
        if header_final != header_atual:
            aba.update([header_final], range_name="A1")
            print(f"  [schema:{aba_nome}] {len(cols_novas)} coluna(s) nova(s): {cols_novas}")

    df_export = (
        df_log.reindex(columns=header_final, fill_value="")
        .astype(object)
        .where(pd.notna(df_log.reindex(columns=header_final, fill_value="")), "")
        .astype(str)
    )

    aba.append_rows(df_export.values.tolist(), value_input_option="USER_ENTERED")
    print(f"  [append:{aba_nome}] {len(df_export)} linha(s) adicionadas")
    return len(df_export)


def migrar_origem_e_remover_conferida(aba_pesquisas, aba_resultados):
    """Migra o histórico para `origem` e remove a coluna técnica legada.

    A conversão é idempotente e acontece antes de qualquer reconciliação: assim,
    uma linha manual continua reconhecível depois que `conferida` deixa de existir.
    """
    df_p = carregar_df_da_aba(aba_pesquisas)
    df_r = carregar_df_da_aba(aba_resultados)

    if df_p.empty and df_r.empty:
        return

    mudou_p = False
    if not df_p.empty:
        origem_atual = df_p["origem"] if "origem" in df_p.columns else pd.Series("", index=df_p.index)
        conferida_legada = df_p["conferida"] if "conferida" in df_p.columns else pd.Series("", index=df_p.index)
        origem_nova = [
            _origem_migrada(origem, conferida)
            for origem, conferida in zip(origem_atual, conferida_legada)
        ]
        if "origem" not in df_p.columns or origem_nova != origem_atual.astype(str).tolist():
            df_p["origem"] = origem_nova
            mudou_p = True
        if "conferida" in df_p.columns:
            df_p = df_p.drop(columns=["conferida"])
            mudou_p = True

    # Resultados herdam a origem do cenário. Para registros antigos sem cenário
    # correspondente, a única origem histórica possível era o PollingData.
    mudou_r = False
    if not df_r.empty:
        origem_por_cenario = {}
        if not df_p.empty and "scenario_id" in df_p.columns and "origem" in df_p.columns:
            origem_por_cenario = {
                _norm_ws(scenario_id): _norm_ws(origem)
                for scenario_id, origem in zip(df_p["scenario_id"], df_p["origem"])
                if _norm_ws(scenario_id) and _norm_ws(origem)
            }
        origem_atual = df_r["origem"] if "origem" in df_r.columns else pd.Series("", index=df_r.index)
        scenario_ids = df_r["scenario_id"] if "scenario_id" in df_r.columns else pd.Series("", index=df_r.index)
        origem_nova = [
            _norm_ws(origem) or origem_por_cenario.get(_norm_ws(scenario_id), ORIGEM_POLLINGDATA)
            for origem, scenario_id in zip(origem_atual, scenario_ids)
        ]
        if "origem" not in df_r.columns or origem_nova != origem_atual.astype(str).tolist():
            df_r["origem"] = origem_nova
            mudou_r = True
        if "conferida" in df_r.columns:
            df_r = df_r.drop(columns=["conferida"])
            mudou_r = True

    if mudou_p:
        sobrescrever_aba(aba_pesquisas, df_p)
        print("[migracao] aba 'pesquisas': origem preenchida e coluna 'conferida' removida")
    if mudou_r:
        sobrescrever_aba(aba_resultados, df_r)
        print("[migracao] aba 'resultados': origem preenchida e coluna 'conferida' removida")


def normalizar_institutos_retroativo(aba_pesquisas, aba_resultados):
    """Realinha instituto e nota nas entradas já gravadas. Idempotente: se a
    planilha já está em dia, não toca em nada.

    Duas passadas independentes:

    1. Nome: aplica ALIASES_INSTITUTO. A metodologia é reescrita junto, e só
       nas linhas cujo nome mudou, porque para o resto o texto pode ser livre,
       digitado no polling manual.

    2. Nota: recalcula classificacao_instituto em TODAS as linhas, não só nas
       que trocaram de nome. A troca do ranking do PollsterGraph pelo do
       Pindograma (01/09/2026) mudou a nota de instituto cujo nome não mudou, e
       a versão anterior desta rotina só olhava para o nome. O resultado é que a
       matriz guardava as duas escalas ao mesmo tempo: linha nova com B+ e linha
       antiga do mesmo instituto com A+, e os filtros dos painéis liam as duas
       como se fossem a mesma régua.
    """
    def _aplicar(df: pd.DataFrame, recomputar_classif_e_metod: bool) -> tuple[pd.DataFrame, int, int]:
        if df.empty or "instituto" not in df.columns:
            return df, 0, 0
        df = df.copy()
        original = df["instituto"].astype(str)
        novo = original.apply(normalizar_instituto)
        # Exceção histórica solicitada: esta pesquisa já publicada conserva o
        # nome que recebeu originalmente, mesmo quando a rotina retroativa é
        # executada numa atualização futura.
        if "registro_tse" in df.columns:
            preservar = df["registro_tse"].astype(str).str.strip().eq("RN-06422/2026")
            novo.loc[preservar] = original.loc[preservar]
        mudou = (original != novo)
        n_nome = int(mudou.sum())
        if n_nome:
            df["instituto"] = novo

        n_nota = 0
        if recomputar_classif_e_metod:
            if mudou.any() and "metodologia" in df.columns:
                # só sobrescreve quando o canônico tem metodologia conhecida;
                # caso contrário mantém o texto anterior (pode ser texto livre do polling manual)
                def _metod_se_conhecida(inst):
                    m = obter_metodologia(inst)
                    return m if m else None
                metod_nova = df.loc[mudou, "instituto"].apply(_metod_se_conhecida)
                idx_validos = metod_nova.dropna().index
                df.loc[idx_validos, "metodologia"] = metod_nova.loc[idx_validos]

            if "classificacao_instituto" in df.columns:
                # classificar_instituto é determinística: passar a coluna
                # inteira devolve exatamente o que já está lá quando a linha
                # está em dia, o que mantém a rotina idempotente.
                nota_atual = df["classificacao_instituto"].astype(str).map(_norm_ws)
                nota_nova = df["instituto"].apply(classificar_instituto)
                nota_mudou = (nota_atual != nota_nova)
                n_nota = int(nota_mudou.sum())
                if n_nota:
                    df.loc[nota_mudou, "classificacao_instituto"] = nota_nova.loc[nota_mudou]

        return df, n_nome, n_nota

    for nome_aba, aba in (("pesquisas", aba_pesquisas), ("resultados", aba_resultados)):
        df_existente = carregar_df_da_aba(aba)
        df_norm, n_nome, n_nota = _aplicar(df_existente, recomputar_classif_e_metod=True)
        if n_nome or n_nota:
            sobrescrever_aba(aba, df_norm)
            print(
                f"[normalizacao] aba '{nome_aba}': {n_nome} linha(s) com instituto "
                f"normalizado, {n_nota} com nota realinhada"
            )


# Ordem do ranking. As notas com + e - são da escala anterior e ficam na lista
# porque a nota histórica pode reaparecer.
ORDEM_NOTAS = ["A+", "A", "A-", "B+", "B", "B-", "C+", "C", "C-", "D"]


def montar_df_institutos() -> pd.DataFrame:
    """A base de institutos como tabela: nome, nota, peso e metodologia.

    Sai dos dicts deste arquivo, que são a fonte. A aba existe para consultar a
    nota sem abrir código; quem manda continua sendo o dict, e por isso a aba é
    reescrita inteira a cada rebuild. Editar a aba na mão não muda nada.
    """
    # Os três dicts usam grafias diferentes para o mesmo instituto: a
    # CLASSIFICACAO usa o nome curto que a matriz grava, a METODOLOGIA usa a
    # grafia que o texto tinha quando foi escrito. Somar as chaves cruas fazia
    # "Ipec" e "Ipec (antigo Ibope)" virarem duas linhas. Resolver pelo nome
    # canônico primeiro deixa uma linha por instituto.
    canonicos: dict[str, None] = {}
    for nome_bruto in (list(CLASSIFICACAO_INSTITUTOS) + list(CLASSIFICACAO_SEM_FONTE)
                       + list(METODOLOGIA_INSTITUTOS)):
        canonicos.setdefault(normalizar_instituto(nome_bruto), None)

    linhas = []
    nomes = sorted(canonicos, key=lambda n: n.lower())
    for nome in nomes:
        nota = classificar_instituto(nome)
        if nome in INDICE_PINDOGRAMA:
            fonte = "Índice do Pindograma"
        elif nome in CLASSIFICACAO_SEM_FONTE:
            fonte = "Sem fonte confirmada"
        else:
            fonte = ""
        linhas.append({
            "instituto": nome,
            "classificacao": nota,
            "peso": score_instituto(nota),
            "indice_pindograma": INDICE_PINDOGRAMA.get(nome, ""),
            "posicao_pindograma": POSICAO_PINDOGRAMA.get(nome, ""),
            "classificacao_pindograma": CLASSIFICACAO_INSTITUTOS.get(nome, ""),
            "fonte_da_nota": fonte,
            "metodologia": obter_metodologia(nome),
        })
    df = pd.DataFrame(linhas)
    if df.empty:
        return df
    df["_ordem"] = df["classificacao"].apply(
        lambda g: ORDEM_NOTAS.index(g) if g in ORDEM_NOTAS else len(ORDEM_NOTAS))
    df = (df.sort_values(["_ordem", "instituto"], kind="stable")
            .drop(columns="_ordem")
            .reset_index(drop=True))
    df.insert(0, "#", range(1, len(df) + 1))
    return df


def publicar_aba_institutos(sh) -> None:
    """Escreve a aba 'institutos' da matriz a partir dos dicts deste arquivo."""
    df = montar_df_institutos()
    if df.empty:
        return
    aba = garantir_aba(sh, "institutos", rows=max(200, len(df) + 20), cols=len(df.columns))
    sobrescrever_aba(aba, df)
    n_pindograma = int((df["fonte_da_nota"] == "Índice do Pindograma").sum())
    print(f"[+] institutos: {len(df)} linhas ({n_pindograma} com índice do Pindograma)")


def _publicar_cache_parquet(gc, sheet_id: str) -> None:
    """Publica o cache Parquet das abas pesadas no Drive.

    Roda depois das correções numéricas de propósito: o que vai para o Parquet
    tem que ser o que a aba devolve depois de pronta. Falha aqui não derruba o
    rebuild — sem o Parquet, os painéis voltam a ler direto do Sheets.
    """
    try:
        from compartilhado.cache_parquet import publicar_abas
        publicar_abas(gc, sheet_id)
    except Exception as erro:
        print(f"  [cache] não publicado (painéis seguem no Sheets): {erro}", flush=True)


def reconstruir_resultados_bi(gc, sheet_id: str):
    """Reconstrói resultados_bi a partir do que já está salvo em `resultados`,
    sem raspar o site de novo. Usado por `relatorios_extracao_segmentos.py rebuild_bi` pra aplicar
    correções de agregação/média móvel no histórico já coletado, sem esperar
    a próxima rodada de raspagem."""
    # A API do Google Sheets ocasionalmente devolve 500 ao abrir planilhas
    # grandes. É uma falha transitória: tentar novamente evita que um rebuild
    # manual falhe antes mesmo de ler os resultados já existentes.
    sh = None
    ultimo_erro = None
    for tentativa in range(1, 4):
        try:
            sh = gc.open_by_key(sheet_id)
            break
        except gspread.exceptions.APIError as erro:
            ultimo_erro = erro
            if tentativa == 3:
                raise
            espera = tentativa * 5
            print(
                f"  [rebuild] API do Sheets indisponível (tentativa {tentativa}/3); "
                f"tentando de novo em {espera}s"
            )
            time.sleep(espera)

    if sh is None:
        raise ultimo_erro

    aba_resultados = garantir_aba(sh, "resultados", rows=20000, cols=35)
    aba_pesquisas = garantir_aba(sh, "pesquisas", rows=50000, cols=35)
    aba_resultados_bi = garantir_aba(sh, "resultados_bi", rows=20000, cols=40)

    # Antes de consolidar: a nota de cada linha tem que estar na escala vigente,
    # senão o resultados_bi e a média saem ponderados por uma régua misturada.
    # A rotina também roda no salvar_tudo; aqui ela dá o caminho de consertar a
    # matriz sob demanda, disparando só o rebuild.
    normalizar_institutos_retroativo(aba_pesquisas, aba_resultados)

    df_resultados_all = carregar_df_da_aba(aba_resultados)
    df_pesquisas_all = carregar_df_da_aba(aba_pesquisas)
    df_resultados_bi = construir_resultados_bi(df_resultados_all, df_pesquisas_all)
    sobrescrever_aba(aba_resultados_bi, df_resultados_bi)
    print(f"[+] resultados_bi: {len(df_resultados_bi)} linhas consolidadas para Looker")

    corrigir_coluna_numerica_na_aba(aba_resultados_bi, "percentual_base")
    corrigir_coluna_numerica_na_aba(aba_resultados_bi, "media_movel_13d")
    corrigir_coluna_numerica_na_aba(aba_resultados_bi, COLUNA_MODELO_AMOSTRAL)
    corrigir_coluna_numerica_na_aba(aba_resultados_bi, COLUNA_MODELO_HIBRIDO)

    publicar_aba_institutos(sh)

    _publicar_cache_parquet(gc, sheet_id)


def salvar_tudo(gc, spreadsheet_id: str, df_p: pd.DataFrame, df_r: pd.DataFrame):
    sem_novidades = (df_p is None or df_p.empty) and (df_r is None or df_r.empty)
    sh = gc.open_by_key(spreadsheet_id)
    if FORCAR_LOCALE_PLANILHA:
        try:
            sh.update_locale(LOCALE_PLANILHA)
        except Exception as e:
            print(f"  [fmt] não foi possível definir locale da planilha para {LOCALE_PLANILHA}: {e}")

    aba_pesquisas = garantir_aba(sh, "pesquisas", rows=50000, cols=35)
    aba_resultados = garantir_aba(sh, "resultados", rows=20000, cols=35)
    aba_resultados_bi = garantir_aba(sh, "resultados_bi", rows=20000, cols=40)

    migrar_origem_e_remover_conferida(aba_pesquisas, aba_resultados)

    # Passada de normalização retroativa: aplica ALIASES_INSTITUTO em entradas
    # já gravadas. Idempotente; só reescreve se algo de fato mudou.
    normalizar_institutos_retroativo(aba_pesquisas, aba_resultados)

    if sem_novidades:
        print("[-] nada novo para salvar; migração de schema concluída")
        return

    # Reconciliação automática: apenas uma entrada oficial do PollingData remove
    # a duplicata manual equivalente. Um PDF de relatório tem procedência própria.
    if df_p is not None and not df_p.empty:
        df_p_chk = df_p.copy()
        if "origem" not in df_p_chk.columns:
            df_p_chk["origem"] = ORIGEM_POLLINGDATA
        tem_linha_oficial_entrante = df_p_chk["origem"].apply(_eh_linha_oficial_pollingdata).any()

        if tem_linha_oficial_entrante:
            df_p_existente = carregar_df_da_aba(aba_pesquisas)
            df_r_existente = carregar_df_da_aba(aba_resultados)
            df_p_limpo, df_r_limpo, df_recon = reconciliar_manuais_com_oficiais(
                df_p_existente=df_p_existente,
                df_r_existente=df_r_existente,
                df_p_novo=df_p,
                df_r_novo=df_r if df_r is not None else pd.DataFrame(),
            )
            if not df_recon.empty:
                sobrescrever_aba(aba_pesquisas, df_p_limpo)
                sobrescrever_aba(aba_resultados, df_r_limpo)
                print(
                    "[reconciliacao] removidas "
                    f"{len(df_recon)} pesquisa(s) manual(is) substituída(s) por entrada oficial"
                )
                for _, row in df_recon.iterrows():
                    print(
                        "  - manual: "
                        f"{row.get('poll_id_manual_removido', '')} -> oficial: {row.get('poll_id_oficial', '')} "
                        f"[{row.get('regra_match', '')}]"
                    )

    if df_p is not None and not df_p.empty:
        novos, exist = dedup_e_salvar(aba_pesquisas, df_p, key_col="scenario_id")
        print(f"[+] pesquisas: {novos} novas | {exist} já existiam")

    if df_r is not None and not df_r.empty:
        df_r = df_r.copy()

        df_r = adicionar_posicao_pesquisa(df_r)

        df_r["_dedup_key"] = (
            df_r["scenario_id"].astype(str)
            + "|" + df_r["data_campo"].fillna("").astype(str)
            + "|" + df_r["tipo"].fillna("").astype(str)
            + "|" + df_r["candidato_partido"].fillna("").astype(str)
        )
        novos, exist = dedup_e_salvar(aba_resultados, df_r, key_col="_dedup_key")
        print(f"[+] resultados: {novos} novas | {exist} já existiam")

    preencher_posicao_pesquisa_na_aba(aba_resultados)

    df_resultados_all = carregar_df_da_aba(aba_resultados)
    df_pesquisas_all = carregar_df_da_aba(aba_pesquisas)
    df_resultados_bi = construir_resultados_bi(df_resultados_all, df_pesquisas_all)
    sobrescrever_aba(aba_resultados_bi, df_resultados_bi)
    print(f"[+] resultados_bi: {len(df_resultados_bi)} linhas consolidadas para Looker")

    corrigir_coluna_numerica_na_aba(aba_resultados, "percentual")
    corrigir_coluna_numerica_na_aba(aba_resultados, "percentual_media_cenarios")
    corrigir_coluna_numerica_na_aba(aba_resultados_bi, "percentual_base")
    corrigir_coluna_numerica_na_aba(aba_resultados_bi, "media_movel_13d")
    corrigir_coluna_numerica_na_aba(aba_resultados_bi, COLUNA_MODELO_AMOSTRAL)
    corrigir_coluna_numerica_na_aba(aba_resultados_bi, COLUNA_MODELO_HIBRIDO)

    _publicar_cache_parquet(gc, spreadsheet_id)


def _buscar_urls_no_json(obj, urls: set, pattern: str):
    if isinstance(obj, str):
        if re.search(pattern, obj, re.I):
            urls.add(obj)
    elif isinstance(obj, list):
        for item in obj:
            _buscar_urls_no_json(item, urls, pattern)
    elif isinstance(obj, dict):
        for v in obj.values():
            _buscar_urls_no_json(v, urls, pattern)


def descobrir_urls_presidente_t2(driver) -> list[str]:
    """
    Carrega a página de 2º turno do PollingData e coleta todos os slugs
    de presidente via JSON embutido ou HTML.
    """
    PATTERN = r"/presidente/[a-z]{2}/t\d_[^/\s\"'>]+"
    BASE = "https://www.pollingdata.com.br"
    URL_T2 = "https://www.pollingdata.com.br/capa2026/previsao_brasil2026.html"

    print("[descoberta t2] carregando página de 2º turno...")
    driver.get(URL_T2)

    try:
        WebDriverWait(driver, 20).until(
            EC.presence_of_element_located((By.CSS_SELECTOR, "script[type='application/json']"))
        )
    except Exception:
        pass
    time.sleep(4)

    encontradas: set[str] = set()

    # Abordagem 1: JSON embutido nos script tags
    for script in driver.find_elements(By.CSS_SELECTOR, "script[type='application/json']"):
        try:
            raw = script.get_attribute("innerHTML") or script.text
            _buscar_urls_no_json(json.loads(raw), encontradas, PATTERN)
        except Exception:
            pass

    # Abordagem 2: busca em todo o HTML da página
    if not encontradas:
        matches = re.findall(r'(/2026/presidente/[a-z]{2}/t\d_[^/"\'\s>]+)', driver.page_source, re.I)
        encontradas.update(matches)

    urls_completas = sorted({
        BASE + p.rstrip("/") + "/"
        if not p.startswith("http") else p.rstrip("/") + "/"
        for p in encontradas
    })

    print(f"  [descoberta t2] {len(urls_completas)} URL(s) encontrada(s)")
    return urls_completas


def main():
    incluir_governador = env_bool("INCLUIR_GOVERNADOR", True)
    incluir_senado = env_bool("INCLUIR_SENADO", True)
    incluir_presidente = env_bool("INCLUIR_PRESIDENTE", True)
    incluir_presidente_t2 = env_bool("INCLUIR_PRESIDENTE_T2", True)

    tem_t1 = incluir_governador or incluir_senado or incluir_presidente
    spreadsheet_id = obter_spreadsheet_id() if tem_t1 else None
    # URLs de t2 são descobertas dinamicamente após iniciar o driver; passa False aqui
    urls = montar_urls(incluir_governador, incluir_senado, incluir_presidente, incluir_presidente_t2=False)

    if not urls and not incluir_presidente_t2:
        print("[-] Nenhuma URL selecionada. Ajuste INCLUIR_*.")
        return

    horario_raspagem = datetime.now(
        zoneinfo.ZoneInfo("America/Recife")
    ).strftime("%Y-%m-%d %H:%M:%S")

    print("[+] Conectando ao Google Sheets...")
    gc = gs_client_from_env()

    print("[+] Iniciando Chrome...")
    driver = criar_driver()

    if incluir_presidente_t2:
        try:
            urls_t2 = descobrir_urls_presidente_t2(driver)
        except Exception as e:
            print(f"[!] descoberta de t2 falhou, usando lista padrão: {e}")
            urls_t2 = list(PRESIDENTE_T2_URLS_DEFAULT)
        if not urls_t2:
            print("[!] descoberta de t2 retornou vazio, usando lista padrão")
            urls_t2 = list(PRESIDENTE_T2_URLS_DEFAULT)
        urls += urls_t2

    if not urls:
        print("[-] Nenhuma URL selecionada. Ajuste INCLUIR_*.")
        driver.quit()
        return

    all_p = []
    all_r = []

    try:
        for url in urls:
            try:
                df_p, df_r = scrape_url(driver, url, horario_raspagem)

                if df_p is not None and not df_p.empty:
                    all_p.append(df_p)

                if df_r is not None and not df_r.empty:
                    all_r.append(df_r)
            except Exception as e:
                print(f"  [-] erro ao processar URL {url}: {e}")
    finally:
        driver.quit()

    all_p = [df for df in all_p if df is not None and not df.empty]
    all_r = [df for df in all_r if df is not None and not df.empty]
    df_p_all = pd.concat(all_p, ignore_index=True) if all_p else pd.DataFrame()
    df_r_all = pd.concat(all_r, ignore_index=True) if all_r else pd.DataFrame()

    def _filtrar_turno(df: pd.DataFrame, turno: str) -> pd.DataFrame:
        if df.empty or "turno" not in df.columns:
            return pd.DataFrame()
        return df[df["turno"].astype(str).str.lower() == turno].reset_index(drop=True)

    df_p_t1 = _filtrar_turno(df_p_all, "t1") if not df_p_all.empty else df_p_all
    df_r_t1 = _filtrar_turno(df_r_all, "t1") if not df_r_all.empty else df_r_all
    df_p_t2 = _filtrar_turno(df_p_all, "t2") if not df_p_all.empty else df_p_all
    df_r_t2 = _filtrar_turno(df_r_all, "t2") if not df_r_all.empty else df_r_all

    # Planilha principal — tudo que não é t2
    if not df_p_all.empty and "turno" not in df_p_all.columns:
        df_p_t1, df_r_t1 = df_p_all, df_r_all

    if tem_t1 and spreadsheet_id:
        salvar_tudo(gc, spreadsheet_id, df_p_t1, df_r_t1)

    if incluir_presidente_t2:
        print("[+] Salvando 2º turno na planilha separada...")
        spreadsheet_id_t2 = obter_spreadsheet_id_t2()
        salvar_tudo(gc, spreadsheet_id_t2, df_p_t2, df_r_t2)

    print("[+] OK")


if __name__ == "__main__":
    main()
