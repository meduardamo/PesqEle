"""
Análise dos planos de governo.

Para cada plano, extrai o texto (PyMuPDF, com OCR só nas páginas sem texto) e pede
ao Gemini para classificar cada tema numa escala de maturidade:
  Ausente  - tema não aparece
  Genérico - citado, mas vago
  Proposta - citado com ação concreta
  Meta     - citado com alvo mensurável (número, %, prazo)
"""

from __future__ import annotations
import json
import os
import time
import re
import unicodedata
from pathlib import Path

import fitz                      # PyMuPDF
import pandas as pd

# Paleta Eixo
COR = {
    # Paleta oficial Eixo (Manual de Identidade Visual, "Cores para gráficos").
    "azul":    "#192D4E",   # marinho
    "vinho":   "#962E4D",
    "amarelo": "#E8A600",
    "azul_claro":  "#BAC8DD",
    "azul_medio":  "#6091D8",
    "cinza":   "#E1E1E1",
}

# Restrições de linguagem coladas em todo prompt que gera prosa (justificativa de
# coerência, síntese comparativa). É a "versão cotidiano" de Sampaio (2026),
# "Prompts para a IA não lavar seu texto", que a Eduarda usa nos textos dela.
# Sem isso o modelo devolve o vocabulário do próprio enunciado: as 16
# justificativas gravadas em 03/08/2026 começavam todas com "O plano", 14 delas
# seguiam o mesmo molde de elogio seguido de "Contudo" mais lacuna, e sete usavam
# "apresenta"/"possui" no lugar de "tem".
RESTRICOES_LINGUAGEM = (
    "RESTRIÇÕES DE LINGUAGEM (obrigatórias):\n"
    "- Proibido travessão (—), ponto e vírgula (;) e dois pontos (:) fora de lista formal.\n"
    "- Proibidos adjetivos vagos ou promocionais: abrangente, crucial, essencial, "
    "estratégico, fundamental, robusto, significativo, notável, sólido, claro, "
    "consistente, inovador, multifacetado.\n"
    "- Proibidas cópulas infladas. Escreva 'tem', 'é', 'traz', 'cita'. Nunca "
    "'apresenta', 'possui', 'oferece', 'serve como', 'destaca-se como'.\n"
    "- Proibidos verbos inflados: destacar, ressaltar, enfatizar, evidenciar, "
    "promover, refletir, contribuir para, aprimorar.\n"
    "- Proibido começar frase com conectivo opinativo: além disso, contudo, "
    "portanto, no entanto, assim, na verdade, notavelmente.\n"
    "- Proibido gerúndio conclusivo vago no fim da frase ('garantindo que', "
    "'resultando em', 'refletindo a importância').\n"
    "- Proibido paralelismo negativo ('não apenas X, mas também Y') e regra dos "
    "três decorativa (três adjetivos ou três itens só para parecer completo).\n"
    "- Proibida editorialização: 'vale destacar', 'é importante notar', "
    "'cabe ressaltar'.\n"
    "- Proibida metáfora morta: cenário, panorama, ótica, lente, chave, âmbito, "
    "marco, mosaico, esfera, horizonte.\n"
    "- Máximo de 25 palavras por frase. Frases declarativas, sujeito, verbo, objeto.\n"
    "- Dado concreto no lugar de adjetivo. Nome de programa, número, prazo, página.\n"
    "- Se a frase serve para qualquer outro plano sem mudar nada, reescreva."
)

# Escala de maturidade (ordem importa)
NIVEIS = ["Não menciona", "Menciona vagamente", "Propõe ação", "Define meta"]
# A escala é ORDINAL: 0, 1, 2 e 3 pontos. Rampa sequencial de um tom só, do
# claro ao escuro, no azul da marca — mais escuro = mais maduro. Mantido igual
# ao painel, que é quem exibe. Ver o comentário longo lá.
COR_NIVEL = {
    "Não menciona":       "#E3E8F0",
    "Menciona vagamente": "#9DB4D3",
    "Propõe ação":        "#3F6FB5",
    "Define meta":        COR["azul"],
}

# Nomes antigos de tema. O nome do tema é a chave que indexa a análise salva, então
# renomear sem tradução jogaria fora o que já foi processado. "Ensino Médio Integral"
# virou "Tempo Integral" em 04/08/2026: dos 13 trechos que o tema tinha capturado,
# 11 falavam de tempo integral sem dizer a etapa, e o nome prometia um recorte que a
# análise não fazia.
TEMAS_LEGADO = {"Ensino Médio Integral": "Tempo Integral"}

# Nomes antigos da escala, usados em análises salvas antes da renomeação.
# Traduzido na leitura para que dados ainda não reprocessados continuem funcionando.
NIVEIS_LEGADO = {
    "Ausente":  "Não menciona",
    "Genérico": "Menciona vagamente",
    "Proposta": "Propõe ação",
    "Meta":     "Define meta",
}

# Temas por eixo de política pública, cada um com uma descrição curta que
# orienta o Gemini.
#
# Os seis temas de Educação são os mesmos de antes, com os MESMOS nomes: a
# análise já salva na planilha é indexada por nome de tema, e renomear jogaria
# fora o que já foi processado.
#
# Custa quase nada ampliar: classificar_plano faz UMA chamada com todos os
# temas juntos, então 22 temas custam praticamente o mesmo que 6.
EIXOS = {
    "Educação": {
        "Alfabetização": "alfabetização de crianças na idade certa (PNAIC, PNA, Compromisso Nacional Criança Alfabetizada ou equivalente)",
        "Primeira Infância": "creches, pré-escola, educação infantil, primeira infância (0–5 anos)",
        "Fundamental": "ensino fundamental anos iniciais (2º ao 5º ano) e anos finais (6º ao 9º ano) — exclui alfabetização do 1º ano, que é tema próprio",
        "Ensino Médio": "ensino médio regular da rede estadual: matrícula, evasão, novo ensino médio, currículo e resultado de aprendizagem — o tempo integral é tema próprio",
        "Tempo Integral": "educação em tempo integral e jornada escolar ampliada, em qualquer etapa da educação básica",
        "Educação Profissional": "educação profissional e técnica (EPT, SENAI, SENAC, institutos federais, cursos técnicos)",
        "Valorização Docente": "carreira, salário, piso, concurso, formação continuada e condições de trabalho de professores e demais profissionais da educação",
        "Educação Inclusiva e EJA": "educação especial e inclusiva, estudante com deficiência, educação de jovens e adultos, educação no campo, indígena e quilombola",
        "Tecnologia na Educação": "tecnologia, conectividade, inclusão digital e uso de IA nas escolas",
    },
    "Saúde": {
        "Atenção Primária": "atenção primária, saúde da família, UBS, agentes comunitários de saúde",
        "Média e Alta Complexidade": "hospitais, leitos, UTI, cirurgias eletivas, filas e regulação de vagas",
        "Urgência e Emergência": "SAMU, UPA, pronto-socorro, pronto atendimento, regulação de urgência e transporte sanitário",
        # Bets e imposto seletivo entraram a pedido da Lívia (05/08/2026), como
        # palavras-chave do tema que já existe: o nome fica, porque é ele que
        # indexa a análise salva, e ampliar a descrição basta para o modelo
        # reconhecer o assunto. Ludopatia é dependência, e o imposto seletivo é
        # tributação de produto nocivo à saúde.
        "Saúde Mental": "saúde mental, CAPS, prevenção ao suicídio, dependência química, álcool e outras drogas, ludopatia e transtorno do jogo, apostas de quota fixa (bets), e imposto seletivo sobre produtos nocivos à saúde",
    },
    "Segurança pública": {
        "Policiamento e Efetivo": "efetivo policial, salário, equipamento, presença ostensiva, videomonitoramento",
        "Enfrentamento ao Crime Organizado": "facções, tráfico, inteligência policial, fronteiras",
        "Violência contra a Mulher": "violência doméstica, Lei Maria da Penha, delegacia da mulher, casa da mulher brasileira",
        "Sistema Prisional e Socioeducativo": "presídios, vagas prisionais, ressocialização, egresso e sistema socioeducativo de adolescentes",
    },
    "Economia e emprego": {
        "Geração de Emprego": "geração de emprego e renda, qualificação profissional para o trabalho, intermediação de mão de obra",
        "Ambiente de Negócios": "atração de investimento, desburocratização, incentivo fiscal, apoio a micro e pequena empresa",
        "Agropecuária": "agricultura, pecuária, agronegócio, agricultura familiar, crédito rural",
        "Ciência, Tecnologia e Inovação": "pesquisa científica, fundação de amparo à pesquisa, universidade estadual e ensino superior, parque tecnológico, startups",
    },
    "Meio ambiente e clima": {
        "Desmatamento e Conservação": "desmatamento, unidades de conservação, fiscalização ambiental, queimadas",
        "Saneamento e Recursos Hídricos": "água, esgoto, resíduos sólidos, bacias hidrográficas, seca",
        "Transição Energética": "energia renovável, solar, eólica, crédito de carbono, economia verde",
        "Defesa Civil e Desastres": "defesa civil, enchente, seca, deslizamento, prevenção e resposta a desastres, adaptação climática",
    },
    "Assistência social e pobreza": {
        "Transferência de Renda": "programas de transferência de renda, complemento ao Bolsa Família, benefício estadual",
        "Segurança Alimentar": "fome, insegurança alimentar, restaurante popular, banco de alimentos, cesta básica",
        "Habitação": "moradia, habitação popular, regularização fundiária, aluguel social",
    },
    "Infraestrutura e mobilidade": {
        "Transporte e Rodovias": "rodovias, pavimentação, ferrovias, portos, aeroportos, logística",
        "Mobilidade Urbana": "transporte público, tarifa, metrô, BRT, ciclovia",
    },
    "Gestão pública e transparência": {
        "Eficiência e Gasto Público": "reforma administrativa, corte de gasto, teto de despesa, equilíbrio fiscal",
        "Transparência e Combate à Corrupção": "transparência, controle interno, dados abertos, combate à corrupção",
        "Governo Digital": "digitalização de serviços, atendimento ao cidadão, governo eletrônico",
        "Servidores e Municípios": "servidor público estadual, carreira e concurso, e a relação do estado com os municípios (consórcio, repasse, apoio técnico)",
    },
    "Direitos humanos e igualdade": {
        "Igualdade Racial": "promoção da igualdade racial, população negra, racismo, cotas e ações afirmativas",
        "Mulheres": "políticas para mulheres além do enfrentamento à violência, que é tema próprio: autonomia econômica, cuidado, saúde da mulher",
        "Pessoa com Deficiência": "acessibilidade, direitos e serviços para pessoas com deficiência",
        "Juventude e Pessoa Idosa": "políticas para a juventude e para a pessoa idosa, primeiro emprego, centro de convivência, cuidado",
        "Povos Indígenas e Quilombolas": "povos indígenas, comunidades quilombolas, povos e comunidades tradicionais, território e políticas específicas",
        "População LGBTQIA+": "políticas para a população LGBTQIA+, enfrentamento à LGBTfobia, nome social, acolhimento e serviços específicos",
    },
    "Cultura, esporte e turismo": {
        "Cultura": "cultura, patrimônio, economia criativa, fomento e editais, equipamentos culturais",
        "Esporte e Lazer": "esporte, esporte escolar, lazer, equipamentos esportivos e grandes eventos",
        "Turismo": "turismo, atrativos, promoção e infraestrutura turística",
    },
}

# Termos-âncora por tema, usados só como guarda contra falso "Não menciona".
#
# Existem porque a ausência era a única classificação que ninguém conferia. O
# nível acima de "Não menciona" precisa de citação, que verificar_trecho testa
# contra o PDF; "Não menciona" não precisava de nada e por isso passava batido.
# Em 07/08/2026, no plano do Zema, 12 dos 15 temas gravados como ausentes tinham
# ocorrência no texto: "primeira infância" aparece 6 vezes e o plano tem seção
# própria na página 50.
#
# São termos discriminantes, não sinônimos amplos: "creche" serve, "criança" não,
# porque o que aparece em qualquer plano não distingue tema nenhum e faria a
# guarda disparar sempre. Casam sem acento e sem caixa, por _norm_busca.
#
# LIMITE CONHECIDO, medido nos 43 planos em 07/08/2026: a guarda pega 96 das 312
# ausências gravadas, 31%. O resto passa porque o plano trata do tema sem usar
# nenhum termo da lista, e isso é a regra e não a exceção: 32% das citações da
# base não contêm o termo do próprio tema. "A Bahia aprova crianças no terceiro
# ano sem que saibam ler" é ensino fundamental sem dizer "fundamental". Fechar
# esse resto exigiria mandar o plano inteiro de volta ao modelo por tema
# ausente, e a lista de termos é o que dá para conferir de graça. Ao ampliar,
# prefira expressão de duas palavras: termo solto e comum faz a guarda disparar
# em todo plano e gasta chamada sem achar nada.
TERMOS_ANCORA = {
    "Alfabetização": ["alfabetiza", "pnaic", "pna ", "crianca alfabetizada"],
    "Primeira Infância": ["primeira infancia", "creche", "pre escola", "educacao infantil"],
    "Fundamental": ["ensino fundamental", "anos iniciais", "anos finais",
                    "educacao basica", "aprender a ler", "saibam ler",
                    "terceiro ano", "quinto ano", "nono ano", "aprendizagem"],
    "Ensino Médio": ["ensino medio", "novo ensino medio"],
    "Tempo Integral": ["tempo integral", "ensino integral", "jornada ampliada",
                       "escola integral", "educacao integral"],
    "Educação Profissional": ["educacao profissional", "ensino tecnico", "curso tecnico",
                              "senai", "senac", "instituto federal", "profissionalizante"],
    "Valorização Docente": ["professor", "docente", "magisterio", "piso salarial"],
    "Educação Inclusiva e EJA": ["educacao especial", "educacao inclusiva", "jovens e adultos",
                                 "eja", "autis", "estudante com deficiencia"],
    "Tecnologia na Educação": ["conectividade", "inclusao digital", "internet nas escolas",
                               "tecnologia educacional", "internet", "laboratorio de informatica",
                               "transformacao digital", "computador", "tablet"],
    "Atenção Primária": ["atencao primaria", "atencao basica", "saude da familia", "ubs",
                         "agente comunitario", "unidade basica"],
    "Média e Alta Complexidade": ["leito", "uti", "cirurgia eletiva", "fila de cirurgia",
                                  "media e alta complexidade", "hospital"],
    "Urgência e Emergência": ["samu", "upa", "pronto socorro", "pronto atendimento",
                              "urgencia e emergencia"],
    "Saúde Mental": ["saude mental", "caps", "suicidio", "dependencia quimica",
                     "ludopatia", "bets", "imposto seletivo"],
    "Policiamento e Efetivo": ["efetivo policial", "policia militar", "videomonitoramento",
                               "policiamento", "seguranca ostensiva", "policial", "policia civil",
                               "forcas de seguranca", "camera corporal", "viatura"],
    "Enfrentamento ao Crime Organizado": ["faccao", "crime organizado", "trafico",
                                          "inteligencia policial"],
    "Violência contra a Mulher": ["violencia domestica", "maria da penha", "feminicidio",
                                  "delegacia da mulher", "casa da mulher"],
    "Sistema Prisional e Socioeducativo": ["presidio", "sistema prisional", "vaga prisional",
                                           "ressocializacao", "socioeducativo", "penitenciaria"],
    "Geração de Emprego": ["geracao de emprego", "qualificacao profissional", "posto de trabalho",
                           "intermediacao de mao de obra", "emprego e renda", "empregabilidade",
                           "frentes de trabalho", "vagas de trabalho", "gerar emprego",
                           "criacao de emprego"],
    "Ambiente de Negócios": ["desburocratiza", "incentivo fiscal", "atracao de investimento",
                             "micro e pequena empresa", "ambiente de negocios", "empreendedorismo",
                             "iniciativa privada", "credito", "banco de fomento", "comercio popular"],
    "Agropecuária": ["agronegocio", "agricultura familiar", "credito rural", "pecuaria",
                     "produtor rural", "agropecuaria"],
    "Ciência, Tecnologia e Inovação": ["pesquisa cientifica", "parque tecnologico", "startup",
                                        "fapes", "inovacao", "pos graduacao"],
    "Desmatamento e Conservação": ["desmatamento", "unidade de conservacao", "queimada",
                                   "fiscalizacao ambiental"],
    "Saneamento e Recursos Hídricos": ["saneamento", "esgoto", "residuos solidos",
                                       "bacia hidrografica", "abastecimento de agua"],
    "Transição Energética": ["energia renovavel", "energia solar", "eolica",
                             "credito de carbono", "transicao energetica"],
    "Defesa Civil e Desastres": ["defesa civil", "enchente", "deslizamento", "desastre",
                                 "adaptacao climatica"],
    "Transferência de Renda": ["transferencia de renda", "bolsa familia", "auxilio",
                               "beneficio estadual", "renda minima"],
    "Segurança Alimentar": ["seguranca alimentar", "inseguranca alimentar", "fome",
                            "restaurante popular", "banco de alimentos", "cesta basica"],
    "Habitação": ["habitacao", "moradia", "regularizacao fundiaria", "aluguel social",
                  "minha casa"],
    "Transporte e Rodovias": ["rodovia", "pavimentacao", "ferrovia", "porto", "aeroporto",
                              "logistica"],
    "Mobilidade Urbana": ["transporte publico", "mobilidade urbana", "metro", "brt",
                          "ciclovia", "tarifa"],
    "Eficiência e Gasto Público": ["reforma administrativa", "corte de gasto", "teto de gasto",
                                   "equilibrio fiscal", "gasto publico", "custeio", "despesa",
                                   "cargo comissionado", "auditar", "auditoria", "orcamento",
                                   "recursos publicos"],
    "Transparência e Combate à Corrupção": ["transparencia", "dados abertos", "corrupcao",
                                            "controle interno"],
    "Governo Digital": ["governo digital", "digitalizacao", "servico digital",
                        "governo eletronico", "atendimento ao cidadao"],
    "Servidores e Municípios": ["servidor publico", "concurso publico", "plano de carreira",
                                "consorcio", "repasse aos municipios"],
    "Igualdade Racial": ["igualdade racial", "populacao negra", "racismo", "cotas raciais",
                         "acao afirmativa"],
    "Mulheres": ["mulher", "autonomia economica das mulheres", "saude da mulher",
                 "politica de cuidado"],
    "Pessoa com Deficiência": ["pessoa com deficiencia", "acessibilidade", "pcd"],
    "Juventude e Pessoa Idosa": ["juventude", "primeiro emprego", "pessoa idosa", "idoso",
                                 "centro de convivencia", "terceira idade"],
    "Povos Indígenas e Quilombolas": ["indigena", "quilombola", "povos tradicionais",
                                      "comunidade tradicional"],
    "População LGBTQIA+": ["lgbt", "lgbtfobia", "nome social", "diversidade sexual"],
    "Cultura": ["cultura", "patrimonio", "economia criativa", "edital de cultura",
                "equipamento cultural"],
    "Esporte e Lazer": ["esporte", "lazer", "atleta", "quadra poliesportiva",
                        "equipamento esportivo"],
    "Turismo": ["turismo", "turistic", "atrativo turistico"],
}

# Mapa plano tema -> eixo, para a página agrupar sem repetir a estrutura.
EIXO_DO_TEMA = {tema: eixo for eixo, temas in EIXOS.items() for tema in temas}

# Achatado: é isso que vai no prompt e que indexa a análise salva.
TEMAS = {tema: desc for temas in EIXOS.values() for tema, desc in temas.items()}

# Só os de educação, para quem quiser o recorte antigo.
TEMAS_EDUCACAO = dict(EIXOS["Educação"])

# Trocável por env var para dar para comparar dois modelos no mesmo plano sem
# mexer no código, que é o único jeito de saber se a troca melhora a citação
# inventada e o recall em plano longo, os dois pontos fracos medidos aqui.
#
# Fica no 2.5-flash porque a análise gravada foi calibrada nele: prompt, régua e
# guardas foram medidos contra o que ele devolve. O sucessor é o gemini-3.6-flash,
# recomendado pelo Google para produção nova. A doc oficial de modelos não anuncia
# aposentadoria do 2.5-flash (o "16/10/2026" que circula em blog de terceiro não
# aparece lá), então a troca é escolha, não prazo. Antes de trocar de vez, rode o
# mesmo estado duas vezes com cada modelo e compare nível por tema.
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")


_CLIENT = None


def _gemini_client():
    # mesma config do gerador de envios: chave por env ou secrets.
    # O cliente fica guardado num global de propósito: o `Client` do google-genai
    # fecha a conexão HTTP no __del__, então um cliente temporário em
    # `_gemini_client().models.generate_content(...)` é coletado antes da chamada
    # sair e o erro vira "Cannot send a request, as the client has been closed".
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    from google import genai
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        try:
            import streamlit as st
            key = st.secrets.get("GEMINI_API_KEY", "")
        except Exception:
            pass
    if not key:
        raise RuntimeError("Faltou a GEMINI_API_KEY (env var ou secrets).")
    _CLIENT = genai.Client(api_key=key)
    return _CLIENT


# extração de texto

# O TSE aceita o que o candidato subir: PDF nativo, PDF escaneado, PDF com
# fonte sem mapa ToUnicode, PDF protegido por senha de dono, e às vezes nem PDF.
# O link também falha sozinho (blip da API). Cada uma dessas situações tem
# tratamento próprio aqui; o objetivo é nunca devolver vazio sem dizer por quê.

PAGINAS_OCR_MAX = 80      # teto de páginas OCRadas por plano
OCR_DPI = 250
OCR_DPI_FALLBACK = 150    # 2ª tentativa quando o pixmap estoura memória
BAIXAR_TENTATIVAS = 4
BAIXAR_TIMEOUT = 90


class PlanoIlegivel(Exception):
    """O arquivo veio, mas não dá para virar texto (não é PDF, está corrompido,
    tem senha de abertura). A mensagem explica qual dos casos."""


class PlanoIndisponivel(PlanoIlegivel):
    """Falha TEMPORÁRIA: o DivulgaCand caiu, deu timeout ou devolveu 5xx.

    Separada de PlanoIlegivel de propósito. O DivulgaCand sai do ar por alguns
    minutos e volta; se isso virasse "plano ilegível", o painel gravaria uma
    análise vazia que só sairia da frente por reprocessamento. Quem chama deve
    PULAR o candidato e tentar de novo depois, nunca persistir resultado.
    """


def _frac_invalido(t: str) -> float:
    """Fração de caracteres fora do conjunto esperado em português. Valor alto
    indica codificação quebrada (glifos espelhados ou sem mapa ToUnicode), caso
    em que a camada de texto vem embaralhada e precisa de OCR."""
    t = (t or "").strip()
    if not t:
        return 1.0
    validos = re.findall(r"[A-Za-zÀ-ÿ0-9\s.,;:!?()\-\"'/%R$º°ªü]", t)
    return 1 - len(validos) / len(t)


_TESSERACT_OK = None


def tesseract_disponivel() -> bool:
    """Checa uma vez se dá para OCRar. Sem isso o OCR falhava em silêncio e o
    plano escaneado voltava vazio sem explicação — que é o que acontece hoje no
    Streamlit Cloud, onde o tesseract não vem instalado."""
    global _TESSERACT_OK
    if _TESSERACT_OK is None:
        try:
            import pytesseract
            pytesseract.get_tesseract_version()
            _TESSERACT_OK = True
        except Exception:
            _TESSERACT_OK = False
    return _TESSERACT_OK


def _ocr_pagina(page, dpi: int = OCR_DPI, lang: str = "por") -> str:
    if not tesseract_disponivel():
        return ""
    for tentativa_dpi in (dpi, OCR_DPI_FALLBACK):
        try:
            import pytesseract
            from PIL import Image
            pm = page.get_pixmap(dpi=tentativa_dpi, alpha=False)
            img = Image.frombytes("RGB", (pm.width, pm.height), pm.samples)
            return pytesseract.image_to_string(img, lang=lang) or ""
        except MemoryError:
            continue            # página gigante: tenta de novo em resolução menor
        except Exception:
            return ""
    return ""


def _texto_da_pagina(page) -> str:
    """Texto de uma página, tentando os modos do PyMuPDF em ordem de qualidade.
    Uma página quebrada não pode derrubar o documento inteiro."""
    for modo in ("text", "blocks"):
        try:
            bruto = page.get_text(modo)
        except Exception:
            continue
        if modo == "blocks":
            bruto = " ".join(b[4] for b in (bruto or []) if len(b) > 4 and isinstance(b[4], str))
        if (bruto or "").strip():
            return bruto
    return ""


def _abrir_pdf(data: bytes):
    """Abre o PDF tolerando senha de dono e lixo antes do cabeçalho."""
    if not data:
        raise PlanoIlegivel("download veio vazio")

    inicio = data[:1024]
    if b"%PDF" not in inicio:
        if inicio.lstrip()[:1] in (b"<", b"{"):
            raise PlanoIlegivel("o link devolveu HTML/JSON, não o arquivo do plano")
        if inicio[:2] == b"PK":
            raise PlanoIlegivel("o arquivo é DOCX/ODT, não PDF")
        raise PlanoIlegivel("o arquivo não é PDF")

    # Alguns planos vêm com bytes antes do %PDF; o MuPDF não abre assim.
    if not data.startswith(b"%PDF"):
        data = data[data.find(b"%PDF"):]

    try:
        doc = fitz.open(stream=data, filetype="pdf")
    except Exception as e:
        raise PlanoIlegivel(f"PDF corrompido: {e}") from e

    # Senha de DONO (restringe cópia/impressão) abre com senha vazia. Senha de
    # ABERTURA não abre, e aí não há o que fazer.
    if doc.is_encrypted and not doc.authenticate(""):
        doc.close()
        raise PlanoIlegivel("PDF protegido por senha de abertura")
    return doc


def _extrair_paginas(doc, usar_ocr: bool, min_chars: int) -> list[str]:
    """Texto de cada página, na ordem do documento.

    A classificação usa tudo junto, mas guardar a divisão por página é o que
    permite dizer depois em que página do PDF está o trecho citado.
    """
    partes, ocradas = [], 0
    for page in doc:
        raw = _texto_da_pagina(page)
        # OCR quando a página está sem texto OU quando a camada de texto veio
        # embaralhada (codificação de fonte quebrada).
        precisa = len(raw.strip()) < min_chars or _frac_invalido(raw) > 0.10
        if usar_ocr and precisa and ocradas < PAGINAS_OCR_MAX:
            ocr = _ocr_pagina(page)
            ocradas += 1
            if len(ocr.strip()) > len(raw.strip()):
                raw = ocr
        partes.append(raw)
    return partes


def _extrair_doc(doc, usar_ocr: bool, min_chars: int) -> str:
    return " ".join(_extrair_paginas(doc, usar_ocr, min_chars))


def extrair_texto(pdf_path: str | Path, usar_ocr: bool = True,
                  min_chars: int = 200) -> str:
    """Texto de um PDF em disco. Faz OCR só nas páginas sem camada de texto."""
    with fitz.open(pdf_path) as doc:
        return _extrair_doc(doc, usar_ocr, min_chars)


def extrair_texto_bytes(data: bytes, usar_ocr: bool = True,
                        min_chars: int = 200) -> str:
    """Texto de um PDF recebido como bytes (ex.: upload no Streamlit)."""
    doc = _abrir_pdf(data)
    try:
        return _extrair_doc(doc, usar_ocr, min_chars)
    finally:
        doc.close()


def baixar_plano(url: str) -> bytes:
    """Baixa o arquivo do plano, repetindo em falha temporária.

    404/410 é definitivo (o plano não está lá) e sai na hora. Timeout, erro de
    conexão e 5xx são o DivulgaCand fora do ar: repete e, se não voltar,
    levanta PlanoIndisponivel para quem chama saber que é para tentar depois.
    """
    import requests
    ultimo = None
    for tentativa in range(1, BAIXAR_TENTATIVAS + 1):
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"},
                             timeout=BAIXAR_TIMEOUT, allow_redirects=True)
            # 4xx = a requisição é que está errada, repetir não muda nada. O TSE
            # responde 422 (não 404) para id de arquivo inexistente. Exceções:
            # 408 e 429, que pedem justamente para tentar de novo.
            if 400 <= r.status_code < 500 and r.status_code not in (408, 429):
                raise PlanoIlegivel(
                    f"o TSE não entrega esse arquivo (HTTP {r.status_code})")
            r.raise_for_status()
            if r.content:
                return r.content
            ultimo = "resposta vazia"
        except PlanoIlegivel:
            raise
        except Exception as e:
            ultimo = f"{type(e).__name__}: {e}"
        if tentativa < BAIXAR_TENTATIVAS:
            time.sleep(2 * tentativa)
    raise PlanoIndisponivel(
        f"DivulgaCand não respondeu após {BAIXAR_TENTATIVAS} tentativas ({ultimo}). "
        "Costuma ser queda passageira; tente de novo em alguns minutos.")


def extrair_paginas_url(url: str, usar_ocr: bool = True) -> list[str]:
    """Baixa o PDF do link e devolve o texto de cada página, na ordem."""
    doc = _abrir_pdf(baixar_plano(url))
    try:
        return _extrair_paginas(doc, usar_ocr, 200)
    finally:
        doc.close()


# ─── Em que página está o trecho ──────────────────────────────────────────────
# O painel guarda a frase que justifica cada classificação. Saber a página em
# que ela aparece é o que transforma "confie na análise" em "confira você
# mesmo": sem isso, conferir significa abrir um PDF de 90 páginas e procurar à
# mão. A conta é feita aqui, uma vez, e vai gravada na planilha — não faz
# sentido cada pessoa que abre o painel baixar o plano de novo.
_JANELA_TRECHO = 6      # palavras por janela na busca aproximada
_MIN_ESCORE = 0.25      # abaixo disso o trecho não está no PDF em forma literal


def _norm_busca(t: str) -> str:
    """Texto comparável: sem acento, sem caixa e sem pontuação. A quebra de
    linha do PDF vira espaço, senão nenhuma frase de duas linhas casa."""
    t = unicodedata.normalize("NFD", str(t or "").lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", " ", t).strip()


_CORTE_CITACAO = r"\[\s*[.…]{1,3}\s*\]|\.{3,}|…"


def _sem_espaco(t: str) -> str:
    """O texto sem nenhum espaço, para comparar citação com PDF.

    A extração quebra palavra no meio quando o PDF hifeniza na virada de linha:
    o plano do Alan Rick (AC) traz "mobili dade" no lugar de "mobilidade". Com
    isso, três citações dele que são transcrição fiel foram gravadas em
    07/08/2026 como "nao localizado", o rótulo que o painel usa para dizer que a
    frase é redação do modelo. Comparar sem espaço nenhum tira a extração da
    conta e não afrouxa a checagem: a sequência de caracteres continua tendo que
    existir contínua no plano.
    """
    return re.sub(r"\s+", "", t)


def _pedacos_de_citacao(trecho: str) -> list[str]:
    """A citação quebrada nos cortes que o modelo marcou, já normalizada.

    O prompt manda marcar com [...] toda junção de partes distantes do plano.
    Cada pedaço entre marcadores é que precisa existir contínuo no PDF: a frase
    inteira, com as junções, não existe assim em lugar nenhum.
    """
    return [p for p in (_norm_busca(x) for x in
                        re.split(_CORTE_CITACAO, str(trecho or ""))) if p]


def verificar_trecho(paginas_norm: list[str], trecho: str) -> str:
    """Se o trecho gravado é transcrição do plano ou redação do modelo.

    Roda na mesma passagem da análise, contra o mesmo texto que o modelo leu.
    Refazer a conta depois, reextraindo o PDF, dá resultado diferente: em
    06/08/2026, medindo os 1.261 trechos já gravados, os 31 do Allyson (RN)
    apareceram como não localizados só porque a extração de hoje trouxe um
    caractere quebrado no meio de palavras que a extração da análise não tinha.

    Valores:
      "literal"        todo pedaço da citação está contínuo no plano;
      "junta partes"   as palavras são do plano, a frase contínua não é. É o
                       modelo colando itens de uma lista sem marcar o corte;
      "nao localizado" a redação é do modelo, não do plano;
      "curto"          citação de até três palavras, que não distingue nada.

    A diferença entre os dois últimos importa e foi medida: dos 267 trechos não
    literais de 06/08/2026, 214 tinham 60% ou mais das janelas de três palavras
    presentes no plano (costura), e só 15 ficaram abaixo de 30% (redação
    própria). Chamar os dois de "não localizado" jogaria fora essa diferença.
    """
    if not paginas_norm or not str(trecho or "").strip():
        return ""
    pedacos = [p for p in _pedacos_de_citacao(trecho) if len(p.split()) >= 4]
    if not pedacos:
        return "curto"
    # O texto corrido entra junto porque citação verbatim pode atravessar a
    # virada de página, e aí não está inteira em nenhuma página sozinha.
    doc = _sem_espaco(" ".join(paginas_norm))
    achados = sum(1 for p in pedacos if _sem_espaco(p) in doc)
    if achados == len(pedacos):
        return "literal"
    if achados:
        return "junta partes"
    palavras = " ".join(pedacos).split()
    janelas = [" ".join(palavras[i:i + 3]) for i in range(len(palavras) - 2)]
    cobertura = (sum(1 for j in janelas if _sem_espaco(j) in doc) / len(janelas)
                 if janelas else 0)
    return "junta partes" if cobertura >= 0.6 else "nao localizado"


# O que faz de uma citação uma meta, e não uma ação. A régua da escala é "alvo
# mensurável", e o modelo estava lendo quantificador vago como número: o plano da
# Samara (UP) promete "geração de milhões de empregos" e o tema saiu como "Define
# meta". "Milhões" não é alvo, é adjetivo de tamanho. Medido em 07/08/2026: 66 dos
# 192 "Define meta" da base, um terço, não tinham alvo mensurável nenhum.
_META_NUMERO = r"\d"
_META_PRAZO = (r"\b(ate o final|ate o fim|primeiro ano|no primeiro mandato|"
               r"ao longo do mandato|anualmente|por ano|primeiros? \w+ "
               r"(dias|meses|anos))\b")
# Alvo absoluto é mensurável mesmo sem número: universalizar é 100%, zerar é 0.
_META_ABSOLUTO = (r"\b(universaliza\w*|zerar|zero|erradic\w*|elimina\w*|dobrar|"
                  r"triplicar|quadruplicar|toda a rede|todas as escolas|"
                  r"todos os municipios)\b")


def tem_alvo_mensuravel(trecho: str) -> bool:
    """Se a citação traz alvo que dá para conferir depois: número, prazo ou
    absoluto. É o que separa 'Define meta' de 'Propõe ação' na régua."""
    n = _norm_acentos(trecho)
    return bool(re.search(_META_NUMERO, n) or re.search(_META_PRAZO, n)
                or re.search(_META_ABSOLUTO, n))


def _norm_acentos(t: str) -> str:
    t = unicodedata.normalize("NFD", str(t or "").lower())
    return "".join(c for c in t if unicodedata.category(c) != "Mn")


def tema_e_item_de_enumeracao(trecho: str, tema: str) -> bool:
    """Triagem, não veredito: o tema parece ser item de uma lista na citação.

    O plano do Alan Rick (AC) promete "infraestrutura, logística, saneamento,
    mobilidade e integração regional", e daí saíram três "Propõe ação", um por
    item. O plano do Elizeu Aguiar (NOVO/PI) lista "Cursos em Agronegócio,
    Energias Renováveis, Tecnologia, Turismo, Logística" e ganhou Turismo.

    Marca 2,6% das citações, e cerca de um terço disso é proposta legítima que
    só enumera os próprios componentes: "Fortalecer atenção psicossocial,
    prevenção do suicídio, reabilitação" é proposta de saúde mental, e o termo
    do tema é item curto do mesmo jeito. Separar os dois é semântico, não cabe
    em regex, então quem decide é reanalisar_tema. Aqui é só a peneira que
    escolhe o que vale reperguntar, e falso positivo custa uma chamada.

    Uma versão anterior media só "termo citado uma vez dentro de vírgulas" e
    marcava 114 casos, metade deles proposta boa. Esta exige que o termo esteja
    num item curto de uma enumeração de verdade, com dois itens ou mais, e não
    mexe em citação que já traz alvo mensurável.
    """
    t = str(trecho or "")
    if tem_alvo_mensuravel(t):
        return False
    if len(ocorrencias_ancora(_norm_busca(t), tema)) != 1:
        return False
    segmentos = [s.strip() for s in re.split(r"[,;]|\be\b(?=\s+[A-ZÀ-Ú])", t) if s.strip()]
    curtos = [s for s in segmentos if len(s.split()) <= 3]
    if len(curtos) < 2:
        return False
    return any(ocorrencias_ancora(_norm_busca(s), tema) for s in curtos)


# Quem executa, reduzido a ente federativo. O campo `responsavel` é texto livre
# do modelo e chegou a 198 valores distintos em 1.486 linhas: "governo estadual",
# "estado", "gdf", "governo de sergipe" e "governo do estado" são a mesma coisa,
# e "governo dos trabalhadores, sem capitalistas" não é ente nenhum. Sem reduzir,
# o campo não dá filtro nem contagem.
#
# Serve para o que é verificável sem julgar mérito: candidato a governador cujo
# plano atribui a proposta ao governo federal está prometendo o que não depende
# do cargo que disputa. Eram 38 linhas na base de 07/08/2026.
_ENTES = [
    ("federal", r"\bfederal\b|\buniao\b|\bgoverno da uniao\b|\bcongresso\b|"
                r"\bministerio\b|\bmec\b|\bfnde\b"),
    ("municipal", r"\bmunicip|\bprefeitura|\bconsorcio intermunicipal"),
    ("privado", r"\bprivad|\bppp\b|\bparceria publico|\bempresa|\biniciativa privada|"
                r"\bconcessao|\bsistema s\b|\bsetor produtivo"),
    # "governo" sozinho fica fora daqui: casava dentro de "governo federal" e
    # marcava as duas esferas na mesma linha. Entra só no fallback abaixo.
    ("estadual", r"\bestad|\bgdf\b|\bsecretaria\b|\bagencia\b|\binstituto\b"),
]
_ENTES_RE = [(nome, re.compile(padrao)) for nome, padrao in _ENTES]
_GOVERNO_GENERICO = re.compile(r"\bgoverno\b|\bgestao\b")


def normalizar_responsavel(texto: str) -> str:
    """Os entes citados em `responsavel`, em ordem fixa e separados por vírgula.

    Devolve vazio quando não dá para reconhecer ente nenhum, que é resposta
    honesta: "governo dos trabalhadores, sem capitalistas" e "partido" não dizem
    quem executa. A ordem é fixa para o valor servir de chave de agrupamento.
    """
    n = _norm_busca(texto)
    if not n:
        return ""
    achados = {nome for nome, padrao in _ENTES_RE if padrao.search(n)}
    # "governo" e "gestão" sem esfera valem como estadual, que é o cargo em
    # disputa, mas só quando nenhuma outra esfera foi nomeada: senão "governo
    # federal" contaria como as duas.
    if not achados and _GOVERNO_GENERICO.search(n):
        achados = {"estadual"}
    ordem = ["estadual", "federal", "municipal", "privado"]
    return ", ".join(e for e in ordem if e in achados)


def citacao_sustenta(paginas_norm: list[str], trecho: str) -> bool:
    """Se a citação é lastro suficiente para afirmar um nível.

    A regra em um lugar só, porque testar `!= "nao localizado"` deixava passar
    dois casos que não sustentam nada: trecho vazio, em que verificar_trecho
    devolve "" e o nível ficava afirmado sem citação nenhuma, e "curto", a
    citação de até três palavras, que casa com qualquer plano e por isso não
    distingue candidato de candidato.
    """
    return verificar_trecho(paginas_norm, trecho) in ("literal", "junta partes")


def paginas_do_trecho(paginas_norm: list[str], trecho: str,
                      limite: int = 3) -> list[int]:
    """Páginas (1-based) em que o trecho aparece.

    O trecho gravado costuma ser uma citação com corte: o modelo junta partes
    distantes do plano com "[…]" ou reticências, e a extração ainda trunca em
    240 caracteres. Procurar a frase inteira nesse caso nunca acha nada, porque
    ela não existe assim em lugar nenhum do PDF. Por isso cada pedaço é
    procurado por conta própria e as páginas se somam.
    """
    pedacos = _pedacos_de_citacao(trecho)
    longos = [p for p in pedacos if len(p.split()) >= 4]
    if longos:
        achadas: list[int] = []
        for pedaco in longos:
            for n in _paginas_de_um_pedaco(paginas_norm, pedaco, limite):
                if n not in achadas:
                    achadas.append(n)
        return sorted(achadas)[:limite]

    # Nenhum pedaço tem quatro palavras. Acontece quando o modelo corta o meio
    # de uma lista: "Investiremos em... saneamento" vira "investiremos em" e
    # "saneamento", curtos demais para localizar sozinhos. Juntos, na MESMA
    # página, são sinal forte. Se muitas páginas têm os dois, o trecho não
    # distingue nada e é melhor não apontar nenhuma.
    if sum(len(p.split()) for p in pedacos) < 3 or len(pedacos) < 2:
        return []
    juntas = [i + 1 for i, pag in enumerate(paginas_norm)
              if pag and all(p in pag for p in pedacos)]
    return juntas if len(juntas) <= limite else []


def _paginas_de_um_pedaco(paginas_norm: list[str], trecho: str,
                          limite: int = 3) -> list[int]:
    """Um pedaço contínuo de citação.

    Primeiro tenta a frase inteira. Como o modelo às vezes resume em vez de
    citar, o segundo passo conta quantas janelas de seis palavras do trecho
    estão na página e fica com a melhor. Lista vazia significa que a frase não
    está literalmente no PDF.
    """
    alvo = _norm_busca(trecho)
    palavras = alvo.split()
    if len(palavras) < 4:
        return []
    # Mesma comparação sem espaço de verificar_trecho: senão a citação é aceita
    # como literal mas fica sem página, e o selo do painel manda o leitor
    # conferir num PDF de 90 páginas sem dizer onde.
    alvo_ce = _sem_espaco(alvo)
    exatas = [i + 1 for i, t in enumerate(paginas_norm)
              if t and alvo_ce in _sem_espaco(t)]
    if exatas:
        return exatas[:limite]
    # Janela menor em trecho curto. Com janela fixa de seis, uma citação de
    # quatro palavras vira uma janela só, que precisa bater inteira: "preservar
    # sua riqueza ambiental" não achava a página onde está escrito "preserve
    # sua riqueza ambiental", porque o modelo trocou a conjugação do verbo.
    # Com janelas de três, "sua riqueza ambiental" casa e a página aparece.
    _jan = min(_JANELA_TRECHO, max(3, len(palavras) - 1))
    janelas = [_sem_espaco(" ".join(palavras[i:i + _jan]))
               for i in range(max(1, len(palavras) - _jan + 1))]
    escores = [(sum(1 for j in janelas if j in _sem_espaco(t)) / len(janelas))
               if t else 0.0 for t in paginas_norm]
    melhor = max(escores) if escores else 0.0
    if melhor < _MIN_ESCORE:
        return []
    return [i + 1 for i, s in enumerate(escores) if s >= melhor * 0.9][:limite]


def extrair_texto_url(url: str, usar_ocr: bool = True) -> str:
    """Baixa o PDF do link (coluna LINK_PLANO) e extrai o texto, na hora da análise."""
    return extrair_texto_bytes(baixar_plano(url), usar_ocr=usar_ocr)


def extrair_plano_diagnostico(url: str, usar_ocr: bool = True) -> dict:
    """Igual a extrair_texto_url, mas devolve também POR QUE deu no que deu.

    A página usa isso para dizer na tela se o plano ficou de fora porque o link
    caiu, porque não é PDF ou porque é escaneado e falta OCR no ambiente.
    """
    saida = {"texto": "", "paginas": 0, "chars": 0, "ocr_usado": False,
             "status": "ok", "detalhe": ""}
    try:
        dados = baixar_plano(url)
        doc = _abrir_pdf(dados)
    except PlanoIndisponivel as e:
        saida["status"] = "indisponivel"
        saida["detalhe"] = str(e)
        return saida
    except PlanoIlegivel as e:
        saida["status"] = "ilegivel"
        saida["detalhe"] = str(e)
        return saida

    try:
        saida["paginas"] = doc.page_count
        sem_ocr = _extrair_doc(doc, usar_ocr=False, min_chars=200)
        por_pagina = len(sem_ocr.strip()) / max(1, doc.page_count)
        if por_pagina >= 200:
            saida["texto"] = sem_ocr
        elif not usar_ocr:
            saida["status"] = "escaneado"
            saida["detalhe"] = "PDF sem camada de texto; OCR desligado nesta chamada"
            saida["texto"] = sem_ocr
        elif not tesseract_disponivel():
            saida["status"] = "escaneado_sem_ocr"
            saida["detalhe"] = ("PDF sem camada de texto e o tesseract não está "
                                "instalado neste ambiente (falta packages.txt no deploy)")
            saida["texto"] = sem_ocr
        else:
            saida["texto"] = _extrair_doc(doc, usar_ocr=True, min_chars=200)
            saida["ocr_usado"] = True
            if len(saida["texto"].strip()) / max(1, doc.page_count) < 50:
                saida["status"] = "vazio"
                saida["detalhe"] = "nem a camada de texto nem o OCR devolveram conteúdo"
    finally:
        doc.close()

    saida["chars"] = len(saida["texto"].strip())
    return saida


def _limpa(t: str, n: int = 400) -> str:
    """Normaliza espaços e corta em `n` caracteres, sem partir palavra.

    O corte antigo era em 240 e caía no meio da palavra: 143 dos 346 trechos
    gravados em 03/08/2026 terminavam assim ('Reduzir a distorção idade-s…').
    Agora o corte recua até o fim da última frase, ou até o último espaço.
    """
    t = re.sub(r"\s+", " ", t or "").strip()
    if len(t) <= n:
        return t
    pedaco = t[:n]
    fim_frase = max(pedaco.rfind(". "), pedaco.rfind("! "), pedaco.rfind("? "))
    if fim_frase >= n * 0.6:
        return pedaco[:fim_frase + 1]
    fim_palavra = pedaco.rfind(" ")
    return (pedaco[:fim_palavra] if fim_palavra > 0 else pedaco).rstrip(" ,;") + "…"


# classificação por maturidade (Gemini)

class RespostaIlegivel(RuntimeError):
    """O modelo respondeu num formato que não conseguimos mapear em temas."""


def _carregar_json(raw: str, onde: str = "resposta"):
    """json.loads com os reparos que o modelo costuma exigir.

    O JSON vem quebrado de vez em quando, e a citação literal é a razão: o plano
    tem aspas dentro da frase e o modelo não escapa. Em 07/08/2026 isso derrubou
    o Rafael Fonteles (PT/PI) inteiro, com "Expecting ',' delimiter: line 100
    column 210", porque json.JSONDecodeError não era JSONDecodeError capturado
    pela retentativa: só RespostaIlegivel era, e o candidato ia embora.

    Reparos, na ordem: tira a cerca de código, corta o que vier antes da primeira
    chave e depois da última, e remove vírgula sobrando antes de fechar. O que
    não abrir depois disso vira RespostaIlegivel, que tem retentativa, porque
    pedir de novo é mais confiável que adivinhar onde faltava a aspa.
    """
    texto = (raw or "").strip()
    if not texto:
        raise RespostaIlegivel(f"{onde}: o modelo devolveu vazio")
    try:
        return json.loads(texto)
    except json.JSONDecodeError:
        pass
    limpo = re.sub(r"^```(?:json)?\s*|\s*```$", "", texto, flags=re.IGNORECASE)
    abre, fecha = limpo.find("{"), limpo.rfind("}")
    if abre == -1 or fecha <= abre:
        abre, fecha = limpo.find("["), limpo.rfind("]")
    if abre != -1 and fecha > abre:
        limpo = limpo[abre:fecha + 1]
    limpo = re.sub(r",\s*([}\]])", r"\1", limpo)
    try:
        return json.loads(limpo)
    except json.JSONDecodeError as e:
        raise RespostaIlegivel(f"{onde}: JSON inválido ({e})") from e


def _chave(nome: str) -> str:
    """Chave tolerante: ignora caixa, acento e espaço extra.

    O modelo às vezes devolve 'EDUCAÇÃO PROFISSIONAL' ou 'Educacao Profissional'
    no lugar do nome exato do tema.
    """
    t = unicodedata.normalize("NFD", str(nome))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return " ".join(t.lower().split())


def _achatar_por_tema(data: dict, temas: dict) -> dict:
    """Mapa {chave do tema: item} a partir da resposta do modelo.

    O prompt apresenta os temas agrupados por eixo, e o modelo às vezes responde
    na mesma forma, aninhado ({'Educação': {'Alfabetização': {...}}}), em vez de
    um objeto plano por tema. Antes disso derrubar a leitura em silêncio, aqui
    aceitamos as duas formas.
    """
    esperados = {_chave(t) for t in temas}
    achatado = {}

    ALIAS_NIVEL = ("nivel", "nível", "classificacao", "classificação")

    def tem_nivel(v: dict) -> bool:
        return any(k in v for k in ALIAS_NIVEL)

    def normaliza(v: dict) -> dict:
        """Garante a chave 'nivel'. Aceitar 'nível' aqui e não normalizar faria o
        item ser reconhecido e, logo depois, lido como 'Não menciona'."""
        if "nivel" not in v:
            for k in ALIAS_NIVEL:
                if k in v:
                    v["nivel"] = v[k]
                    break
        return v

    def visita(no):
        # Lista de objetos ([{'tema': 'Alfabetização', 'nivel': ...}, ...]) é a
        # terceira forma que o modelo já devolveu. Sem tratar aqui, a resposta
        # inteira era descartada como ilegível, que foi o que aconteceu com o
        # plano do Lucien Rezende em 03/08/2026.
        if isinstance(no, list):
            for item in no:
                visita(item)
            return
        if not isinstance(no, dict):
            return
        # Item que carrega o nome do tema num campo, em vez de ser a chave.
        for campo in ("tema", "nome", "titulo", "título"):
            rotulo = no.get(campo)
            if isinstance(rotulo, str) and _chave(rotulo) in esperados and tem_nivel(no):
                achatado.setdefault(_chave(rotulo), normaliza(no))
                return
        for k, v in no.items():
            if isinstance(v, list):
                visita(v)
                continue
            if not isinstance(v, dict):
                continue
            ck = _chave(k)
            if ck in esperados and tem_nivel(v):
                achatado.setdefault(ck, normaliza(v))
            else:
                visita(v)          # provavelmente um eixo: desce mais um nível

    visita(data)
    return achatado


def _lista_por_eixo(temas: dict) -> str:
    """Lista os temas agrupados pelo eixo a que pertencem. O agrupamento ajuda o
    modelo a não confundir tema de eixos vizinhos (ex.: 'Tecnologia na Educação'
    com 'Governo Digital')."""
    por_eixo = {}
    for tema, desc in temas.items():
        por_eixo.setdefault(EIXO_DO_TEMA.get(tema, "Outros"), []).append((tema, desc))
    blocos = []
    for eixo, itens in por_eixo.items():
        linhas = "\n".join(f"  - {t}: {d}" for t, d in itens)
        blocos.append(f"{eixo}:\n{linhas}")
    return "\n\n".join(blocos)


# Tamanho do bloco mandado ao modelo em cada chamada, e quanto um bloco repete
# do anterior.
#
# Existiam como `texto[:80000]`, um corte silencioso: 19 dos 43 planos de
# 07/08/2026 passavam disso e metade do conteúdo da base nunca foi lida. O plano
# do Zema tem 145 mil caracteres e o corte caía na página 45 de 81, o que gravou
# como "Não menciona" a primeira infância que tem seção própria na página 50. Nos
# planos truncados, 16,5% das citações eram redação do modelo, contra 0,7% nos
# que couberam inteiros: sem o texto, o modelo preenche a lacuna.
#
# 120 mil e não o plano inteiro numa chamada só porque recall cai em contexto
# muito longo, e aqui são 43 temas por chamada. A sobreposição existe para a
# proposta que cai bem na emenda entre dois blocos não ser perdida pelos dois.
BLOCO_CHARS = 120_000
BLOCO_SOBREPOSICAO = 2_000


def _blocos(texto: str, tamanho: int = BLOCO_CHARS,
            sobreposicao: int = BLOCO_SOBREPOSICAO) -> list[str]:
    """O texto do plano em blocos que cabem numa chamada, com sobreposição."""
    texto = texto or ""
    if len(texto) <= tamanho:
        return [texto]
    passo = tamanho - sobreposicao
    blocos = [texto[i:i + tamanho] for i in range(0, len(texto), passo)
              if texto[i:i + tamanho].strip()]
    # O resto da divisão vira um bloco de sobra que pode ter poucos milhares de
    # caracteres. Perguntar os 43 temas sobre um pedaço desses custa uma chamada
    # inteira e devolve quase só "Não menciona", que ainda entra no merge. Vai
    # junto com o bloco anterior: passar um pouco de BLOCO_CHARS não é problema,
    # o limite é de recall, não da janela do modelo.
    if len(blocos) > 1 and len(blocos[-1]) < tamanho // 4:
        sobra = blocos.pop()
        blocos[-1] = blocos[-1] + sobra[sobreposicao:]
    return blocos


def _juntar_classificacoes(parciais: list[dict], temas: dict) -> dict:
    """Junta a classificação de cada bloco ficando com o nível mais alto.

    O tema costuma aparecer em mais de um bloco: a menção de passagem na
    apresentação e a proposta no capítulo próprio. A regra é a mesma que o prompt
    já dá ao modelo dentro de um bloco, "se o tema aparecer em múltiplos trechos,
    use o de maior maturidade", aplicada agora entre blocos.
    """
    out = {}
    for tema in temas:
        melhor = None
        for parcial in parciais:
            item = parcial.get(tema)
            if not item:
                continue
            # Entre dois blocos, ganha o nível mais alto, mas citação vazia perde
            # de citação cheia mesmo com nível menor. Sem isso o bloco que
            # devolve "Define meta" sem trecho apaga o "Propõe ação" com a frase
            # do plano, e o tema chega à conferência sem lastro nenhum, quando
            # havia lastro à mão.
            chave = (bool(str(item.get("trecho", "")).strip()), item["score"])
            if melhor is None or chave > melhor[0]:
                melhor = (chave, item)
        out[tema] = (melhor[1] if melhor else None) or {
            "nivel": "Não menciona", "score": 0, "trecho": "",
            "responsavel": "", "prazo": "", "publico_alvo": "", "programa_nome": "",
        }
    return out


def classificar_plano(texto: str, temas: dict = TEMAS) -> dict:
    """Classifica todos os temas do plano, lendo o plano inteiro.

    Retorna {tema: {nivel, score, trecho, responsavel, prazo, publico_alvo,
    programa_nome}}. Uma chamada ao Gemini por bloco de BLOCO_CHARS.
    """
    parciais = []
    for bloco in _blocos(texto):
        try:
            parciais.append(_classificar_bloco(bloco, temas))
        except RespostaIlegivel:
            # A resposta do modelo não é determinística. Com o plano em vários
            # blocos, a chance de um deles voltar ilegível multiplica, e refazer
            # o plano inteiro por causa de um bloco desperdiça as chamadas que já
            # deram certo. A segunda tentativa é do bloco, não do plano.
            time.sleep(3)
            parciais.append(_classificar_bloco(bloco, temas))
    return _juntar_classificacoes(parciais, temas)


def _classificar_bloco(texto: str, temas: dict = TEMAS) -> dict:
    """Uma chamada ao Gemini: classifica todos os temas num pedaço do plano."""
    from google.genai import types

    lista = _lista_por_eixo(temas)
    prompt = (
        "Você é analista sênior de políticas públicas. Leia o plano de governo abaixo "
        "e classifique, para CADA tema listado, o nível de maturidade da proposta.\n\n"
        "ESCALA (use exatamente esses nomes):\n"
        "  Não menciona       — o tema não é mencionado no plano\n"
        "  Menciona vagamente — é citado de forma vaga, sem ação ou medida definida\n"
        "                       Ex.: 'valorizaremos a educação profissional', "
        "'a segurança será prioridade'\n"
        "  Propõe ação        — há ação ou medida concreta, mas sem alvo mensurável\n"
        "                       Ex.: 'criaremos centros de educação profissional em todos os estados'\n"
        "  Define meta        — há alvo mensurável: número, percentual, prazo ou combinação\n"
        "                       Ex.: 'ampliar para 1 milhão de vagas em EPT até 2027'\n\n"
        "ATENÇÃO: promessas de investimento genéricas ('investiremos R$ X em educação', "
        "'mais recursos para a saúde') SEM vínculo com um programa específico do tema "
        "NÃO contam como Define meta.\n\n"
        # Sem esta regra o tema entra como item de enumeração e sai como
        # proposta: no plano da Samara (UP), "fortalecer, universalizar o
        # saneamento básico, acesso a internet, esporte, etc. das escolas" virou
        # "Propõe ação" em Esporte e Lazer, e a proposta é sobre escola. Tentei
        # pegar isso por regra no código, medindo termo citado uma vez dentro de
        # lista, e a conferência nos 43 planos reprovou: dos 114 casos, metade
        # era proposta legítima que só listava seus componentes. Fica com o
        # modelo, que lê a frase.
        "ATENÇÃO 4: tema que aparece só como item de uma enumeração é Menciona "
        "vagamente, não Propõe ação. Em 'reformar as escolas com saneamento, "
        "internet, esporte, etc.', o esporte é item de lista e não proposta de "
        "esporte. Diferente de 'construir 20 quadras poliesportivas', que é "
        "proposta própria. Pergunte-se: a frase diz o que será feito NESTE tema, "
        "ou só cita o tema no meio de uma proposta sobre outra coisa?\n\n"
        "ATENÇÃO 3: quantificador vago não é alvo mensurável. 'milhões de empregos', "
        "'milhares de vagas', 'ampliar significativamente', 'vários municípios' são "
        "Propõe ação, não Define meta. Também não é meta o indicador citado sem valor "
        "a alcançar ('acompanhar o tempo médio de espera'): medir não é prometer "
        "chegar a um número. Só use Define meta quando a frase disser QUANTO ou "
        "ATÉ QUANDO.\n\n"
        "ATENÇÃO 2: classifique cada tema pelo que o plano diz DAQUELE tema. Um plano "
        "forte em segurança não puxa para cima os temas de educação.\n\n"
        # O plano do Elizeu Aguiar (NOVO/PI) tem 3 páginas de tópicos curtos e
        # saiu com 17 "Propõe ação", entre eles "Internet de alta velocidade" e
        # "Ampliação da acessibilidade". Nenhum diz como. O exemplo da escala já
        # trazia "valorizaremos a educação profissional" como Menciona
        # vagamente, e mesmo assim "Valorização dos professores" virou proposta:
        # faltava a pergunta que separa os dois.
        "TESTE DO COMO (aplique em todo tema antes de fechar o nível): a frase diz "
        "COMO a coisa será feita? Instrumento, programa nomeado, obra, concurso, "
        "mudança de regra, valor. Se diz só O QUE se quer alcançar, é Menciona "
        "vagamente, por mais concreto que o assunto pareça.\n"
        "  - 'Ampliação da acessibilidade' e 'Internet de alta velocidade' são "
        "Menciona vagamente: nomeiam o desejo, não o meio.\n"
        "  - 'Reforço do efetivo por meio de concursos públicos' e 'Criação da "
        "Estatal Eólica do Ceará' são Propõe ação: dizem o meio.\n"
        "Título de seção e item solto de lista quase nunca passam neste teste.\n\n"
        "REGRA: se o tema aparecer em múltiplos trechos, use o de maior maturidade.\n\n"
        # Medido em 06/08/2026 nos 1.261 trechos já gravados: 78% eram
        # transcrição fiel, e quase toda a diferença vinha de junção sem
        # marcador, não de texto inventado. O modelo colava dois itens de uma
        # lista com "e" ou ". " e o resultado parecia uma frase contínua que o
        # plano nunca teve. Pedir "cite literalmente" não resolvia porque o
        # modelo já achava que estava citando: o que faltava era a regra do
        # marcador, que é verificável (ver verificar_trecho).
        "REGRA DE CITAÇÃO (a mais importante): 'trecho' é transcrição, não resumo.\n"
        "  - copie do plano exatamente como está escrito, sem trocar nenhuma palavra, "
        "sem corrigir concordância e sem acrescentar comentário seu;\n"
        "  - para juntar partes distantes do plano, marque CADA corte com [...]. "
        "Ex.: 'ampliar a rede de creches [...] até 2030';\n"
        "  - nunca junte dois pedaços com 'e', com ponto ou com ponto e vírgula "
        "sem o [...]: o resultado vira uma frase que o plano não tem;\n"
        "  - se nenhuma frase do plano sustentar a classificação, baixe o nível em "
        "vez de escrever uma frase sua.\n\n"
        f"Temas a classificar, agrupados por eixo:\n{lista}\n\n"
        "Responda APENAS um objeto JSON válido, sem texto extra. "
        "Estrutura: uma chave por tema (nome exato), valor = objeto com:\n"
        "  'nivel': um dos quatro valores da escala\n"
        "  'trecho': transcrição do plano que justifica a classificação, seguindo a "
        "REGRA DE CITAÇÃO (vazio se Não menciona)\n"
        "  'responsavel': quem implementa — ex: 'governo estadual', 'municípios', "
        "'parceria público-privada', 'governo federal' (vazio se Não menciona)\n"
        "  'prazo': horizonte temporal mencionado — ex: 'até 2027', 'primeiro ano de mandato' "
        "(vazio se não houver)\n"
        "  'publico_alvo': a quem se destina — ex: 'estudantes do ensino médio', "
        "'crianças de 0 a 5 anos' (vazio se Não menciona)\n"
        "  'programa_nome': nome de programa específico citado — ex: 'PNAIC', 'PRONATEC' "
        "(vazio se não houver)\n\n"
        f"PLANO DE GOVERNO:\n{texto}"
    )
    resp = _gemini_client().models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(response_mime_type="application/json"),
    )
    raw = (getattr(resp, "text", "") or "").strip()
    data = _achatar_por_tema(_carregar_json(raw, "classificação"), temas)
    out = {}
    achou = 0
    for tema in temas:
        item = data.get(_chave(tema)) or {}
        if item:
            achou += 1
        nivel = item.get("nivel", "Não menciona")
        if nivel not in NIVEIS:
            nivel = "Não menciona"
        out[tema] = {
            "nivel":         nivel,
            "score":         NIVEIS.index(nivel),
            "trecho":        _limpa(item.get("trecho", "")),
            "responsavel":   _limpa(item.get("responsavel", ""),   n=120),
            "prazo":         _limpa(item.get("prazo", ""),          n=80),
            "publico_alvo":  _limpa(item.get("publico_alvo", ""),  n=120),
            "programa_nome": _limpa(item.get("programa_nome", ""), n=80),
        }
    if achou == 0:
        # Nenhuma chave bateu: a resposta veio em formato que não sabemos ler.
        # Devolver 26 "Não menciona" aqui seria indistinguível de um plano que
        # realmente não fala de nada, e foi assim que a análise do Marcio Bittar
        # (AC 2022) ficou gravada como vazia. Melhor falhar e tentar de novo.
        # A forma que veio entra na mensagem: sem isso, cada ocorrência exige
        # reproduzir a chamada à mão para descobrir o que o modelo devolveu.
        bruto = _carregar_json(raw, "classificação")
        if isinstance(bruto, dict):
            forma = f"objeto com as chaves {list(bruto)[:5]}"
        elif isinstance(bruto, list):
            forma = f"lista de {len(bruto)} itens, primeiro {str(bruto[:1])[:120]}"
        else:
            forma = f"{type(bruto).__name__}"
        raise RespostaIlegivel(
            "O modelo respondeu num formato não reconhecido: nenhum dos "
            f"{len(temas)} temas foi encontrado na resposta. Veio {forma}."
        )
    return out


def _regex_ancora(termo: str) -> re.Pattern:
    """O termo-âncora como padrão que aceita plural.

    Sem isso o acerto depende de eu ter escrito o termo na mesma flexão do plano:
    "pessoa com deficiência" não achava as "pessoas com deficiência" do plano do
    Zema, e a guarda passava batido justamente no caso que ela existe para pegar.
    O `s?` entra só em palavra de quatro letras ou mais, senão vira ruído em "com",
    "de" e "e".
    """
    palavras = [re.escape(p) + ("s?" if len(p) >= 4 else "")
                for p in _norm_busca(termo).split()]
    return re.compile(r"\b" + r"\s+".join(palavras))


_ANCORAS_RE = {tema: [_regex_ancora(t) for t in termos]
               for tema, termos in TERMOS_ANCORA.items()}


def ocorrencias_ancora(texto_norm: str, tema: str) -> list[int]:
    """Onde os termos-âncora do tema aparecem no texto normalizado."""
    achadas = set()
    for padrao in _ANCORAS_RE.get(tema, []):
        for m in padrao.finditer(texto_norm):
            achadas.add(m.start())
            if len(achadas) >= 12:
                return sorted(achadas)
    return sorted(achadas)


def contexto_do_tema(texto: str, texto_norm: str, tema: str,
                     janela: int = 3000, maximo: int = 5) -> str:
    """Os pedaços do plano em volta dos termos-âncora do tema.

    Serve para a segunda pergunta, dirigida a um tema só. Mandar o plano inteiro
    de novo custa o mesmo da primeira passagem e volta a diluir 43 temas numa
    resposta; mandar só o entorno das ocorrências deixa a pergunta específica e a
    resposta conferível.
    """
    posicoes = ocorrencias_ancora(texto_norm, tema)[:maximo]
    if not posicoes:
        return ""
    # texto_norm e texto têm comprimentos diferentes: a posição normalizada é uma
    # aproximação da posição no original, e a janela é larga o bastante para o
    # deslize não tirar o trecho de dentro.
    fator = len(texto) / max(len(texto_norm), 1)
    pedacos = []
    for p in posicoes:
        centro = int(p * fator)
        pedacos.append(texto[max(0, centro - janela):centro + janela])
    return "\n[...]\n".join(pedacos)


def reanalisar_tema(contexto: str, tema: str, desc: str = "") -> dict:
    """Pergunta de novo, sobre um tema só, com o entorno onde o termo aparece.

    Duas coisas caem aqui: o tema que o modelo deu como ausente mas cujo termo
    está no plano, e o tema cuja citação não passou em verificar_trecho. Nos dois
    casos o que falta é a mesma coisa, uma frase do plano que sustente o nível, e
    a saída "AUSENTE" é resposta legítima: o termo pode aparecer de passagem sem
    nenhuma proposta atrás.
    """
    from google.genai import types

    prompt = (
        "Você é analista sênior de políticas públicas. Abaixo estão os trechos de "
        f"um plano de governo que citam o tema '{tema}'"
        + (f" ({desc})" if desc else "") + ".\n\n"
        "ESCALA (use exatamente esses nomes):\n"
        "  Não menciona       — o tema não é tratado nestes trechos\n"
        "  Menciona vagamente — citado de forma vaga, sem ação ou medida definida\n"
        "  Propõe ação        — há ação ou medida concreta, sem alvo mensurável\n"
        "  Define meta        — há alvo mensurável: número, percentual ou prazo\n\n"
        "REGRA DE CITAÇÃO: 'trecho' é transcrição literal, copiada palavra por "
        "palavra do texto abaixo. Não corrija concordância, não encurte dentro da "
        "frase e não escreva frase sua. Para juntar partes distantes, marque cada "
        "corte com [...].\n\n"
        "Se os trechos só citam a palavra de passagem, sem nada sobre o tema, "
        "responda nivel 'Não menciona' e trecho vazio. Não invente proposta.\n\n"
        "Responda APENAS um objeto JSON com as chaves 'nivel', 'trecho', "
        "'responsavel', 'prazo', 'publico_alvo' e 'programa_nome'.\n\n"
        f"TRECHOS DO PLANO:\n{contexto}"
    )
    resp = _gemini_client().models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(response_mime_type="application/json"),
    )
    item = _carregar_json(getattr(resp, "text", ""), f"reanálise de {tema!r}")
    if not isinstance(item, dict):
        raise RespostaIlegivel(f"reanálise de '{tema}' não veio como objeto JSON")
    nivel = item.get("nivel", "Não menciona")
    if nivel not in NIVEIS:
        nivel = "Não menciona"
    return {
        "nivel":         nivel,
        "score":         NIVEIS.index(nivel),
        "trecho":        _limpa(item.get("trecho", "")),
        "responsavel":   _limpa(item.get("responsavel", ""),   n=120),
        "prazo":         _limpa(item.get("prazo", ""),          n=80),
        "publico_alvo":  _limpa(item.get("publico_alvo", ""),  n=120),
        "programa_nome": _limpa(item.get("programa_nome", ""), n=80),
    }


_ASPAS = r"[\"“”]([^\"“”]{8,})[\"“”]"


def tirar_aspas_sem_lastro(justificativa: str, citacoes: list[str]) -> str:
    """Tira as aspas do que não é cópia de uma citação verificada.

    A regra de aspas do prompt não basta sozinha. A justificativa do Zema gravada
    em 07/08/2026 traz quatro frases entre aspas e três não existem no plano: o
    texto diz "classificar nacional e internacionalmente as facções criminosas
    como organizações terroristas" e a justificativa encurtou para "classificar
    facções como organizações terroristas", mantendo as aspas. Encurtar dentro
    das aspas é fabricar citação, e ninguém conferia esse campo.

    Tira só as aspas, não a frase: o conteúdo continua sendo leitura defensável
    do analista, o que ele não pode é se passar por transcrição.
    """
    corpus = _sem_espaco(" ".join(_norm_busca(c) for c in citacoes if c))
    if not str(justificativa or "").strip():
        return justificativa

    def troca(m):
        dentro = m.group(1)
        return dentro if _sem_espaco(_norm_busca(dentro)) not in corpus else m.group(0)

    return re.sub(_ASPAS, troca, str(justificativa))


def _quadro_da_analise(classif: dict, temas: dict) -> str:
    """A classificação já verificada, agrupada por eixo, como texto para o prompt."""
    por_eixo = {}
    for tema in temas:
        item = classif.get(tema) or {}
        por_eixo.setdefault(EIXO_DO_TEMA.get(tema, "Outros"), []).append(
            (tema, item.get("nivel", "Não menciona"), item.get("trecho", "")))
    blocos = []
    for eixo, itens in por_eixo.items():
        linhas = []
        for tema, nivel, trecho in itens:
            linhas.append(f"  - {tema}: {nivel}"
                          + (f'\n      citação: "{trecho}"' if trecho else ""))
        blocos.append(f"{eixo}:\n" + "\n".join(linhas))
    return "\n\n".join(blocos)


def avaliar_coerencia(classif: dict, temas: dict = TEMAS) -> dict:
    """Avalia se as propostas formam uma estratégia coerente no plano.
    Retorna {"score": 1–5, "justificativa": str}.

    Com mais de um eixo, a pergunta deixa de ser só "a educação se sustenta" e
    passa a incluir a articulação ENTRE eixos, que é onde plano de governo
    costuma se revelar: segurança que conversa com assistência social, educação
    profissional que conversa com geração de emprego.

    Recebe a classificação já verificada, não o texto do plano. Antes lia
    `texto[:60000]` e por isso julgava o plano por um pedaço dele: a justificativa
    do Zema gravada em 07/08/2026 afirma que "Educação Básica e Saúde não têm
    capítulos próprios", e os dois estão no sumário do próprio PDF, fora do corte.
    Ler o mesmo material da outra aba resolve as duas coisas de uma vez. A
    contradição entre abas deixa de ser possível, porque a fonte é uma só, e a
    justificativa não pode inventar aspas, porque as únicas frases do plano à mão
    são citações que já passaram em verificar_trecho.
    """
    from google.genai import types

    prompt = (
        "Você é analista sênior de políticas públicas. Abaixo está a análise "
        "tema a tema de um plano de governo, com o nível de cada tema e a citação "
        "do plano que sustenta esse nível.\n\n"
        f"{_quadro_da_analise(classif, temas)}\n\n"
        "Avalie se as propostas formam uma ESTRATÉGIA COERENTE e articulada, "
        "ou se são menções isoladas e desconexas.\n\n"
        "Escala:\n"
        "  1 — Apenas menções genéricas, sem proposta concreta em nenhum eixo\n"
        "  2 — Propostas isoladas, sem articulação entre temas nem entre eixos\n"
        "  3 — Alguns temas articulados dentro de um eixo, mas estratégia parcial\n"
        "  4 — Estratégia clara na maioria dos eixos, com alguma articulação entre eles\n"
        "  5 — Estratégia integrada: os eixos se conectam e reforçam entre si\n\n"
        "Um plano que cobre bem UM eixo e ignora os outros não passa de 3.\n\n"
        "Responda APENAS um objeto JSON com:\n"
        "  'score': número inteiro de 1 a 5\n"
        "  'justificativa': 2 a 4 frases factuais que sustentem o score, com no "
        "máximo 500 caracteres no total.\n\n"
        "COMO ESCREVER A JUSTIFICATIVA:\n"
        "- Comece pelo dado, nunca pela palavra 'O plano'. Abra com o nome de um "
        "programa, uma meta ou um eixo concreto do texto.\n"
        "- Não repita as palavras da escala acima ('integrada', 'articulada', "
        "'parcial', 'isoladas', 'coerente'). Descreva o que o plano faz, não em "
        "que degrau ele caiu.\n"
        "- Não use o molde 'o plano faz X, contudo falta Y'. Se um eixo não tem "
        "proposta, diga em frase própria qual eixo e o que falta nele.\n"
        "- Nomeie programas, metas, números e prazos que estão no texto. Uma "
        "justificativa sem nenhum nome próprio de programa ou número não serve.\n"
        "- Nada de juízo de valor. Só o que está escrito no plano.\n\n"
        "REGRA DE ASPAS: só use aspas para copiar, palavra por palavra, uma das "
        "citações listadas acima. Nunca encurte a frase dentro das aspas nem "
        "escreva entre aspas algo que não está ali. Sem citação à mão para o "
        "ponto que quer fazer, escreva sem aspas.\n\n"
        "REGRA DE AUSÊNCIA: só diga que um tema falta se ele está acima como "
        "'Não menciona'. Tema com nível diferente disso tem proposta no plano.\n\n"
        f"{RESTRICOES_LINGUAGEM}"
    )
    resp = _gemini_client().models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(response_mime_type="application/json"),
    )
    raw = (getattr(resp, "text", "") or "").strip()
    data = _carregar_json(raw, "coerência")
    score = int(data.get("score", 1))
    if score not in range(1, 6):
        score = 1
    # 600 e não 400: no limite antigo, 4 das 16 justificativas gravadas em
    # 03/08/2026 terminavam cortadas no meio da palavra.
    justificativa = _limpa(data.get("justificativa", ""), n=600)
    citacoes = [(classif.get(t) or {}).get("trecho", "") for t in temas]
    return {"score": score,
            "justificativa": tirar_aspas_sem_lastro(justificativa, citacoes)}


def sintetizar_comparacao(candidatos_info: list, tema: str) -> str:
    """Gera narrativa comparativa entre candidatos para um dado tema.
    candidatos_info: [{"nome": str, "partido": str, "nivel": str, "trecho": str}, ...]"""
    if not candidatos_info:
        return ""
    bloco = "\n\n".join(
        f"{i+1}. {c['nome']} ({c['partido']}) — Nível: {c['nivel']}\n"
        f"   Trecho do plano: {c['trecho'] or '(sem trecho)'}"
        for i, c in enumerate(candidatos_info)
    )
    prompt = (
        f"Compare as propostas dos candidatos abaixo sobre o tema '{tema}'.\n\n"
        f"Candidatos:\n{bloco}\n\n"
        "Escreva uma análise comparativa em 3 a 5 parágrafos curtos. Inclua:\n"
        "- O que cada candidato propõe de concreto (ou a ausência de proposta)\n"
        "- Diferenças e semelhanças entre as abordagens\n"
        "- Quem tem a proposta mais estruturada e por quê\n\n"
        "Seja direto e técnico. Use prosa corrida, sem listas ou marcadores. "
        "Não se apresente nem mencione seu papel, vá direto ao conteúdo.\n\n"
        f"{RESTRICOES_LINGUAGEM}"
    )
    resp = _gemini_client().models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
    )
    return (getattr(resp, "text", "") or "").strip()


# pipeline principal

def analisar_da_base(df, link_col="LINK_PLANO",
                     id_cols=("NM_URNA_CANDIDATO", "SG_UF", "SG_PARTIDO", "DS_CARGO"),
                     temas: dict = TEMAS) -> pd.DataFrame:
    """Analisa os planos direto da base consolidada: para cada linha com link,
    baixa o PDF, extrai o texto e classifica no Gemini. Retorna a tabela longa."""
    linhas = []
    for _, r in df.iterrows():
        link = r.get(link_col)
        if not isinstance(link, str) or not link:
            continue
        try:
            texto = extrair_texto_url(link)
            classif = classificar_plano(texto, temas)
        except Exception as e:
            print(f"  [erro] {r.get(id_cols[0])}: {e}")
            continue
        meta = {c: r.get(c) for c in id_cols}
        for tema, res in classif.items():
            linhas.append({**meta, "tema": tema, **res})
    return pd.DataFrame(linhas)


def analisar_pasta(pasta: str | Path, metadados_csv: str | Path,
                   temas: dict = TEMAS) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Processa PDFs de uma pasta. Retorna (matriz cand×tema de níveis, tabela longa)."""
    pasta = Path(pasta)
    meta = pd.read_csv(metadados_csv)
    linhas = []
    for _, row in meta.iterrows():
        pdf = pasta / row["arquivo"]
        if not pdf.exists():
            continue
        try:
            texto = extrair_texto(pdf)
            classif = classificar_plano(texto, temas)
        except Exception as e:
            print(f"  [erro] {row['arquivo']}: {e}")
            continue
        for tema, res in classif.items():
            linhas.append({**row.to_dict(), "tema": tema, **res})

    long = pd.DataFrame(linhas)
    long["rotulo"] = long["nome"] + " (" + long["partido"] + "/" + long["uf"] + ")"
    matriz = long.pivot_table(index="rotulo", columns="tema",
                              values="score", aggfunc="first")
    matriz = matriz.reindex(columns=list(temas.keys()))
    return matriz, long
