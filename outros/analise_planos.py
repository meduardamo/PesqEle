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
        "Tempo Integral": "educação em tempo integral e jornada escolar ampliada, em qualquer etapa da educação básica: desenho da jornada, vagas, escolas de tempo integral — o conteúdo que ocupa a jornada ampliada, quando é arte ou cultura, é o tema Educação, Arte e Cultura",
        "Educação Profissional": "educação profissional e técnica (EPT, SENAI, SENAC, institutos federais, cursos técnicos)",
        "Valorização Docente": "carreira, salário, piso, concurso, formação continuada e condições de trabalho de professores e demais profissionais da educação",
        "Educação Inclusiva e EJA": "educação especial e inclusiva, estudante com deficiência, educação de jovens e adultos, educação no campo, indígena e quilombola",
        "Tecnologia na Educação": "tecnologia, conectividade, inclusão digital e uso de IA nas escolas",
        # Entraram em 19/08/2026 a pedido do Itaú:
        "Educação, Arte e Cultura": "integração de arte, cultura e linguagens artísticas no currículo escolar e na educação integral: arte na escola, oficinas e projetos culturais com estudantes — a política cultural fora da escola (fomento, editais, patrimônio, equipamentos) é o tema Cultura, no eixo de Cultura, esporte e turismo",
        "Recomposição das Aprendizagens": "recomposição, recuperação e aceleração de aprendizagens, reforço escolar, correção de fluxo, defasagem idade-série e superação de lacunas pedagógicas, inclusive as herdadas da pandemia — alfabetizar na idade certa é o tema Alfabetização, e a etapa em si é Fundamental ou Ensino Médio",
        # Entrou em 10/08/2026. Os outros temas de educação são por etapa, então
        # proposta de dinheiro não achava onde cair: no plano da Samara Martins
        # (UP), "10% do PIB para educação" foi classificada em Ciência,
        # Tecnologia e Inovação, e a educação inteira ficou como "Não menciona".
        "Financiamento da Educação": "financiamento e orçamento da educação: percentual do PIB ou da receita, Fundeb, custo aluno, vinculação de recursos e gratuidade do ensino público",
        # Entrou em 10/08/2026. Até aqui ensino superior morava na descrição de
        # Ciência, Tecnologia e Inovação, e foi de lá que "Livre acesso à
        # universidade e fim do vestibular (ENEM)", do plano da Samara Martins,
        # saiu classificada como CT&I. Ao criar o tema, a menção some da
        # descrição de CT&I, senão os dois disputam a mesma citação.
        "Ensino Superior": "ensino superior e universidades: acesso e vagas, vestibular e ENEM, gratuidade, expansão de campus, assistência estudantil e permanência",
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
        # Entrou em 10/08/2026, pelo mesmo motivo do financiamento da educação.
        # Os outros temas de saúde são por nível de atenção, e o item 11 do plano
        # da Samara Martins, "Nacionalização dos planos de Saúde. Aumento do
        # Orçamento que permita o funcionamento de 100% da saúde pública (SUS)",
        # não caiu em nenhum. A justificativa então afirmou que o plano não fala
        # de saúde.
        "Financiamento e Gestão do SUS": "financiamento e orçamento da saúde, percentual vinculado, gestão do SUS, judicialização, planos de saúde privados e relação entre público e privado",
    },
    "Segurança pública": {
        "Policiamento e Efetivo": "efetivo policial, salário, equipamento, presença ostensiva, videomonitoramento",
        "Enfrentamento ao Crime Organizado": "facções, narcoterrorismo, tráfico, inteligência policial, fronteiras, lavagem de dinheiro e uso de plataformas de apostas/bets para crimes financeiros",
        "Violência contra a Mulher": "violência doméstica, Lei Maria da Penha, delegacia da mulher, casa da mulher brasileira",
        "Sistema Prisional e Socioeducativo": "presídios, vagas prisionais, ressocialização, egresso e sistema socioeducativo de adolescentes",
    },
    "Economia e emprego": {
        "Geração de Emprego": "geração de emprego e renda, qualificação profissional para o trabalho, intermediação de mão de obra",
        "Ambiente de Negócios": "atração de investimento, desburocratização, incentivo fiscal, apoio a micro e pequena empresa, regulação do mercado de apostas esportivas (bets) e restrições à publicidade de jogos",
        "Agropecuária": "agricultura, pecuária, agronegócio, agricultura familiar, crédito rural",
        "Ciência, Tecnologia e Inovação": "pesquisa científica, fundação de amparo à pesquisa, parque tecnológico, startups e inovação — o ensino superior é tema próprio, no eixo de Educação",
    },
    "Meio ambiente e clima": {
        "Desmatamento e Conservação": "desmatamento, unidades de conservação, fiscalização ambiental, queimadas",
        "Saneamento e Recursos Hídricos": "água, esgoto, resíduos sólidos, bacias hidrográficas, seca",
        "Transição Energética": "energia renovável, solar, eólica, crédito de carbono, economia verde",
        "Defesa Civil e Desastres": "defesa civil, enchente, seca, deslizamento, prevenção e resposta a desastres, adaptação climática",
    },
    "Assistência social e pobreza": {
        "Transferência de Renda": "programas de transferência de renda, complemento ao Bolsa Família, benefício estadual, proteção contra endividamento e restrição ao uso de benefícios em apostas/bets",
        "Segurança Alimentar": "fome, insegurança alimentar, restaurante popular, banco de alimentos, cesta básica",
        "Habitação": "moradia, habitação popular, regularização fundiária, aluguel social",
    },
    "Infraestrutura e mobilidade": {
        "Transporte e Rodovias": "rodovias, pavimentação, ferrovias, portos, aeroportos, logística",
        "Mobilidade Urbana": "transporte público, tarifa, metrô, BRT, ciclovia",
    },
    "Gestão pública e transparência": {
        "Eficiência e Gasto Público": "reforma administrativa, corte de gasto, teto de despesa, equilíbrio fiscal, eficiência orçamentária e gestão de emendas parlamentares/impositivas",
        "Transparência e Combate à Corrupção": "transparência, controle interno, dados abertos, combate à corrupção, integridade pública, fiscalização de emendas parlamentares e orçamento secreto",
        "Regulação de Plataformas Digitais": "regulação de plataformas digitais e redes sociais, moderação de conteúdo, combate à desinformação e fake news, soberania digital, proteção de dados e inteligência artificial",
        "Governo Digital": "digitalização de serviços, atendimento ao cidadão, governo eletrônico",
        "Servidores e Municípios": "servidor público estadual em geral, carreira e concurso, e a relação do estado com os municípios (consórcio, repasse, apoio técnico) — professor e demais profissionais da educação são o tema Valorização Docente, no eixo de Educação",
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
        "Cultura": "política cultural: patrimônio, economia criativa, fomento e editais, equipamentos e trabalhadores da cultura — arte e cultura dentro da escola é tema próprio, Educação, Arte e Cultura, no eixo de Educação",
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
    "Alfabetização": ["alfabetiza*", "pnaic", "pna ", "crianca alfabetizada"],
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
                                 "eja", "autis*", "estudante com deficiencia"],
    "Tecnologia na Educação": ["conectividade*", "inclusao digital", "internet nas escolas",
                               "tecnologia educacional", "internet", "laboratorio de informatica",
                               "transformacao digital", "computador", "tablet"],
    "Educação, Arte e Cultura": ["arte e cultura", "cultura na escola", "cultura nas escolas",
                                 "arte na escola", "artes na escola", "linguagens artisticas",
                                 "educacao artistica", "ensino de arte"],
    "Recomposição das Aprendizagens": ["recomposicao*", "recuperacao da aprendizagem", "reforco escolar",
                                       "defasagem", "distorcao idade serie", "lacunas de aprendizagem"],
    "Ensino Superior": ["ensino superior", "universidade", "universitari*", "vestibular",
                        "enem", "prouni", "fies", "assistencia estudantil",
                        "graduacao", "campus"],
    "Financiamento da Educação": ["por cento do pib", "% do pib", "do pib para educacao",
                                  "fundeb", "custo aluno", "orcamento da educacao",
                                  "financiamento da educacao", "vinculacao de recursos"],
    "Financiamento e Gestão do SUS": ["orcamento da saude", "financiamento da saude",
                                      "subfinanciamento", "gestao do sus", "plano de saude",
                                      "planos de saude", "saude suplementar",
                                      "judicializacao da saude", "piso da saude"],
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
                               "policias", "desmilitariza*", "forcas de seguranca",
                               "camera corporal", "viatura"],
    "Enfrentamento ao Crime Organizado": ["faccao*", "crime organizado", "trafico*",
                                          "inteligencia policial", "narcoterrorismo", "lavagem de dinheiro"],
    "Violência contra a Mulher": ["violencia domestica", "maria da penha", "feminicidio",
                                  "delegacia da mulher", "casa da mulher"],
    "Sistema Prisional e Socioeducativo": ["presidio", "sistema prisional", "vaga prisional",
                                           "ressocializacao", "socioeducativo", "penitenciaria"],
    "Geração de Emprego": ["geracao de emprego", "qualificacao profissional", "posto de trabalho",
                           "intermediacao de mao de obra", "emprego e renda", "empregabilidade",
                           "frentes de trabalho", "vagas de trabalho", "gerar emprego",
                           "criacao de emprego"],
    "Ambiente de Negócios": ["desburocratiza*", "incentivo fiscal", "atracao de investimento",
                             "micro e pequena empresa", "ambiente de negocios", "empreendedorismo",
                             "iniciativa privada", "credito", "banco de fomento", "comercio popular",
                             "publicidade de apostas", "propaganda de bets"],
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
                               "beneficio estadual", "renda minima", "bets", "jogos de aposta"],
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
                                   "recursos publicos", "emendas parlamentares", "emendas impositivas"],
    "Transparência e Combate à Corrupção": ["transparencia", "dados abertos", "corrup*",
                                            "controle interno", "orcamento secreto", "emendas parlamentares"],
    "Regulação de Plataformas Digitais": ["redes sociais", "plataformas digitais", "desinformacao",
                                          "fake news", "soberania digital", "moderacao de conteudo",
                                          "big techs", "inteligencia artificial"],
    "Governo Digital": ["governo digital", "digitalizacao*", "servico digital",
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
    "Povos Indígenas e Quilombolas": ["indigena*", "quilombola", "povos tradicionais",
                                      "comunidade tradicional"],
    "População LGBTQIA+": ["lgbt*", "lgbtfobia", "nome social", "diversidade sexual"],
    "Cultura": ["cultura", "patrimonio", "economia criativa", "edital de cultura",
                "equipamento cultural"],
    "Esporte e Lazer": ["esporte", "lazer", "atleta", "quadra poliesportiva",
                        "equipamento esportivo"],
    "Turismo": ["turismo", "turistic*", "atrativo turistico"],
}


# Listas de ausência: o vocabulário de cada tema, para provar o "Não menciona".
#
#
# POR QUE DUAS LISTAS E NÃO UMA
# TERMOS_ANCORA é discriminante de propósito ("creche" serve, "criança" não) e é
# ela que dispara a repergunta ao modelo. Termo comum ali faz a guarda disparar em
# todo plano e gasta chamada à toa, que é o problema já medido do "uti" (248
# ocorrências, 5 reais). Esta lista aqui não gasta chamada nenhuma: ela só carimba
# o selo que vai gravado ao lado do nível. Por isso ela é generosa.
#
# A DIREÇÃO DO ERRO É O QUE MUDA
# Nesta lista, termo que dispara à toa é conservador: rebaixa "Ausência
# confirmada" para "Revisar ausência" e ninguém afirma nada de errado. Termo
# FALTANDO é que estraga, porque carimba ausência confirmada num tema que o plano
# trata com outro vocabulário. Então, na dúvida, inclua.
#
# O QUE A BORDA DE PALAVRA RESOLVE E O QUE ELA NÃO RESOLVE
# _regex_ancora monta \btermo\b, com plural opcional e radical quando o termo
# termina em "*". Medido no plano do Lula 2026: "uti" cai de 85 casamentos por
# substring para 0, "leito" de 3 (todos dentro de "eleito") para 0, "esf" de 8
# (dentro de "esforço") para 0, "fome" de 45 (dentro de "fomento") para 11.
# O que a borda NÃO resolve é palavra inteira com outro sentido. No mesmo plano,
# "aposta" casa 4 vezes com borda, e as 4 são "este programa aposta nessa força".
# Por isso aqui não existe "aposta" solta, e sim "jogos de apostas", "apostas on
# line", "casas de aposta" e "aposta esportiva". Mesma razão para não usar
# "urgência" solta (aparece como "urgência cada vez maior"), "meta", "campo" e
# "rede".
#
# LIMITE CONHECIDO DO PLURAL AUTOMÁTICO
# O sufixo do _regex_ancora cobre "s" e "es", que é o plural regular. Não cobre
# "m" virando "ns": "trem" não casa "trens", "armazém" não casa "armazéns". Nesses
# casos as duas formas estão escritas na lista.
#
# COLISÕES CONHECIDAS, MANTIDAS DE PROPÓSITO
# "média e alta complexidade" também é vocabulário de assistência social: no plano
# do Lula a única ocorrência é "proteção social especial de média e alta
# complexidade", que é SUAS. Mantido porque na maioria dos planos estaduais o
# sentido é saúde e porque o erro cai para o lado seguro.
TERMOS_AUSENCIA = {
    # ------------------------------------------------------------ Educação
    "Alfabetização": [
        "alfabetiza*", "analfabet*", "pnaic", "pna", "crianca alfabetizada",
        "criancas alfabetizadas", "idade certa", "fluencia leitora",
        "leitura e escrita", "aprender a ler", "saibam ler",
        "ciclo de alfabetizacao", "avaliacao de fluencia",
    ],
    "Primeira Infância": [
        "primeira infancia", "creche*", "pre escola", "pre escolar",
        "educacao infantil", "bercario", "primeiros anos de vida",
        "desenvolvimento infantil", "crianca feliz", "vaga em creche",
        "vagas em creche", "matricula na creche", "puericultura",
    ],
    "Fundamental": [
        "ensino fundamental", "anos iniciais", "anos finais", "educacao basica",
        "aprendizagem", "recomposicao de aprendizagem", "defasagem",
        "distorcao idade serie", "reforco escolar", "recuperacao da aprendizagem",
        "saeb", "ideb", "prova brasil", "aprender a ler", "saibam ler",
        "terceiro ano", "quinto ano", "nono ano", "rede estadual de ensino",
        "escolas estaduais", "rede estadual", "avaliacao externa",
        "avaliacoes externas", "aprovacao automatica", "reprovacao",
    ],
    "Ensino Médio": [
        "ensino medio", "novo ensino medio", "itinerario formativo",
        "ensino medio integrado", "evasao escolar", "abandono escolar",
        "pe de meia", "enem", "matricula no ensino medio", "saeb", "ideb",
    ],
    "Tempo Integral": [
        "tempo integral", "ensino integral", "escola integral",
        "educacao integral", "jornada ampliada", "jornada escolar",
        "contraturno", "escola de tempo integral", "turno unico",
    ],
    "Educação Profissional": [
        "educacao profissional", "ensino tecnico", "curso tecnico",
        "cursos tecnicos", "escola tecnica", "etec", "senai", "senac", "sesi",
        "instituto federal", "institutos federais", "profissionalizante",
        "qualificacao tecnica", "formacao tecnica", "itinerario tecnico",
        "aprendizagem profissional",
    ],
    "Valorização Docente": [
        "professor*", "docente*", "magisterio", "piso salarial", "piso nacional",
        "carreira docente", "formacao continuada", "formacao de professores",
        "concurso para professor", "valorizacao do magisterio",
        "plano de carreira do magisterio", "residencia pedagogica", "pibid",
    ],
    "Educação Inclusiva e EJA": [
        "educacao especial", "educacao inclusiva", "jovens e adultos", "eja",
        "autis*", "tea", "neurodivergen*", "estudante com deficiencia",
        "atendimento educacional especializado", "sala de recursos",
        "educacao bilingue", "libras", "educacao do campo", "educacao no campo",
        "educacao escolar indigena", "educacao quilombola",
    ],
    "Tecnologia na Educação": [
        "conectividade*", "inclusao digital", "internet nas escolas", "internet",
        "tecnologia educacional", "laboratorio de informatica", "computador*",
        "tablet*", "chromebook", "plataforma de ensino", "ensino hibrido",
        "letramento digital", "transformacao digital", "banda larga",
        "celular na escola", "proibicao de celular",
    ],
    "Educação, Arte e Cultura": [
        "arte e cultura", "cultura na escola", "cultura nas escolas",
        "educacao e cultura", "linguagens artisticas", "educacao artistica",
        "ensino de arte", "oficinas culturais na escola",
        "arte na escola", "artes na escola", "formacao artistica",
    ],
    "Recomposição das Aprendizagens": [
        "recomposicao", "recomposicao da aprendizagem", "recomposicao de aprendizagem",
        "recomposicao das aprendizagens", "recuperacao da aprendizagem",
        "recuperacao das aprendizagens", "reforco escolar", "aceleracao da aprendizagem",
        "defasagem idade serie", "distorcao idade serie", "lacunas de aprendizagem",
    ],
    "Ensino Superior": [
        "ensino superior", "universidade*", "universitari*", "faculdade*",
        "vestibular", "enem", "prouni", "fies", "assistencia estudantil",
        "graduacao", "pos graduacao", "campus", "bolsa de estudo", "mestrado",
        "doutorado", "expansao de vagas", "ead",
    ],
    "Financiamento da Educação": [
        "fundeb", "custo aluno", "caq", "orcamento da educacao",
        "financiamento da educacao", "vinculacao de recursos", "por cento do pib",
        "% do pib", "do pib para educacao", "salario educacao",
        "aplicacao minima em educacao", "recursos para a educacao",
        "plano nacional de educacao", "pne", "despesas com educacao",
        "investimento em educacao", "investimento na educacao",
        "repasse para as escolas", "recursos para as escolas", "pdde",
        "minimo constitucional", "da receita para a educacao",
    ],

    # -------------------------------------------------------------- Saúde
    "Atenção Primária": [
        "atencao primaria", "atencao basica", "saude da familia", "ubs*",
        "unidade basica", "agente comunitario", "agente de saude", "esf",
        "medicina de familia", "medico de familia", "mais medicos",
        "equipe multiprofissional", "equipes multiprofissionais", "saude bucal",
        "brasil sorridente", "visita domiciliar", "atencao domiciliar",
        "previne brasil", "nasf", "porta de entrada do sistema",
        "cobertura vacinal", "vacinacao", "farmacia popular",
        "assistencia farmaceutica",
    ],
    "Média e Alta Complexidade": [
        "media e alta complexidade", "alta complexidade", "media complexidade",
        "atencao especializada", "leito*", "uti", "terapia intensiva", "cirurgi*",
        "fila de cirurgia", "fila unica", "mutirao*", "consulta especializada",
        "centro de especialidades", "policlinica*", "ambulatori*",
        "exames de imagem", "exame laboratorial", "diagnostico por imagem",
        "tomografia", "ressonancia", "radioterapia", "quimioterap*", "oncolog*",
        "transplante*", "hemodialise", "santa casa", "hospital*",
        "referencia e contrarreferencia", "central de regulacao",
    ],
    "Urgência e Emergência": [
        "urgencia e emergencia", "rede de urgencia", "atendimento de urgencia",
        "pronto socorro", "pronto atendimento", "samu", "upa*", "ambulancia*",
        "transporte sanitario", "sala de estabilizacao", "classificacao de risco",
        "emergencia hospitalar", "socorro medico", "resgate aeromedico",
    ],
    "Saúde Mental": [
        "saude mental", "atencao psicossocial", "psicossocial", "caps", "raps",
        "psicolog*", "psiquiatr*", "sofrimento psiquico", "suicidio",
        "autoextermin*", "automutilacao", "setembro amarelo",
        "dependencia quimica", "alcool", "outras drogas", "drogas",
        "reducao de danos", "comunidade terapeutica", "ludopatia",
        "jogos de apostas", "apostas on line", "apostas online",
        "casas de aposta", "aposta esportiva", "bets", "jogo de azar",
        "jogos de azar", "imposto seletivo", "acolhimento psicologico",
    ],
    "Financiamento e Gestão do SUS": [
        "financiamento da saude", "orcamento da saude", "custeio da saude",
        "subfinanciamento", "piso da saude", "piso constitucional",
        "recursos para a saude", "investimento em saude", "fundo de saude",
        "repasse para a saude", "gestao do sus", "regionalizacao",
        "consorcio de saude", "pactuacao", "tabela sus", "judicializacao",
        "plano de saude", "planos de saude", "saude suplementar", "prontuario",
        "rnds", "dados em saude", "saude digital", "telessaude",
        "regulacao do acesso", "governanca do sus", "financiamento do sus",
        "recursos da saude", "gestao privada", "privatizacao da saude",
        "organizacao social", "planos privados", "rede credenciada",
        "emendas parlamentares", "prestadores de servico",
    ],

    # -------------------------------------------------- Segurança pública
    "Policiamento e Efetivo": [
        "policia*", "policial*", "efetivo policial", "aumento do efetivo",
        "policiamento", "policiamento comunitario", "seguranca ostensiva",
        "guarda municipal", "bombeiro*", "videomonitoramento", "camera corporal",
        "viatura*", "base comunitaria de seguranca", "patrulhamento", "ronda",
        "desmilitariza*", "forcas de seguranca", "concurso da policia",
        "salario dos policiais", "equipamento policial",
    ],
    "Enfrentamento ao Crime Organizado": [
        "faccao*", "crime organizado", "organizacao criminosa", "trafico*",
        "narcotrafico", "milicia*", "inteligencia policial", "lavagem de dinheiro",
        "asfixia financeira", "roubo de carga", "homicidio*", "latrocinio",
        "apreensao de armas", "controle de armas", "fronteira*",
    ],
    "Violência contra a Mulher": [
        "violencia domestica", "violencia contra a mulher",
        "violencia contra as mulheres", "violencia contra mulheres",
        "violencia de genero", "maria da penha", "feminicidio",
        "delegacia da mulher", "casa da mulher", "medida protetiva",
        "patrulha maria da penha", "botao do panico", "sala lilas", "agressor*",
        "rede de protecao a mulher", "importunacao sexual", "abuso sexual",
    ],
    "Sistema Prisional e Socioeducativo": [
        "presidio*", "penitenciaria*", "sistema prisional", "unidade prisional",
        "vaga prisional", "ressocializacao", "socioeducativo",
        "medida socioeducativa", "internacao de adolescente", "apac", "egresso*",
        "reincidencia", "trabalho do preso", "audiencia de custodia",
        "monitoracao eletronica", "tornozeleira",
    ],

    # ------------------------------------------------- Economia e emprego
    "Geração de Emprego": [
        "geracao de emprego", "gerar emprego", "criacao de emprego",
        "posto de trabalho", "vagas de trabalho", "vagas de emprego",
        "emprego e renda", "geracao de renda", "empregabilidade",
        "qualificacao profissional", "curso de qualificacao", "requalificacao profissional",
        "capacitacao para o trabalho", "intermediacao de mao de obra", "sine",
        "frentes de trabalho", "trabalho decente", "informalidade",
        "carteira assinada", "primeiro emprego",
    ],
    "Ambiente de Negócios": [
        "ambiente de negocios", "desburocratiza*", "simplificacao",
        "abertura de empresa", "incentivo fiscal", "atracao de investimento",
        "atracao de empresas", "micro e pequena empresa", "pequenos negocios",
        "mei", "microempreendedor", "sebrae", "empreendedorismo",
        "iniciativa privada", "credito", "linha de credito", "garantia de credito",
        "banco de fomento", "comercio popular", "distrito industrial",
        "licenciamento",
    ],
    "Agropecuária": [
        "agronegocio", "agropecuaria", "agricultura familiar", "produtor rural",
        "pecuaria", "credito rural", "plano safra", "ater",
        "assistencia tecnica rural", "extensao rural", "irrigacao", "cooperativa*",
        "armazenagem", "silo", "defesa agropecuaria", "sanidade animal",
        "embrapa", "reforma agraria", "assentamento*", "agroecologia",
        "organico*", "pesca", "aquicultura",
    ],
    "Ciência, Tecnologia e Inovação": [
        "pesquisa cientifica", "pesquisa e desenvolvimento",
        "fundacao de amparo a pesquisa", "fapes", "cnpq", "capes",
        "bolsa de pesquisa", "parque tecnologico", "startup*", "incubadora",
        "hub de inovacao", "ecossistema de inovacao", "inovacao", "pos graduacao",
        "propriedade intelectual", "transferencia de tecnologia",
    ],

    # --------------------------------------------- Meio ambiente e clima
    "Desmatamento e Conservação": [
        "desmatamento", "reflorestamento", "restauracao florestal",
        "unidade de conservacao", "area protegida", "parque estadual",
        "parque nacional", "bioma*", "cerrado", "caatinga", "amazonia",
        "mata atlantica", "pantanal", "biodiversidade", "queimada*",
        "manejo do fogo", "brigadista", "fiscalizacao ambiental",
        "licenciamento ambiental", "crime ambiental", "garimpo",
        "codigo florestal",
    ],
    "Saneamento e Recursos Hídricos": [
        "saneamento", "esgoto", "esgotamento sanitario", "agua tratada",
        "abastecimento de agua", "residuos solidos", "coleta de lixo",
        "aterro sanitario", "reciclagem", "catador*", "bacia hidrografica",
        "seguranca hidrica", "cisterna*", "adutora", "barragem",
        "poco artesiano", "dessalinizacao", "drenagem",
        "universalizacao do saneamento",
    ],
    "Transição Energética": [
        "energia renovavel", "energia solar", "energia limpa", "eolica",
        "painel solar", "parque eolico", "transicao energetica",
        "matriz energetica", "hidrogenio verde", "biocombustivel*", "etanol",
        "biodiesel", "biometano", "biogas", "credito de carbono",
        "mercado de carbono", "descarbonizacao", "tarifa de energia",
    ],
    "Defesa Civil e Desastres": [
        "defesa civil", "desastre*", "enchente*", "inundacao", "alagamento",
        "deslizamento", "seca", "estiagem", "area de risco",
        "contencao de encosta", "sistema de alerta", "alerta precoce",
        "plano de contingencia", "sirene", "adaptacao climatica",
        "evento climatico extremo", "calor extremo", "realocacao de familias",
    ],

    # ------------------------------------- Assistência social e pobreza
    "Transferência de Renda": [
        "transferencia de renda", "bolsa familia", "auxilio", "beneficio estadual",
        "renda minima", "programa de renda", "complemento de renda", "cadunico",
        "cadastro unico", "bpc", "vale gas", "cartao alimentacao", "bets",
        "apostas", "jogos de aposta", "apostas eletronicas",
    ],
    "Segurança Alimentar": [
        "seguranca alimentar", "inseguranca alimentar", "fome",
        "restaurante popular", "banco de alimentos", "cesta basica",
        "cesta do povo", "preco dos alimentos",
        "cozinha comunitaria", "cozinha solidaria", "alimentacao escolar",
        "pnae", "paa", "agricultura urbana", "horta comunitaria",
        "desperdicio de alimentos", "mapa da fome",
    ],
    "Habitação": [
        "habitacao", "habitacional", "moradia", "minha casa", "casa propria",
        "unidade habitacional", "deficit habitacional", "regularizacao fundiaria", "aluguel social", "favela*", "urbanizacao de favelas",
        "lote urbanizado", "reforma de moradia", "ocupacao irregular",
    ],

    # ---------------------------------------- Infraestrutura e mobilidade
    "Transporte e Rodovias": [
        "rodovia*", "estrada*", "pavimentacao", "asfalto", "ponte*",
        "duplicacao", "malha rodoviaria", "concessao rodoviaria", "pedagio",
        "ferrovia*", "ferroviari*", "hidrovia*", "porto*", "aeroporto*",
        "logistica", "trem", "trens", "armazem", "armazens",
    ],
    "Mobilidade Urbana": [
        "mobilidade urbana", "transporte publico", "transporte coletivo",
        "onibus", "metro", "brt", "vlt", "ciclovia*", "bicicleta",
        "corredor exclusivo", "bilhete unico", "passe livre", "tarifa",
        "tarifa zero", "gratuidade no transporte", "terminal de onibus",
        "frota de onibus", "mobilidade ativa", "calcada*",
    ],

    # ------------------------------------- Gestão pública e transparência
    "Eficiência e Gasto Público": [
        "reforma administrativa", "corte de gasto", "teto de gasto",
        "equilibrio fiscal", "gasto publico", "eficiencia do gasto",
        "revisao de despesas", "despesa*", "custeio", "folha de pagamento",
        "arrecadacao", "receita corrente", "capacidade de investimento",
        "endividamento", "recuperacao fiscal", "cargo comissionado",
        "auditoria", "auditar", "licitacao", "compras publicas", "orcamento",
        "recursos publicos", "emendas parlamentares", "emenda parlamentar",
        "emendas impositivas", "emendas individuais",
    ],
    "Transparência e Combate à Corrupção": [
        "transparencia", "portal da transparencia", "dados abertos", "corrupcao",
        "controle interno", "controladoria", "ouvidoria",
        "lei de acesso a informacao", "lai", "integridade", "compliance",
        "prestacao de contas", "controle social", "tribunal de contas",
        "improbidade", "emendas parlamentares", "emenda parlamentar",
        "orcamento secreto", "emendas de relator", "emendas de comissao",
        "rastreabilidade das emendas",
    ],
    "Regulação de Plataformas Digitais": [
        "redes sociais", "rede social", "plataformas digitais", "plataforma digital",
        "desinformacao", "fake news", "noticias falsas", "soberania digital",
        "moderacao de conteudo", "big tech", "big techs", "marco civil da internet",
        "inteligencia artificial", "liberdade de expressao nas redes", "discurso de odio",
        "algoritmos", "crimes virtuais", "seguranca digital", "dados pessoais", "lgpd",
    ],
    "Governo Digital": [
        "governo digital", "governo eletronico", "digitalizacao*",
        "servico digital", "servicos online", "atendimento ao cidadao",
        "aplicativo*", "plataforma digital", "gov br", "assinatura digital",
        "interoperabilidade", "balcao unico", "autoatendimento",
    ],
    "Servidores e Municípios": [
        "servidor publico", "servidores", "concurso publico", "plano de carreira",
        "reajuste", "negociacao coletiva", "valorizacao do servidor",
        "capacitacao de servidores", "consorcio", "consorcio intermunicipal",
        "repasse aos municipios", "apoio aos municipios",
        "cooperacao federativa", "regiao metropolitana",
    ],

    # ----------------------------------------- Direitos humanos e igualdade
    "Igualdade Racial": [
        "igualdade racial", "promocao da igualdade racial", "populacao negra",
        "racismo", "racismo estrutural", "racial", "afrodescendente",
        "negros e negras", "pretos e pardos", "juventude negra",
        "mulheres negras", "saude da populacao negra", "povo preto", "povo negro",
        "genocidio do povo", "cotas raciais",
        "politica de cotas", "cotas no servico publico", "acao afirmativa", "intolerancia religiosa",
    ],
    "Mulheres": [
        "mulher*", "equidade de genero", "igualdade de genero",
        "autonomia das mulheres", "autonomia economica das mulheres",
        "empreendedorismo feminino", "mulheres no mercado de trabalho",
        "politica de cuidado", "politica nacional de cuidados",
        "economia do cuidado", "saude da mulher", "direitos reprodutivos",
        "licenca maternidade", "igualdade salarial",
    ],
    "Pessoa com Deficiência": [
        "pessoa com deficiencia", "pessoas com deficiencia", "pcd",
        "deficiencia", "acessibilidade", "tecnologia assistiva", "ortese*",
        "protese*", "reabilitacao", "capacitismo", "inclusao no mercado",
        "autis*", "tea",
    ],
    "Juventude e Pessoa Idosa": [
        "juventude", "jovens", "protagonismo juvenil", "primeiro emprego",
        "pessoa idosa", "pessoas idosas", "idoso*", "terceira idade",
        "envelhecimento", "longevidade", "centro de convivencia", "centro dia",
        "ilpi", "cuidador de idosos", "aposentad*",
    ],
    "Povos Indígenas e Quilombolas": [
        "indigena*", "quilombola*", "quilombo*", "terra indigena", "demarcacao",
        "aldeia*", "povos tradicionais", "comunidade tradicional",
        "povos e comunidades tradicionais", "ribeirinho*", "extrativista*",
        "titulacao de territorio", "remanescente de quilombo",
    ],
    "População LGBTQIA+": [
        "lgbt*", "lgbtfobia", "homofobia", "transfobia", "nome social",
        "diversidade sexual", "orientacao sexual", "identidade de genero",
        "transexual*", "travesti*", "populacao trans",
        "centro de cidadania lgbt",
    ],

    # ------------------------------------------- Cultura, esporte e turismo
    "Cultura": [
        "cultura", "cultural", "patrimonio", "patrimonio historico",
        "patrimonio cultural",
        "patrimonio imaterial",
        "economia criativa", "edital de cultura", "fomento cultural",
        "equipamento cultural", "lei aldir blanc", "lei rouanet",
        "ponto de cultura", "biblioteca*", "museu*", "teatro*", "artista*",
        "audiovisual", "carnaval", "festival*", "sistema de cultura",
    ],
    "Esporte e Lazer": [
        "esporte", "esportiv*", "lazer", "atleta*", "bolsa atleta",
        "esporte escolar", "jogos escolares", "quadra poliesportiva",
        "equipamento esportivo", "ginasio", "campo de futebol",
        "praca esportiva", "academia ao ar livre", "paradesporto",
        "alto rendimento", "atividade fisica",
    ],
    "Turismo": [
        "turismo", "turistic*", "atrativo turistico", "roteiro turistico",
        "destino turistico", "ecoturismo", "turismo religioso", "hotelaria",
        "rede hoteleira", "gastronomia", "promocao do destino",
        "infraestrutura turistica",
    ],
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
# Era 2.5-flash, calibrado aqui: prompt, régua e guardas foram medidos contra o
# que ele devolve. Em 14/08/2026 a troca deixou de ser escolha. Depois que o
# projeto GCP antigo foi suspenso e apagado, a chave nova nasceu em projeto novo
# e o 2.5-flash passou a responder 404 "no longer available to new users" nas 17
# análises da fila. Não dá mais nem para comparar os dois modelos no mesmo plano,
# porque o antigo não atende esta chave.
GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")


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


# Quantas palavras seguidas precisam se repetir para a segunda cópia sair.
# Mínimo 4 porque abaixo disso a coincidência é comum ("de acordo com a"), e
# teto 12 porque o que se repete aqui é linha, não parágrafo.
_DUP_MIN, _DUP_MAX = 4, 12


def desduplicar_linhas(texto: str) -> str:
    """Tira a cópia quando o PDF escreve a mesma linha duas vezes seguidas.

    Existe por causa do plano da Samara (UP, presidente), o único da base de
    17/08/2026 em que isso aparece: 42,8% do texto sai em duplicata, contra 2,4%
    no segundo pior. A página 22 entrega "efetivar programa de erradicacao
    efetivar programa de erradicacao do analfabetismo no pais envolvendo do
    analfabetismo no pais envolvendo".

    O estrago não é o modelo perder conteúdo, que ele lê repetido e entende: é
    que a frase citada não existe corrida em lugar nenhum do arquivo, então
    `verificar_trecho` não acha e carimba "junta partes do plano". Ela ficou com
    40 dos 43 trechos assim, contra 6% na base inteira, e o selo diz ao leitor
    que a citação foi costurada quando ela é transcrição fiel.

    Só remove repetição COLADA da mesma sequência: não aproxima trechos
    distantes, então citação de fato costurada continua reprovando e a
    conferência não afrouxa.

    Medido nos 201 planos antes de entrar: muda 27 linhas, todas dela, todas de
    "junta partes" para "literal", e nenhuma piora. Nos outros 25 planos em que
    a regra dispara, o que ela come é enfeite de diagramação (pontilhado de
    sumário, cabeçalho repetido na mesma página, caixa de seleção), e isso o
    modelo não deveria estar lendo.
    """
    palavras = str(texto or "").split()
    saida, i = [], 0
    while i < len(palavras):
        repetido = 0
        for n in range(_DUP_MAX, _DUP_MIN - 1, -1):
            if (i + 2 * n <= len(palavras)
                    and palavras[i:i + n] == palavras[i + n:i + 2 * n]):
                repetido = n
                break
        if repetido:
            saida.extend(palavras[i:i + repetido])
            i += 2 * repetido
        else:
            saida.append(palavras[i])
            i += 1
    return " ".join(saida)


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
        # Depois do OCR: a duplicata pode vir da camada de texto ou da leitura,
        # e a decisão de OCRar olha o texto como o PDF entrega.
        partes.append(desduplicar_linhas(raw))
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


# Como o pedido sai para a rede.
#
# O 403 que derrubou o dia 19/08/2026 não era instabilidade do TSE nem bloqueio
# por IP: o domínio inteiro (divulgacandcontas, cdn.tse, www.tse) respondia
# "Access Denied" para requests e para curl, do runner do Actions e de IP
# residencial brasileiro, enquanto o navegador abria normalmente. O que separa
# um do outro é a impressão digital de TLS: o WAF olha a ordem das extensões do
# handshake, e a da urllib3 não é a de nenhum navegador. Nenhum cabeçalho
# resolve, porque a recusa acontece antes do HTTP.
#
# curl_cffi refaz o handshake com o perfil do Chrome e o mesmo link volta 200
# com o PDF. Fica opcional: sem o pacote instalado, cai no requests de antes,
# que funciona sempre que o WAF estiver frouxo.
_IMPERSONAR = os.getenv("TSE_IMPERSONAR", "chrome124")


def _pegador():
    """Devolve o `get` a usar: o do curl_cffi, se houver, senão o do requests."""
    if os.getenv("TSE_SEM_IMPERSONAR", "").strip():
        import requests
        return requests.get
    try:
        from curl_cffi import requests as _cr
    except Exception:
        import requests
        return requests.get

    def _get(url, headers=None, timeout=None, allow_redirects=True, **kw):
        return _cr.get(url, headers=headers, timeout=timeout,
                       allow_redirects=allow_redirects,
                       impersonate=_IMPERSONAR, **kw)

    return _get


# Fonte alternativa do PDF, ligada por outros.espelhar_planos.registrar_espelho.
# Recebe o link do TSE e devolve os bytes da cópia guardada no Drive, ou None
# quando aquele plano ainda não foi espelhado. Fica como variável de módulo, e
# não como parâmetro, porque baixar_plano é chamado lá do fundo de
# extrair_paginas_url e passar a fonte por toda a cadeia mudaria seis
# assinaturas para um caso só.
FONTE_ESPELHO = None


def baixar_plano(url: str) -> bytes:
    """Baixa o arquivo do plano, repetindo em falha temporária.

    Se houver espelho no Drive para este link, ele vem primeiro: o DivulgaCand
    responde 403 de WAF por horas seguidas e a releitura de 206 planos não pode
    depender disso. O TSE continua sendo a fonte de quem ainda não foi copiado.

    404/410 é definitivo (o plano não está lá) e sai na hora. Timeout, erro de
    conexão e 5xx são o DivulgaCand fora do ar: repete e, se não voltar,
    levanta PlanoIndisponivel para quem chama saber que é para tentar depois.
    """
    from pathlib import Path
    import re
    import requests

    if FONTE_ESPELHO is not None:
        try:
            do_espelho = FONTE_ESPELHO(url)
        except Exception:
            do_espelho = None
        if do_espelho and do_espelho[:1024].lstrip().startswith(b"%PDF"):
            return do_espelho

    if url.startswith("file://"):
        p = Path(url[7:])
        if p.exists():
            return p.read_bytes()

    m = re.search(r"path=[^&]*/([a-f0-9]{32,64})", url)
    if m:
        candidato_local = Path("dados_tse/planos_pje") / f"{m.group(1)}.pdf"
        if candidato_local.exists():
            return candidato_local.read_bytes()

    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/128.0.0.0 Safari/537.36",
        "Accept": "application/pdf,application/octet-stream,text/html,*/*",
        "Accept-Language": "pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7",
        "Referer": "https://divulgacandcontas.tse.jus.br/divulga/",
        "Origin": "https://divulgacandcontas.tse.jus.br",
    }
    pegar = _pegador()
    ultimo = None
    for tentativa in range(1, BAIXAR_TENTATIVAS + 1):
        try:
            r = pegar(url, headers=headers,
                      timeout=BAIXAR_TIMEOUT, allow_redirects=True)
            # 4xx = requisição errada. 403, 408 e 429 são limites/bloqueios transitórios do TSE
            # que costumam responder nas tentativas seguintes com backoff.
            if 400 <= r.status_code < 500 and r.status_code not in (403, 408, 429):
                raise PlanoIlegivel(
                    f"o TSE não entrega esse arquivo (HTTP {r.status_code})")
            if r.status_code == 403:
                ultimo = f"HTTP 403 (tentativa {tentativa})"
                time.sleep(3 * tentativa)
                continue
            r.raise_for_status()
            if r.content:
                # WAF/bloqueio por IP fora do Brasil (ex: runner do GitHub Actions)
                if r.content[:1024].lstrip()[:1] in (b"<", b"{") and m:
                    candidato_local = Path("dados_tse/planos_pje") / f"{m.group(1)}.pdf"
                    if candidato_local.exists():
                        return candidato_local.read_bytes()
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
# Número por extenso é alvo do mesmo jeito: "implantar quatro Centros
# Hospitalares de Referência" (Joel Rodrigues, PI) estava fora da régua só por
# não ter dígito. "Um" e "uma" ficam de fora porque são artigo, não contagem.
_META_EXTENSO = (r"\b(dois|duas|tres|quatro|cinco|seis|sete|oito|nove|dez|onze|"
                 r"doze|treze|quatorze|catorze|quinze|vinte|trinta|quarenta|"
                 r"cinquenta|sessenta|setenta|oitenta|noventa|cem|mil)\b")
# Ano é alvo quando vem depois de preposição temporal ("até 2030"). Sozinho
# costuma ser nome de plano: "RN Água Segura 2035" e "Plano Estadual de
# Infraestrutura 2050" (Álvaro Dias, RN) entraram como meta só por causa disso.
_META_ANO_ALVO = r"\b(ate|a partir de|para|em|no prazo de)\s+(19|20)\d{2}\b"
# Dígito que nunca é alvo: numeração do item, número de página que veio junto na
# citação e ano solto. No plano do Marcelo Maranata (RS) as propostas são uma
# lista numerada, e "11. Estabelecer metas de inclusão financeira" virou "Define
# meta" pelo 11. No do Professor Marcus Sodré (SC) o dígito é o "(pg 40)" que a
# própria citação carrega.
_META_DECORATIVO = [
    r"^\s*\d+(\.\d+)*\s*[\.\)\-]\s*",
    r"\[\.\.\.\]\s*\d+(\.\d+)*\s*[\.\)]\s*",
    r"\(\s*p[ag]{1,2}\.?\s*\d+\s*\)",
    r"\bp[ag]{1,2}\.?\s*\d+\b",
    r"\b(pilar|eixo|capitulo|anexo|item)\s*\d+\b",
    r"\b(19|20)\d{2}\b",
]


# Absoluto que é nome de programa ou figura de linguagem, não alvo. "Tolerância
# zero ao feminicídio" (Mailza Assis, PP/AC) e "ensino 100% público" (Edmilson
# Costa, PCB/BR) davam Define meta sem que houvesse número nenhum na frase: o
# primeiro é retórica, o segundo é modelo de propriedade, não cobertura.
_ABSOLUTO_DECORATIVO = [
    r"\btoleranci\w*\s+zero\b",
    r"\b(juros?|lixo|papel|burocracia|fome)\s+zero\b",
    r"\bzero\s+(papel|burocracia)\b",
    r"\b100\s*%\s*(estatal|publica?|privada?|nacional|brasileir\w+|gratuit\w+)\b",
    r"\bmarco\s+zero\b",
]


def _sem_absoluto_decorativo(n: str) -> str:
    for padrao in _ABSOLUTO_DECORATIVO:
        n = re.sub(padrao, " ", n)
    return n


# Quantificador vago no lugar exato onde deveria estar o número. Prazo sozinho
# continua valendo como alvo: "cadastro no ar até o fim do primeiro ano"
# (Veterinário Wilson Grassi, DEMOCRATA/BR) é entregável com data e dá para
# conferir depois. O que não vale é "reduzir a violência de forma significativa
# já no primeiro ano" (Eduardo Paes, PSD/RJ), que não diz quanto, nem "meta de
# inverter a proporção ao longo do mandato" (Economista Renato Gomes, DC/MS),
# que nomeia o indicador sem fixar valor. Medido em 25/08/2026: 7 linhas.
_QUANTUM_VAGO = re.compile(
    r"\bde forma significativa\b|\bsignificativamente\b|\bdrasticamente\b|"
    r"\bconsideravelmente\b|\bsubstancialmente\b|\bexpressivamente\b|"
    r"\bos principais\b|\bas principais\b|"
    r"\bmetas? de (inverter|ampliar|reduzir|aumentar|elevar|melhorar|inclusao)\b|"
    r"\b(percentual|indice|numero|proporcao) minim\w+ de (?!\d)\w")


_META_EXTERNA_ALINHAMENTO = (
    r"\b(pne|plano nacional de educacao|agenda 2030|ods|objetivos de desenvolvimento sustentavel|"
    r"metas? do milenio|metas? nacionais)\b"
)


def _e_meta_externa_ou_terceiro(n: str) -> bool:
    """Detecta citações onde números/prazos pertencem a leis federais ou pactos
    externos (como PNE, ODS, metas nacionais do SUS/Fundeb) e o candidato apenas
    manifesta intenção de 'alinhar-se', 'cumprir' ou 'superar onde possível',
    sem fixar meta própria com verbos de ação executiva do mandato.
    """
    if not re.search(_META_EXTERNA_ALINHAMENTO, n):
        return False
    if re.search(r"\b(alinhar|alinhamento|alinhar-se|cumprir as metas|atingir as metas|aderir ao|metas aprovadas)\b", n):
        if not re.search(r"\b(criaremos|construiremos|implantaremos|contrataremos|destinaremos|investiremos|garantiremos)\b", n):
            return True
    return False


# Horizonte maior que um mandato. A eleição de 2026 dá um mandato de 2027 a
# 2030, então "nos próximos 10 anos" não é compromisso de quem está pedindo o
# voto: é promessa para depois de qualquer mandato dele. Decisão da Eduarda em
# 21/08/2026, olhando o plano do Escritor Augusto Cury (AVANTE/BR), que promete
# dobrar cooperados e produção de alimentos "nos próximos 10 anos".
_ANOS_EXTENSO = {"cinco": 5, "seis": 6, "sete": 7, "oito": 8, "nove": 9,
                 "dez": 10, "onze": 11, "doze": 12, "quinze": 15, "vinte": 20,
                 "trinta": 30}
_MANDATO_ANOS = 4
_ANO_FIM_MANDATO = 2030
_RE_HORIZONTE = re.compile(
    r"\b(?:em|nos? proximos?|ao longo dos proximos?|dentro de|num prazo de|"
    r"no prazo de|ate)\s+(\d{1,2}|" + "|".join(_ANOS_EXTENSO) + r")\s+anos?\b")
_RE_ANO_ALVO_LONGE = re.compile(r"\b(?:ate|em|para)\s+(20[3-9]\d)\b")


def _prazo_alem_do_mandato(n: str) -> bool:
    """O único horizonte da frase passa do mandato que está em disputa."""
    for m in _RE_HORIZONTE.finditer(n):
        _v = m.group(1)
        _anos = _ANOS_EXTENSO.get(_v, None)
        if _anos is None:
            try:
                _anos = int(_v)
            except ValueError:
                continue
        if _anos > _MANDATO_ANOS:
            return True
    for m in _RE_ANO_ALVO_LONGE.finditer(n):
        if int(m.group(1)) > _ANO_FIM_MANDATO:
            return True
    return False


def tem_alvo_mensuravel(trecho: str) -> bool:
    """Se a citação traz alvo que dá para conferir depois: número, prazo,
    absoluto ou número por extenso. É o que separa 'Define meta' de 'Propõe
    ação' na régua.

    O dígito sozinho não basta. Medido em 09/08/2026: 24 dos 299 "Define meta"
    da base tinham como único número a numeração do item, o número da página ou
    o ano no nome de um plano, e nenhum alvo.
    """
    n = _norm_acentos(trecho)
    if _e_meta_externa_ou_terceiro(n):
        return False
    if _prazo_alem_do_mandato(n):
        return False
    # "perseguir o deficit proximo de zero" não é alvo: "proximo de" diz que o
    # número é aproximação, não compromisso. Sem isto, o zero da frase contava
    # como número e o trecho subia para Define meta.
    n = re.sub(r"\bproximo[s]?\s+(?:de|a|do|da)\s+\S+", " ", n)
    # O absoluto conta depois de tirar o que é nome de programa ou retórica.
    if (re.search(_META_ABSOLUTO, _sem_absoluto_decorativo(n))
            or re.search(_META_ANO_ALVO, n)):
        return True
    n_num = n
    for padrao in _META_DECORATIVO:
        n_num = re.sub(padrao, " ", n_num)
    if re.search(_META_EXTENSO, n_num) or re.search(_META_NUMERO, n_num):
        return True
    # Sobrou só o prazo. Vale, menos quando a frase põe um advérbio vago
    # exatamente onde deveria estar o número.
    return bool(re.search(_META_PRAZO, n) and not _QUANTUM_VAGO.search(n))


def _norm_acentos(t: str) -> str:
    t = unicodedata.normalize("NFD", str(t or "").lower())
    return "".join(c for c in t if unicodedata.category(c) != "Mn")


# Meta que é de lei federal, e não do candidato. _e_meta_externa_ou_terceiro só
# olha PNE e ODS; a universalização do saneamento tem prazo próprio no Marco
# Legal de 2020, e "tomar como referência as metas do Marco" é adesão, não meta
# fixada por quem pede o voto.
_META_DE_LEI = re.compile(
    r"\bmarco legal\b|\bmarco do saneamento\b|"
    r"\bmetas? de universalizacao (previstas?|fixadas?|do marco)\b|"
    r"\bavanco das metas de universalizacao\b")

# As duas famílias em que o alvo é o próprio verbo: universalizar vale 100% e
# zerar vale 0, sem depender do que vem depois. "100%", "toda a rede", "todas
# as escolas" e "todos os municípios" ficaram DE FORA de propósito: medido em
# 25/08/2026, nessas quatro o absoluto quase sempre descreve o alcance de um
# sistema ("prontuário que integra toda a rede") ou o modelo de propriedade
# ("ensino 100% público"), e não um alvo de cobertura. Promover por elas
# trocava um erro por outro.
_ABSOLUTO_FORTE = re.compile(
    r"\buniversaliza\w*|\bzerar\b|\berradica\w*|"
    r"\bdobrar\b|\btriplicar\b|\bquadruplicar\b")
# Verbo que compromete o mandato. Sem ele a frase é diagnóstico, título ou
# princípio, e promover viraria erro novo.
_VERBO_COMPROMISSO = re.compile(
    r"\b(vamos|iremos|ira|irao|"
    r"\w+aremos|\w+eremos|\w+iremos|\w+ara|\w+arao|\w+era|\w+erao|"
    r"universalizar|zerar|erradicar|dobrar|triplicar|garantir|assegurar|"
    r"implantar|implementar|ampliar|expandir|criar|construir|atingir|alcancar|"
    r"elevar|reduzir|concluir|entregar|instituir)\b")
# Descreve o que já foi feito. "Em 2026, a modalidade foi universalizada em 148
# municípios" (Elmano de Freitas, PT/CE) estava como Define meta e é balanço.
_PASSADO = re.compile(
    r"\b(foi|foram|passou|passaram|ja (foi|foram|temos|alcancamos)|"
    r"em 20(1\d|2[0-6]),)\b")
# Frase que enuncia problema, não compromisso.
_DIAGNOSTICO = re.compile(
    r"\b(principais desafios|o desafio e|diagnostico|cenario atual|"
    r"situacao atual|hoje o estado|atualmente)\b")


def tem_alvo_absoluto(trecho: str) -> bool:
    """Alvo absoluto verificável, dito como compromisso do mandato.

    É a régua que falta para o caminho de subida. tem_alvo_mensuravel só era
    usada para rebaixar "Define meta" sem alvo, nunca para subir "Propõe ação"
    que tem alvo, e a inconsistência do modelo sobrevivia inteira: em
    25/08/2026, "Universalizar, em todas as escolas estaduais, o ensino de
    tempo integral" (Jerônimo Rodrigues, PT/BA) era Define meta e "Universalizar
    a oferta do Ensino Médio em tempo integral em Pernambuco" (João Campos,
    PSB/PE) era Propõe ação. Eram 62 linhas assim na base.

    Mais estreita que tem_alvo_mensuravel de propósito: subir de nível é
    afirmar coisa mais forte sobre o plano do candidato, então só entra o que a
    frase sustenta sozinha.
    """
    n = _norm_acentos(trecho)
    if _e_meta_externa_ou_terceiro(n) or _prazo_alem_do_mandato(n):
        return False
    if _META_DE_LEI.search(n):
        return False
    if _PASSADO.search(n) or _DIAGNOSTICO.search(n):
        return False
    if not _ABSOLUTO_FORTE.search(_sem_absoluto_decorativo(n)):
        return False
    return bool(_VERBO_COMPROMISSO.search(n))


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


# Quanto do plano vai junto do trecho. É a janela pedida, não o que sai: os dois
# cortes em fronteira de frase, um de cada lado, consomem até 400 caracteres
# cada. Medido em 480 classificações de 12 planos, uma janela de 1.400 entregava
# 795 caracteres na média. Com 2.000 o entorno fica em torno de 1.200 a 1.400,
# que é o parágrafo ou o bloco de "ações propostas" inteiro, o formato em que a
# maioria dos planos trata o tema.
CHARS_CONTEXTO = 2000


def contexto_do_trecho(paginas: list[str], paginas_norm: list[str],
                       trecho: str, chars: int = CHARS_CONTEXTO) -> str:
    """O texto do plano em volta da citação, para ler sem abrir o PDF.

    Existe por reclamação do Felipe em 19/08/2026, repassada pela Lemann: o
    painel mostra a frase que sustenta a classificação e, para entender o que
    o plano diz de verdade sobre o tema, é preciso baixar o PDF. A frase é curta
    porque o prompt pede citação, não resumo; o problema não é o tamanho da
    caixa na tela, é que a frase sozinha não conta a proposta.

    Devolve um pedaço contínuo da MESMA página onde a citação está, com a
    citação dentro. Não tenta remontar citação que junta partes distantes: nesse
    caso pega o entorno da primeira parte, que é onde a leitura começa. Vazio
    quando a citação não foi localizada no PDF, e aí não há entorno a mostrar.
    """
    pedacos = _pedacos_de_citacao(trecho)
    longos = [p for p in pedacos if len(p.split()) >= 4]
    if not longos:
        return ""
    alvo = _sem_espaco(_norm_busca(longos[0]))
    if not alvo:
        return ""

    for bruta, norm in zip(paginas, paginas_norm):
        if not bruta or not norm:
            continue
        # A busca acontece no texto sem espaço (como em verificar_trecho), e a
        # posição precisa voltar para o texto original. O mapa liga cada
        # caractere sem espaço ao índice de onde ele veio.
        mapa, compacto = [], []
        for i, c in enumerate(_norm_busca(bruta)):
            if not c.isspace():
                compacto.append(c)
                mapa.append(i)
        pos = "".join(compacto).find(alvo)
        if pos < 0:
            continue
        ini_orig = mapa[pos]
        fim_orig = mapa[min(pos + len(alvo) - 1, len(mapa) - 1)] + 1
        # A normalização de _norm_busca preserva o comprimento (tira acento,
        # baixa a caixa), então o índice vale no texto original da página.
        sobra = max(0, chars - (fim_orig - ini_orig))
        ini = max(0, ini_orig - sobra // 2)
        fim = min(len(bruta), fim_orig + sobra // 2)
        # Fecha em fronteira de frase, para o pedaço não começar no meio de uma
        # palavra. Se não houver ponto por perto, corta onde estava mesmo.
        corte = bruta.rfind(". ", ini, ini_orig)
        if corte != -1 and ini_orig - corte < 400:
            ini = corte + 2
        # No fim, fecha na ÚLTIMA frase que cabe na janela, não na primeira
        # depois da citação: fechar na primeira devolvia o entorno cortado logo
        # ali, e o pedido era justamente ver mais do plano.
        corte = bruta.rfind(". ", fim_orig, fim)
        if corte != -1 and fim - corte < 400:
            fim = corte + 1
        else:
            corte = bruta.find(". ", fim, fim + 300)
            if corte != -1:
                fim = corte + 1
        pedaco = " ".join(bruta[ini:fim].split())
        # O PDF quebra palavra no fim da linha e a extração vira "produ- tores".
        # Sem juntar, o entorno chega à tela com a palavra partida.
        pedaco = re.sub(r"(\w)- (\w)", r"\1\2", pedaco)
        if not pedaco:
            return ""
        return (("… " if ini > 0 else "") + pedaco
                + (" …" if fim < len(bruta) else ""))
    return ""


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
        "ATENÇÃO 7: metas de leis federais, planos nacionais ou pactos de terceiros "
        "(ex.: Plano Nacional de Educação / PNE, metas do Milênio/ODS da ONU, diretrizes do SUS/Fundeb) "
        "NÃO são metas do candidato. 'Cumprir as metas do PNE', 'alinhar-se às metas do PNE "
        "(como 90% dos estudantes no Ensino Médio)' ou 'atingir os ODS' são Propõe ação "
        "(ou Menciona vagamente), e NÃO Define meta. Só classifique como Define meta se o candidato "
        "fixar um alvo numérico, percentual ou prazo próprio e exclusivo para a sua gestão/mandato estadual.\n\n"
        "ATENÇÃO 2: classifique cada tema pelo que o plano diz DAQUELE tema. Um plano "
        "forte em segurança não puxa para cima os temas de educação.\n\n"
        # Medido em 09/08/2026, na base de 65 planos: 11 citações apareciam em
        # dois temas com níveis diferentes. No plano do Fábio Trad (MS),
        # "Universalizar escolas conectadas e tecnologicamente equipadas com
        # internet" saiu "Define meta" em Tecnologia na Educação e "Propõe ação"
        # em Fundamental. O texto é o mesmo, então o degrau não pode mudar: o que
        # muda de tema para tema é se aquela frase trata do tema, não o quanto
        # ela promete.
        "ATENÇÃO 5: se a MESMA citação sustentar mais de um tema, o nível dela tem "
        "que ser o mesmo nos dois. O que pode variar entre temas é se a frase "
        "trata daquele tema; o degrau vem do texto, e o texto é um só.\n\n"
        # O plano do Alexandre Kalil (MG) é numerado por seção, e três títulos
        # entraram como proposta: "6.8. Mulheres: autonomia, proteção e
        # oportunidades" e "6.12. Povos indígenas, comunidades quilombolas e
        # tradicionais" viraram "Propõe ação" sem nada abaixo deles.
        "ATENÇÃO 6: título de capítulo ou de seção não é proposta. '6.8. Mulheres: "
        "autonomia, proteção e oportunidades' anuncia o assunto do trecho, não diz "
        "o que será feito. Cite o que vem DEPOIS do título; se depois dele não "
        "houver proposta, o tema é Menciona vagamente.\n\n"
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
        "  - 'Ampliação da acessibilidade', 'Internet de alta velocidade', 'Implantação de programas "
        "de alfabetização' e 'Criação de programas de incentivo' são Menciona vagamente: "
        "nomeiam o desejo genérico, não o meio/instrumento executivo.\n"
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
    """O termo-âncora como padrão que aceita plural e para na borda da palavra.

    Sem plural o acerto dependeria de eu ter escrito o termo na mesma flexão do
    plano: "pessoa com deficiência" não achava as "pessoas com deficiência" do
    plano do Zema. O sufixo cobre "s" e "es", porque só o "s" deixava de fora
    "mulheres" e "professores", que são a forma que os planos usam.

    A borda no fim faltava e o estrago era grande: sem ela "fome" casava dentro
    de "fomento ao empreendedorismo" e "uti" dentro de "utilizadas". Medindo em
    15 planos, "uti" aparecia 248 vezes e só 5 eram unidade de terapia intensiva.
    Isso mandava a guarda de ausência reperguntar tema que o plano nem trata.

    Termo terminado em "*" é radical de propósito e continua casando o resto da
    palavra: "alfabetiza*" pega alfabetização e alfabetizados, "turistic*" pega
    turístico e turística. Sem a marca, o termo é palavra inteira.
    """
    prefixo = termo.endswith("*")
    palavras = _norm_busca(termo.rstrip("*")).split()
    partes = []
    for i, p in enumerate(palavras):
        ultima = i == len(palavras) - 1
        if ultima and prefixo:
            partes.append(re.escape(p) + r"\w*")
        else:
            partes.append(re.escape(p) + ("(?:e?s)?" if len(p) >= 4 else ""))
    fim = "" if prefixo else r"\b"
    return re.compile(r"\b" + r"\s+".join(partes) + fim)


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


_AUSENCIA_RE = {tema: [(t, _regex_ancora(t)) for t in termos]
                for tema, termos in TERMOS_AUSENCIA.items()}

# Selos do cruzamento entre a contagem de termos e o nível que o modelo deu.
# São quatro casos e cada um pede uma leitura diferente de quem usa o painel.
SELO_AUSENCIA_OK = "Ausência confirmada"   # nenhum termo e o modelo não achou
SELO_AUSENCIA_REVER = "Revisar ausência"   # termo no plano e o modelo não achou
SELO_CONFIRMADO = "Confirmado"             # termo no plano e o modelo achou
SELO_SO_CONTEXTO = "Achado por contexto"   # sem termo e o modelo achou mesmo assim


def termos_do_tema(texto_norm: str, tema: str) -> list[tuple[str, int]]:
    """Quais termos do vocabulário do tema aparecem no plano, e quantas vezes.

    Diferente de ocorrencias_ancora, que devolve posições e existe para a guarda
    de ausência decidir se vale gastar uma chamada. Aqui o resultado é evidência
    para gravar ao lado do nível: é o que permite escrever "o plano não trata
    disso" mostrando o que foi procurado.

    As posições são deduplicadas entre termos: "psicossocial" e "atenção
    psicossocial" casam no mesmo pedaço de texto e contariam duas vezes a mesma
    menção. A contagem por termo continua sendo a do próprio termo, o que muda é
    o total, que é quem responde "tem ou não tem".
    """
    achados = []
    for termo, padrao in _AUSENCIA_RE.get(tema, []):
        n = len(padrao.findall(texto_norm))
        if n:
            achados.append((termo, n))
    return achados


def posicoes_do_tema(texto_norm: str, tema: str) -> set[int]:
    """Posições distintas cobertas pelo vocabulário do tema, sem contar duas
    vezes o trecho em que dois termos da lista se sobrepõem."""
    pos = set()
    for _, padrao in _AUSENCIA_RE.get(tema, []):
        for m in padrao.finditer(texto_norm):
            pos.add(m.start())
    return pos


def selo_termos(nivel: str, tem_termo: bool) -> str:
    """Cruza a contagem com o nível do modelo.

    O erro das duas listas anda em direções opostas, e é isso que torna o selo
    legível. Termo que dispara à toa em TERMOS_AUSENCIA só rebaixa "Ausência
    confirmada" para "Revisar ausência", ninguém afirma nada de errado. Termo
    faltando é que estragaria, carimbando ausência num tema que o plano trata
    com outro vocabulário. Por isso aquela lista é generosa e esta função não
    tenta consertar nada: ela só descreve o que as duas fontes disseram.
    """
    ausente = (nivel or "").strip() in ("", "Não menciona", "Ausente")
    if ausente:
        return SELO_AUSENCIA_REVER if tem_termo else SELO_AUSENCIA_OK
    return SELO_CONFIRMADO if tem_termo else SELO_SO_CONTEXTO


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


def contexto_do_vocabulario(texto: str, texto_norm: str, tema: str,
                            janela: int = 3000, maximo: int = 5) -> str:
    """Igual ao contexto_do_tema, mas em volta do vocabulário largo do tema.

    Existe porque as duas listas servem a momentos diferentes. O contexto_do_tema
    usa TERMOS_ANCORA, que é discriminante e por isso não encontra o plano que
    trata do tema sem usar a palavra do tema. Quando a guarda de ausência decide
    reperguntar por causa do vocabulário largo, é o entorno DESSAS ocorrências
    que precisa ir na pergunta, senão não há o que reperguntar.
    """
    posicoes = sorted(posicoes_do_tema(texto_norm, tema))[:maximo]
    if not posicoes:
        return ""
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
        "ATENÇÃO: metas de leis federais preexistentes (como Plano Nacional de Educação / PNE, ODS) "
        "NÃO são metas do candidato. 'Cumprir as metas do PNE' ou 'alinhar-se às metas do PNE' é Propõe ação, "
        "NÃO Define meta. Só use Define meta se houver meta própria e exclusiva do plano.\n\n"
        "REGRA DE CITAÇÃO: 'trecho' é transcrição literal, copiada palavra por "
        "palavra do texto abaixo. Não corrija concordância, não encurte dentro da "
        "frase e não escreva frase sua. Para juntar partes distantes, marque cada "
        "corte com [...].\n\n"
        # "Não menciona" aqui significava duas coisas diferentes e a segunda era
        # falsa: o tema não estar no plano, e o tema estar citado sem proposta.
        # Medido em 07/08/2026, dos 22 temas ainda gravados como ausentes com o
        # termo presente no texto, o padrão é este: o plano do Ciro Gomes (PSDB)
        # cita "a população negra, as comunidades quilombolas, os povos
        # originários" num diagnóstico, e os três saíram como não mencionados.
        "Se o tema aparece nos trechos, ainda que de passagem ou só no "
        "diagnóstico, o nível é no mínimo 'Menciona vagamente'. Nesse caso "
        "copie a frase em que ele aparece. 'Não menciona' é só para quando o "
        "tema não está nos trechos. Não invente proposta em nenhum dos casos.\n\n"
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


# Sem mínimo de caracteres dentro das aspas. Com o mínimo de 8 que estava aqui,
# um par curto era pulado e o par seguinte casava errado: a justificativa do
# Hertz Dias (10/08/2026) tinha "Turismo" (7 caracteres) seguido de
# "Alfabetização", e o que saiu foi 'ou "Turismo, e menciona vagamente
# Alfabetização"', com as aspas atravessando o meio da frase. Aspas curtas
# também não podem se passar por transcrição, então cair na mesma regra é o
# comportamento certo, não uma concessão.
_ASPAS = r"[\"“”]([^\"“”]+)[\"“”]"


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
    texto = str(justificativa or "")
    if not texto.strip():
        return justificativa

    # Aspas em número ímpar não dão para parear: qualquer corte aqui junta o
    # fim de uma citação com o começo da seguinte. Fica como está, que é
    # estranho de ler mas não inventa transcrição.
    if sum(texto.count(c) for c in '"“”') % 2:
        return texto

    def troca(m):
        dentro = m.group(1)
        return dentro if _sem_espaco(_norm_busca(dentro)) not in corpus else m.group(0)

    return re.sub(_ASPAS, troca, texto)


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


_CONECTIVOS_NOME = {"da", "das", "de", "do", "dos", "e"}
# Abreviação de tratamento, que é palavra e não sigla: sai "Dr.", não "DR.".
_TRATAMENTOS = {"dr", "dra", "prof", "profa", "sgt", "cb", "ten", "cel", "sr", "sra"}


# Siglas que aparecem dentro de nome de urna e têm vogal, então a regra geral
# abaixo não alcança. Lista curta de propósito: cada entrada é um caso visto na
# base, e o custo de faltar uma é um nome escrito errado, não dado errado.
SIGLAS_NOME = {"ACM", "PCO", "PCB", "PSOL", "PSTU", "MST", "UNE", "OAB"}


def _e_sigla(token: str) -> bool:
    """Se o token em caixa alta é sigla, e não palavra escrita em caixa alta.

    Tamanho não separa os dois, e foi por tentar isso que a primeira versão
    desta função devolveu "ZÉ Batista", "RUI Costa Pimenta" e "GAL Leite". O que
    separa na maioria dos casos é a vogal: "JHC" e "MLB" não têm, palavra sempre
    tem.

    Também não serve olhar o conectivo anterior, tentado e descartado no mesmo
    dia: "do PCO" e "do MLB" seguem conectivo, mas "CADU DE LULA" também, e
    LULA virava sigla.
    """
    letras = re.sub(r"[^A-Za-zÀ-ÿ]", "", token)
    if not letras.isupper() or len(letras) > 4:
        return False
    return letras in SIGLAS_NOME or not re.search(r"[AEIOUÀ-Ý]", letras)


def nome_proprio(nome: str) -> str:
    """O nome de urna do TSE, que vem em caixa alta, escrito como nome próprio.

    Sem isto o resumo sai com "PROFESSORA CAMILA prevê" no meio da frase, porque
    o modelo copia o que recebe.

    Sigla fica intacta. Baixar tudo com `capitalize()` gravou "Acm Neto" na
    Bahia, "Jhc" em Alagoas e "Arinalda do Mlb" no RN, medido nos 201 resumos de
    17/08/2026, e sigla virada em nome próprio é erro que o leitor vê antes de
    ler a frase.

    Quem sobe é a primeira letra, não o primeiro caractere: o apelido vem entre
    parênteses no nome de urna, e `capitalize()` em "(lau)" devolve "(lau)".
    Letra depois de ponto sobe junto, porque o TSE grava "DR.LUISINHO" sem
    espaço.
    """
    nome = re.sub(r"\s+", " ", str(nome or "")).strip()
    if not nome:
        return ""
    palavras = []
    for p in nome.split(" "):
        pl = p.lower()
        if pl in _CONECTIVOS_NOME:
            palavras.append(pl)
        elif pl.rstrip(".") in _TRATAMENTOS or not _e_sigla(p):
            palavras.append(re.sub(r"(^\W*|\.)([a-zà-ÿ])",
                                   lambda m: m.group(1) + m.group(2).upper(), pl))
        else:
            palavras.append(p)
    return " ".join(palavras)


def _regras_sujeito(nome: str, genero: str) -> str:
    """Como o modelo deve nomear quem propõe, na justificativa da coerência.

    Duas coisas dependem de quem é o candidato. O gênero, porque "o candidato"
    escrito sobre uma mulher é erro de fato, e o painel mostra o cadastro do TSE
    logo abaixo da frase. E a repetição: com um sujeito só à disposição, saíam
    quatro frases seguidas abrindo em "O candidato".

    Sem gênero no cadastro, o modelo não recebe pronome nenhum: ele deduziria do
    nome, e nome não diz gênero.
    """
    nome = nome_proprio(nome)
    g = str(genero or "").strip().upper()
    fem = g.startswith("FEMIN")
    conhecido = g.startswith(("FEMIN", "MASCUL"))
    quem = "a candidata" if fem else "o candidato"
    pron = "ela" if fem else "ele"

    regra = (f"- Toda proposta é {'da candidata' if fem else 'do candidato'}, e a "
             f"frase precisa dizer isso. Comece pelo sujeito, na forma "
             f"'{quem} propõe', '{quem} condiciona', '{quem} não registra'. "
             f"Nunca escreva proposta do plano como afirmação sua nem como fato "
             f"do país.\n")
    if nome and conhecido:
        regra += (f"- Escreva '{quem}' uma vez só, na primeira frase. Depois "
                  f"alterne o sujeito entre '{nome}' e '{pron}', nessa ordem. "
                  f"Quatro frases abrindo com o mesmo sujeito viram ladainha.\n")
    elif nome:
        # Cadastro sem gênero: o nome serve de sujeito, o pronome não, porque
        # aqui ele seria chute a partir do nome.
        regra += (f"- Escreva '{quem}' uma vez só, na primeira frase. Nas "
                  f"seguintes use '{nome}' como sujeito. Não use pronome "
                  f"pessoal para se referir a quem propõe.\n")
    return regra


# Termos que RESTRICOES_LINGUAGEM já proíbe e que o modelo usou assim mesmo.
# Medido em 10/08/2026 nas 79 justificativas gravadas: 6 traziam pelo menos um.
# Instrução sozinha não segura, então a lista vira conferência depois da
# resposta. Palavra inteira, e "fundamental" fica de fora porque no corpus é
# quase sempre a etapa de ensino, não o adjetivo.
# Fora da lista de propósito, medido nos 10.385 resumos de 21/08/2026:
# "horizonte" só aparecia em Belo Horizonte (7 de 7), "solido" e variantes só em
# resíduos sólidos (7 de 7), e "essencial" em serviços, alimentos e trechos
# rodoviários essenciais (9 de 9), que é uso corrente e não adjetivo de reforço.
# Eram 23 dos 48 acusados. A régua existe para barrar escrita inflada, não o
# nome da coisa.
TERMOS_PROIBIDOS = [
    "abrangente", "abrangentes", "crucial", "cruciais",
    "estrategico", "estrategica", "estrategicos", "estrategicas",
    "robusto", "robusta", "robustos", "robustas", "significativo", "significativa",
    "notavel", "notaveis",
    "consistente", "consistentes", "inovador", "inovadora", "multifacetado",
    "apresenta", "apresentam", "possui", "possuem", "oferece", "oferecem",
    "ressalta", "ressaltam", "enfatiza", "enfatizam", "evidencia", "evidenciam",
    "aprimorar", "aprimora", "vale destacar", "cabe ressaltar", "e importante notar",
    "panorama", "otica", "mosaico",
    # Eco da própria régua. O prompt já pedia para não repetir as palavras da
    # escala, e a justificativa da Samara de 10/08/2026 saiu com "Ela estabelece
    # duas ligações sustentadas por um mesmo instrumento em eixos distintos".
    # Isso descreve o degrau em que ela caiu, não o que o plano propõe.
    "ligacao sustentada", "ligacoes sustentadas", "eixos distintos",
    "estrategia parcial", "estrategia integrada", "mencoes isoladas",
    "propostas isoladas", "metas isoladas", "acoes isoladas",
]
_PROIBIDOS_RE = re.compile(r"\b(" + "|".join(TERMOS_PROIBIDOS) + r")\b")


def _sem_acento_mesmo_tamanho(t: str) -> str:
    """Como _norm_acentos, mas um caractere entra, um caractere sai.

    _norm_acentos decompõe em NFD e joga fora os acentos, o que encurta a
    string e desalinha as posições. Aqui a busca precisa devolver o índice do
    achado no texto original, para olhar a letra que ele tem lá.
    """
    saida = []
    for c in str(t or "").lower():
        d = unicodedata.normalize("NFD", c)
        saida.append(d[0] if d else c)
    return "".join(saida)


def _e_nome_proprio(texto: str, ini: int) -> bool:
    """O achado começa com maiúscula e não é a primeira palavra da frase."""
    if not texto[ini:ini + 1].isupper():
        return False
    antes = texto[:ini].rstrip()
    return bool(antes) and antes[-1] not in ".!?:;"


def termos_proibidos(texto: str) -> list[str]:
    """Termos proibidos presentes no texto, fora das aspas e fora de nome próprio.

    Fora das aspas porque nome de programa citado do plano não é escolha de
    quem escreve: "Consolidar Política Integrada de Governo Digital" e
    "Instituto Estadual de Robótica" são do candidato, não do analista.

    Fora de nome próprio pelo mesmo motivo, e porque o plano nomeia sem aspas.
    Medido nos 10.385 resumos de 21/08/2026: 16 dos 48 acusados eram nome
    próprio, entre eles Fundo Estadual de Recursos Estratégicos, Sistema
    Estadual de Governança Estratégica e Programa Piauí Inovador. Pedir ao
    modelo que reescreva sem essas palavras é pedir que ele mude o nome do
    programa, e ele não muda: era isso que travava a rodada de refação.

    Maiúscula no meio da frase, e não em qualquer lugar: senão a primeira
    palavra de toda frase escaparia da régua.
    """
    sem_citacao = re.sub(r'["“][^"“”]*["”]', " ", str(texto or ""))
    normalizado = _sem_acento_mesmo_tamanho(sem_citacao)
    achados = [m.group(1) for m in _PROIBIDOS_RE.finditer(normalizado)
               if not _e_nome_proprio(sem_citacao, m.start())]
    return sorted(set(achados))


# ─── Nome de tema vazando para o texto do resumo ──────────────────────────────
# O resumo é o primeiro texto que o cliente lê sobre cada plano, e o nome dos
# temas é vocabulário interno da análise, não do plano. Medido nos 201 resumos
# gravados em 17/08/2026: 93 traziam pelo menos um, quase sempre pendurado num
# programa ("reforma o Planserv, que abrange Financiamento e Gestão do SUS e
# Média e Alta Complexidade", "programa para Educação Profissional, Juventude e
# Pessoa Idosa"). A origem era o próprio prompt, que mandava dizer "quais temas"
# o programa cobre.
#
# Por que a conferência não pode ser só "o nome do tema aparece": metade dos
# nomes é português corrente. "Ampliar o tempo integral em 80 escolas" é frase
# boa, e "Tempo Integral" é nome de tema. O que denuncia o vazamento não é a
# palavra, é a construção, então a busca é por duas:
#
#   1. verbo de cobertura (cobre, abrange, atravessa, contempla…) seguido de
#      nome de tema logo adiante. Esses verbos não têm outro uso aqui.
#   2. dois nomes de tema encadeados por 'e'/vírgula no mesmo período, que é a
#      lista de rótulos que o modelo produzia.
#
# A regra 2 tem falso positivo possível ("universalizar o Ensino Médio em tempo
# integral"), e ele é barato: custa uma chamada e uma reescrita, e não muda dado
# nenhum. Ausência da guarda é que custa, porque o texto errado vai para a tela.
_LIGA_TEMA = re.compile(
    r"\b(?:cobre|cobrem|cobrindo|abrange|abrangem|abrangendo|atravessa|"
    r"atravessam|atravessando|engloba|englobam|englobando|contempla|"
    r"contemplam|contemplando|integra|integrando|une|unindo|"
    r"nos temas|os temas|para os temas|tema de|temas de|eixos de|"
    r"nas areas de|que cruza|que liga)\b")
# O elo aceita artigo e preposição no meio ("Esporte e Lazer e para Transporte
# e Rodovias"), senão a lista com preposição escapa.
_ELO_TEMA = re.compile(
    r"[\s,]*(?:e|ou)?[\s,]*(?:para|a|o|as|os|em|no|na|nos|nas|de|do|da)?[\s,]*")


def temas_no_texto(texto: str, temas: dict = None) -> list[str]:
    """Nomes de tema usados como rótulo dentro do texto, fora das aspas.

    Fora das aspas pela mesma razão de `termos_proibidos`: nome de programa
    copiado do plano não é escolha de quem escreve.
    """
    nomes = sorted(temas if temas is not None else TEMAS, key=len, reverse=True)
    if not nomes:
        return []
    tema_re = re.compile(
        r"\b(?:" + "|".join(re.escape(_norm_acentos(n)) for n in nomes) + r")\b")
    sem_citacao = re.sub(r'["“][^"“”]*["”]', " ", str(texto or ""))
    n = _norm_acentos(sem_citacao)
    achados = []
    for m in _LIGA_TEMA.finditer(n):
        achados += [x.group(0) for x in tema_re.finditer(n[m.end():m.end() + 90])]
    for periodo in re.split(r"[.;]", n):
        pos = [(x.start(), x.end(), x.group(0)) for x in tema_re.finditer(periodo)]
        for i in range(len(pos) - 1):
            if _ELO_TEMA.fullmatch(periodo[pos[i][1]:pos[i + 1][0]]):
                achados += [pos[i][2], pos[i + 1][2]]
    return sorted(set(achados))


def _ligacoes_validas(ligacoes: list, classif: dict) -> list:
    """Rebaixa o score quando o modelo não sustentou a articulação que afirmou.

    A régua antiga só descrevia articulação, e o modelo não tinha material que
    a descrevesse: recebia a grade de temas com nível e citação, que é uma
    tabela de cobertura. O resultado, medido em 10/08/2026, foi a nota virar
    contagem de temas com proposta, com 56 dos 79 planos no mesmo 4 e faixas
    que se atropelam (plano com 40 temas propostos em 3, plano com 20 em 4).

    Agora o modelo tem que nomear os pares de temas que se sustentam e o
    instrumento que os liga. Par sem instrumento, ou com tema que não tem
    proposta no plano, não conta.
    """
    def mesma_citacao(a: str, b: str) -> bool:
        """Os dois temas se apoiam na mesma frase do plano.

        Não é ligação, é uma frase indexada em dois temas. Na primeira rodada
        com esta guarda, 10/08/2026, o plano da Samara Martins tirou 4 com dois
        pares assim: "creche no local de trabalho" sustentava Primeira Infância
        e Mulheres, e "Controle estatal dos preços dos alimentos" sustentava
        Agropecuária e Segurança Alimentar. Articulação exige duas propostas,
        não uma proposta que atravessa dois temas.
        """
        ta = _sem_espaco(_norm_busca((classif.get(a) or {}).get("trecho", "")))
        tb = _sem_espaco(_norm_busca((classif.get(b) or {}).get("trecho", "")))
        if not ta or not tb:
            return False
        return ta in tb or tb in ta

    validas = []
    for lig in ligacoes or []:
        if not isinstance(lig, dict):
            continue
        de, para = str(lig.get("de", "")).strip(), str(lig.get("para", "")).strip()
        instrumento = str(lig.get("instrumento", "")).strip()
        com_proposta = [t for t in (de, para)
                        if (classif.get(t) or {}).get("nivel") in ("Propõe ação", "Define meta")]
        if (len(com_proposta) == 2 and de != para and instrumento
                and not mesma_citacao(de, para)):
            validas.append({"de": de, "para": para, "instrumento": instrumento})
    return validas


def _teto_por_ligacoes(score: int, validas: list) -> int:
    """O degrau que a contagem de ligações permite.

    Os cortes mudaram em 10/08/2026, na primeira medição real. A versão
    anterior pedia até 3 ligações e dava 5 a quem tivesse 3: o teto da lista
    era o gatilho da nota máxima, então bastava preencher a lista. Resultado,
    19 dos 40 planos refeitos em 5, o mesmo amontoado que a régua velha tinha
    no 4, só que no outro extremo.

    Agora a lista vai até 6 e o topo exige mais que quantidade: as ligações
    precisam tocar pelo menos três eixos, senão três pares dentro da mesma área
    valeriam o mesmo que um plano amarrado de ponta a ponta.
    """
    n = len(validas)
    eixos = {EIXO_DO_TEMA.get(t, "Outros")
             for lig in validas for t in (lig["de"], lig["para"])}
    if n >= 4 and len(eixos) >= 3:
        return min(score, 5)
    if n >= 2:
        return min(score, 4)
    if n == 1:
        return min(score, 3)
    return min(score, 2)


# Palavras que aparecem no nome de quase todo programa e não distinguem um do
# outro. Sem tirar, "Programa Estadual X" e "X" contam como programas
# diferentes e a ponte entre temas se perde.
_NOME_GENERICO = {
    "programa", "programas", "projeto", "projetos", "plano", "planos", "politica",
    "politicas", "estadual", "estaduais", "municipal", "nacional", "novo", "nova",
    "modelo", "sistema", "rede", "pacto", "fundo", "estado", "governo",
    "de", "da", "do", "das", "dos", "e", "para", "com", "em", "a", "o", "as", "os",
}


def _nome_programa(nome: str) -> str:
    t = re.sub(r"[^a-z0-9 ]", " ", _norm_busca(str(nome or "")))
    return " ".join(w for w in t.split() if w not in _NOME_GENERICO and len(w) > 2)


def _quadro_das_pontes(pontes: dict) -> str:
    """As pontes encontradas, como texto para o prompt."""
    if not pontes:
        return ("PROGRAMAS QUE ATRAVESSAM MAIS DE UM TEMA: nenhum. As propostas "
                "deste plano não compartilham um programa nomeado.")
    linhas = [f"  - {nome}: {', '.join(sorted(temas))}"
              for nome, temas in sorted(pontes.items(), key=lambda x: -len(x[1]))]
    return "PROGRAMAS QUE ATRAVESSAM MAIS DE UM TEMA:\n" + "\n".join(linhas)


def pontes_de_programa(classif: dict) -> dict:
    """Programas nomeados pelo plano que aparecem em mais de um tema.

    Esta é a definição de articulação que o dado sustenta. "Trilhas de Futuro"
    citado em Educação Profissional, Ensino Médio e Geração de Emprego é uma
    ponte escrita pelo candidato, não uma leitura de quem analisa.

    Por que substituiu a contagem que o modelo fazia: pedir "liste até N
    ligações" e dar nota pela quantidade listada é circular, porque o pedido
    convida a listar N e nada premia responder "não há". Medido em 10/08/2026,
    a régua anterior punha 36 dos 60 planos refeitos na nota máxima, o mesmo
    amontoado da régua de antes, só que no outro extremo.

    Os nomes são juntados por continência, e não por igualdade: com igualdade
    exata, "Trilhas de Futuro" e "Programa Trilhas de Futuro" viram dois
    programas. A diferença não é detalhe, o Rafael Fonteles (PT/PI) sai de zero
    para três pontes, sobre 7 temas e 5 eixos, só com essa junção.
    """
    nomes, originais = {}, {}
    for tema, item in classif.items():
        if (item or {}).get("nivel") == "Não menciona":
            continue
        bruto = str((item or {}).get("programa_nome", "")).strip()
        chave = _nome_programa(bruto)
        if len(chave) >= 5:
            nomes.setdefault(chave, set()).add(tema)
            # O nome que vai para a tela e para o prompt é como o plano
            # escreveu, não a chave sem acento e sem "Programa". Fica a grafia
            # mais completa entre as variantes.
            if len(bruto) > len(originais.get(chave, "")):
                originais[chave] = bruto

    chaves = sorted(nomes, key=len)
    pai = {k: k for k in chaves}

    def raiz(x):
        while pai[x] != x:
            x = pai[x]
        return x

    for i, a in enumerate(chaves):
        for b in chaves[i + 1:]:
            if a in b or set(a.split()) <= set(b.split()):
                pai[raiz(a)] = raiz(b)

    juntos, rotulo = {}, {}
    for chave, temas in nomes.items():
        r = raiz(chave)
        juntos.setdefault(r, set()).update(temas)
        if len(originais.get(chave, "")) > len(rotulo.get(r, "")):
            rotulo[r] = originais[chave]
    return {rotulo.get(p, p): t for p, t in juntos.items() if len(t) >= 2}




def gerar_resumos_eixos(classif: dict, temas: dict = TEMAS, nome: str = "", genero: str = "") -> dict:
    from google.genai import types
    
    eixos_validos = {}
    for tema, result in classif.items():
        nivel = result.get("nivel")
        if nivel and nivel not in ["Não menciona", ""]:
            eixo = EIXO_DO_TEMA.get(tema, "Outros")
            if eixo != "Outros":
                eixos_validos.setdefault(eixo, {})[tema] = result
                
    if not eixos_validos:
        return {}
        
    prompt = (
        "Você é repórter de política escrevendo resumos jornalísticos (estilo G1/Folha) "
        "sobre as propostas de um candidato.\n\n"
        "Sua tarefa tem duas partes:\n"
        "PARTE 1: Para CADA EIXO, escreva um texto corrido de até 3 frases.\n"
        "PARTE 2: Para CADA TEMA específico, escreva um resumo super curto de 1 única frase condensando a proposta.\n\n"
        "Regras de estilo:\n"
        "1. Vá direto para as propostas concretas, metas e prazos com verbos objetivos (propõe, defende).\n"
        "2. Texto fluido, SEM BULLET POINTS, sem listas.\n"
        "3. NUNCA cite os nomes das categorias da grade.\n\n"
        + _regras_sujeito(nome, genero) +
        f"\n\n{RESTRICOES_LINGUAGEM}\n\n"
        "DADOS DO CANDIDATO:\n"
    )
    
    for eixo, tms in eixos_validos.items():
        prompt += f"\nEixo: {eixo}\n"
        for t, res in tms.items():
            prompt += f"  - {t}: {res.get('trecho')}\n"
            
    prompt += "\nResponda APENAS um objeto JSON plano contendo as chaves com o nome de CADA EIXO e de CADA TEMA analisado, e os valores sendo os resumos gerados."
    
    # As chaves que a tela sabe procurar. Ela busca por igualdade exata
    # (`resumos_eixos[rotulo]`), então chave que o modelo inventou é resumo que
    # não aparece para ninguém.
    validas = set(eixos_validos) | {t for tms in eixos_validos.values() for t in tms}

    def pedir(evitar: list[str] | None = None) -> dict:
        texto = prompt
        if evitar:
            texto += ("\n\nSUA RESPOSTA ANTERIOR FOI RECUSADA porque usou: "
                      + ", ".join(evitar)
                      + ". Reescreva sem nenhuma dessas palavras, sem trocar por "
                      "sinônimo do mesmo tipo.")
        resp = _gemini_client().models.generate_content(
            model=GEMINI_MODEL,
            contents=texto,
            config=types.GenerateContentConfig(response_mime_type="application/json"),
        )
        data = _carregar_json((getattr(resp, "text", "") or "").strip(), "resposta")
        return {k: _limpa(v, n=800) for k, v in data.items()
                if isinstance(v, str) and v.strip()}

    try:
        data = pedir()
    except Exception as e:
        print(f"(erro ao gerar resumos de eixo: {e})", end=" ", flush=True)
        return {}

    # Uma rodada de recusa, como no resumo do plano. Sem ela o texto do eixo
    # saía com a linguagem que `termos_proibidos` barra no resumo: em 21/08/2026
    # eram 47 textos na base, com "estratégicas", "horizonte" e "essenciais".
    sujos = sorted({t for v in data.values() for t in termos_proibidos(v)})
    if sujos:
        print(f"(linguagem proibida nos resumos de eixo: {', '.join(sujos)}, refazendo)",
              end=" ", flush=True)
        time.sleep(3)
        try:
            nova = pedir(evitar=sujos)
        except Exception as e:
            print(f"(refazer falhou: {e})", end=" ", flush=True)
            nova = {}
        # Só troca se melhorou: resposta pior devolveria menos texto com o mesmo
        # defeito, e o primeiro conteúdo já estava escrito.
        restantes = sorted({t for v in nova.values() for t in termos_proibidos(v)})
        if nova and len(restantes) < len(sujos):
            data = nova
            if restantes:
                print(f"(continuou com {', '.join(restantes)})", end=" ", flush=True)
        else:
            print("(continuou com a mesma linguagem)", end=" ", flush=True)

    return _casar_chaves(data, validas)


def _casar_chaves(data: dict, validas: set) -> dict:
    """Casa a chave que o modelo devolveu com o rótulo que a tela procura.

    O modelo às vezes junta dois rótulos numa chave só. Em 21/08/2026 o plano do
    Carlos Cley (PSTU/AP) voltou com "Primeira Infância: Educação Infantil", e o
    tema Primeira Infância dele ficou sem resumo na tela, porque a busca é por
    igualdade exata.

    Casa em três passos, do mais estrito para o mais frouxo: igual, igual sem
    acento e sem caixa, e o pedaço antes de ':', '/' ou '(' pelas mesmas duas
    réguas. Chave que não casa em nenhum dos três sai: texto sob rótulo errado é
    pior do que rótulo sem texto, porque ninguém percebe.
    """
    por_norma = {_norm_acentos(v): v for v in validas}
    saida = {}
    for bruto, texto in data.items():
        chave = str(bruto).strip()
        candidatos = [chave, re.split(r"[:/(]", chave)[0].strip()]
        alvo = ""
        for c in candidatos:
            if c in validas:
                alvo = c
                break
            if _norm_acentos(c) in por_norma:
                alvo = por_norma[_norm_acentos(c)]
                break
        if not alvo:
            print(f"(chave fora da grade descartada: {chave!r})", end=" ", flush=True)
            continue
        # Primeira grafia vence: com "Saúde" e "Saude: Atenção Básica" na mesma
        # resposta, o texto do rótulo exato é o que descreve o eixo inteiro.
        saida.setdefault(alvo, texto)
    return saida


def resumir_plano(classif: dict, temas: dict = TEMAS,
                  nome: str = "", genero: str = "") -> dict:
    """Escreve o resumo do plano, em 3 ou 4 frases, e devolve as pontes.

    Substituiu avaliar_coerencia em 10/08/2026. A nota de 1 a 5 saiu: ela
    prometia medir articulação e media cobertura, mudava um ponto em três de
    quatro planos entre rodadas, e não tem correspondente na literatura da
    área, que mede saliência e se a promessa é verificável, não coerência.

    O texto ficou, porque ele nunca dependeu da nota. A matéria-prima é a
    mesma: a classificação verificada, tema a tema, e os programas que
    aparecem em mais de um tema.

    Não fala do que falta. Ausência já está na tela, tema a tema, e dita em
    prosa vira frase de efeito sobre o que o candidato não escreveu.
    """
    from google.genai import types

    pontes = pontes_de_programa(classif)

    prompt = (
        "Você é analista sênior de políticas públicas. Abaixo está a análise "
        "tema a tema de um plano de governo, com o nível de cada tema e a citação "
        "do plano que sustenta esse nível.\n\n"
        f"{_quadro_da_analise(classif, temas)}\n\n"
        f"{_quadro_das_pontes(pontes)}\n\n"
        "Escreva um RESUMO do plano em 3 ou 4 frases, no máximo 500 caracteres.\n"
        "- Comece pela proposta mais concreta, a que tem nome de programa, "
        "número ou prazo.\n"
        "- Se houver programa atravessando mais de um tema, diga qual é e o que "
        "ele faz, com as palavras do plano. NUNCA liste os nomes dos temas do "
        "quadro acima: eles são a grade desta análise, não o texto do "
        "candidato. Errado: 'o Planserv, que abrange Financiamento e Gestão do "
        "SUS e Média e Alta Complexidade'. Certo: 'reforma o Planserv, o plano "
        "de saúde do servidor, do custeio à fila de cirurgia'.\n"
        "- Descreva o que o plano propõe. NÃO diga o que falta, o que não é "
        "mencionado nem o que o plano deixa de fazer.\n"
        "- Não dê nota, não classifique, não fale em nível, degrau ou escala.\n\n"
        "Responda APENAS um objeto JSON com a chave 'resumo'.\n\n"
        "COMO ESCREVER:\n"
        + _regras_sujeito(nome, genero) +
        "- Depois da atribuição, diga o que a pessoa especifica, com nome de "
        "programa, prazo, número ou público.\n"
        "- Proibidas as palavras 'integrada', 'articulada', 'parcial', "
        "'isoladas' e 'coerente'.\n"
        "- Nomeie programas, metas, números e prazos que estão no texto. Um "
        "resumo sem nenhum nome próprio de programa ou número não serve.\n"
        "- Sigla se escreve como o plano escreve, em caixa alta: IDEB, SUS, "
        "EJA, UBS. Nunca 'Ideb' nem 'Sus'.\n"
        "- Nada de juízo de valor. Só o que está escrito no plano.\n\n"
        "REGRA DE ASPAS: só use aspas para copiar, palavra por palavra, uma das "
        "citações listadas acima. Nunca encurte a frase dentro das aspas nem "
        "escreva entre aspas algo que não está ali. Sem citação à mão para o "
        "ponto que quer fazer, escreva sem aspas.\n\n"
        f"{RESTRICOES_LINGUAGEM}"
    )

    def pedir(evitar: list[str] | None = None, recusa: str = ""):
        texto = prompt
        if evitar:
            texto += ("\n\nSUA RESPOSTA ANTERIOR FOI RECUSADA porque usou: "
                      + ", ".join(evitar)
                      + ". Reescreva sem nenhuma dessas palavras, sem trocar por "
                      "sinônimo do mesmo tipo.")
        if recusa:
            texto += recusa
        resp = _gemini_client().models.generate_content(
            model=GEMINI_MODEL,
            contents=texto,
            config=types.GenerateContentConfig(response_mime_type="application/json"),
        )
        return _carregar_json((getattr(resp, "text", "") or "").strip(), "resumo")

    data = pedir()
    resumo = _limpa(data.get("resumo", ""), n=600)
    proibidos = termos_proibidos(resumo)
    if proibidos:
        print(f"(linguagem proibida em {', '.join(proibidos)}, refazendo)",
              end=" ", flush=True)
        time.sleep(3)
        data = pedir(evitar=proibidos)
        resumo = _limpa(data.get("resumo", ""), n=600)
        ainda = termos_proibidos(resumo)
        if ainda:
            print(f"(continuou com {', '.join(ainda)})", end=" ", flush=True)

    # A instrução do prompt não segura sozinha, pelo mesmo motivo de
    # `termos_proibidos`: o modelo tem o quadro de temas à vista e cola o rótulo
    # no programa. A recusa diz o que fazer no lugar, senão a segunda resposta
    # repete a primeira.
    rotulos = temas_no_texto(resumo, temas)
    if rotulos:
        print(f"(nome de tema no resumo: {', '.join(rotulos)}, refazendo)",
              end=" ", flush=True)
        time.sleep(3)
        data = pedir(recusa=(
            "\n\nSUA RESPOSTA ANTERIOR FOI RECUSADA porque nomeou os temas do "
            "quadro (" + ", ".join(rotulos) + "), que são a grade desta análise "
            "e não aparecem no plano. Reescreva dizendo o que cada programa faz, "
            "com as palavras do plano, sem listar tema nenhum."))
        novo = _limpa(data.get("resumo", ""), n=600)
        # Só troca se a segunda tentativa melhorou: resposta pior devolveria ao
        # cliente um resumo com o mesmo defeito e sem o conteúdo do primeiro.
        if novo and len(temas_no_texto(novo, temas)) < len(rotulos):
            resumo = novo
        else:
            print("(continuou nomeando tema)", end=" ", flush=True)

    citacoes = [(classif.get(t) or {}).get("trecho", "") for t in temas]
    
    # Gera os resumos de cada eixo usando a nova funcionalidade
    resumos_eixos = gerar_resumos_eixos(classif, temas, nome, genero)
    
    return {"resumo": tirar_aspas_sem_lastro(resumo, citacoes),
            "pontes": pontes,
            "pontes_texto": " | ".join(
                f"{n}: {', '.join(sorted(t))}"
                for n, t in sorted(pontes.items(), key=lambda x: -len(x[1]))),
            "resumos_eixos": __import__('json').dumps(resumos_eixos, ensure_ascii=False) if resumos_eixos else ""}


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
