# -*- coding: utf-8 -*-
"""Garantias contra citação fabricada e contra ausência inventada.

Cada checagem aqui corresponde a um defeito que chegou à planilha em 07/08/2026
e foi medido antes de ser corrigido. Rodar depois de mexer em classificar_plano,
verificar_trecho, conferir_classificacao ou nos termos-âncora.

    python -m outros.testes.teste_conferencia

Baixa dois planos do TSE na primeira execução e guarda em outros/testes/_planos/.
Sem rede, o teste avisa e sai sem falhar, para não quebrar CI por indisponibilidade
do DivulgaCand.
"""

from __future__ import annotations

import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(os.path.abspath(__file__)), ".."))

import analise_planos as ap          # noqa: E402
import processar_planos as pp        # noqa: E402

CACHE = os.path.join(os.path.dirname(os.path.abspath(__file__)), "_planos")

# Os dois planos que revelaram os defeitos. Zema tem 81 páginas e capítulos de
# educação, saúde e esporte depois da página 45, onde o corte antigo caía. Kalil
# tem 147 páginas e foi o campeão de citação inventada, 36 de 41.
PLANOS = {
    "zema": "https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/doc/280016919931",
    "kalil": "https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/doc/130016911243",
}

falhas: list[str] = []


def check(nome: str, condicao: bool, detalhe: str = "") -> None:
    print(("  ok    " if condicao else "  FALHA ") + nome + (f"  {detalhe}" if detalhe else ""))
    if not condicao:
        falhas.append(nome)


def carregar(nome: str):
    """(texto, páginas normalizadas) do plano, baixando uma vez só."""
    import fitz
    os.makedirs(CACHE, exist_ok=True)
    caminho = os.path.join(CACHE, f"{nome}.pdf")
    if not os.path.exists(caminho):
        with open(caminho, "wb") as f:
            f.write(ap.baixar_plano(PLANOS[nome]))
    doc = fitz.open(caminho)
    try:
        paginas = [p.get_text() for p in doc]
    finally:
        doc.close()
    return " ".join(paginas), [ap._norm_busca(p) for p in paginas]


def item(**kw) -> dict:
    base = {"nivel": "Propõe ação", "score": 2, "trecho": "", "responsavel": "",
            "prazo": "", "publico_alvo": "", "programa_nome": ""}
    base.update(kw)
    return base


def sem_lastro(_contexto, _tema, desc=""):
    return item(nivel="Não menciona", score=0, trecho="")


def main() -> int:
    try:
        zema, zema_pgs = carregar("zema")
        kalil, _ = carregar("kalil")
    except Exception as e:                       # noqa: BLE001
        print(f"planos indisponíveis ({type(e).__name__}: {e}); teste pulado.")
        return 0
    zema_norm = ap._norm_busca(zema)

    print("plano inteiro, sem corte silencioso")
    check("plano de 145 mil cabe numa chamada", len(ap._blocos(zema)) == 1,
          f"{len(ap._blocos(zema))} bloco(s)")
    blocos_kalil = ap._blocos(kalil)
    check("plano de 419 mil vira vários blocos", len(blocos_kalil) > 1,
          f"{len(blocos_kalil)} blocos")
    check("o fim do plano entra em algum bloco", kalil[-500:] in blocos_kalil[-1])
    check("nenhum bloco pequeno demais para valer uma chamada",
          all(len(b) >= ap.BLOCO_CHARS // 4 for b in blocos_kalil),
          f"{[len(b) for b in blocos_kalil]}")

    print("\ncitação conferida contra o PDF")
    # transcrição fiel de um plano que a extração quebra em "mobili dade"
    check("quebra de palavra na extração não vira citação inventada",
          ap.verificar_trecho(zema_pgs, "classificar nacional e internacionalmente as "
                              "facções criminosas como organizações terroristas") == "literal")
    check("frase que o plano não tem é reprovada",
          ap.verificar_trecho(zema_pgs, "O Estado apoiará os MUNICÍPIOS no fortalecimento "
                              "da educação infantil, considerando suas diferentes realidades")
          == "nao localizado")
    check("trecho vazio não sustenta nível", not ap.citacao_sustenta(zema_pgs, ""))
    check("citação de três palavras não sustenta nível",
          not ap.citacao_sustenta(zema_pgs, "mais saude publica"))

    print("\nausência conferida contra o texto")
    for tema in ("Primeira Infância", "Alfabetização", "Valorização Docente",
                 "Saúde Mental", "Habitação", "Esporte e Lazer",
                 "Pessoa com Deficiência"):
        check(f"termo de {tema!r} aparece no plano", bool(ap.ocorrencias_ancora(zema_norm, tema)))
    for tema in ("Igualdade Racial", "População LGBTQIA+"):
        check(f"ausência de {tema!r} se confirma", not ap.ocorrencias_ancora(zema_norm, tema))

    print("\nmerge entre blocos")
    com = {"Habitação": item(trecho="citacao boa e longa vinda do plano")}
    sem = {"Habitação": item(nivel="Define meta", score=3, trecho="")}
    j = ap._juntar_classificacoes([com, sem], {"Habitação": ""})
    check("nível mais alto sem citação não apaga o nível com citação",
          j["Habitação"]["trecho"] != "")

    print("\nconferência antes de gravar")
    pp.reanalisar_tema = sem_lastro
    out = pp.conferir_classificacao({"Habitação": item(trecho="")}, zema, zema_pgs)
    check("nível com trecho vazio cai", out["Habitação"]["nivel"] == "Não menciona")
    out = pp.conferir_classificacao(
        {"Habitação": item(trecho="frase que o plano nunca teve, comprida o bastante")},
        zema, zema_pgs)
    check("citação inventada não fica gravada", out["Habitação"]["trecho"] == "")

    print("\ncitação genérica não sustenta muitos temas")
    generica = "O Brasil precisa mudar essa rota e finalmente colocar no"
    muitos = {t: item(trecho=generica)
              for t in ("Alfabetização", "Saúde Mental", "Habitação")}
    out = pp._conferir_citacao_generica(dict(muitos), zema, zema_norm, zema_pgs)
    check("mesma frase em três temas perde o nível",
          all(out[t]["nivel"] == "Não menciona" for t in muitos))
    dois = {t: item(trecho=generica) for t in ("Fundamental", "Ensino Médio")}
    out = pp._conferir_citacao_generica(dict(dois), zema, zema_norm, zema_pgs)
    check("mesma frase em dois temas vizinhos passa",
          all(out[t]["nivel"] == "Propõe ação" for t in dois))

    print("\nmeta exige alvo mensurável")
    # o plano da Samara (UP) promete "geração de milhões de empregos", e isso saiu
    # como Define meta: quantificador de tamanho não é alvo que dê para conferir
    for frase in ("Criação de frentes emergenciais de trabalho [...] para geração de "
                  "milhões de empregos",
                  "Revogação da Lei do Novo Ensino Médio e reformulação do currículo",
                  "Ampliar a geração de emprego e renda [...] Nº de empregos gerados",
                  "O novo Plano Nacional de Educação (PNE) foi sancionado em abril de 2026 [...] "
                  "Entre as metas aprovadas: [...] pelo menos 90% dos estudantes concluindo o Ensino Médio "
                  "[...] O compromisso do Novo Governo é alinhar-se a essas ambições e, onde possível, "
                  "superá-las com metas estaduais ainda mais ousadas."):
        check(f"não é meta: {frase[:48]}...", not ap.tem_alvo_mensuravel(frase))
    for frase in ("Aumentar, gradativamente, para R$ 3,00 reais por aluno",
                  "Ter no transporte público 40% dos acentos adaptados",
                  "acelerar a universalização do saneamento básico",
                  "zerar a fila de cirurgias eletivas",
                  "ampliar para 1 milhão de vagas em EPT até 2027"):
        check(f"é meta: {frase[:48]}...", ap.tem_alvo_mensuravel(frase))
    baixado = pp._conferir_meta({"Geração de Emprego": item(
        nivel="Define meta", score=3, trecho="geração de milhões de empregos")})
    check("meta sem alvo desce um degrau, sem perder a citação",
          baixado["Geração de Emprego"]["nivel"] == "Propõe ação"
          and baixado["Geração de Emprego"]["trecho"] != "")

    print("\ntema muito citado não fica como menção vaga sem conferência")
    cheios = [t for t in ap.TEMAS
              if len(ap.ocorrencias_ancora(zema_norm, t)) >= pp.OCORRENCIAS_TEMA_TRATADO]
    check("o plano do Zema tem tema tratado a fundo", bool(cheios), f"{len(cheios)} temas")
    tema = cheios[0]
    vago = item(nivel="Menciona vagamente", score=1, trecho="algo vago")
    real = ("classificar nacional e internacionalmente as facções criminosas "
            "como organizações terroristas")
    pp.reanalisar_tema = lambda c, t, desc="": item(trecho=real)
    check("sobe quando a reanálise traz citação do plano",
          pp._conferir_subclassificacao({tema: vago}, zema, zema_norm, zema_pgs)[tema]["nivel"]
          == "Propõe ação")
    pp.reanalisar_tema = lambda c, t, desc="": item(trecho="frase que o plano não tem")
    check("não sobe com citação inventada",
          pp._conferir_subclassificacao({tema: vago}, zema, zema_norm, zema_pgs)[tema]["nivel"]
          == "Menciona vagamente")
    pp.reanalisar_tema = sem_lastro
    check("a guarda nunca desce o nível",
          pp._conferir_subclassificacao({tema: vago}, zema, zema_norm, zema_pgs)[tema]["nivel"]
          == "Menciona vagamente")

    print("\nquem executa, reduzido a ente")
    for texto_resp, esperado in (("governo estadual", "estadual"),
                                 ("gdf", "estadual"),
                                 ("governo federal", "federal"),
                                 ("governo estadual, municípios", "estadual, municipal"),
                                 ("governo federal, setor privado", "federal, privado"),
                                 ("partido", "")):
        check(f"{texto_resp!r} -> {esperado!r}",
              ap.normalizar_responsavel(texto_resp) == esperado,
              repr(ap.normalizar_responsavel(texto_resp)))

    print("\naspas da justificativa")
    just = ('A Segurança Pública tem propostas para "classificar facções como organizações '
            'terroristas" e "construir presídios de segurança máxima".')
    citacoes = ["classificar nacional e internacionalmente as facções criminosas como "
                "organizações terroristas", "construir presídios de segurança máxima"]
    saida = ap.tirar_aspas_sem_lastro(just, citacoes)
    check("aspas encurtadas perdem as aspas",
          '"classificar facções como organizações terroristas"' not in saida)
    check("a frase continua no texto, só sem aspas",
          "classificar facções como organizações terroristas" in saida)
    check("aspas fiéis continuam aspas",
          '"construir presídios de segurança máxima"' in saida)

    # 17/08/2026: 93 dos 201 resumos gravados nomeavam os temas da análise no
    # meio da frase. O resumo é o primeiro texto que o cliente lê de cada plano,
    # e nome de tema ali é vocabulário da grade, não do candidato.
    print("\nnome de tema não vaza para o resumo")
    for texto in ("Ele traz o Programa Estadual de Recomposição das Aprendizagens "
                  "para Alfabetização e Fundamental.",
                  "Acm Neto reforma o Planserv, que abrange Financiamento e Gestão "
                  "do SUS e Média e Alta Complexidade.",
                  "O candidato propõe o PUS-ES, cobrindo Ciência, Tecnologia e "
                  "Inovação, e Eficiência e Gasto Público.",
                  "200 mil jovens em Aprendiz Rio, programa para Educação "
                  "Profissional, Juventude e Pessoa Idosa.",
                  "o programa INFRAESTRUTURA NA TRILHA CERTA para Esporte e Lazer "
                  "e para Transporte e Rodovias."):
        check(f"pega: {texto[:52]}...", bool(ap.temas_no_texto(texto)),
              str(ap.temas_no_texto(texto)))

    # A palavra do tema não basta: metade dos nomes é português corrente, e
    # rebaixar frase boa custaria o conteúdo do resumo.
    for texto in ("O candidato propõe alcançar 90% dos alunos da rede pública com "
                  "alfabetização adequada até o 2º ano.",
                  "Ele cria 600 vagas em creches e amplia a malha rodoviária até 2030.",
                  "Ela define o valor da bolsa-permanência estudantil em R$ 1.874,36."):
        check(f"deixa passar: {texto[:46]}...", not ap.temas_no_texto(texto),
              str(ap.temas_no_texto(texto)))

    check("nome de programa entre aspas não conta",
          not ap.temas_no_texto('Ele institui a "Secretaria de Cultura e Turismo".'))

    # 17/08/2026: o plano da Samara (UP) escreve cada linha duas vezes, e por
    # isso 40 dos 43 trechos dela levavam "junta partes do plano" sendo
    # transcrição fiel. Único plano da base em que isso aparece.
    print("\nlinha escrita duas vezes pelo PDF perde a cópia")
    check("tira a repetição colada",
          ap.desduplicar_linhas(
              "efetivar programa de erradicacao efetivar programa de "
              "erradicacao do analfabetismo no pais")
          == "efetivar programa de erradicacao do analfabetismo no pais")
    check("tira cabeçalho repetido na mesma página",
          ap.desduplicar_linhas("Plano de Governo 2027 Plano de Governo 2027")
          == "Plano de Governo 2027")
    check("texto sem repetição passa intacto",
          ap.desduplicar_linhas("texto normal sem nenhuma repeticao aqui")
          == "texto normal sem nenhuma repeticao aqui")
    # Abaixo de quatro palavras a coincidência é comum em português e a cópia
    # fica: tirar "de acordo com" mudaria frase que ninguém duplicou.
    check("repetição de três palavras não conta",
          ap.desduplicar_linhas("de acordo com de acordo com a lei")
          == "de acordo com de acordo com a lei")
    # O que garante que a conferência não afrouxa: a regra só junta o que já
    # estava colado, então citação costurada de partes distantes continua
    # reprovando.
    check("não aproxima trechos distantes",
          "primeira parte segunda parte" not in
          ap.desduplicar_linhas("primeira parte " + "enchimento " * 30 + "segunda parte"))

    print("\nnome de urna vira nome próprio sem estragar sigla")
    for cru, esperado in (("ACM NETO", "ACM Neto"),
                          ("JHC", "JHC"),
                          ("ARINALDA DO MLB", "Arinalda do MLB"),
                          ("BRUNO PEDREIRO DO PCO", "Bruno Pedreiro do PCO"),
                          ("DR.LUISINHO", "Dr.Luisinho"),
                          ("DR. FURLAN", "Dr. Furlan"),
                          ("SARGENTO LAUDICÉRIO (LAU)", "Sargento Laudicério (Lau)"),
                          # curtos que são palavra, não sigla: o corte por
                          # tamanho devolvia "ZÉ Batista" e "CADU DE LULA".
                          ("ZÉ BATISTA", "Zé Batista"),
                          ("RUI COSTA PIMENTA", "Rui Costa Pimenta"),
                          ("CADU DE LULA", "Cadu de Lula"),
                          ("PROFESSORA MARIA DO CARMO", "Professora Maria do Carmo"),
                          ("ÁLVARO DIAS", "Álvaro Dias")):
        check(f"{cru!r} -> {esperado!r}", ap.nome_proprio(cru) == esperado,
              repr(ap.nome_proprio(cru)))

    print()
    if falhas:
        print(f"{len(falhas)} falha(s): {falhas}")
        return 1
    print("todas as garantias passaram.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
