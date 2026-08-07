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

    print()
    if falhas:
        print(f"{len(falhas)} falha(s): {falhas}")
        return 1
    print("todas as garantias passaram.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
