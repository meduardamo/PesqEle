"""
Relatório da rodada do Instagram: e-mail com os grandes números e clipping em PDF.

Roda no fim de `instagram.py --perfis`, com os posts que a rodada acabou de
gravar. O e-mail abre pelo clipping (o PDF vai anexado) e traz a contagem por
UF e por cargo; o PDF traz, por UF e por candidato, cada post com data, tipo,
curtidas, comentários, resumo curto e link.

Pedido da Jessica (04/08/2026): hoje a Michele lê a planilha e manda os links no
canal de notícias, divididos por UF. Sem ela, ninguém tem o dia pronto.

Variáveis de ambiente:
    BREVO_API_KEY, EMAIL           mesmos do alerta de pesquisas (workflow 02)
    DESTINATARIOS_INSTAGRAM        lista separada por vírgula; cai em
                                   DESTINATARIOS se não existir
    SPREADSHEET_ID_PERFIS, ABA_PERFIS   de onde vêm Estado, Cargo e Partido

Sem BREVO_API_KEY ou sem destinatário o módulo não envia e não quebra a rodada:
a coleta é o trabalho caro, o relatório é o acabamento.
"""

from __future__ import annotations

import base64
import os
import re
import unicodedata
from collections import Counter, defaultdict
from datetime import datetime, timedelta

# Paleta da marca, a mesma do painel.
MARINHO = (25, 45, 78)
VINHO = (150, 46, 77)
GELO = (244, 243, 239)
BORDA = (218, 218, 212)
SUBTEXTO = (118, 118, 114)
TINTA = (17, 17, 17)

SEM_CARGO = "Cargo não informado"
# Faixa dos grandes números no topo do PDF, em mm.
ALTURA_FAIXA = 18
ALTURA_NUMERO = 6
ALTURA_ROTULO = 4
# Em palavras, não em caracteres: desde 05/08/2026 o clipping sai antes da
# análise do Gemini e cai na legenda crua do post, que às vezes é enorme.
LIMITE_RESUMO_PALAVRAS = 60
LIMITE_TEMAS_PALAVRAS = 20


def _limpo(texto) -> str:
    """Texto de uma célula, sem marcador de lista nem quebra de linha.

    O Gemini devolve resumo e temas em bullets ("*   item"), que servem na
    planilha e atrapalham em parágrafo corrido.
    """
    t = str(texto or "").strip()
    t = re.sub(r"^\s*[\*\-•]\s+", "", t, flags=re.MULTILINE)
    t = re.sub(r"\s*\n+\s*", " ", t)
    return re.sub(r"\s{2,}", " ", t).strip(" -–—·")


def _latin1(texto: str) -> str:
    """O que a fonte padrão do PDF consegue escrever.

    Fonte core do PDF é latin-1, e legenda de Instagram vem cheia de emoji.
    Acento passa; emoji e outros símbolos fora da tabela viram nada, em vez de
    derrubar a geração inteira.
    """
    saida = []
    for ch in texto:
        if ch in "‘’":
            saida.append("'")
        elif ch in "“”":
            saida.append('"')
        elif ch in "–—":
            saida.append("-")
        elif ch == "…":
            saida.append("...")
        else:
            try:
                ch.encode("latin-1")
            except UnicodeEncodeError:
                continue
            if unicodedata.category(ch)[0] != "C" or ch in "\n\t":
                saida.append(ch)
    return "".join(saida)


def _corte(texto: str, limite: int = LIMITE_RESUMO_PALAVRAS) -> str:
    """Corta em `limite` palavras, fechando no fim da última frase que couber."""
    t = _limpo(texto)
    palavras = t.split()
    if len(palavras) <= limite:
        return t
    pedaco = " ".join(palavras[:limite])
    fim = max(pedaco.rfind(". "), pedaco.rfind("! "), pedaco.rfind("? "))
    if fim > len(pedaco) * 0.5:
        return pedaco[:fim + 1]
    return pedaco + "..."


def _inteiro(valor) -> int:
    try:
        return int(float(str(valor).replace(".", "").replace(",", ".")))
    except (TypeError, ValueError):
        return 0


def _data_curta(iso: str) -> str:
    """'2026-08-05T04:26:03.000Z' vira '05/08 01:26'.

    O Apify devolve o timestamp em UTC. Sem os três fusos de diferença, post
    publicado à noite aparece no clipping com data do dia seguinte.
    """
    bruto = str(iso or "")[:19].replace("Z", "")
    for formato, com_hora in (("%Y-%m-%dT%H:%M:%S", True),
                              ("%Y-%m-%d %H:%M:%S", True),
                              ("%Y-%m-%d", False)):
        try:
            dt = datetime.strptime(bruto, formato)
        except ValueError:
            continue
        if com_hora:
            return (dt - timedelta(hours=3)).strftime("%d/%m %H:%M")
        return dt.strftime("%d/%m")
    return bruto[:10]


# ─── Dados dos perfis ─────────────────────────────────────────────────────────

def _grupo(estado: str) -> str:
    """Sigla da UF, ou o nome do bloco quando a coluna Estado não traz uma UF.

    A planilha agrupa os presidenciáveis num bloco "PRESIDENCIÁVEIS", na mesma
    coluna dos estados. Lido como UF, ele entrava na contagem de UFs e o painel
    dizia 17 estados onde havia 16.
    """
    v = str(estado or "").strip()
    if len(v) == 2 and v.isalpha():
        return v.upper()
    return v.capitalize()


def eh_uf(grupo: str) -> bool:
    return len(grupo) == 2 and grupo.isalpha()


def dados_dos_perfis(gc, spreadsheet_id: str, aba: str) -> dict[str, dict]:
    """Estado, cargo e partido de cada pré-candidato, lidos por nome de coluna.

    Por nome, e não por posição, porque a coluna Cargo é nova e vai ser
    acrescentada à mão na planilha de acompanhamento. Enquanto ela não existir,
    o cargo fica vazio e o relatório diz isso em vez de inventar.
    """
    try:
        valores = gc.open_by_key(spreadsheet_id).worksheet(aba).get_all_values()
    except Exception as erro:
        print(f"Aviso: não consegui ler os perfis para o relatório: {erro}")
        return {}
    if not valores:
        return {}
    cab = [c.strip().lower() for c in valores[0]]

    def _idx(*nomes):
        for nome in nomes:
            if nome in cab:
                return cab.index(nome)
        return -1

    i_nome = _idx("pré-candidato", "pre-candidato", "candidato")
    i_uf = _idx("estado", "uf")
    i_cargo = _idx("cargo")
    i_partido = _idx("partido/frente", "partido")
    if i_nome < 0:
        print("Aviso: a aba de perfis não tem coluna de pré-candidato; "
              "o relatório sai sem UF e sem cargo.")
        return {}

    def _cel(linha, i):
        return linha[i].strip() if 0 <= i < len(linha) else ""

    dados = {}
    uf_corrente = ""
    for linha in valores[1:]:
        nome = _cel(linha, i_nome)
        # A coluna Estado é preenchida só na primeira linha de cada UF, como
        # cabeçalho visual do bloco. Lida ao pé da letra, ela deixa sem UF todo
        # perfil que não é o primeiro do estado: eram 577 de 930 posts no teste.
        uf = _grupo(_cel(linha, i_uf))
        if uf:
            uf_corrente = uf
        if nome:
            dados[nome] = {"uf": uf_corrente,
                           "cargo": _cel(linha, i_cargo),
                           "partido": _cel(linha, i_partido)}
    return dados


def montar_posts(salvos: list[dict], perfis: dict[str, dict]) -> list[dict]:
    """Junta o que a rodada gravou com Estado, cargo e partido do perfil."""
    posts = []
    for s in salvos:
        meta = perfis.get(s.get("candidato", ""), {})
        posts.append({
            "candidato": s.get("candidato", ""),
            "uf": meta.get("uf", "") or "Sem UF",
            "cargo": meta.get("cargo", "") or SEM_CARGO,
            "partido": meta.get("partido", ""),
            "tipo": s.get("tipo", ""),
            "publicado": s.get("publicado", ""),
            "curtidas": _inteiro(s.get("curtidas")),
            "comentarios": _inteiro(s.get("comentarios")),
            "resumo": _corte((s.get("legenda") or s.get("resumo_conteudo") or
                              s.get("resumo_legenda") or "").replace("**", "")),
            "temas": _corte(s.get("temas", ""), LIMITE_TEMAS_PALAVRAS),
            "link": s.get("link", ""),
        })
    return posts


def numeros(posts: list[dict]) -> dict:
    """Os grandes números da rodada."""
    return {
        "posts": len(posts),
        "candidatos": len({p["candidato"] for p in posts}),
        # Só sigla de estado: o bloco dos presidenciáveis é grupo, não UF.
        "ufs": len({p["uf"] for p in posts if eh_uf(p["uf"])}),
        "curtidas": sum(p["curtidas"] for p in posts),
        "comentarios": sum(p["comentarios"] for p in posts),
        "por_uf": Counter(p["uf"] for p in posts),
        "por_cargo": Counter(p["cargo"] for p in posts),
        "por_tipo": Counter(p["tipo"] for p in posts),
    }


def _n(valor: int) -> str:
    return f"{valor:,}".replace(",", ".")


def _por_uf_candidato(posts: list[dict]) -> dict:
    agrupado: dict[str, dict[str, list]] = defaultdict(lambda: defaultdict(list))
    for p in posts:
        agrupado[p["uf"]][p["candidato"]].append(p)
    return agrupado


# ─── PDF ──────────────────────────────────────────────────────────────────────

def gerar_pdf(posts: list[dict], num: dict, dia: str) -> bytes:
    """Clipping do dia em PDF: agrupado por temas, e dentro de cada tema por UF."""
    from fpdf import FPDF

    pdf = FPDF(format="A4", unit="mm")
    pdf.set_auto_page_break(auto=True, margin=16)
    pdf.set_margins(16, 14, 16)
    pdf.add_page()
    largura = pdf.w - 32

    pdf.set_font("Helvetica", "B", 20)
    pdf.set_text_color(*MARINHO)
    pdf.cell(0, 9, _latin1("Clipping do Instagram"), new_x="LMARGIN", new_y="NEXT")
    pdf.set_font("Helvetica", "", 10)
    pdf.set_text_color(*SUBTEXTO)
    pdf.cell(0, 6, _latin1(f"Candidatos monitorados · {dia}"),
             new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    # Faixa dos números, do mesmo jeito que eles aparecem no e-mail.
    pdf.set_fill_color(*GELO)
    pdf.set_draw_color(*BORDA)
    y0 = pdf.get_y()
    pdf.rect(16, y0, largura, ALTURA_FAIXA, style="DF")
    caixas = [(_n(num["posts"]), "posts"), (_n(num["candidatos"]), "candidatos"),
              (_n(num["ufs"]), "UFs"), (_n(num["curtidas"]), "curtidas"),
              (_n(num["comentarios"]), "comentários")]
    passo = largura / len(caixas)
    topo = y0 + (ALTURA_FAIXA - ALTURA_NUMERO - ALTURA_ROTULO) / 2
    for i, (valor, rotulo) in enumerate(caixas):
        pdf.set_xy(16 + i * passo, topo)
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(*MARINHO)
        pdf.cell(passo, ALTURA_NUMERO, _latin1(valor), align="C", new_x="LEFT", new_y="NEXT")
        pdf.set_x(16 + i * passo)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*SUBTEXTO)
        pdf.cell(passo, ALTURA_ROTULO, _latin1(rotulo.upper()), align="C")
    pdf.set_y(y0 + ALTURA_FAIXA + 4)

    # Agrupa posts por tema principal
    posts_por_tema = defaultdict(list)
    for p in posts:
        res_leg = p.get("resumo") or ""
        tema = mapear_tema_principal(p.get("temas", ""), res_leg)
        posts_por_tema[tema].append(p)

    temas_ordenados = sorted(
        posts_por_tema.keys(),
        key=lambda t: (-len(posts_por_tema[t]), t)
    )

    # ── Sumário ───────────────────────────────────────────────────────────────
    links = {tema: pdf.add_link() for tema in temas_ordenados}
    pdf.set_font("Helvetica", "B", 9)
    pdf.set_text_color(*SUBTEXTO)
    pdf.cell(0, 5, _latin1("SUMÁRIO"), new_x="LMARGIN", new_y="NEXT")
    pdf.ln(1)
    pdf.set_font("Helvetica", "", 9)
    pdf.set_text_color(*MARINHO)

    for tema in temas_ordenados:
        qtd = len(posts_por_tema[tema])
        plural_post = "posts" if qtd != 1 else "post"
        tema_exibido = tema
        if len(tema_exibido) > 80:
            tema_exibido = tema_exibido[:77].strip() + "..."
        pdf.cell(0, 6, _latin1(f"{tema_exibido} · {qtd} {plural_post}"), link=links[tema],
                 new_x="LMARGIN", new_y="NEXT")
    pdf.ln(4)

    # ── Seções por Tema ───────────────────────────────────────────────────────
    for tema in temas_ordenados:
        posts_do_tema = posts_por_tema[tema]
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(255, 255, 255)
        pdf.set_fill_color(*MARINHO)
        
        # Garante que o cabeçalho do tema não quebre
        if pdf.get_y() > pdf.h - 40:
            pdf.add_page()
            
        pdf.set_link(links[tema], y=pdf.get_y(), page=pdf.page_no())
        
        tema_exibido = tema
        if len(tema_exibido) > 80:
            tema_exibido = tema_exibido[:77].strip() + "..."
            
        pdf.start_section(f"{tema_exibido} ({len(posts_do_tema)} posts)")
        pdf.cell(0, 7, _latin1(f"  {tema_exibido} · {len(posts_do_tema)} post(s)"), fill=True,
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        # Agrupa os posts deste tema por UF
        agrupado_uf = defaultdict(lambda: defaultdict(list))
        for p in posts_do_tema:
            agrupado_uf[p["uf"]][p["candidato"]].append(p)

        for uf in sorted(agrupado_uf.keys()):
            # Subcabeçalho da UF
            pdf.set_font("Helvetica", "B", 9)
            pdf.set_text_color(*SUBTEXTO)
            pdf.cell(0, 5, _latin1(f"UF: {uf}"), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(1)

            por_candidato = agrupado_uf[uf]
            for candidato in sorted(por_candidato):
                do_candidato = por_candidato[candidato]
                meta = do_candidato[0]
                etiqueta = candidato
                if meta["partido"]:
                    etiqueta += f" ({meta['partido']}/{uf})"
                if meta["cargo"] != SEM_CARGO:
                    etiqueta += f" · {meta['cargo']}"

                pdf.set_font("Helvetica", "B", 9)
                pdf.set_text_color(*TINTA)
                pdf.multi_cell(0, 4.5, _latin1(etiqueta), new_x="LMARGIN", new_y="NEXT")
                pdf.ln(1)

                for p in do_candidato:
                    pdf.set_font("Helvetica", "B", 8)
                    pdf.set_text_color(*VINHO)
                    pdf.cell(0, 4, _latin1(
                        f"{_data_curta(p['publicado'])} · {p['tipo']} · "
                        f"{_n(p['curtidas'])} curtidas · {_n(p['comentarios'])} comentários"),
                        new_x="LMARGIN", new_y="NEXT")
                    if p["resumo"]:
                        pdf.set_font("Helvetica", "", 8.5)
                        pdf.set_text_color(*TINTA)
                        pdf.multi_cell(0, 4, _latin1(p["resumo"]),
                                       new_x="LMARGIN", new_y="NEXT")
                    if p["link"]:
                        pdf.set_font("Helvetica", "", 7.5)
                        pdf.set_text_color(*MARINHO)
                        pdf.cell(0, 4, _latin1(p["link"]), link=p["link"],
                                 new_x="LMARGIN", new_y="NEXT")
                    pdf.ln(2)
                pdf.ln(1)
            pdf.ln(1)

    return bytes(pdf.output())


# ─── Classificação temática ───────────────────────────────────────────────────
#
# Até 28/08/2026 a classificação era `if palavra in texto`, substring pura, com
# o primeiro tema da lista que casasse levando o post. Duas falhas somadas:
#
#   1. Substring sem fronteira de palavra. "rio", de Meio Ambiente, casava
#      dentro de comentáRIOs, pRIOridade, inteRIOr, aniversáRIO, secretáRIO,
#      ministéRIO. No clipping de 28/08 isso pôs 155 dos 571 posts em Meio
#      Ambiente, 89 deles só por essa substring, e sobrou 1 post ambiental
#      de verdade em cada 20. Mesma armadilha em "saúd" dentro de saudade,
#      "escol" dentro de escolha, "social" dentro de redes sociais,
#      "solidariedade" dentro do nome do partido Solidariedade.
#   2. Primeiro match vence. Meio Ambiente vinha antes de Saúde e Segurança,
#      então post sobre concurso da PM ou sobre hospital era publicado como
#      ambiental sem nunca chegar a ser testado nos temas certos.
#
# O que existe agora:
#   - texto normalizado (minúscula, sem acento) e hashtag em CamelCase quebrada
#     em palavras, para "#PreservacaoAmbiental" continuar valendo;
#   - casamento por fronteira de palavra, com RADICAIS (casa do começo da
#     palavra em diante: "seguranc" pega segurança e seguranças) e EXATOS
#     (só a palavra inteira: "rio", "pm", "sus");
#   - pontuação em vez de primeiro match: o tema com mais termos distintos
#     vence, e empate cai na ordem da lista, do mais específico ao mais geral;
#   - dois andares. Em ano eleitoral quase todo post fala de campanha, então
#     "Atos de Campanha e Propaganda" e "Alianças e Apoios Políticos" são
#     RESIDUAIS: só levam o post quando nenhum tema de pauta chegou a LIMIAR.
#     Sem isso a lixeira só muda de nome (no teste de 28/08, campanha ficou com
#     253 dos 336 posts);
#   - as tags do Gemini (coluna Temas) valem PESO_TAG contra PESO_LEGENDA da
#     legenda, porque a tag é rótulo do assunto e a legenda é texto solto;
#   - termos FRACOS ("trabalho", "apoio", "agenda") valem uma fração, então
#     levam o post só quando nada mais casou;
#   - ANTIPALAVRAS derrubam o casamento de um termo que virou homônimo, e
#     ANTIFRASES somem do texto antes do casamento (topônimo com "rio").

PESO_TAG = 3.0
PESO_LEGENDA = 1.0
PESO_FORTE = 1.0
PESO_FRACO = 0.35

# Tiradas do texto antes de classificar: nome de lugar com 2+ palavras que bate
# no vocabulário abaixo. São 160, gerados dos 5.570 municípios e das 27 UFs do
# IBGE (localidades/municipios?view=nivelado) cruzados com os termos daqui.
# Sem isso "Nísia Floresta/RN", "Mãe do Rio/PA" e "Rio Branco/AC" viram post de
# meio ambiente. Nome de município de uma palavra só (Floresta/PE) fica de
# fora de propósito: derrubaria a palavra comum junto.
ANTIFRASES = (
    "alianca do tocantins", "alta floresta", "alta floresta d'oeste",
    "alto rio doce", "alto rio novo", "aparecida do rio doce",
    "aparecida do rio negro", "arroio do meio", "barra do rio azul",
    "caicara do rio do vento", "campo do meio", "carmo do rio claro",
    "carmo do rio verde", "chapada da natividade", "chapada de areia",
    "chapada do norte", "chapada dos guimaraes", "chapada gaucha",
    "chapadao do ceu", "chapadao do lageado", "chapadao do sul",
    "conceicao do rio verde", "dario meira", "desterro de entre rios",
    "dores do rio preto", "duas estradas", "entre rios",
    "entre rios de minas", "entre rios do oeste", "entre rios do sul",
    "fazenda rio grande", "flora rica", "floresta azul",
    "floresta do araguaia", "floresta do piaui", "formosa do rio preto",
    "grandes rios", "igarape do meio", "lagoa dos gatos", "lagoa seca",
    "lucas do rio verde", "mae do rio", "nisia floresta",
    "nossa senhora dos remedios", "nova alianca", "nova alianca do ivai",
    "nova floresta", "nova petropolis", "nova ponte", "petrolina de goias",
    "piedade de ponte nova", "piedade do rio grande", "pires do rio",
    "ponte alta", "ponte alta do bom jesus", "ponte alta do norte",
    "ponte alta do tocantins", "ponte branca", "ponte nova", "ponte preta",
    "ponte serrada", "pontes e lacerda", "pontes gestal", "presidente medici",
    "professor jamil", "quatro pontes", "queimada nova", "restinga seca",
    "ribas do rio pardo", "rio acima", "rio azul", "rio bananal", "rio bom",
    "rio bonito", "rio bonito do iguacu", "rio branco", "rio branco do ivai",
    "rio branco do sul", "rio brilhante", "rio casca", "rio claro",
    "rio crespo", "rio da conceicao", "rio das antas", "rio das flores",
    "rio das ostras", "rio das pedras", "rio de contas", "rio de janeiro",
    "rio do antonio", "rio do campo", "rio do fogo", "rio do oeste",
    "rio do pires", "rio do prado", "rio do sul", "rio doce", "rio dos bois",
    "rio dos cedros", "rio dos indios", "rio espera", "rio formoso",
    "rio fortuna", "rio grande", "rio grande da serra", "rio grande do norte",
    "rio grande do piaui", "rio grande do sul", "rio largo", "rio manso",
    "rio maria", "rio negrinho", "rio negro", "rio novo", "rio novo do sul",
    "rio paranaiba", "rio pardo", "rio pardo de minas", "rio piracicaba",
    "rio pomba", "rio preto", "rio preto da eva", "rio quente", "rio real",
    "rio rufino", "rio sono", "rio tinto", "rio verde",
    "rio verde de mato grosso", "rio vermelho", "santa cruz do rio pardo",
    "santa isabel do rio negro", "santana da ponte pensa",
    "santo antonio do rio abaixo", "sao benedito do rio preto",
    "sao goncalo do rio abaixo", "sao goncalo do rio preto",
    "sao joao d'alianca", "sao joao da ponte", "sao joao das duas pontes",
    "sao joao do rio do peixe", "sao jose do rio claro",
    "sao jose do rio pardo", "sao jose do rio preto",
    "sao jose do vale do rio preto", "sao sebastiao do rio preto",
    "sao sebastiao do rio verde", "sao vicente", "sao vicente de minas",
    "sao vicente do serido", "sao vicente do sul", "sao vicente ferrer",
    "saudade do iguacu", "senhora dos remedios", "serra da saudade",
    "tres rios", "vargem grande do rio pardo", "vicente dutra",
    "visconde do rio branco", "vitor meireles",
)


# Pontuação mínima para um tema de pauta ganhar do residual. 1.0 é um termo
# forte na legenda, ou qualquer termo na tag do Gemini; termo fraco solto na
# legenda (0,35) não basta.
LIMIAR_PAUTA = 1.0

# Tema estreito: quando o vocabulário dele aparece, é ele a notícia, mesmo que
# um tema largo cite mais termos. Sem isso, post de delegacia de proteção
# animal ia para Segurança Pública porque "segurança" e "delegacia" somam mais
# que "animal" e "maus-tratos".
PESO_TEMA = {
    "Causa Animal": 1.6,
    "Habitação": 1.3,
    "Pesquisa Eleitoral": 1.3,
}

# (tema, radicais, exatos, fracos, antipalavras). A ordem é o critério de
# desempate: do assunto mais específico para o mais genérico. TEMAS_RESIDUAIS
# vem depois, e só entra se nenhum tema de pauta bater LIMIAR_PAUTA.
TEMAS_MAPEAMENTO = [
    ("Causa Animal",
     ["veterinar", "castrac"],
     ["animal", "animais", "pet", "pets", "cachorro", "cachorros", "gato",
      "gatos", "zoonose", "zoonoses", "maus-tratos", "adocao de animais",
      "protetor de animais", "protetores de animais", "canil", "abrigo animal"],
     [],
     []),

    ("Pesquisa Eleitoral",
     ["pesquisa"],
     ["datafolha", "quaest", "ipec", "atlasintel", "ipsos", "real time big data",
      "intencao de voto", "intencoes de voto", "levantamento", "sondagem",
      "amostra", "margem de erro", "primeiro turno", "segundo turno"],
     [],
     ["pesquisador", "pesquisadora", "pesquisadores", "pesquisando"]),

    ("Saúde",
     ["saud", "medic", "hospital", "vacin", "enferm", "ambulator", "cirurg"],
     ["upa", "ubs", "sus", "samu", "postinho", "posto de saude", "remedio",
      "remedios", "dengue", "leito", "leitos", "consulta", "consultas",
      "atendimento medico", "saude mental", "farmacia"],
     [],
     # "saudade" e "saudação" não são saúde; "medida" e "medieval" não são médico.
     ["saudade", "saudades", "saudacao", "saudacoes", "saudar", "saudou",
      "saudamos", "saudoso", "saudosa", "hospitalidade", "hospitaleiro",
      "hospitaleira"]),

    ("Educação",
     ["educac", "profess", "alun", "ensin", "universi", "estudant", "matricul",
      "creche", "analfabet"],
     ["escola", "escolas", "escolar", "escolares", "fundeb", "enem", "aula",
      "aulas", "merenda", "merenda escolar", "tempo integral", "educacao",
      "faculdade", "bolsa de estudo", "bolsas de estudo", "vestibular",
      "alfabetizacao"],
     [],
     []),

    ("Segurança Pública",
     ["seguranc", "polic", "violen", "crimin", "delegac", "penitenciar",
      "presidi", "homicid"],
     ["crime", "crimes", "pm", "policia civil", "policia militar",
      "guarda municipal", "trafico", "faccao", "faccoes", "bandido",
      "bandidos", "assalto", "assaltos", "roubo", "roubos", "bombeiros",
      "camera corporal", "cameras corporais", "porte de arma"],
     [],
     ["seguranca alimentar", "seguranca juridica"]),

    ("Habitação",
     ["habitac", "moradi"],
     ["minha casa minha vida", "casa propria", "regularizacao fundiaria",
      "titulo de posse", "aluguel", "deficit habitacional",
      "conjunto habitacional", "sem-teto"],
     [],
     []),

    ("Meio Ambiente",
     ["ambient", "sustentab", "sustentav", "climat", "desmat", "poluic",
      "recicl", "reflorest", "preservac", "ecolog"],
     ["meio ambiente", "arvore", "arvores", "floresta", "florestas",
      "nascente", "nascentes", "bioma", "biodiversidade",
      "incendio florestal", "licenciamento ambiental", "recursos hidricos",
      "crise hidrica", "energia solar", "energia eolica", "energia limpa",
      "energia renovavel", "credito de carbono", "cop30", "cop 30",
      "residuos", "lixao", "desastre ambiental", "desastre natural",
      "el nino", "estiagem"],
     # Peso fraco: sozinhos não seguram o post. "Rio", "Queimadas" e "Floresta"
     # também são nome de município de uma palavra, que a lista de ANTIFRASES
     # não pega; bioma é lugar tanto quanto assunto ("cultura ribeirinha da
     # Amazônia"); e "clima" costuma ser clima de campanha.
     ["rio", "rios", "clima", "seca", "queimada", "queimadas", "amazonia",
      "cerrado", "pantanal", "caatinga", "fauna", "flora"],
     ["ambiente de trabalho", "ambiente familiar", "climatizacao",
      "climatizado", "climatizados"]),

    ("Agricultura e Abastecimento",
     ["agricultur", "agroneg", "agropecu", "pecuar", "irrigac"],
     ["agro", "rural", "rurais", "safra", "abastecimento", "feirante",
      "feirantes", "produtor rural", "produtores rurais",
      "agricultura familiar", "plantio", "colheita", "trator", "tratores",
      "assentamento", "cooperativa", "cooperativas"],
     [],
     []),

    ("Infraestrutura e Obras",
     ["infraestrutur", "paviment", "asfalt", "saneament", "duplicac"],
     ["obra", "obras", "estrada", "estradas", "rodovia", "rodovias", "ponte",
      "pontes", "calcamento", "mobilidade urbana", "transporte",
      "transporte publico", "transito", "viaduto", "metro", "aeroporto",
      "esgoto", "iluminacao publica", "energia eletrica", "internet",
      "banda larga"],
     [],
     ["obrigado", "obrigada"]),

    ("Assistência Social",
     ["vulnerab", "assistencia social"],
     ["pobreza", "fome", "bolsa familia", "cras", "creas", "cesta basica",
      "cestas basicas", "doacao", "doacoes", "asilo", "auxilio",
      "inclusao social", "programa social", "programas sociais",
      "politica social", "politicas sociais", "seguranca alimentar",
      "pessoa com deficiencia", "acolhimento", "miseria", "extrema pobreza"],
     ["comunidade", "comunidades", "idosos"],
     []),

    ("Gestão e Transparência",
     ["transparenc", "corrup", "desburocratiz", "digitaliz", "licitac"],
     ["governo digital", "servidor publico", "servidores publicos",
      "concurso publico", "prestacao de contas", "tribunal de contas",
      "auditoria", "nepotismo", "folha de pagamento", "reforma administrativa"],
     # O Gemini marca "Gestão pública" em quase todo post de quem governa:
     # peso fraco, para não roubar o post que tem assunto de verdade.
     ["gestao", "gestao publica", "eficiencia"],
     []),

    ("Cultura, Lazer e Esporte",
     ["cultur", "esport", "turism", "music", "artist"],
     ["show", "shows", "lazer", "festa", "festas", "festival", "carnaval",
      "museu", "teatro", "cinema", "patrimonio", "praca", "futebol", "atleta",
      "atletas", "biblioteca", "ginasio", "quadra", "quadras"],
     ["parque", "parques"],
     []),

    ("Economia, Trabalho e Renda",
     ["econom", "desemprego", "industri", "comerci", "salari", "empreendedor"],
     ["emprego", "empregos", "imposto", "impostos", "icms", "renda", "6x1",
      "escala 6x1", "inflacao", "tarifa", "tarifas", "mei", "juros", "pib",
      "credito", "geracao de emprego", "geracao de empregos", "carteira assinada",
      "custo de vida", "investimento", "investimentos"],
     ["trabalho", "trabalhador", "trabalhadores", "trabalhar"],
     ["comercial", "comerciais"]),

    ("Alianças e Apoios Políticos",
     ["alianc", "coliga", "federac"],
     ["rompimento", "vice-governador", "chapa majoritaria", "apoio politico",
      "apoios politicos", "dobradinha", "filiacao", "palanque",
      "dividir palanque", "puxador de voto"],
     # Fora de propósito: "apoio" solto (em campanha todo post fala do apoio da
     # rua) e "partido"/"vice"/"chapa" (citar sigla não é notícia de aliança).
     [],
     []),

]

TEMAS_RESIDUAIS = [
    ("Atos de Campanha e Propaganda",
     ["campanha", "comici", "carreata", "panflet", "propaganda", "adesivac",
      "eleic", "eleitor", "eleit", "militan"],
     ["jingle", "convencao", "passeata", "santinho", "horario eleitoral",
      "programa eleitoral", "urna", "urnas", "voto", "votos", "votar",
      "caminhada", "bandeiraco", "debate", "sabatina", "guia eleitoral",
      "numero na urna"],
     ["agenda", "evento", "eventos", "aniversario", "visita", "reuniao"],
     []),
]


FALLBACK_TEMA = "Outros Assuntos"


def _normalizar(texto: str) -> str:
    """Minúscula, sem acento, com hashtag em CamelCase quebrada em palavras.

    "#PreservacaoAmbiental" vira "preservacao ambiental": sem isso a fronteira
    de palavra perderia o assunto que só aparece na hashtag, que em legenda de
    Instagram é metade do conteúdo.
    """
    t = str(texto or "")
    t = re.sub(r"#(\w+)", lambda m: " " + re.sub(r"(?<=[a-zà-ÿ0-9])(?=[A-ZÀ-Þ])", " ", m.group(1)) + " ", t)
    t = unicodedata.normalize("NFD", t.lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"\s+", " ", t)


def _padrao(termo: str, radical: bool) -> re.Pattern:
    corpo = re.escape(termo).replace(r"\ ", r"[\s\-]+")
    sufixo = r"[a-z0-9]*" if radical else ""
    return re.compile(rf"(?<![a-z0-9]){corpo}{sufixo}(?![a-z0-9])")


# (tema, [(padrão, termo, peso)], antipalavras) já compilado no import.
def _compilar(tabela: list[tuple]) -> list[tuple]:
    compilado = []
    for tema, radicais, exatos, fracos, antipalavras in tabela:
        termos = ([(_padrao(t, True), t, PESO_FORTE) for t in radicais]
                  + [(_padrao(t, False), t, PESO_FORTE) for t in exatos]
                  + [(_padrao(t, False), t, PESO_FRACO) for t in fracos])
        compilado.append((tema, termos, tuple(_normalizar(a) for a in antipalavras)))
    return compilado


PAUTAS_COMPILADAS = _compilar(TEMAS_MAPEAMENTO)
RESIDUAIS_COMPILADOS = _compilar(TEMAS_RESIDUAIS)


def _pontuar(texto: str, termos: list, antipalavras: tuple) -> tuple[float, list[str]]:
    """Soma o peso dos termos distintos que casam, ignorando os homônimos."""
    if not texto:
        return 0.0, []
    total = 0.0
    achados = []
    for padrao, termo, peso in termos:
        casou = False
        for m in padrao.finditer(texto):
            trecho = m.group(0)
            janela = texto[max(0, m.start() - 24):m.end() + 24]
            if trecho in antipalavras or any(a in janela for a in antipalavras if " " in a):
                continue
            casou = True
            break
        if casou:
            total += peso
            achados.append(termo)
    return total, achados


def _melhor_tema(tabela: list[tuple], tags: str, legenda: str) -> tuple:
    """(pontos, tema, termos) do tema mais pontuado da tabela."""
    melhor = (0.0, None, [])
    for tema, termos, antipalavras in tabela:
        p_tag, t_tag = _pontuar(tags, termos, antipalavras)
        _, t_leg = _pontuar(legenda, termos, antipalavras)
        # Termo que aparece nos dois campos conta uma vez, pelo peso da tag.
        pontos = p_tag * PESO_TAG + PESO_LEGENDA * sum(
            peso for _, termo, peso in termos
            if termo in t_leg and termo not in t_tag)
        pontos *= PESO_TEMA.get(tema, 1.0)
        if pontos > melhor[0]:
            melhor = (pontos, tema, sorted(set(t_tag + t_leg)))
    return melhor


def mapear_tema_principal(temas_str: str, legenda_str: str,
                          detalhar: bool = False):
    """Classifica o post em uma das grandes pautas.

    Primeiro os temas de pauta: vence o de maior pontuação, e empate cai na
    ordem de TEMAS_MAPEAMENTO. Só se nenhum deles chegar a LIMIAR_PAUTA o post
    desce para os residuais (campanha, alianças). Sem nenhum termo em lugar
    nenhum ele vai para "Outros Assuntos", que é o lugar honesto de quem não
    casou, e não um tema qualquer que casou por acidente.

    Com `detalhar=True` devolve (tema, pontos, termos que casaram), que é como
    se audita uma classificação estranha sem reler o clipping inteiro.
    """
    tags = _normalizar(temas_str)
    legenda = _normalizar(legenda_str)
    for frase in ANTIFRASES:
        tags = tags.replace(frase, " ")
        legenda = legenda.replace(frase, " ")

    melhor = _melhor_tema(PAUTAS_COMPILADAS, tags, legenda)
    if melhor[0] < LIMIAR_PAUTA:
        residual = _melhor_tema(RESIDUAIS_COMPILADOS, tags, legenda)
        if residual[0] > 0:
            melhor = residual

    tema = melhor[1] or FALLBACK_TEMA
    if detalhar:
        return tema, round(melhor[0], 2), melhor[2]
    return tema


# ─── E-mail ───────────────────────────────────────────────────────────────────

def html_email(posts: list[dict], num: dict, dia: str, nome_pdf: str,
               planilha_url: str) -> str:
    """Corpo do e-mail: o clipping com posts agrupados por temas e UFs."""
    # Agrupa posts por tema
    posts_por_tema = defaultdict(list)
    for p in posts:
        res_leg = p.get("resumo") or ""
        tema = mapear_tema_principal(p.get("temas", ""), res_leg)
        posts_por_tema[tema].append(p)

    # Ordena os temas pela quantidade de posts (descrescente), depois alfabeticamente
    temas_ordenados = sorted(
        posts_por_tema.keys(),
        key=lambda t: (-len(posts_por_tema[t]), t)
    )

    # Helper para gerar âncora a partir do nome do tema
    def _slug(tema: str) -> str:
        s = tema.lower()
        s = re.sub(r"[ãáâä]", "a", s)
        s = re.sub(r"[ẽéêë]", "e", s)
        s = re.sub(r"[ĩíîï]", "i", s)
        s = re.sub(r"[õóôö]", "o", s)
        s = re.sub(r"[ũúûü]", "u", s)
        s = re.sub(r"[ç]", "c", s)
        s = re.sub(r"[^a-z0-9]", "-", s)
        return s.strip("-")

    # Gera o sumário clicável
    itens_sumario = []
    for tema in temas_ordenados:
        qtd = len(posts_por_tema[tema])
        plural_post = "posts" if qtd != 1 else "post"
        itens_sumario.append(
            f"<li><a href='#{_slug(tema)}' style='color:#192D4E;text-decoration:none;font-weight:bold;'>"
            f"{tema} ({qtd} {plural_post})</a></li>"
        )
    sumario_html = (
        "<div style='margin: 0 0 20px 0; background: #f6f7fa; border: 1px solid #da8093; padding: 12px; border-radius: 4px; border-color: #dadad4;'>"
        "<h4 style='margin: 0 0 8px 0; color: #192D4E; font-size: 14px;'>Sumário</h4>"
        "<ul style='margin: 0; padding-left: 20px; font-size: 13px; line-height: 1.6;'>"
        f"{''.join(itens_sumario)}"
        "</ul>"
        "</div>"
    )

    # Helper para formatar cada post
    def _bloco_post_html(p: dict) -> str:
        ident = f"{p['candidato']}"
        if p["partido"]:
            ident += f" ({p['partido']}/{p['uf']})"
        if p["cargo"] and p["cargo"] != SEM_CARGO:
            ident += f" · {p['cargo']}"

        tipo_str = "Vídeo" if p["tipo"] == "Vídeo" else "Foto"
        likes_str = f"{_n(p['curtidas'])} curtidas"
        comments_str = f"{_n(p['comentarios'])} comentários"
        data_str = _data_curta(p["publicado"])
        meta_info = f"{data_str} · {tipo_str} · {likes_str} · {comments_str}"
        resumo = p["resumo"] or ""

        link_html = ""
        if p["link"] and p["link"].startswith("http"):
            link_html = f'<a href="{p["link"]}" style="color:#192D4E;font-size:12px;font-weight:bold;text-decoration:none">abrir o post</a>'

        return f"""
        <div style="border-left:3px solid #192D4E;padding:8px 12px;margin:0 0 14px 0;background:#f6f7fa">
            <strong style="color:#192D4E">{ident}</strong>
            <div style="color:#6b7280;font-size:12px;margin:2px 0 6px 0">{meta_info}</div>
            {f'<div style="font-size:13px;margin:0 0 6px 0">{resumo}</div>' if resumo else ''}
            {link_html}
        </div>"""

    # Monta as seções por tema
    secoes_temas = []
    for tema in temas_ordenados:
        qtd = len(posts_por_tema[tema])
        plural_post = "posts" if qtd != 1 else "post"

        # Agrupa os posts deste tema por UF
        posts_por_uf = defaultdict(list)
        for p in posts_por_tema[tema]:
            posts_por_uf[p["uf"]].append(p)

        # Gera os blocos do tema, separados por UF
        blocos_uf = []
        for uf in sorted(posts_por_uf.keys()):
            blocos_uf.append(
                f"<div style='margin: 12px 0 6px 0;'>"
                f"<strong style='color:#767672; font-size:12px; text-transform:uppercase;'>{uf}</strong>"
                f"</div>"
            )
            for p in posts_por_uf[uf]:
                blocos_uf.append(_bloco_post_html(p))

        secoes_temas.append(
            f"<div style='margin-bottom: 24px;'>"
            f"<a name='{_slug(tema)}'></a>"
            f"<h3 style='margin:20px 0 8px 0; font-size:16px; color:#192D4E; border-bottom:1px solid #e5e7eb; padding-bottom:4px;'>"
            f"{tema} ({qtd} {plural_post})</h3>"
            f"{''.join(blocos_uf)}"
            f"</div>"
        )

    kpis = "".join(
        f"<td style='padding:0 14px 0 0'>"
        f"<div style='font-size:22px;font-weight:700;color:#192D4E'>{_n(v)}</div>"
        f"<div style='font-size:11px;color:#767672;text-transform:uppercase'>{r}</div></td>"
        for v, r in [(num["posts"], "posts"), (num["candidatos"], "candidatos"),
                     (num["ufs"], "UFs"), (num["curtidas"], "curtidas"),
                     (num["comentarios"], "comentários")]
    )

    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#111;line-height:1.4;">
      <h2 style="margin:0 0 4px 0">Clipping do Instagram</h2>
      <div style="color:#374151;margin:0 0 14px 0">{dia}</div>

      <div style="background:#eef0f6;border-left:3px solid #192D4E;padding:12px;margin:0 0 18px 0">
        <strong style="color:#192D4E">Clipping do dia em anexo:</strong>
        <span style="font-family:monospace">{nome_pdf}</span>
        <div style="font-size:13px;color:#374151;margin-top:6px">
          O clipping em PDF está anexado com a diagramação para impressão.
          Os posts estão listados abaixo por tema e UF.
        </div>
      </div>

      {sumario_html}

      {"".join(secoes_temas)}

      <hr style="border:0;border-top:1px solid #e5e7eb;margin:24px 0;" />

      <h3 style="margin:0 0 12px 0;font-size:14px;color:#192D4E">Grandes Números da Rodada</h3>
      <table style="border-collapse:collapse;margin:0 0 18px 0"><tr>{kpis}</tr></table>

      <p style="font-size:13px;color:#374151;margin:22px 0 0">
        Os posts detalhados e o histórico ficam salvos na aba <b>Resultados</b> da
        <a href="{planilha_url}" style="color:#192D4E">planilha de mapeamento</a>.
      </p>
    </body></html>
    """


def _destinatarios() -> list[str]:
    bruto = os.getenv("DESTINATARIOS_INSTAGRAM") or os.getenv("DESTINATARIOS", "")
    return [e.strip(" <>") for e in re.split(r"[,;\s]+", bruto) if "@" in e]


def enviar(assunto: str, html: str, pdf: bytes, nome_pdf: str) -> None:
    """Mesmo caminho do alerta de pesquisas (Brevo), com o PDF anexado."""
    api_key, remetente = os.getenv("BREVO_API_KEY"), os.getenv("EMAIL")
    dests = _destinatarios()
    if not (api_key and remetente and dests):
        print("Relatório do Instagram: config de e-mail incompleta; pulando envio.")
        return
    from brevo_python import ApiClient, Configuration
    from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
    from brevo_python.models.send_smtp_email import SendSmtpEmail

    cfg = Configuration()
    cfg.api_key["api-key"] = api_key
    api = TransactionalEmailsApi(ApiClient(configuration=cfg))
    anexo = [{"content": base64.b64encode(pdf).decode(), "name": nome_pdf}]
    for dest in dests:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}], sender={"email": remetente},
                subject=assunto, html_content=html, attachment=anexo))
            print(f"Relatório do Instagram enviado para {dest}")
        except Exception as erro:
            print(f"Relatório do Instagram: falha para {dest}: {erro}")


def posts_gravados_no_dia(gc, spreadsheet_id: str, aba: str, dia: str) -> list[dict]:
    """Relê da aba de resultados o que foi gravado num dia.

    Serve para refazer ou reenviar o relatório sem chamar a Apify de novo, que
    é a parte cara da rodada. O filtro é pela coluna Data/Hora, o carimbo de
    gravação, e não pela data de publicação: o clipping é do que entrou hoje.
    """
    valores = gc.open_by_key(spreadsheet_id).worksheet(aba).get_all_values()
    if len(valores) < 2:
        return []
    cab = valores[0]

    def _i(*nomes):
        for nome in nomes:
            if nome in cab:
                return cab.index(nome)
        return -1

    i_quando, i_cand = _i("Data/Hora"), _i("Candidato", "Pré-candidato")
    if i_quando < 0 or i_cand < 0:
        return []
    campos = {"link": _i("Link"), "usuario": _i("Usuário"), "tipo": _i("Tipo"),
              "publicado": _i("Data de publicação"), "curtidas": _i("Curtidas"),
              "comentarios": _i("Comentários"), "legenda": _i("Legenda"),
              "resumo_conteudo": _i("Resumo do conteúdo"),
              "resumo_legenda": _i("Resumo da legenda"), "temas": _i("Temas")}

    def _cel(linha, i):
        return linha[i] if 0 <= i < len(linha) else ""

    return [dict({"candidato": _cel(l, i_cand)},
                 **{k: _cel(l, i) for k, i in campos.items()})
            for l in valores[1:] if _cel(l, i_quando)[:10] == dia]


def enviar_relatorio(salvos: list[dict], gc, spreadsheet_id_perfis: str,
                     aba_perfis: str, spreadsheet_id_resultados: str) -> None:
    """Ponto de entrada chamado no fim da rodada.

    Não levanta exceção: a coleta é o trabalho caro e já está gravada quando
    isto roda. Falha aqui vira aviso no log.
    """
    try:
        if not salvos:
            print("Nenhum post novo na rodada; sem e-mail de relatório.")
            return
        perfis = dados_dos_perfis(gc, spreadsheet_id_perfis, aba_perfis)
        posts = montar_posts(salvos, perfis)
        num = numeros(posts)
        dia = datetime.now().strftime("%d/%m/%Y")
        nome_pdf = f"clipping_instagram_{datetime.now():%Y-%m-%d}.pdf"
        pdf = gerar_pdf(posts, num, dia)
        html = html_email(posts, num, dia, nome_pdf,
                          f"https://docs.google.com/spreadsheets/d/{spreadsheet_id_resultados}/edit")
        enviar(f"Clipping do Instagram · {dia} · {num['posts']} post(s)",
               html, pdf, nome_pdf)
    except Exception as erro:
        print(f"Aviso: falha ao montar ou enviar o relatório do Instagram: {erro}")
