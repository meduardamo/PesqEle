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
            "resumo": _corte(s.get("resumo_conteudo") or s.get("resumo_legenda") or
                             s.get("legenda") or ""),
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


# ─── E-mail ───────────────────────────────────────────────────────────────────

def mapear_tema_principal(temas_str: str, legenda_str: str) -> str:
    """Classifica um post em uma das grandes pautas temáticas a partir de suas tags."""
    t = (temas_str or "").lower()
    l = (legenda_str or "").lower()

    if "pesquisa" in t or "pesquisa" in l:
        return "Pesquisa Eleitoral"
    if any(k in t for k in ("alian", "rompimento")):
        return "Alianças e Apoios Políticos"
    if "saúd" in t or "saúd" in l:
        return "Saúde"
    if any(k in t or k in l for k in ("seguran", "políc", "polic", "crim", "violên", "violen")):
        return "Segurança Pública"
    if any(k in t or k in l for k in ("educa", "escola", "creche", "profess", "alun", "ensino", "universi", "fundeb")):
        return "Educação"
    if any(k in t or k in l for k in ("infra", "obra", "saneamento", "estrada", "ponte", "asfalt", "habita", "moradia", "minha casa")):
        return "Infraestrutura e Obras"
    if any(k in t or k in l for k in ("econom", "emprego", "trabalh", "impost", "indústri", "industri", "comérci", "comerci", "salár", "salar", "6x1")):
        return "Economia, Trabalho e Renda"
    if any(k in t or k in l for k in ("campanha", "jingle", "elei", "conven", "comíci", "comici", "carreata", "passeata", "comício", "palanque", "propaganda")):
        return "Atos de Campanha e Propaganda"

    # Tenta usar o primeiro tema específico da lista se houver
    temas_lista = [x.strip("*-• ") for x in temas_str.replace("\n", ",").split(",") if x.strip()]
    if temas_lista:
        return temas_lista[0].capitalize()
    return "Outros Assuntos"


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
