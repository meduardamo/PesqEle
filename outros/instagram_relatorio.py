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
LIMITE_RESUMO = 320


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


def _corte(texto: str, limite: int = LIMITE_RESUMO) -> str:
    """Corta no fim da última frase que couber, e não no meio da palavra."""
    t = _limpo(texto)
    if len(t) <= limite:
        return t
    pedaco = t[:limite]
    fim = max(pedaco.rfind(". "), pedaco.rfind("! "), pedaco.rfind("? "))
    if fim > limite * 0.5:
        return pedaco[:fim + 1]
    return pedaco.rsplit(" ", 1)[0] + "..."


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
        uf = _cel(linha, i_uf).upper()
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
            "temas": _corte(s.get("temas", ""), 140),
            "link": s.get("link", ""),
        })
    return posts


def numeros(posts: list[dict]) -> dict:
    """Os grandes números da rodada."""
    return {
        "posts": len(posts),
        "candidatos": len({p["candidato"] for p in posts}),
        "ufs": len({p["uf"] for p in posts}),
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
    """Clipping do dia: uma seção por UF, um bloco por post."""
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
    pdf.cell(0, 6, _latin1(f"Pré-candidatos monitorados · {dia}"),
             new_x="LMARGIN", new_y="NEXT")
    pdf.ln(3)

    # Faixa dos números, do mesmo jeito que eles aparecem no e-mail.
    pdf.set_fill_color(*GELO)
    pdf.set_draw_color(*BORDA)
    y0 = pdf.get_y()
    pdf.rect(16, y0, largura, 18, style="DF")
    caixas = [(_n(num["posts"]), "posts"), (_n(num["candidatos"]), "candidatos"),
              (_n(num["ufs"]), "UFs"), (_n(num["curtidas"]), "curtidas"),
              (_n(num["comentarios"]), "comentários")]
    passo = largura / len(caixas)
    for i, (valor, rotulo) in enumerate(caixas):
        pdf.set_xy(16 + i * passo, y0 + 3)
        pdf.set_font("Helvetica", "B", 13)
        pdf.set_text_color(*MARINHO)
        pdf.cell(passo, 6, _latin1(valor), align="C", new_x="LEFT", new_y="NEXT")
        pdf.set_x(16 + i * passo)
        pdf.set_font("Helvetica", "", 7)
        pdf.set_text_color(*SUBTEXTO)
        pdf.cell(passo, 4, _latin1(rotulo.upper()), align="C")
    pdf.set_y(y0 + 22)

    for uf in sorted(_por_uf_candidato(posts)):
        por_candidato = _por_uf_candidato(posts)[uf]
        total_uf = sum(len(v) for v in por_candidato.values())
        pdf.set_font("Helvetica", "B", 11)
        pdf.set_text_color(255, 255, 255)
        pdf.set_fill_color(*MARINHO)
        pdf.cell(0, 7, _latin1(f"  {uf} · {total_uf} post(s)"), fill=True,
                 new_x="LMARGIN", new_y="NEXT")
        pdf.ln(2)

        for candidato in sorted(por_candidato):
            do_candidato = por_candidato[candidato]
            meta = do_candidato[0]
            etiqueta = candidato
            if meta["partido"]:
                etiqueta += f" ({meta['partido']}/{uf})"
            # Cargo na mesma linha do nome, e nada quando a coluna está vazia:
            # "Cargo não informado" embaixo de cada bloco só ocupava espaço.
            if meta["cargo"] != SEM_CARGO:
                etiqueta += f" · {meta['cargo']}"
            pdf.set_font("Helvetica", "B", 10)
            pdf.set_text_color(*TINTA)
            pdf.multi_cell(0, 5, _latin1(etiqueta), new_x="LMARGIN", new_y="NEXT")
            pdf.ln(1)

            for p in do_candidato:
                pdf.set_font("Helvetica", "B", 8)
                pdf.set_text_color(*VINHO)
                pdf.cell(0, 4.5, _latin1(
                    f"{_data_curta(p['publicado'])} · {p['tipo']} · "
                    f"{_n(p['curtidas'])} curtidas · {_n(p['comentarios'])} comentários"),
                    new_x="LMARGIN", new_y="NEXT")
                if p["resumo"]:
                    pdf.set_font("Helvetica", "", 9)
                    pdf.set_text_color(*TINTA)
                    pdf.multi_cell(0, 4.4, _latin1(p["resumo"]),
                                   new_x="LMARGIN", new_y="NEXT")
                if p["link"]:
                    pdf.set_font("Helvetica", "", 8)
                    pdf.set_text_color(*MARINHO)
                    pdf.cell(0, 4.5, _latin1(p["link"]), link=p["link"],
                             new_x="LMARGIN", new_y="NEXT")
                pdf.ln(2.5)
            pdf.ln(1)

    return bytes(pdf.output())


# ─── E-mail ───────────────────────────────────────────────────────────────────

def html_email(posts: list[dict], num: dict, dia: str, nome_pdf: str,
               planilha_url: str) -> str:
    """Corpo do e-mail: o clipping em cima, os números embaixo.

    Sem lista de post nenhum aqui de propósito: quem quer o post abre o PDF, e
    e-mail com 80 blocos vira rolagem infinita no celular.
    """
    def _tabela(titulo, contagem, rotulo_col):
        linhas = "".join(
            f"<tr><td style='padding:5px 10px;border-top:1px solid #e5e7eb'>{chave}</td>"
            f"<td style='padding:5px 10px;border-top:1px solid #e5e7eb;text-align:right'>"
            f"{_n(valor)}</td></tr>"
            for chave, valor in sorted(contagem.items(), key=lambda kv: (-kv[1], kv[0]))
        )
        return (f"<h3 style='margin:20px 0 6px 0;font-size:14px;color:#192D4E'>{titulo}</h3>"
                "<table style='width:100%;border-collapse:collapse;font-size:14px'>"
                "<tr style='text-align:left;background:#f3f4f6'>"
                f"<th style='padding:5px 10px'>{rotulo_col}</th>"
                "<th style='padding:5px 10px;text-align:right'>Posts</th></tr>"
                f"{linhas}</table>")

    cargo = ("<p style='font-size:13px;color:#767672;margin:6px 0 0'>"
             "O cargo sai do que estiver na coluna <b>Cargo</b> da aba de perfis. "
             "As linhas sem cargo preenchido aparecem como "
             f"\"{SEM_CARGO}\".</p>"
             if SEM_CARGO in num["por_cargo"] else "")

    kpis = "".join(
        f"<td style='padding:0 14px 0 0'>"
        f"<div style='font-size:22px;font-weight:700;color:#192D4E'>{_n(v)}</div>"
        f"<div style='font-size:11px;color:#767672;text-transform:uppercase'>{r}</div></td>"
        for v, r in [(num["posts"], "posts"), (num["candidatos"], "candidatos"),
                     (num["ufs"], "UFs"), (num["curtidas"], "curtidas"),
                     (num["comentarios"], "comentários")]
    )

    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#111">
      <h2 style="margin:0 0 4px 0">Clipping do Instagram</h2>
      <div style="color:#374151;margin:0 0 14px 0">{dia}</div>
      <div style="background:#eef0f6;border-left:3px solid #192D4E;padding:12px;margin:0 0 18px 0">
        <strong style="color:#192D4E">Clipping do dia em anexo:</strong>
        <span style="font-family:monospace">{nome_pdf}</span>
        <div style="font-size:13px;color:#374151;margin-top:6px">
          Uma seção por UF e, dentro dela, cada pré-candidato com os posts do
          período: data, tipo, curtidas, comentários, resumo e link.
        </div>
      </div>
      <table style="border-collapse:collapse;margin:0 0 4px 0"><tr>{kpis}</tr></table>
      {_tabela("Posts por UF", num["por_uf"], "UF")}
      {_tabela("Posts por cargo", num["por_cargo"], "Cargo")}
      {cargo}
      <p style="font-size:13px;color:#374151;margin:22px 0 0">
        Os posts ficam na aba <b>Resultados</b> da
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
