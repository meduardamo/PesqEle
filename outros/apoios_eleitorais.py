"""
Monitoramento de apoios entre candidaturas nas eleicoes de 2026.

Cada linha da base e UMA RELACAO DIRECIONADA: alguem (pessoa, partido, lideranca,
movimento ou grupo) apoia, negocia apoio, rompe ou se opoe a uma candidatura.
"A apoia B" e diferente de "B apoia A": apoio mutuo vira DUAS linhas, nunca uma.

A saida e uma lista de arestas pronta pra virar grafo de rede com seta do apoiador
pro apoiado, e tambem pra ser lida como tabela pesquisavel.

Uso:
  python -m outros.apoios_eleitorais
  python -m outros.apoios_eleitorais --max-linhas 30
  python -m outros.apoios_eleitorais --dry-run
  python -m outros.apoios_eleitorais --force        # reconsulta quem ja tem relacao gravada
  python -m outros.apoios_eleitorais --incluir-sem-convencao   # ignora o filtro de convencao

Secrets/env:
  GOOGLE_CREDENTIALS_JSON
  GEMINI_API_KEY
  SPREADSHEET_ID_CONVENCOES   (origem: aba de convencoes)
  SPREADSHEET_ID_TSE          (destino: planilha "Candidaturas 2026 (TSE + Noticias)")
  CONVENCOES_ABA  (origem dos nomes, padrao "Convenções partidárias")
  APOIOS_ABA      (destino, padrao "apoio por candidatura")
  SPREADSHEET_ID_POLLINGDATA  (matriz T1, de onde saem os nomes do Senado)

Por padrao so processa candidato cuja convencao JA ACONTECEU (Data convenção <=
hoje na aba de origem): e na convencao que a alianca vira fato registrado.
"""

import argparse
import html as html_mod
import json
import os
import re
import time
import unicodedata
import urllib.parse
import xml.etree.ElementTree as ET
from datetime import datetime, timedelta, timezone

import gspread
import requests
from googlenewsdecoder import gnewsdecoder
from newspaper import Article
from google import genai
from google.genai import types
from google.oauth2.service_account import Credentials
from compartilhado.relatorios_sheets_utils import autorizar_com_retry as _autorizar


BRT = timezone(timedelta(hours=-3))
GEMINI_MODEL = os.getenv("GEMINI_MODEL_APOIOS", "gemini-3.6-flash")
ABA_ORIGEM_PADRAO = "Convenções partidárias"
ABA_DESTINO_PADRAO = "apoio por candidatura"
ABA_CONTROLE_PADRAO = "_apoios_buscados"
ANO_ELEICAO = 2026

# Recorte dos nomes do Senado vindos da matriz T1. A aba de convencoes so tem
# governador e presidente; quem disputa o Senado aparece nas pesquisas.
#
# Instituto testa muita gente que nao e pre-candidatura real, entao o corte e por
# DESEMPENHO: so entra quem ficou entre os N primeiros em algum cenario, medido
# pelo percentual dentro daquele cenario. Estar na lista de testados nao diz nada;
# aparecer na frente diz. O Senado elege duas vagas por estado nesta eleicao;
# top 3 cobre os dois favoritos mais o primeiro de fora, sem encher a fila
# (era 4, reduzido em 22/07/2026 a pedido da Eduarda).
SENADO_TOP_N = int(os.getenv("APOIOS_SENADO_TOP_N", "3"))
SENADO_DESDE = os.getenv("APOIOS_SENADO_DESDE", "2026-07-01")

# Grava a cada N candidatos em vez de so no fim. A varredura inicial leva horas;
# gravando so no final, uma queda ou um cancelamento no meio joga fora tudo que
# ja foi pesquisado e o custo do Gemini junto.
LOTE_GRAVACAO = int(os.getenv("APOIOS_LOTE", "20"))

USO_TOKENS = {"chamadas": 0, "entrada": 0, "saida": 0, "pensamento": 0}

# Fonte unica por enquanto: G1, que tem secao estadual em todas as UFs e e a
# fonte mais confiavel do recorte. A busca e por Google News RSS com
# site:g1.globo.com (mesmo esquema do noticias_eleicoes_scraper), nao pelo
# grounding do Gemini: o grounding cobra POR REQUISICAO de busca e devolvia
# link de redirect que expira; no RSS a busca e gratis, o Gemini so le o texto
# dos artigos, e o link gravado e o da reportagem real que NOS baixamos.
FONTE_HOST = re.compile(r"^(www\.)?g1\.globo\.com$", re.I)
JANELA_DIAS = int(os.getenv("APOIOS_JANELA_DIAS", "90"))
MAX_ARTIGOS = int(os.getenv("APOIOS_MAX_ARTIGOS", "8"))
HEADERS_HTTP = {"User-Agent": "Mozilla/5.0"}

# Termos de apoio na query: sem eles o RSS devolve qualquer noticia com o nome
# do candidato e a janela de 20 itens do feed se enche de ruido.
TERMOS_APOIO = "(apoio OR aliança OR palanque OR chapa OR convenção OR candidatura)"


def _rss_g1(cand):
    """Busca no Google News RSS as reportagens do G1 sobre o candidato."""
    uf = "" if _norm(cand.get("cargo")) in ("presidente", "vicepresidente") else cand["estado"]
    q = urllib.parse.quote(f'"{cand["candidato"]}" {uf} {TERMOS_APOIO} site:g1.globo.com')
    url = f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
    r = requests.get(url, headers=HEADERS_HTTP, timeout=30)
    r.raise_for_status()
    corte = datetime.now(BRT) - timedelta(days=JANELA_DIAS)
    itens = []
    for item in ET.fromstring(r.content).findall(".//item"):
        pub = item.findtext("pubDate", "")
        try:
            dt = datetime.strptime(pub, "%a, %d %b %Y %H:%M:%S %Z").replace(tzinfo=timezone.utc)
        except ValueError:
            dt = None
        if dt and dt.astimezone(BRT) < corte:
            continue
        itens.append({
            "titulo": item.findtext("title", ""),
            "data": dt.astimezone(BRT).strftime("%d/%m/%Y") if dt else "",
            "link": item.findtext("link", ""),
        })
    return itens


def _texto_g1(url):
    """Extrai o corpo da reportagem do G1.

    A pagina normal do G1 e casca JS: requests + newspaper3k so pegam titulo e
    subtitulo. A versao AMP (inserir google/amp/ depois do dominio) vem com o
    corpo renderizado no HTML — e o caminho principal. O newspaper3k fica de
    fallback pra quando o AMP nao existe (paginas de video/multicontent, cujo
    "corpo" curto e o texto todo mesmo).
    """
    amp = re.sub(r"^(https?://(?:www\.)?g1\.globo\.com/)", r"\1google/amp/", url)
    try:
        r = requests.get(amp, headers=HEADERS_HTTP, timeout=30)
        if r.status_code == 200:
            corpo = re.sub(r"<(script|style).*?</\1>", " ", r.text, flags=re.S)
            paras = [re.sub(r"<[^>]+>", "", p).strip()
                     for p in re.findall(r"<p[^>]*>(.*?)</p>", corpo, re.S)
                     if len(p) > 60]
            texto = html_mod.unescape("\n".join(paras)).strip()
            if len(texto) > 200:
                return texto[:6000]
    except Exception:
        pass
    try:
        art = Article(url, language="pt")
        art.download()
        art.parse()
        return (art.text or "").strip()[:6000]
    except Exception:
        return ""


def buscar_artigos_g1(cand):
    """Devolve ate MAX_ARTIGOS reportagens do G1 com texto extraido.

    O <link> do RSS e uma pagina de redirecionamento JS do Google Noticias;
    o gnewsdecoder resolve pra URL real (mesmo truque do scraper de noticias).
    So entra artigo cuja URL final e do g1.globo.com — o site: da query ja
    filtra, mas o feed as vezes vaza outro dominio — e que cite o candidato
    no titulo ou no texto (o feed tambem vaza materia fora do assunto).
    """
    artigos = []
    nome = _norm(cand["candidato"])
    for it in _rss_g1(cand):
        if len(artigos) >= MAX_ARTIGOS:
            break
        try:
            decoded = gnewsdecoder(it["link"], interval=1)
            url_real = decoded.get("decoded_url") if decoded.get("status") else ""
        except Exception:
            url_real = ""
        host = re.match(r"https?://([^/]+)", url_real or "")
        if not host or not FONTE_HOST.match(host.group(1)):
            continue
        texto = _texto_g1(url_real)
        if not texto:
            continue
        if nome not in _norm(it["titulo"]) and nome not in _norm(texto):
            continue
        artigos.append({"titulo": it["titulo"], "data": it["data"],
                        "url": url_real, "texto": texto})
        time.sleep(0.5)
    return artigos

CABECALHO = [
    "estado",
    "apoiador",
    "cargo do apoiador",
    "partido do apoiador",
    "apoiado",
    "cargo do apoiado",
    "partido do apoiado",
    "tipo de relação",
    "status",
    "data",
    "fonte",
    "link da fonte",
    "observação",
]

# Vocabularios fechados. O que vier fora deles e normalizado pelo alias ou cai
# no valor mais conservador, nunca num rotulo novo inventado pelo modelo.
# So pessoa fisica com candidatura propria em 2026. Partido, federacao e
# diretorio ficaram de fora: a base e de apoio entre PESSOAS, e legenda apoiando
# candidato e outra natureza de vinculo, que polui o grafo de relacoes pessoais.
# Nao e mais coluna da planilha (seria constante "candidato(a)" em toda linha),
# so filtro: o que o modelo devolver fora disso e descartado.
TIPOS_APOIADOR = {
    "candidato": "candidato(a)", "candidata": "candidato(a)",
    "candidatoa": "candidato(a)", "candidatura": "candidato(a)",
    "precandidato": "candidato(a)", "precandidata": "candidato(a)",
}

# Cargos em disputa em 2026. Serve pra confirmar que as duas pontas da relacao
# sao candidaturas, e nao gente sem candidatura propria.
CARGOS_2026 = {
    "presidente": "Presidente", "presidenta": "Presidente",
    "vicepresidente": "Vice-presidente",
    "governador": "Governador", "governadora": "Governador",
    "vicegovernador": "Vice-governador", "vicegovernadora": "Vice-governador",
    "senador": "Senador", "senadora": "Senador",
    "deputadofederal": "Deputado federal", "deputadafederal": "Deputado federal",
    "deputadoestadual": "Deputado estadual", "deputadaestadual": "Deputado estadual",
    "deputadodistrital": "Deputado distrital", "deputadadistrital": "Deputado distrital",
}

TIPOS_RELACAO = {
    "apoiodeclarado": "apoio declarado", "apoio": "apoio declarado",
    "declaracaodeapoio": "apoio declarado",
    "atoconjunto": "ato conjunto", "evento": "ato conjunto", "palanque": "ato conjunto",
    "negociacao": "negociação", "conversas": "negociação", "tratativas": "negociação",
    "alianca": "aliança", "coligacao": "aliança", "federacao": "aliança",
    "rompimento": "rompimento", "ruptura": "rompimento",
    "oposicao": "oposição", "adversario": "oposição",
}
STATUS = {
    "confirmado": "confirmado", "oficializado": "confirmado", "fechado": "confirmado",
    "emnegociacao": "em negociação", "negociacao": "em negociação",
    "emtratativas": "em negociação",
    "especulacao": "especulação", "bastidor": "especulação", "cotado": "especulação",
    "encerrado": "encerrado", "rompido": "encerrado", "desfeito": "encerrado",
}


def _registrar_uso(resp):
    meta = getattr(resp, "usage_metadata", None)
    if not meta:
        return
    USO_TOKENS["chamadas"] += 1
    USO_TOKENS["entrada"] += getattr(meta, "prompt_token_count", 0) or 0
    USO_TOKENS["saida"] += getattr(meta, "candidates_token_count", 0) or 0
    USO_TOKENS["pensamento"] += getattr(meta, "thoughts_token_count", 0) or 0


def _custo_estimado(entrada, saida, pensamento):
    # Faixa "flash": ~$0,30/1M de entrada, ~$2,50/1M de saida (pensamento cobra
    # como saida). Sem grounding de busca (a busca e por RSS, gratis), o custo
    # e so token mesmo.
    return (entrada / 1_000_000 * 0.30) + ((saida + pensamento) / 1_000_000 * 2.50)


def _resumo_uso_tokens():
    if not USO_TOKENS["chamadas"]:
        return
    custo = _custo_estimado(USO_TOKENS["entrada"], USO_TOKENS["saida"], USO_TOKENS["pensamento"])
    print(f"\nGemini (apoios): {USO_TOKENS['chamadas']} chamada(s) · "
          f"{USO_TOKENS['entrada']:,} tokens entrada · {USO_TOKENS['saida']:,} saída · "
          f"{USO_TOKENS['pensamento']:,} pensamento · custo estimado ${custo:.4f}")


def _sem_acento(valor):
    return unicodedata.normalize("NFKD", str(valor or "")).encode("ascii", "ignore").decode()


def _norm(valor):
    return re.sub(r"[^a-z0-9]+", "", _sem_acento(valor).strip().lower())


def _creds_info():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    if not raw:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON não encontrado.")
    return json.loads(raw)


def _normalizar_spreadsheet_id(valor):
    s = str(valor or "").strip()
    for padrao in (r"/spreadsheets/d/([A-Za-z0-9_-]+)", r"\b([A-Za-z0-9_-]{30,})\b"):
        m = re.search(padrao, s)
        if m:
            return m.group(1)
    return s


def _planilha_destino():
    """Planilha onde a base de apoios e gravada.

    Fica junto das candidaturas do TSE, e nao na planilha de convencoes de onde
    sai a semente: a Eduarda centralizou tudo que e produto final ali.
    """
    creds = Credentials.from_service_account_info(
        _creds_info(), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    sheet_id = _normalizar_spreadsheet_id(
        os.getenv("SPREADSHEET_ID_APOIOS", "") or os.getenv("SPREADSHEET_ID_TSE", "")
    )
    if not sheet_id:
        raise RuntimeError("Defina SPREADSHEET_ID_TSE (ou SPREADSHEET_ID_APOIOS).")
    return _autorizar(creds).open_by_key(sheet_id)


def _planilha():
    creds = Credentials.from_service_account_info(
        _creds_info(), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    sheet_id = _normalizar_spreadsheet_id(os.getenv("SPREADSHEET_ID_CONVENCOES", ""))
    if not sheet_id:
        raise RuntimeError("Defina SPREADSHEET_ID_CONVENCOES.")
    try:
        return _autorizar(creds).open_by_key(sheet_id)
    except gspread.exceptions.SpreadsheetNotFound as exc:
        email = str(_creds_info().get("client_email", "")).strip()
        raise RuntimeError(
            f"Planilha não encontrada pela service account. Confira o ID ({sheet_id}) "
            f"e compartilhe com {email}."
        ) from exc


def _gemini():
    api_key = os.getenv("GEMINI_API_KEY", "")
    if not api_key:
        raise RuntimeError("GEMINI_API_KEY não encontrada.")
    return genai.Client(api_key=api_key)


def _extrair_json_objeto(texto):
    bruto = re.sub(r"^```(?:json)?\s*|\s*```$", "", (texto or "").strip(), flags=re.I)
    decoder = json.JSONDecoder()
    for m in re.finditer(r"\{", bruto):
        try:
            obj, _ = decoder.raw_decode(bruto[m.start():])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    raise RuntimeError("JSON não encontrado na resposta do Gemini")


def _data_busca():
    return datetime.now(BRT).strftime("%Y-%m-%d %H:%M")


def _hoje():
    return datetime.now(BRT).date()


def _data_convencao(valor):
    """Primeira data concreta do campo livre de convenção da aba de origem.

    A coluna traz formatos soltos: "25/07", "01/08/2026", "01/08/2026 ou
    02/08/2026", "agosto de 2026", "Início de agosto de 2026". Só serve quando
    tem dia E mês; o resto devolve None e a linha fica fora da rodada.
    """
    s = str(valor or "").strip()
    m = re.search(r"\b(\d{1,2})/(\d{1,2})(?:/(\d{2,4}))?\b", s)
    if not m:
        return None
    dia, mes, ano = m.group(1), m.group(2), m.group(3)
    if not ano:
        ano = str(ANO_ELEICAO)
    elif len(ano) == 2:
        ano = f"20{ano}"
    try:
        return datetime(int(ano), int(mes), int(dia)).date()
    except ValueError:
        return None


def _limpar_data(valor):
    s = str(valor or "").strip()
    if not s:
        return ""
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(s, fmt).strftime("%d/%m/%Y")
        except ValueError:
            pass
    return s


def _de_vocabulario(valor, mapa, padrao):
    return mapa.get(_norm(valor), padrao)


# Um mesmo par nao pode ser apoio e oposicao ao mesmo tempo. Na primeira rodada
# o PL apareceu como "negociação" E como "oposição" com Soldado Sampaio, e as
# duas linhas entraram porque o tipo faz parte da chave de dedup.
RELACOES_POSITIVAS = {"apoio declarado", "ato conjunto", "negociação", "aliança"}
RELACOES_NEGATIVAS = {"rompimento", "oposição"}


def _polaridade(relacao):
    if relacao in RELACOES_NEGATIVAS:
        return "negativa"
    return "positiva" if relacao in RELACOES_POSITIVAS else ""


def _cargo_2026(valor):
    """Cargo em disputa em 2026, ou "" quando nao reconhecido.

    E o que separa candidatura de figura politica sem candidatura propria: a base
    e de apoio ENTRE CANDIDATURAS, entao as duas pontas precisam ter cargo valido.
    """
    return CARGOS_2026.get(_norm(valor), "")


def _tipo_relacao(valor):
    """Traduz pro vocabulário fechado, ou devolve "" pra linha ser descartada.

    Rótulo fora da lista significa que o modelo saiu do script. Cair pra "apoio
    declarado" nesse caso afirmaria apoio a partir de texto que ninguém validou,
    que é exatamente o que a regra da base proíbe. Só aceita o palpite quando o
    próprio texto fala em apoio ("apoio informal", "apoio tácito").
    """
    conhecido = TIPOS_RELACAO.get(_norm(valor))
    if conhecido:
        return conhecido
    return "apoio declarado" if "apoi" in _norm(valor) else ""


def _mapa_header(header):
    return {_norm(nome): pos for pos, nome in enumerate(header)}


def _ler_origem(sh):
    aba = os.getenv("CONVENCOES_ABA", ABA_ORIGEM_PADRAO)
    try:
        ws = sh.worksheet(aba)
    except gspread.exceptions.WorksheetNotFound as exc:
        abas = ", ".join(w.title for w in sh.worksheets())
        raise RuntimeError(f"Aba '{aba}' não encontrada. Disponíveis: {abas}") from exc

    valores = ws.get_all_values()
    if not valores:
        return []
    header, linhas = valores[0], valores[1:]
    mapa = _mapa_header(header)

    def campo(row, *nomes):
        for nome in nomes:
            pos = mapa.get(_norm(nome))
            if pos is not None and pos < len(row) and str(row[pos]).strip():
                return str(row[pos]).strip()
        return ""

    candidatos = []
    for row in linhas:
        nome = campo(row, "Pré-candidato", "Candidato")
        if not nome:
            continue
        candidatos.append({
            "estado": campo(row, "Estado", "UF"),
            "candidato": nome,
            "cargo": campo(row, "Cargo"),
            "partido": campo(row, "Partido/Federação", "Partido"),
            "data_convencao": campo(row, "Data convenção"),
        })
    return candidatos


def _nome_pessoa(valor):
    """Chave de pessoa: sem acento e sem prefixo de tratamento.

    A matriz grava a mesma pessoa de varias formas ("Alvaro Dias"/"Álvaro Dias",
    "Dr. Junior Feitosa"/"Júnior Feitosa"). Sem isso a mesma candidatura entraria
    duas vezes na fila e viraria dois nos no grafo.
    """
    s = _sem_acento(valor).lower().strip()
    s = re.sub(r"\b(dr|dra|sr|sra|prof|cel|coronel|delegado|delegada|pastor|padre)\b", "", s)
    return re.sub(r"[^a-z0-9]+", "", s)


def _ler_senado_matriz():
    """Candidaturas ao Senado, tiradas de quem e testado nas pesquisas da T1."""
    sheet_id = _normalizar_spreadsheet_id(os.getenv("SPREADSHEET_ID_POLLINGDATA", ""))
    if not sheet_id:
        print("Aviso: SPREADSHEET_ID_POLLINGDATA não definido; Senado fica de fora.")
        return []
    creds = Credentials.from_service_account_info(
        _creds_info(), scopes=["https://www.googleapis.com/auth/spreadsheets"]
    )
    try:
        ws = _autorizar(creds).open_by_key(sheet_id).worksheet("resultados")
        valores = ws.get_all_values()
    except Exception as exc:
        print(f"Aviso: não consegui ler a matriz T1 ({str(exc)[:120]}); Senado fica de fora.")
        return []
    if not valores:
        return []

    header = valores[0]
    idx = {c.strip(): i for i, c in enumerate(header)}
    precisa = ["uf", "cargo", "ano", "tipo", "candidato", "partido",
               "scenario_id", "percentual", "data_campo"]
    if any(c not in idx for c in precisa):
        print("Aviso: matriz T1 sem as colunas esperadas; Senado fica de fora.")
        return []

    # Passo 1: percentual de cada candidato em cada cenario, pra poder ranquear.
    cenarios = {}
    for row in valores[1:]:
        if len(row) <= max(idx[c] for c in precisa):
            continue
        if (row[idx["cargo"]].strip().lower() != "senador"
                or row[idx["ano"]].strip() != str(ANO_ELEICAO)
                or row[idx["tipo"]].strip() != "candidato"):
            continue
        nome = row[idx["candidato"]].strip()
        if not nome:
            continue
        try:
            pct = float(str(row[idx["percentual"]]).replace(",", "."))
        except (ValueError, TypeError):
            continue
        cenarios.setdefault(row[idx["scenario_id"]].strip(), []).append((pct, row))

    # Passo 2: so quem ficou entre os N primeiros de algum cenario.
    agrupado = {}
    for itens in cenarios.values():
        for _, row in sorted(itens, key=lambda t: -t[0])[:SENADO_TOP_N]:
            nome = row[idx["candidato"]].strip()
            chave = (row[idx["uf"]].strip().upper(), _nome_pessoa(nome))
            d = agrupado.setdefault(chave, {"datas": [], "nomes": {}, "partidos": {}})
            d["nomes"][nome] = d["nomes"].get(nome, 0) + 1
            partido = row[idx["partido"]].strip().upper()
            if partido and partido != "SEM PARTIDO":
                d["partidos"][partido] = d["partidos"].get(partido, 0) + 1
            data = row[idx["data_campo"]].strip()
            if len(data) == 10:
                d["datas"].append(data)

    candidatos = []
    for (uf, _), d in agrupado.items():
        if not d["datas"] or max(d["datas"]) < SENADO_DESDE:
            continue
        candidatos.append({
            "estado": uf,
            # Grafia mais usada, mesma regra ja aplicada na matriz.
            "candidato": max(d["nomes"].items(), key=lambda kv: kv[1])[0],
            "cargo": "Senador",
            "partido": max(d["partidos"].items(), key=lambda kv: kv[1])[0] if d["partidos"] else "",
            "data_convencao": "",
            "origem_semente": "matriz T1",
        })
    return candidatos


def _abrir_destino(sh):
    aba = os.getenv("APOIOS_ABA", ABA_DESTINO_PADRAO)
    try:
        ws = sh.worksheet(aba)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=aba, rows=2000, cols=len(CABECALHO))
        ws.update(range_name="A1", values=[CABECALHO], value_input_option="RAW")
        ws.freeze(rows=1)
        return ws, list(CABECALHO)

    header = ws.row_values(1)
    if not header:
        ws.update(range_name="A1", values=[CABECALHO], value_input_option="RAW")
        return ws, list(CABECALHO)

    faltando = [c for c in CABECALHO if _norm(c) not in _mapa_header(header)]
    if faltando:
        novo = header + faltando
        if ws.col_count < len(novo):
            ws.add_cols(len(novo) - ws.col_count)
        ws.update(range_name="A1", values=[novo], value_input_option="RAW")
        header = novo
    return ws, header


def _chave(estado, apoiador, apoiado, relacao):
    """Identidade da aresta. Inclui o tipo de relação porque um mesmo par pode ter
    mais de um vínculo ao longo do tempo (negociação que vira apoio, apoio que
    vira rompimento). Não inclui a direção invertida: A→B e B→A são duas arestas
    distintas de propósito."""
    return (_norm(estado), _norm(apoiador), _norm(apoiado), _norm(relacao))


def _prompt(cand, artigos):
    bloco_artigos = "\n\n".join(
        f"ARTIGO {i}\nTítulo: {a['titulo']}\nData: {a['data']}\n{a['texto']}"
        for i, a in enumerate(artigos, start=1))
    return f"""
Você é um pesquisador eleitoral. Leia as reportagens do G1 numeradas abaixo e extraia relações de apoio ENTRE CANDIDATURAS das eleições de 2026 envolvendo:

Estado/UF: {cand['estado']}
Candidato(a): {cand['candidato']}
Cargo: {cand['cargo']}
Partido/Federação: {cand['partido']}

REPORTAGENS:

{bloco_artigos}

FIM DAS REPORTAGENS. Use APENAS o que está documentado nelas; não use conhecimento externo.

Registre tanto quem apoia essa candidatura quanto quem essa candidatura apoia.

O QUE ENTRA. As duas pontas precisam ser PESSOAS com candidatura propria em 2026 (candidato ou pré-candidato a presidente, vice, governador, vice-governador, senador ou deputado). Exemplos do que interessa:
- candidato ao governo que compõe chapa com candidata ao Senado do mesmo palanque
- candidato estadual que declara apoio a um presidenciável
- presidenciável que declara apoio a candidato estadual
- ministro ou candidato que apoia outra candidatura ao Senado

O QUE NÃO ENTRA, nunca:
- partido, federação ou diretório como apoiador (ex.: "o PL apoia fulano"). Só pessoa entra
- sindicato, central sindical, movimento social, entidade religiosa, associação
- pessoa sem candidatura própria em 2026 (ex-governador aposentado, cacique sem disputa, empresário, artista)
- grupo político, ala partidária ou família política sem candidatura nomeada
Se o apoiador não é uma pessoa com candidatura própria em 2026, NÃO devolva a relação. Legenda apoiando candidato não entra nesta base.

CADA RELAÇÃO É DIRECIONADA. "A apoia B" é diferente de "B apoia A". Nunca presuma reciprocidade: se as duas direções estiverem documentadas, devolva DUAS relações, uma em cada direção.

tipo_apoiador: sempre "candidato(a)". Se a ponta não for uma pessoa candidata, não devolva a relação.

cargo_apoiador e cargo_apoiado: o cargo DISPUTADO em 2026, um destes: Presidente, Vice-presidente, Governador, Vice-governador, Senador, Deputado federal, Deputado estadual, Deputado distrital. Se você não souber o cargo que a pessoa disputa em 2026, não devolva a relação.

tipo_relacao, exatamente um destes:
- "apoio declarado": manifestação pública e nominal de apoio
- "ato conjunto": participaram juntos de evento, agenda ou palanque de campanha
- "negociação": conversas em curso, ainda sem decisão
- "aliança": composição formal (mesma chapa, coligação, acordo anunciado)
- "rompimento": relação de apoio que existia e foi desfeita
- "oposição": declaração pública de que atuará contra a candidatura

status, exatamente um destes:
- "confirmado": oficializado ou declarado publicamente pelas partes
- "em negociação": em curso, sem decisão anunciada
- "especulação": noticiado como bastidor, sem confirmação das partes
- "encerrado": vínculo que acabou (use junto com rompimento)

REGRAS:
1. Só devolva relação documentada numa das reportagens acima. "artigo" é o número da reportagem que documenta a relação (se mais de uma documentar, use a mais recente).
2. NÃO infira apoio por afinidade partidária, por serem do mesmo partido, por serem aliados históricos ou por menção indireta. Só conta declaração, ato ou decisão documentada.
3. Não confunda evento institucional (posse, inauguração, entrega de obra) com ato conjunto de campanha.
4. Não devolva o mesmo par de pessoas com dois tipos de relação contraditórios (ex.: negociação e oposição). Escolha o que a fonte mais recente sustenta.
5. "data" é a data do fato, não a da publicação, quando as duas aparecerem. Formato DD/MM/AAAA.
6. "observacao" é uma frase curta com o que a reportagem diz, sem opinião.
7. Se não encontrar nenhuma relação documentada entre candidaturas, devolva "relacoes": [].

Retorne APENAS JSON válido:
{{
  "relacoes": [
    {{
      "estado": "",
      "apoiador": "",
      "tipo_apoiador": "",
      "cargo_apoiador": "",
      "partido_apoiador": "",
      "apoiado": "",
      "cargo_apoiado": "",
      "partido_apoiado": "",
      "tipo_relacao": "",
      "status": "",
      "data": "",
      "artigo": 1,
      "observacao": ""
    }}
  ]
}}
""".strip()


def buscar_apoios(client, cand):
    """Busca artigos do G1 via RSS e extrai as relações com o Gemini.

    Sem artigo, sem chamada ao Gemini: a maioria dos nomes não tem noticia
    de apoio na janela e sai daqui de graça. O link/fonte de cada relação é
    preenchido AQUI, a partir do artigo que nós baixamos — o modelo só aponta
    o número do artigo, não inventa URL.
    """
    artigos = buscar_artigos_g1(cand)
    if not artigos:
        return []
    print(f"  {len(artigos)} artigo(s) do G1 na janela de {JANELA_DIAS} dias")
    resp = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=_prompt(cand, artigos),
        config=types.GenerateContentConfig(temperature=0.1),
    )
    _registrar_uso(resp)
    relacoes = _extrair_json_objeto(getattr(resp, "text", "") or "").get("relacoes")
    if not isinstance(relacoes, list):
        return []
    for r in relacoes:
        if not isinstance(r, dict):
            continue
        try:
            idx = int(r.get("artigo", 0))
            art = artigos[idx - 1] if 1 <= idx <= len(artigos) else None
        except (ValueError, TypeError):
            art = None
        r["fonte"] = "G1" if art else ""
        r["link_fonte"] = art["url"] if art else ""
        r.setdefault("data", art["data"] if art else "")
    return relacoes


def _abrir_controle(sh):
    """Aba de controle com quem JA foi usado como semente de busca.

    Nao da pra deduzir isso da propria base: aparecer nela como apoiado so quer
    dizer que alguem citou a pessoa, nao que as relacoes dela foram pesquisadas.
    Sem esse registro, quem fosse citado por outro candidato seria pulado pra
    sempre sem nunca ter sido buscado.
    """
    nome = os.getenv("APOIOS_ABA_CONTROLE", ABA_CONTROLE_PADRAO)
    try:
        ws = sh.worksheet(nome)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=nome, rows=2000, cols=4)
        ws.update(range_name="A1", values=[["estado", "candidato", "cargo", "data da busca"]],
                  value_input_option="RAW")
        ws.freeze(rows=1)
    return ws


def _ja_buscados(ws_controle):
    valores = ws_controle.get_all_values()[1:]
    return {(_norm(r[0]), _nome_pessoa(r[1])) for r in valores if len(r) >= 2 and r[1].strip()}


def _linhas_existentes(ws, header):
    mapa = _mapa_header(header)
    valores = ws.get_all_values()[1:]

    def campo(row, nome):
        pos = mapa.get(_norm(nome))
        return str(row[pos]).strip() if pos is not None and pos < len(row) else ""

    chaves = set()
    for row in valores:
        if not any(str(c).strip() for c in row):
            continue
        chaves.add(_chave(campo(row, "estado"), campo(row, "apoiador"),
                          campo(row, "apoiado"), campo(row, "tipo de relação")))
    return chaves


def atualizar(max_linhas=40, force=False, incluir_sem_convencao=False, dry_run=False):
    sh_origem = _planilha()
    sh = _planilha_destino()
    candidatos = _ler_origem(sh_origem)
    # Senado nao esta na aba de convencoes; vem de quem e testado nas pesquisas.
    # Nao passa pelo filtro de convencao: estar numa pesquisa registrada no TSE
    # ja e sinal de pre-candidatura, e esperar a convencao do governador daquela
    # UF pra buscar o senador nao faz sentido.
    senado = _ler_senado_matriz()
    vistos = {(_norm(c["estado"]), _nome_pessoa(c["candidato"])) for c in candidatos}
    novos_senado = [c for c in senado
                    if (_norm(c["estado"]), _nome_pessoa(c["candidato"])) not in vistos]
    candidatos = candidatos + novos_senado
    print(f"Semente: {len(candidatos) - len(novos_senado)} da aba de convenções + "
          f"{len(novos_senado)} do Senado (matriz T1).")
    ws, header = _abrir_destino(sh)
    chaves = _linhas_existentes(ws, header)
    ws_controle = _abrir_controle(sh)
    ja_buscados = _ja_buscados(ws_controle)

    hoje = _hoje()
    fila, sem_convencao, ja_feitos = [], 0, 0
    for c in candidatos:
        # Só entra quem já teve convenção. É na convenção que a aliança vira
        # fato registrado, e é esse histórico que a base precisa capturar. Quem
        # tem convenção marcada pra frente entra sozinho nas rodadas seguintes,
        # conforme a data chega.
        if not incluir_sem_convencao and c.get("origem_semente") != "matriz T1":
            data = _data_convencao(c["data_convencao"])
            if data is None or data > hoje:
                sem_convencao += 1
                continue
        if not force and (_norm(c["estado"]), _nome_pessoa(c["candidato"])) in ja_buscados:
            ja_feitos += 1
            continue
        fila.append(c)
    fila = fila[:max_linhas]
    print(f"Apoios: {len(candidatos)} candidato(s) na origem · {len(fila)} para buscar · "
          f"{sem_convencao} com convenção futura ou sem data · {ja_feitos} já buscado(s).")
    if not fila:
        return

    client = _gemini()
    mapa = _mapa_header(header)
    novas, sem_resultado, erros, descartadas = [], 0, 0, 0
    fora_do_recorte, contraditorias, fora_da_fonte = 0, 0, 0
    buscados_agora = []
    polaridade_por_par = {}

    total_gravado = 0

    def gravar_lote(final=False):
        """Descarrega o que esta em memoria na planilha.

        table_range ancorado na ultima linha com dado de propósito: sem ele a API
        do Sheets procura o fim da tabela a partir de A1 e escreve dentro do
        primeiro buraco de linhas vazias que encontrar, no meio da aba.
        """
        nonlocal novas, buscados_agora, total_gravado
        if dry_run or not (novas or buscados_agora):
            return
        if novas:
            ws.append_rows(novas, value_input_option="USER_ENTERED",
                           table_range=f"A{len(ws.get_all_values())}")
            total_gravado += len(novas)
            novas = []
        if buscados_agora:
            ws_controle.append_rows(buscados_agora, value_input_option="USER_ENTERED",
                                    table_range=f"A{len(ws_controle.get_all_values())}")
            buscados_agora = []
        print(f"  >> gravado na planilha ({total_gravado} relação(ões) no total)"
              + ("" if final else f", {processados}/{len(fila)} candidatos"))

    processados = 0
    for cand in fila:
        processados += 1
        print(f"[{processados}/{len(fila)}] {cand['estado']} / {cand['candidato']} ({cand['partido']})...")
        try:
            relacoes = buscar_apoios(client, cand)
        except Exception as e:
            erros += 1
            print(f"  [erro] {str(e)[:180]}")
            continue

        adicionadas = 0
        for r in relacoes:
            if not isinstance(r, dict):
                continue
            apoiador = str(r.get("apoiador") or "").strip()
            apoiado = str(r.get("apoiado") or "").strip()
            link = str(r.get("link_fonte") or "").strip()
            fonte = str(r.get("fonte") or "").strip()
            # Sem as duas pontas, sem fonte ou sem link a relação não entra: a base
            # existe pra ser auditável, e aresta sem origem verificável contamina o grafo.
            if not (apoiador and apoiado and fonte and link.lower().startswith("http")):
                descartadas += 1
                continue
            # O link ja e a URL real do G1, preenchida em buscar_apoios a partir
            # do artigo baixado. O guarda fica: se o modelo apontar artigo
            # invalido, a relacao vem sem link G1 e nao entra.
            host = re.match(r"https?://([^/]+)", link)
            if not host or not FONTE_HOST.match(host.group(1)):
                fora_da_fonte += 1
                print(f"  [fora do G1] {apoiador} x {apoiado}")
                continue
            if _norm(apoiador) == _norm(apoiado):
                descartadas += 1
                continue

            relacao = _tipo_relacao(r.get("tipo_relacao"))
            if not relacao:
                descartadas += 1
                continue

            # As duas pontas precisam ser candidatura de 2026 (ou um partido, do
            # lado do apoiador). Apoiador sem candidatura propria e sem legenda
            # nao entra: a base e de apoio ENTRE CANDIDATURAS.
            tipo_apoiador = TIPOS_APOIADOR.get(_norm(r.get("tipo_apoiador")), "")
            cargo_apoiado = _cargo_2026(r.get("cargo_apoiado"))
            cargo_apoiador = _cargo_2026(r.get("cargo_apoiador"))
            # Pessoa fisica dos dois lados: sem cargo em 2026 numa das pontas,
            # nao e apoio entre candidaturas e nao entra.
            if not tipo_apoiador or not cargo_apoiado or not cargo_apoiador:
                fora_do_recorte += 1
                continue

            estado = str(r.get("estado") or cand["estado"]).strip()
            par = (_norm(estado), _norm(apoiador), _norm(apoiado))
            polaridade = _polaridade(relacao)
            if par in polaridade_por_par and polaridade_por_par[par] != polaridade:
                contraditorias += 1
                print(f"  [conflito] {apoiador} x {apoiado}: já gravado como "
                      f"{polaridade_por_par[par]}, ignorando '{relacao}'")
                continue

            chave = _chave(estado, apoiador, apoiado, relacao)
            if chave in chaves:
                continue
            chaves.add(chave)
            polaridade_por_par[par] = polaridade

            valores = {
                "estado": estado,
                "apoiador": apoiador,
                "cargo do apoiador": cargo_apoiador,
                "partido do apoiador": str(r.get("partido_apoiador") or "").strip().upper(),
                "apoiado": apoiado,
                "cargo do apoiado": cargo_apoiado,
                "partido do apoiado": str(r.get("partido_apoiado") or "").strip().upper(),
                "tipo de relação": relacao,
                "status": _de_vocabulario(r.get("status"), STATUS, "especulação"),
                "data": _limpar_data(r.get("data")),
                "fonte": fonte,
                "link da fonte": link,
                "observação": str(r.get("observacao") or "").strip()[:500],
            }
            linha = [""] * len(header)
            for nome_col, valor in valores.items():
                pos = mapa.get(_norm(nome_col))
                if pos is not None:
                    linha[pos] = valor
            novas.append(linha)
            adicionadas += 1
            print(f"  [{valores['status']}] {apoiador} --{relacao}--> {apoiado}")

        if not adicionadas:
            sem_resultado += 1
            print("  [vazio] nenhuma relação com fonte e link")
        # Registra mesmo sem resultado: senao a proxima rodada gastaria uma
        # chamada de novo no mesmo nome que ja se sabe que nao rende nada.
        buscados_agora.append([cand["estado"], cand["candidato"], cand["cargo"], _data_busca()])
        if processados % LOTE_GRAVACAO == 0:
            gravar_lote()

    gravar_lote(final=True)

    print("\nResumo:")
    print(f"* relações novas: {total_gravado if not dry_run else len(novas)}")
    print(f"* descartadas por falta de fonte/link ou por auto-referência: {descartadas}")
    print(f"* fora do recorte (não é pessoa com candidatura em 2026): {fora_do_recorte}")
    print(f"* fora da fonte (notícia que não é do G1 ou link morto): {fora_da_fonte}")
    print(f"* ignoradas por contradizer relação já gravada: {contraditorias}")
    print(f"* candidatos sem relação encontrada: {sem_resultado}")
    print(f"* erros técnicos: {erros}")
    if dry_run:
        print("* dry-run: nada foi gravado.")

    _resumo_uso_tokens()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-linhas", type=int, default=int(os.getenv("APOIOS_MAX_LINHAS", "40")))
    parser.add_argument("--force", action="store_true",
                        default=os.getenv("APOIOS_FORCE", "").lower() == "true")
    parser.add_argument("--incluir-sem-convencao", action="store_true",
                        default=os.getenv("APOIOS_INCLUIR_SEM_CONVENCAO", "").lower() == "true",
                        help="busca também quem ainda não teve convenção")
    parser.add_argument("--dry-run", action="store_true")
    args = parser.parse_args()
    atualizar(max_linhas=args.max_linhas, force=args.force,
              incluir_sem_convencao=args.incluir_sem_convencao, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
