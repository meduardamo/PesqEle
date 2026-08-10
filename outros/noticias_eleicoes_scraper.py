"""
Alertas de notícias sobre candidaturas.

Usado na fase das convenções (julho), quando o TSE ainda não tem dado. A gente
monitora notícia por palavra-chave e usa pra montar a matriz de quem foi anunciado.
Fonte: Google Notícias (RSS). A classificação fica por conta do Gemini (ver TODO).

Além de classificar, a rodada decide o que é alerta (régua em ALERTA_TEMAS),
escreve o texto pronto pro WhatsApp na coluna 'resumo' e manda um email com os
alertas novos ao fim da raspagem. A régua veio do prompt do agente de
monitoramento que o time usa no Slack, então a classificação daqui e o alerta
que a Monique cria por lá seguem o mesmo critério.

Secrets do email: BREVO_API_KEY, EMAIL, DESTINATARIOS_NOTICIAS (ou DESTINATARIOS).
Sem eles a rodada segue normal, só não envia.
"""

import json
import os
import re
import threading
import time
import urllib.parse
import xml.etree.ElementTree as ET
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from email.utils import parsedate_to_datetime

import requests
from googlenewsdecoder import gnewsdecoder
from newspaper import Article
from compartilhado.email_utils import EIXO_MARINHO, destinatarios, enviar_email
from compartilhado.relatorios_sheets_utils import (
    _col_count_atual, _sem_acento, autorizar_com_retry as _autorizar)

HEADERS = {"User-Agent": "Mozilla/5.0"}

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

# Classificar manchete é tarefa de extração, não de raciocínio: numa rodada real
# (335 notícias) o modelo gastou 473 mil tokens de pensamento para 21 mil de
# saída, ou seja, quase todo o custo e boa parte do tempo de resposta foram
# pensamento. Com orçamento 0 o flash responde direto. Suba pra 512/1024 pelo
# env se a qualidade da classificação cair; -1 devolve o pensamento dinâmico
# (comportamento antigo).
GEMINI_THINKING_BUDGET = int(os.getenv("GEMINI_THINKING_BUDGET", "0"))

# Coleta e classificação são espera de rede, não CPU: rodando uma de cada vez o
# scraper levava 66 min (9 de coleta + 56 classificando ~10s por notícia).
# Medido com os links reais da planilha: 8 threads baixam artigo em 0,54s em vez
# de 3,7s, e 6 buscas em paralelo no RSS do Google não tomam bloqueio.
WORKERS_RSS = int(os.getenv("NOTICIAS_WORKERS_RSS", "6"))
WORKERS_CLASSIFICACAO = int(os.getenv("NOTICIAS_WORKERS", "8"))

# Uso acumulado de tokens do Gemini nesta execução (processo novo a cada rodada
# do workflow, então não precisa resetar entre chamadas).
USO_TOKENS = {"chamadas": 0, "entrada": 0, "saida": 0, "pensamento": 0}
_LOCK_USO = threading.Lock()


def _registrar_uso(resp):
    meta = getattr(resp, "usage_metadata", None)
    if not meta:
        return
    with _LOCK_USO:   # a classificação roda em várias threads
        USO_TOKENS["chamadas"] += 1
        USO_TOKENS["entrada"] += getattr(meta, "prompt_token_count", 0) or 0
        USO_TOKENS["saida"] += getattr(meta, "candidates_token_count", 0) or 0
        USO_TOKENS["pensamento"] += getattr(meta, "thoughts_token_count", 0) or 0


def _custo_estimado(entrada, saida, pensamento):
    # preço aproximado da faixa "flash" (~$0,30/1M tokens de entrada, ~$2,50/1M de
    # saída, saída e pensamento cobram na mesma tabela). Ajuste se trocar de modelo
    # (GEMINI_MODEL) ou se o preço mudar. Estimativa, não fatura oficial; confira o
    # console de billing do Google pro valor exato.
    return (entrada / 1_000_000 * 0.30) + ((saida + pensamento) / 1_000_000 * 2.50)


def _resumo_uso_tokens(rotulo, uso):
    if not uso["chamadas"]:
        return
    custo = _custo_estimado(uso["entrada"], uso["saida"], uso["pensamento"])
    print(f"\nGemini ({rotulo}): {uso['chamadas']} chamada(s) · "
          f"{uso['entrada']:,} tokens entrada · {uso['saida']:,} saída · "
          f"{uso['pensamento']:,} pensamento · custo estimado ${custo:.4f}")


SHEET_ID  = os.getenv("SPREADSHEET_ID_TSE", "1Vo-2oa11JpPaYC051Z0UYNR1yJZdhYW4RJeylHfX-bA")
SHEET_ABA = "noticias"

# Planilha com os sites/blogs regionais (colunas Link, Estado).
SITES_ID  = os.getenv("SPREADSHEET_ID_SITES", "")
SITES_ABA = os.getenv("SITES_ABA", "deduplicado")

NOME_UF = {
    'acre': 'AC', 'alagoas': 'AL', 'amapá': 'AP', 'amapa': 'AP', 'amazonas': 'AM',
    'bahia': 'BA', 'ceará': 'CE', 'ceara': 'CE', 'distrito federal': 'DF',
    'espírito santo': 'ES', 'espirito santo': 'ES', 'goiás': 'GO', 'goias': 'GO',
    'maranhão': 'MA', 'maranhao': 'MA', 'mato grosso': 'MT',
    'mato grosso do sul': 'MS', 'minas gerais': 'MG', 'pará': 'PA', 'para': 'PA',
    'paraíba': 'PB', 'paraiba': 'PB', 'paraná': 'PR', 'parana': 'PR',
    'pernambuco': 'PE', 'piauí': 'PI', 'piaui': 'PI', 'rio de janeiro': 'RJ',
    'rio grande do norte': 'RN', 'rio grande do sul': 'RS', 'rondônia': 'RO',
    'rondonia': 'RO', 'roraima': 'RR', 'santa catarina': 'SC', 'são paulo': 'SP',
    'sao paulo': 'SP', 'sergipe': 'SE', 'tocantins': 'TO',
}

_SA_FIELDS = {
    "type", "project_id", "private_key_id", "private_key", "client_email",
    "client_id", "auth_uri", "token_uri", "auth_provider_x509_cert_url",
    "client_x509_cert_url", "universe_domain",
}


_CLIENT = None
_LOCK_CLIENT = threading.Lock()

def _gemini_client():
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    with _LOCK_CLIENT:   # várias threads pedem o cliente na primeira classificação
        if _CLIENT is not None:
            return _CLIENT
        return _criar_gemini_client()


def _criar_gemini_client():
    global _CLIENT
    from google import genai
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        try:
            import streamlit as st
            key = st.secrets.get("GEMINI_API_KEY", "")
        except Exception:
            pass
    if not key:
        try:
            cred_path = "credentials.json"
            with open(cred_path, encoding="utf-8") as f:
                key = json.load(f).get("genai_api_key", "")
        except Exception:
            pass
    if not key:
        raise RuntimeError("Faltou a GEMINI_API_KEY (env var, secrets ou credentials.json).")
    _CLIENT = genai.Client(api_key=key)
    return _CLIENT

UFS = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB',
       'PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']


BRT = timezone(timedelta(hours=-3))

# Só entram notícias publicadas nos últimos N dias (cobre as duas rodadas diárias
# e o fuso). Ajustável pelo secret NOTICIAS_JANELA_DIAS.
JANELA_DIAS = int(os.getenv("NOTICIAS_JANELA_DIAS", "2"))


def _parse_data(pubdate):
    """Data do RSS -> datetime em BRT, ou None se não der pra ler."""
    if not pubdate:
        return None
    try:
        return parsedate_to_datetime(pubdate).astimezone(BRT)
    except Exception:
        return None


def _formatar_data(pubdate):
    """Converte a data do RSS (GMT) para 'dd/mm/aaaa HH:MM' no horário de Brasília."""
    dt = _parse_data(pubdate)
    return dt.strftime("%d/%m/%Y %H:%M") if dt else (pubdate or "")


def _data_planilha(valor):
    """Lê de volta a data no formato que a planilha guarda ('dd/mm/aaaa HH:MM')."""
    try:
        return datetime.strptime(str(valor).strip(), "%d/%m/%Y %H:%M").replace(tzinfo=BRT)
    except Exception:
        return None


def google_news_rss(busca, max_itens=20, tentativas=3):
    """Retorna as notícias recentes de uma busca (título, fonte, data, link).
    Descarta o que for mais antigo que JANELA_DIAS.

    Repete em erro de rede/HTTP: com as buscas em paralelo, uma falha isolada do
    Google deixaria um estado inteiro sem notícia na rodada."""
    q = urllib.parse.quote(busca)
    url = f"https://news.google.com/rss/search?q={q}&hl=pt-BR&gl=BR&ceid=BR:pt-419"
    for tentativa in range(1, tentativas + 1):
        try:
            r = requests.get(url, headers=HEADERS, timeout=30)
            r.raise_for_status()
            break
        except Exception:
            if tentativa == tentativas:
                raise
            time.sleep(2 * tentativa)
    root = ET.fromstring(r.content)
    corte = datetime.now(BRT) - timedelta(days=JANELA_DIAS)
    itens = []
    for item in root.findall(".//item")[:max_itens]:
        pub = item.findtext("pubDate", "")
        dt = _parse_data(pub)
        if dt and dt < corte:      # notícia velha, ignora
            continue
        fonte = item.find("{*}source")
        itens.append({
            "titulo": item.findtext("title", ""),
            "fonte": fonte.text if fonte is not None else "",
            "data": _formatar_data(pub),
            "link": item.findtext("link", ""),
        })
    return itens


# Bloco 1: termos de candidatura (convenção etc.) + pesquisa de intenção de voto
# (frases genéricas e nomes de institutos, o sinal mais forte).
TERMOS = ('(convenção OR pré-candidato OR "coordenador de campanha" '
          'OR "lançamento de candidatura" '
          'OR "pesquisa eleitoral" OR "intenção de voto" OR "pesquisa de opinião" '
          'OR Datafolha OR Quaest OR Ipec OR AtlasIntel OR "Paraná Pesquisas" '
          'OR "Real Time Big Data")')

# Bloco 2: o que acontece na campanha depois que a candidatura está de pé —
# agenda, debate, mudança de aliança, declaração de apoio e educação (para pegar
# crítica sobre política pública de educação). Fica numa busca separada em vez de
# entrar no bloco 1: o RSS do Google devolve no máximo ~20 itens por busca, então
# um OR gigante faria os termos novos disputarem espaço com convenção/pesquisa e
# ninguém apareceria direito. O Gemini separa depois pelo campo tipo.
TERMOS_CAMPANHA = ('("agenda de campanha" OR comício OR caravana '
                   'OR debate OR sabatina '
                   'OR coligação OR aliança OR federação OR palanque '
                   'OR "muda de partido" OR "troca de partido" '
                   'OR "declara apoio" OR "declaração de apoio" OR "anuncia apoio" '
                   'OR educação)')

BLOCOS = (TERMOS, TERMOS_CAMPANHA)


def gerar_buscas(cargos=('presidente', 'governador', 'senador')):
    buscas = []
    for termos in BLOCOS:
        for cargo in cargos:
            if cargo == 'presidente':        # presidente é nacional, sem UF
                buscas.append(f"eleições 2026 presidente {termos}")
            else:
                buscas += [f"eleições 2026 {cargo} {uf} {termos}" for uf in UFS]
    return buscas


def _buscar_em_paralelo(buscas, workers=None):
    """Roda as buscas em threads e devolve [(busca, itens)] NA ORDEM PEDIDA.

    A ordem importa porque quem chama deduplica por título ficando com a
    primeira ocorrência: mantendo a ordem das buscas, a notícia fica sempre
    atribuída à mesma busca, rodada após rodada.
    """
    buscas = list(buscas)

    def _uma(busca):
        try:
            return google_news_rss(busca)
        except Exception as e:
            print(f"erro em '{busca}': {e}")
            return []

    with ThreadPoolExecutor(max_workers=workers or WORKERS_RSS) as ex:
        return list(zip(buscas, ex.map(_uma, buscas)))


def coletar(cargos=('presidente', 'governador', 'senador')):
    """Roda todas as buscas e junta as notícias, sem repetir título."""
    vistos, resultado = set(), []
    for busca, itens in _buscar_em_paralelo(gerar_buscas(cargos)):
        for it in itens:
            chave = it['titulo'].strip().lower()
            if chave and chave not in vistos:
                vistos.add(chave)
                it['busca'] = busca
                resultado.append(it)
    return resultado


def _ler_pagina(url: str, limite: int = 6000) -> tuple[str, str]:
    """Devolve (texto do artigo, URL real) por trás do link do Google Notícias.

    O <link> do RSS (news.google.com/rss/articles/...) não é o artigo: é uma
    página de redirecionamento via JavaScript. Um navegador executa o JS e
    cai no site real; um requests.get() direto só pega a casca do Google,
    sem o texto da notícia. Por isso decodifica pra URL real (gnewsdecoder,
    mesmo truque usado pra esse tipo de link) antes de extrair — mesmo padrão
    do repo googlenews, usando newspaper3k pra extração (mais robusto que
    tirar tag na unha, lida melhor com a variedade de sites de notícia).

    A URL real vale por si: é ela que vai no alerta de WhatsApp e no email, no
    lugar do endereço news.google.com/rss/articles/... , que é comprido, opaco e
    não abre direto em alguns aparelhos.

    Falha em qualquer etapa (decodificação, paywall, bloqueio, timeout) só volta
    texto vazio e a URL que deu pra apurar; quem chama cai de volta pra
    classificar só pela manchete."""
    if not url or not url.startswith("http"):
        return "", ""
    try:
        decoded = gnewsdecoder(url, interval=1)
        url_real = decoded.get("decoded_url") if decoded.get("status") else url
    except Exception:
        url_real = url
    url_real = url_real or url
    try:
        art = Article(url_real, language="pt")
        art.download()
        art.parse()
        return (art.text or "").strip()[:limite], url_real
    except Exception:
        return "", url_real


# Variações de texto livre que o Gemini gera pra federação/partido, canonicalizadas
# pra sigla oficial em maiúsculas com "/" entre siglas (ex: PSOL/REDE, UNIÃO/PP).
# Ver normalize_partido() no painel (pages/5_Notícias.py) pro mesmo mapeamento,
# aplicado também nos dados antigos que já estão na planilha.
_PARTIDO_ALIAS = {
    "UNIAO": "UNIÃO",
    "UNIÃO BRASIL": "UNIÃO", "UNIAO BRASIL": "UNIÃO",
    "UNIÃO PROGRESSISTA": "UNIÃO/PP", "UNIAO PROGRESSISTA": "UNIÃO/PP",
    "UNIÃO/PROGRESSISTA": "UNIÃO/PP", "UNIAO/PROGRESSISTA": "UNIÃO/PP",
    "FEDERAÇÃO": "", "FEDERACAO": "",
}
_PARTIDO_VAZIO = {"", "NAN", "NONE", "NULL"}


def normalize_partido(raw) -> str:
    v = str(raw or "").strip()
    if not v:
        return ""
    key = re.sub(r"\s*[-/]\s*", "/", v.upper())
    if key in _PARTIDO_VAZIO:
        return ""
    return _PARTIDO_ALIAS.get(key, key)


# Assunto da notícia, independente do status da candidatura. O Gemini responde em
# texto livre, então acento, caixa e variação de escrita são canonicalizados aqui
# para o filtro do painel não virar uma lista de quase-duplicatas (foi o que
# aconteceu com o campo partido).
TIPOS = ("agenda", "pesquisa", "debate", "aliança", "apoio", "crítica-educação",
         "proposta-educação", "outro")
_TIPO_ALIAS = {
    "alianca": "aliança", "aliancas": "aliança", "alianças": "aliança",
    "coligacao": "aliança", "coligação": "aliança", "federacao": "aliança",
    "federação": "aliança",
    "apoios": "apoio", "declaracao de apoio": "apoio", "declaração de apoio": "apoio",
    "critica-educacao": "crítica-educação", "critica educacao": "crítica-educação",
    "crítica educação": "crítica-educação", "critica-educação": "crítica-educação",
    "crítica-educacao": "crítica-educação", "educacao": "crítica-educação",
    "educação": "crítica-educação",
    # proposta de educação é o oposto da crítica (o candidato diz o que vai fazer,
    # não ataca o adversário) e é um dos temas de alerta pedidos pelo time
    "proposta-educacao": "proposta-educação", "proposta educacao": "proposta-educação",
    "proposta educação": "proposta-educação", "propostas de educação": "proposta-educação",
    "proposta de educação": "proposta-educação",
    "proposta de governo em educação": "proposta-educação",
    "debates": "debate", "sabatina": "debate",
    "agendas": "agenda", "agenda de campanha": "agenda",
    "pesquisas": "pesquisa", "pesquisa eleitoral": "pesquisa",
}


def normalize_tipo(raw) -> str:
    v = str(raw or "").strip().lower()
    if not v or v in _PARTIDO_VAZIO or v.upper() in _PARTIDO_VAZIO:
        return "outro"
    v = _TIPO_ALIAS.get(v, v)
    return v if v in TIPOS else "outro"


def _config_gemini():
    """Orçamento de pensamento do flash. None = deixa o padrão do modelo."""
    if GEMINI_THINKING_BUDGET < 0:
        return None
    return {"thinking_config": {"thinking_budget": GEMINI_THINKING_BUDGET}}


# Status da candidatura. Mesma canonicalização do partido e do tipo: o modelo
# responde em texto livre e escorrega (numa rodada real veio "cobertura general",
# em espanhol, 6 vezes), o que vira quase-duplicata no filtro do painel.
STATUS = ("confirmado", "pré-candidato", "em disputa", "renúncia", "desistência",
          "pesquisa", "cobertura geral", "não relacionado", "indefinido")
_STATUS_ALIAS = {
    "cobertura general": "cobertura geral", "cobertura gerall": "cobertura geral",
    "general coverage": "cobertura geral", "cobertura": "cobertura geral",
    "pre-candidato": "pré-candidato", "pre candidato": "pré-candidato",
    "precandidato": "pré-candidato", "pré candidato": "pré-candidato",
    "nao relacionado": "não relacionado", "não-relacionado": "não relacionado",
    "nao-relacionado": "não relacionado", "not related": "não relacionado",
    "renuncia": "renúncia", "desistencia": "desistência",
}


def normalize_status(raw) -> str:
    v = str(raw or "").strip().lower()
    if not v or v.upper() in _PARTIDO_VAZIO:
        return "indefinido"
    v = _STATUS_ALIAS.get(v, v)
    return v if v in STATUS else "indefinido"


# ─── Régua de alerta ──────────────────────────────────────────────────────────
# Os quatro temas que o time trata como alerta, do prompt do agente de
# monitoramento do Slack. "Executivo" aqui é só governador ou presidente: pesquisa
# ou apoio para Senado e Câmara não vira alerta, mesmo sendo notícia relevante.
ALERTA_TEMAS = ("pesquisa-executivo", "apoio", "rompimento", "educação")
_TEMA_ALIAS = {
    "pesquisa": "pesquisa-executivo", "pesquisa executivo": "pesquisa-executivo",
    "pesquisa-eleitoral": "pesquisa-executivo", "pesquisa eleitoral": "pesquisa-executivo",
    "apoios": "apoio", "aliança": "apoio", "alianca": "apoio",
    "apoio e aliança": "apoio", "apoio/aliança": "apoio",
    "rompimentos": "rompimento", "rompimento de aliança": "rompimento",
    "rompimento de alianca": "rompimento",
    "educacao": "educação", "proposta-educação": "educação",
    "proposta-educacao": "educação", "proposta educação": "educação",
    "proposta de governo em educação": "educação", "crítica-educação": "educação",
    "critica-educacao": "educação",
}

# O tema de educação muda o cabeçalho do alerta ("Eixo | Educação" no lugar de
# "Eixo | Eleições"), que é como o time separa os dois envios.
TEMAS_EDUCACAO = ("educação",)
_TEMA_VAZIO = {"", "nenhum", "none", "null", "nan", "não", "nao", "n/a"}

# Só pesquisa desses institutos vira alerta (lista fechada, do prompt do time).
# Ipec e Ibope entram os dois: é a mesma casa antes e depois da troca de nome, e
# manchete regional ainda escreve "Ibope".
INSTITUTOS_PRIORITARIOS = ("Datafolha", "Quaest", "Ipec", "Ibope", "AtlasIntel",
                           "Paraná Pesquisas", "Real Time Big Data")

# Subtemas de educação que contam como proposta de governo.
SUBTEMAS_EDUCACAO = ("tempo integral", "educação profissional", "educação tecnológica",
                     "alfabetização", "escolas cívico-militares", "anos finais")

CARGOS_EXECUTIVO = ("governador", "vice-governador", "presidente")


def _chave_texto(valor) -> str:
    """Minúsculas, sem acento e sem pontuação: 'Paraná Pesquisas' -> 'paranapesquisas'."""
    return re.sub(r"[^a-z0-9]", "", _sem_acento(valor).lower())


_INSTITUTOS_CHAVE = {_chave_texto(i) for i in INSTITUTOS_PRIORITARIOS}
_INSTITUTOS_CHAVE.add("rtbd")   # como o Real Time Big Data às vezes aparece


def normalize_tema(raw) -> str:
    v = str(raw or "").strip().lower()
    if v in _TEMA_VAZIO:
        return ""
    v = _TEMA_ALIAS.get(v, v)
    return v if v in ALERTA_TEMAS else ""


def instituto_prioritario(raw) -> str:
    """Devolve o nome canônico do instituto, ou '' se não for da lista fechada.

    Compara por chave sem acento nem espaço e aceita o nome dentro de uma frase
    ("pesquisa Quaest/Genial"), porque o Gemini às vezes devolve o texto do jeito
    que estava na manchete em vez do nome seco.
    """
    chave = _chave_texto(raw)
    if not chave:
        return ""
    for nome in INSTITUTOS_PRIORITARIOS:
        if _chave_texto(nome) in chave:
            return nome
    return "Real Time Big Data" if "rtbd" in chave else ""


def aplicar_regra_alerta(dados) -> dict:
    """Confere em código o alerta que o Gemini propôs e derruba o que não passa.

    O modelo lê a notícia e sugere o tema; quem decide é esta função. As condições
    aqui são estruturais (o cargo é do executivo? o instituto é da lista?), então
    não dependem de interpretação e não valem uma segunda chamada de Gemini.

    Assimetria de propósito: pesquisa é estrita (cargo precisa estar explícito e
    ser do executivo, instituto precisa ser um dos sete), os outros três são
    permissivos e só caem quando o cargo é claramente de legislativo. É a regra do
    time: na dúvida entre alertar e não alertar, alerta.
    """
    tema = normalize_tema(dados.get("alerta_tema"))
    cargo = str(dados.get("cargo") or "").strip().lower()
    instituto = instituto_prioritario(dados.get("instituto"))
    dados["instituto"] = instituto or ""

    if tema == "pesquisa-executivo" and not (cargo in CARGOS_EXECUTIVO and instituto):
        tema = ""
    elif tema and tema not in TEMAS_EDUCACAO and cargo and cargo not in CARGOS_EXECUTIVO:
        # educação escapa da trava de cargo: o alerta costuma sair de debate com
        # vários candidatos na mesa, e nesse caso o cargo que o modelo devolve é
        # o de quem falou, não o da disputa
        tema = ""
    if dados.get("status") == "não relacionado":
        tema = ""

    dados["alerta_tema"] = tema
    dados["alerta"] = "sim" if tema else "não"
    return dados


def _uf_relevante(n) -> str:
    """UF que descreve o FATO, não o veículo que publicou.

    O Gemini preenche uf mesmo em notícia nacional: em 10/08 a mesma declaração
    do presidente da Câmara apoiando Lula veio como SP no Metrópoles e RO no
    Diário da Amazônia. Com a UF na chave, o mesmo fato virou dois alertas, e o
    cabeçalho ainda anunciou "Subnacional" numa disputa presidencial.

    Em notícia de presidente a UF só significa alguma coisa quando é pesquisa:
    pesquisa presidencial estadual existe e cada UF é um fato diferente, então
    ali a UF fica.
    """
    uf = str(n.get("uf") or "").strip().upper()
    if uf in ("", "NAN", "NONE", "NULL"):
        return ""
    cargo = str(n.get("cargo") or "").strip().lower()
    if cargo == "presidente" and n.get("alerta_tema") != "pesquisa-executivo":
        return ""
    return uf


def chave_alerta(n) -> str:
    """Identifica o FATO, não a notícia: mesmo tema, UF, cargo e candidato.

    Serve pra não alertar duas vezes quando a mesma pesquisa ou o mesmo apoio sai
    em dois veículos, com títulos e links diferentes.

    Sem nome de candidato não dá pra dizer que dois textos falam do mesmo fato, e
    colapsar por (tema, UF, cargo) engoliria notícias distintas. Nesse caso a
    chave leva o título, ou seja, não deduplica nada além do que a deduplicação
    por título já pega.
    """
    if not n.get("alerta_tema"):
        return ""
    cand = _chave_texto(n.get("candidato"))
    partes = [n.get("alerta_tema"), _uf_relevante(n),
              str(n.get("cargo") or "").lower(),
              cand or f"titulo:{_chave_texto(n.get('titulo'))[:60]}"]
    return "|".join(partes)


def classificar_com_gemini(titulo, trecho=""):
    """Lê a manchete (e trecho do artigo) e extrai os campos estruturados (JSON) via Gemini."""
    contexto = f"Manchete: {titulo}"
    if trecho:
        contexto += f"\n\nTrecho do artigo: {trecho}"

    prompt = (
        "Você é um analista eleitoral especializado nas eleições brasileiras de 2026.\n"
        "Analise o conteúdo abaixo e extraia as informações em JSON com exatamente estes campos:\n\n"
        "{\n"
        '  "candidato": "Nome, apelido ou sobrenome de UMA ÚNICA pessoa, como aparece no texto (ex: \'Tarcísio\', \'Lula\', \'Romeu Zema\'). Se a notícia citar vários políticos, escolha só o mais central ao tema da manchete — nunca liste mais de um nome no campo. Use null se nenhum nome próprio de político aparecer",\n'
        '  "cargo": "governador | senador | presidente | vice-governador | outro | null",\n'
        '  "uf": "Sigla do estado (ex: SP, RJ, MG) ou null se cargo federal ou não identificado",\n'
        '  "partido": "Sigla oficial do partido em maiúsculas (ex: PT, PL, MDB). Se for federação, siglas separadas por \'/\' (ex: PSOL/REDE, UNIÃO/PP). null se não mencionado",\n'
        '  "status": "confirmado | pré-candidato | em disputa | renúncia | desistência | pesquisa | cobertura geral | não relacionado | indefinido",\n'
        '  "convencao": true ou false — true SOMENTE se a notícia trata diretamente de uma convenção partidária '
        "(data, realização, resultado ou decisão tomada em convenção). Independente do status da candidatura.\n"
        '  "tipo": "agenda | pesquisa | debate | aliança | apoio | crítica-educação | proposta-educação | outro",\n'
        '  "instituto": "Nome do instituto de pesquisa, se a notícia trata de pesquisa de intenção de voto '
        "(ex: Datafolha, Quaest, Ipec, AtlasIntel). null se não for pesquisa ou se o instituto não for citado\",\n"
        '  "alerta_tema": "pesquisa-executivo | apoio | rompimento | proposta-educação | nenhum",\n'
        '  "confianca": "alto | médio | baixo"\n'
        "}\n\n"
        "Regras de preenchimento:\n"
        "- status='confirmado': candidatura oficializada em convenção partidária ou registro formal\n"
        "- status='pré-candidato': intenção declarada publicamente, sem oficialização ainda\n"
        "- status='em disputa': partido ou coligação ainda decide entre dois ou mais nomes\n"
        "- status='renúncia' ou 'desistência': candidato que retirou ou perdeu a candidatura\n"
        "- status='pesquisa': o tema central da notícia é o resultado ou a divulgação de uma pesquisa eleitoral de "
        "intenção de voto (ex.: Datafolha, Quaest, Ipec, AtlasIntel, Paraná Pesquisas, Real Time Big Data), trazendo "
        "percentuais ou números de candidatos. Independente de citar convenção ou o status de alguma candidatura\n"
        "- status='cobertura geral': cita um político, partido ou estado brasileiro e tem a ver com política/eleições "
        "do Brasil, mas não trata do status da candidatura em si nem é sobre uma pesquisa (ex.: declaração, agenda de "
        "campanha, repercussão de um fato político)\n"
        "- status='não relacionado': NÃO cita nenhum político, partido ou estado brasileiro, ou trata de assunto sem "
        "ligação com política brasileira (ex.: notícia de política internacional — Trump, eleições de outro país — "
        "que não menciona nenhum político/partido/estado do Brasil)\n"
        "- status='indefinido': é sobre candidatura, mas a manchete é ambígua ou falta informação\n"
        "- Notícia de política internacional só NÃO é 'não relacionado' se citar explicitamente um político, "
        "partido ou estado brasileiro (nesse caso, classifique normalmente pelos outros campos)\n"
        "- convencao é independente de status: uma notícia pode ser status='confirmado' e convencao=true "
        "(candidatura saiu de uma convenção) ou status='cobertura geral' e convencao=true (cobertura do evento "
        "em si, sem falar do status de ninguém)\n"
        "- tipo diz do que a notícia TRATA e é independente de status e de convencao (ex.: status='confirmado' "
        "e tipo='apoio'). Escolha um só, o mais central à manchete:\n"
        "  - tipo='agenda': compromisso de campanha do candidato (comício, caravana, visita, viagem, evento, "
        "encontro com eleitor ou categoria), incluindo o anúncio da agenda e a cobertura do que aconteceu nela\n"
        "  - tipo='pesquisa': resultado ou divulgação de pesquisa eleitoral de intenção de voto\n"
        "  - tipo='debate': debate ou sabatina entre candidatos, tanto o anúncio (data, emissora, confirmação de "
        "participação ou ausência) quanto a repercussão depois de realizado\n"
        "  - tipo='aliança': composição partidária mudando (coligação, federação, palanque, partido trocando de "
        "lado, racha, rompimento, negociação de chapa, definição de vice)\n"
        "  - tipo='apoio': uma pessoa, partido, entidade ou grupo declara apoio ou endossa publicamente uma "
        "candidatura, sem que isso seja um acordo entre partidos (aí é 'aliança')\n"
        "  - tipo='crítica-educação': as DUAS condições ao mesmo tempo. (1) Quem critica é um candidato ou "
        "pré-candidato (ou a campanha/o vice/o coordenador falando por ele), e o alvo é outro candidato, a "
        "chapa adversária ou a gestão que o adversário defende. Crítica feita por jornalista, colunista, "
        "editorial, sindicato, entidade, especialista ou eleitor NÃO conta. (2) O objeto da crítica é política "
        "pública de educação (escola, professor, creche, ensino médio, alfabetização, merenda, piso do "
        "magistério, tempo integral, universidade, gestão da rede de ensino). Se o candidato critica o "
        "adversário por outro assunto (saúde, segurança, corrupção, economia), use 'outro'\n"
        "  - tipo='proposta-educação': candidato ou pré-candidato apresenta proposta, promessa ou "
        "compromisso de governo na área de educação. É o que ele diz que vai fazer, não ataque ao "
        "adversário (isso é 'crítica-educação')\n"
        "  - tipo='outro': não se encaixa em nenhum dos anteriores\n"
        "- confianca='alto': candidato, cargo e UF estão todos explícitos no texto\n"
        "- confianca='médio': algum campo foi inferido com boa certeza pelo contexto\n"
        "- confianca='baixo': muita ambiguidade ou faltam dois ou mais campos principais\n"
        "- Use null (não a string 'null') para campos ausentes\n\n"
        "Régua de alerta (campo alerta_tema). Diz se a notícia merece virar alerta para o cliente, e é "
        "independente de status, tipo e convencao. Aqui 'executivo' significa SOMENTE governador ou "
        "presidente, nunca senador, deputado, prefeito ou vereador:\n"
        "- 'pesquisa-executivo': resultado ou divulgação de pesquisa de intenção de voto para governador "
        "ou presidente. Pesquisa para Senado, Câmara ou Assembleia NÃO conta\n"
        "- 'apoio': declaração pública de apoio, endosso ou formação de aliança a favor de um candidato ao "
        "executivo, partindo de partido, dirigente partidário estadual ou nacional, ex-governador, "
        "ex-presidente ou prefeito de capital\n"
        "- 'rompimento': rompimento público de aliança ou retirada de apoio envolvendo candidato ao "
        "executivo e os mesmos atores acima\n"
        "- 'educação': o assunto CENTRAL da notícia é política pública de educação na disputa pelo "
        "executivo. Vale proposta ou plano de governo, fala em debate ou sabatina, e crítica de um "
        "candidato à gestão do adversário. Assuntos que contam: tempo integral, educação profissional e "
        "tecnológica, alfabetização, escolas cívico-militares, anos finais do ensino fundamental, Ideb, "
        "professores, infraestrutura da rede.\n"
        "  NÃO marque 'educação' quando educação for só cenário ou um bloco entre vários: cobertura de "
        "debate cujo tema principal é outro (segurança, saúde, economia, bate-boca entre candidatos), "
        "ainda que educação apareça na lista de assuntos, é 'nenhum'. Pergunte: se eu tirar educação da "
        "notícia, ela deixa de existir? Se continuar de pé, não é 'educação'.\n"
        "  Precisa ser candidato, pré-candidato ou campanha falando; matéria de jornalista, sindicato ou "
        "especialista sobre educação, sem candidato na história, não conta\n"
        "- 'nenhum': todo o resto, inclusive notícia relevante que não se encaixa nos quatro temas acima\n"
        "- Na dúvida entre 'nenhum' e um tema que se encaixa, escolha o tema\n\n"
        "- Responda SOMENTE o objeto JSON, sem texto extra, sem markdown, sem bloco de código\n\n"
        f"{contexto}"
    )
    resp = _gemini_client().models.generate_content(
        model=GEMINI_MODEL, contents=prompt, config=_config_gemini())
    _registrar_uso(resp)
    texto = (getattr(resp, "text", "") or "").strip()
    texto = texto.replace("```json", "").replace("```", "").strip()
    dados = json.loads(texto)
    # o Gemini às vezes desobedece a instrução e devolve a string "null" em vez
    # do null de JSON de verdade — isso vazava pra planilha como texto "null"
    for campo, valor in list(dados.items()):
        if isinstance(valor, str) and valor.strip().lower() in ("null", "none", "n/a"):
            dados[campo] = None
    dados["partido"] = normalize_partido(dados.get("partido"))
    dados["tipo"] = normalize_tipo(dados.get("tipo"))
    dados["status"] = normalize_status(dados.get("status"))
    # normaliza pra string: bool False vira "" com o _safe() de salvar_no_sheets
    # (False é falsy em Python), o que confundiria "não" com "não preenchido"
    dados["convencao"] = "sim" if dados.get("convencao") is True else "não"
    return aplicar_regra_alerta(dados)


def _classificar_uma(n):
    """Baixa o artigo e classifica uma notícia, no lugar (dict mutado)."""
    trecho, url_real = _ler_pagina(n.get("link", ""))
    n["texto_completo"] = trecho
    n["link_real"] = url_real
    n.update(classificar_com_gemini(n["titulo"], trecho))
    # site regional: a UF é conhecida, preenche se o Gemini não achou
    if not n.get("uf") and n.get("uf_regional"):
        n["uf"] = n["uf_regional"]
    return n


def _em_paralelo(itens, tarefa, total_rotulo="classificadas", workers=None,
                 passo=25):
    """Roda `tarefa` sobre `itens` em threads, com log de andamento.

    Erro de um item não derruba o lote: baixar artigo e falar com o Gemini falha
    por motivo isolado (paywall, timeout, cota), e uma rodada tem centenas de
    itens.
    """
    itens = list(itens)
    total = len(itens)
    feitos = [0]
    lock = threading.Lock()

    def _uma(item):
        try:
            tarefa(item)
        except Exception as e:
            print(f"  erro ao classificar: {e}")
        with lock:
            feitos[0] += 1
            if feitos[0] % passo == 0 or feitos[0] == total:
                print(f"[{feitos[0]}/{total}] {total_rotulo}...")

    with ThreadPoolExecutor(max_workers=workers or WORKERS_CLASSIFICACAO) as ex:
        list(ex.map(_uma, itens))
    return itens


def classificar_noticias(noticias):
    """Aplica o Gemini em cada notícia coletada, acrescentando os campos."""
    return _em_paralelo(noticias, _classificar_uma)


# ─── Texto do alerta ──────────────────────────────────────────────────────────
# Mesmo formato do botão de envelope do painel interno (pages/5_Notícias.py):
# quem abre a linha na planilha e quem abre no painel tem que ver o mesmo texto.
# Se mudar aqui, mude lá.
REGRAS_POLITICOS_ALERTA = (
    "Formatação de políticos (obrigatório):\n"
    "- Formato: 'Nome (PARTIDO/UF)'. Use barra, nunca hífen entre PARTIDO e UF.\n"
    "- Escreva a sigla como ela se escreve: PT, PL, PSDB, MDB em maiúsculas, mas "
    "Republicanos, Podemos, Solidariedade, Avante, União Brasil e Cidadania com "
    "inicial maiúscula. Nunca 'REPUBLICANOS'.\n"
    "- Se partido/UF não estiverem na notícia, não invente.\n"
    "- PRIMEIRA menção de um político: use 'Nome (PARTIDO/UF)'. Menções seguintes: só o nome.\n"
)


def _header_alerta(n) -> str:
    """Cabeçalho no formato que o time manda no WhatsApp:

        Alerta | Eixo | Eleições | Subnacional | MG
        Alerta | Eixo | Educação | Subnacional | GO
        Alerta | Eixo | Eleições | Gov. Federal

    O terceiro campo separa os dois envios que o time faz (eleições e educação).
    O quarto é o escopo: "Gov. Federal" quando o fato é da disputa presidencial,
    e nada quando não deu pra saber a UF de uma disputa estadual, porque afirmar
    o escopo errado é pior do que omitir.
    """
    assunto = "Educação" if n.get("alerta_tema") in TEMAS_EDUCACAO else "Eleições"
    uf = _uf_relevante(n)
    if uf:
        return f"Alerta | Eixo | {assunto} | Subnacional | {uf}"
    if str(n.get("cargo") or "").strip().lower() == "presidente":
        return f"Alerta | Eixo | {assunto} | Gov. Federal"
    return f"Alerta | Eixo | {assunto}"


def _encurtar_link(url: str) -> str:
    """Encurta no TinyURL, como o time faz à mão. Falhou, devolve a URL inteira:
    link comprido incomoda, link faltando quebra o alerta."""
    if not url or not url.startswith("http"):
        return url
    try:
        r = requests.get("http://tinyurl.com/api-create.php",
                         params={"url": url}, headers=HEADERS, timeout=8)
        curto = r.text.strip()
        return curto if curto.startswith("http") else url
    except Exception:
        return url


# Pede o JSON pelo schema, não só pelo texto do prompt. Sem isso o modelo escreve
# a quebra de parágrafo como quebra de linha de verdade dentro da string, o que é
# JSON inválido: em 10/08 um alerta saiu com o objeto inteiro no lugar do texto.
CONFIG_TITULO_CORPO = {
    "response_mime_type": "application/json",
    "response_schema": {
        "type": "object",
        "properties": {"titulo": {"type": "string"}, "corpo": {"type": "string"}},
        "required": ["titulo", "corpo"],
    },
}

# Última linha de defesa se o JSON vier quebrado mesmo assim: pesca os dois campos
# no texto cru. Melhor um alerta com o título do RSS do que um alerta com chave e
# chaveta no meio do WhatsApp.
_RE_TITULO = re.compile(r'"titulo"\s*:\s*"(.*?)"\s*,\s*"corpo"', re.S)
_RE_CORPO = re.compile(r'"corpo"\s*:\s*"(.*?)"\s*\}?\s*$', re.S)


def _extrair_titulo_corpo(bruto: str) -> tuple[str, str]:
    bruto = bruto.replace("```json", "").replace("```", "").strip()
    if not bruto:
        return "", ""
    try:
        dados = json.loads(bruto)
        return str(dados.get("titulo") or "").strip(), str(dados.get("corpo") or "").strip()
    except Exception:
        pass
    t = _RE_TITULO.search(bruto)
    c = _RE_CORPO.search(bruto)
    if c:
        return ((t.group(1).strip() if t else ""),
                c.group(1).replace("\\n", "\n").strip())
    # nem JSON nem parecido com JSON: era texto corrido mesmo
    return "", bruto


def _titulo_sem_veiculo(titulo: str) -> str:
    """Tira o ' - Veículo' que o Google Notícias cola no fim de toda manchete."""
    return re.sub(r"\s+[-–]\s+[^-–]{2,40}$", "", str(titulo or "").strip()).strip()


def gerar_texto_alerta(n) -> str:
    """Escreve o alerta pronto pra colar no WhatsApp, a partir da notícia já
    classificada. Só roda pros itens com alerta='sim', que são poucos por rodada.

    Sem thinking_config: aqui o modelo está escrevendo, não extraindo campo, e o
    orçamento zerado que barateia a classificação não se justifica em meia dúzia
    de chamadas.
    """
    contexto = "\n".join(filter(None, [
        f"Título: {n.get('titulo', '')}",
        f"Fonte: {n.get('fonte')}" if n.get("fonte") else "",
        f"Data: {n.get('data')}" if n.get("data") else "",
        f"Candidato citado: {n.get('candidato')} ({n.get('partido')})" if n.get("candidato") else "",
        f"UF: {n.get('uf')}" if n.get("uf") else "",
        f"Cargo: {n.get('cargo')}" if n.get("cargo") else "",
        f"Instituto: {n.get('instituto')}" if n.get("instituto") else "",
        f"Trecho do artigo:\n{n.get('texto_completo')}" if n.get("texto_completo") else "",
    ]))
    prompt = (
        "Você é um analista que produz alertas padronizados para WhatsApp, para uma "
        "consultoria política que acompanha as eleições de 2026.\n"
        "A partir da notícia abaixo, devolve um JSON com exatamente dois campos:\n\n"
        '{"titulo": "...", "corpo": "..."}\n\n'
        "titulo: uma linha, em PT-BR, dizendo o fato principal. É manchete reescrita por "
        "você, não cópia: nunca termine com o nome do veículo, nunca use aspas de citação "
        "no começo. Cite o político central como 'Nome (PARTIDO/UF)' quando isso couber "
        "na linha.\n"
        "corpo: DOIS parágrafos, separados por uma linha em branco, 130 palavras no total "
        "no máximo.\n"
        "  - 1º parágrafo: o fato. Quem fez o quê, quando (com a data por extenso no "
        "formato 'nesta quinta-feira (6)'), onde, e o número principal se houver.\n"
        "  - 2º parágrafo: o desdobramento. O que a decisão destrava ou trava, quem fica "
        "de fora, como fica o quadro da disputa depois disso. Se a notícia não trouxer "
        "desdobramento nenhum, use o 2º parágrafo para o detalhe concreto que sobrou "
        "(outros participantes, propostas citadas, próxima etapa) em vez de encher "
        "linguiça.\n\n"
        "Regras dos dois campos:\n"
        "- Factual e direto. Sem opinião, sem especulação, sem adjetivo de torcida, sem "
        "bullets, sem emoji, sem travessão.\n"
        "- Preserve nomes, cargos, datas e números exatamente como na notícia.\n"
        "- Se houver trecho do artigo, baseie os fatos e números nele, é mais completo que "
        "o título. Sem trecho, use só o título e não invente detalhe que não está nele.\n"
        "- Não escreva 'ALERTA' nem repita o cabeçalho: isso é montado fora daqui.\n"
        "- Responda SOMENTE o JSON, sem markdown e sem bloco de código.\n\n"
        f"{REGRAS_POLITICOS_ALERTA}\n"
        f"NOTÍCIA:\n{contexto}"
    )
    resp = _gemini_client().models.generate_content(
        model=GEMINI_MODEL, contents=prompt, config=CONFIG_TITULO_CORPO)
    _registrar_uso(resp)
    titulo, corpo = _extrair_titulo_corpo(getattr(resp, "text", "") or "")
    if not corpo:
        return ""
    titulo = titulo or _titulo_sem_veiculo(n.get("titulo"))
    link = _encurtar_link(n.get("link_real") or n.get("link") or "")
    partes = [f"*{_header_alerta(n)}*",
              datetime.now(BRT).strftime("%d/%m/%Y"),
              "",
              f"*{titulo}*",
              "",
              corpo]
    if link.startswith("http"):
        partes += ["", f"Link: {link}"]
    return "\n".join(partes)


def escrever_alertas(noticias):
    """Preenche 'resumo' com o texto pronto, nos itens marcados como alerta.

    Não sobrescreve resumo que já exista: em notícia nova ele está sempre vazio,
    mas a função também roda em cima de linha que alguém já editou à mão no
    painel, e o texto da pessoa vale mais que o do modelo.
    """
    alvo = [n for n in noticias
            if n.get("alerta") == "sim" and not str(n.get("resumo") or "").strip()]
    if not alvo:
        return noticias

    def _escrever(n):
        n["resumo"] = gerar_texto_alerta(n)

    print(f"{len(alvo)} alerta(s) para escrever...")
    _em_paralelo(alvo, _escrever, total_rotulo="alertas escritos", passo=5)
    return noticias


# ─── Deduplicação por fato ────────────────────────────────────────────────────
JANELA_DEDUP_HORAS = int(os.getenv("NOTICIAS_JANELA_DEDUP_HORAS", "48"))


def carregar_chaves_recentes(aba):
    """Chaves de alerta já gravadas nas últimas JANELA_DEDUP_HORAS.

    Lê só duas colunas e só o topo da aba (LIMITE_VARREDURA linhas): a inserção é
    sempre no topo, então tudo que é recente está lá, e a aba inteira passa de 11
    mil linhas.
    """
    headers = aba.row_values(1)
    if "alerta_chave" not in headers or "data" not in headers:
        return set()
    corte = datetime.now(BRT) - timedelta(hours=JANELA_DEDUP_HORAS)
    recentes = set()
    for r in _ler_colunas(aba, headers, ("alerta_chave", "data")):
        chave = r.get("alerta_chave", "")
        dt = _data_planilha(r.get("data", ""))
        if chave and dt and dt >= corte:   # sem data legível, fora da janela
            recentes.add(chave)
    return recentes


def marcar_repetidos(noticias, chaves_recentes):
    """Rebaixa para 'repetido' o alerta cujo fato já foi alertado.

    Vale contra o que já está na planilha e contra a própria rodada: a mesma
    pesquisa sai em quatro veículos no mesmo dia e as quatro chegam juntas aqui.
    A linha continua na planilha com o tema preenchido, só não vira email.
    """
    vistos = set(chaves_recentes)
    for n in noticias:
        chave = chave_alerta(n)
        n["alerta_chave"] = chave
        if not chave or n.get("alerta") != "sim":
            continue
        if chave in vistos:
            n["alerta"] = "repetido"
        else:
            vistos.add(chave)
    return noticias


COLUNAS_PLANILHA = [
    "candidato", "cargo", "uf", "partido", "status", "convencao", "tipo",
    "alerta", "alerta_tema", "instituto", "resumo",
    "confianca", "titulo", "fonte", "data", "link", "link_real", "busca",
    "alerta_chave", "alerta_enviado_em", "texto_completo",
]

# A ordem acima só vale pra uma aba criada do zero. Na aba que já existe, coluna
# nova entra no fim do cabeçalho (_garantir_colunas) e a gravação é toda por
# nome, então a posição não muda nada.


def _gc():
    import gspread
    from google.oauth2.service_account import Credentials
    creds_json = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    if creds_json:
        info = {k: v for k, v in json.loads(creds_json).items() if k in _SA_FIELDS}
    else:
        with open("credentials.json", encoding="utf-8") as f:
            info = {k: v for k, v in json.load(f).items() if k in _SA_FIELDS}
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return _autorizar(Credentials.from_service_account_info(info, scopes=scopes))


def _sheets_aba():
    import gspread
    sh = _gc().open_by_key(SHEET_ID)
    try:
        aba = sh.worksheet(SHEET_ABA)
    except gspread.exceptions.WorksheetNotFound:
        print(f"Aba '{SHEET_ABA}' não encontrada — criando com cabeçalho...")
        aba = sh.add_worksheet(title=SHEET_ABA, rows=5000, cols=len(COLUNAS_PLANILHA))
        aba.append_row(COLUNAS_PLANILHA)
        return aba
    _garantir_colunas(aba)
    return aba


def _garantir_colunas(aba):
    """Acrescenta no fim do cabeçalho as colunas que o código já grava e a
    planilha ainda não tem.

    A gravação é toda por nome de coluna (`headers` da planilha viva), então uma
    coluna nova no código sem coluna na planilha é ignorada em silêncio: o campo
    é classificado, custa Gemini e não chega em lugar nenhum.

    A grade cresce antes da escrita: escrever em coluna fora do tamanho da
    planilha devolve 400 ("exceeds grid limits"), e foi o que derrubou a rodada
    de 30/07 quando a coluna 'tipo' entrou no código — a aba tinha exatamente 15
    colunas ocupadas.
    """
    from gspread.utils import rowcol_to_a1
    headers = aba.row_values(1)
    if not headers:
        return
    faltando = [c for c in COLUNAS_PLANILHA if c not in headers]
    if not faltando:
        return
    inicio = len(headers) + 1
    fim = inicio + len(faltando) - 1
    largura = _col_count_atual(aba)
    if fim > largura:
        aba.add_cols(fim - largura)
    faixa = f"{rowcol_to_a1(1, inicio)}:{rowcol_to_a1(1, fim)}"
    aba.update(range_name=faixa, values=[faltando])
    print(f"Coluna(s) acrescentada(s) no cabeçalho: {', '.join(faltando)}")


# Quantas linhas do topo da aba as varreduras de alerta olham. A inserção é
# sempre no topo (salvar_no_sheets), então alerta pendente de envio e chave
# recente estão sempre nas primeiras linhas; a aba inteira já passou de 11 mil e
# ler tudo a cada rodada é caro à toa.
LIMITE_VARREDURA = int(os.getenv("NOTICIAS_LIMITE_VARREDURA", "600"))


def _col_letra(headers, coluna):
    from gspread.utils import rowcol_to_a1
    return rowcol_to_a1(1, headers.index(coluna) + 1).rstrip("1")


def _ler_colunas(aba, headers, colunas, limite=None):
    """batch_get de colunas inteiras pelo nome, do topo até `limite`.

    Devolve [{coluna: valor}] com a linha da planilha em '_linha'. Só as colunas
    pedidas: uma delas é o texto completo do artigo, e get_all_values() na aba
    inteira baixa dezenas de MB por rodada.
    """
    colunas = [c for c in colunas if c in headers]
    if not colunas:
        return []
    fim = limite or LIMITE_VARREDURA
    faixas = [f"{_col_letra(headers, c)}2:{_col_letra(headers, c)}{fim}" for c in colunas]
    blocos = aba.batch_get(faixas)
    total = max((len(b) for b in blocos), default=0)
    registros = []
    for i in range(total):
        reg = {"_linha": i + 2}
        for coluna, bloco in zip(colunas, blocos):
            linha = bloco[i] if i < len(bloco) else []
            reg[coluna] = (linha[0] if linha else "").strip()
        registros.append(reg)
    return registros


def carregar_sites_regionais():
    """Lê a planilha de sites regionais e retorna [(dominio, uf)]."""
    if not SITES_ID:
        print("SPREADSHEET_ID_SITES não definido; pulando sites regionais.")
        return []
    try:
        aba = _gc().open_by_key(SITES_ID).worksheet(SITES_ABA)
        registros = aba.get_all_records()
    except Exception as e:
        print(f"Aviso: não foi possível ler os sites regionais: {e}")
        return []
    sites, vistos = [], set()
    for r in registros:
        link = str(r.get("Link", "") or r.get("link", "")).strip()
        estado = str(r.get("Estado", "") or r.get("estado", "")).strip()
        if not link:
            continue
        alvo = link if "//" in link else "http://" + link
        dom = urllib.parse.urlparse(alvo).netloc.lower()
        dom = dom[4:] if dom.startswith("www.") else dom
        if not dom or dom in vistos:
            continue
        vistos.add(dom)
        sites.append((dom, NOME_UF.get(estado.lower(), "")))
    return sites


def coletar_regionais(sites):
    """Busca no Google Notícias restrito a cada domínio (site:), tagueando a UF."""
    pedidos = [(f"site:{dom} {termos}", dom, uf)
               for dom, uf in sites for termos in BLOCOS]
    origem = {busca: (dom, uf) for busca, dom, uf in pedidos}

    vistos, resultado = set(), []
    for busca, itens in _buscar_em_paralelo([p[0] for p in pedidos]):
        dom, uf = origem[busca]
        for it in itens:
            chave = it['titulo'].strip().lower()
            if chave and chave not in vistos:
                vistos.add(chave)
                it['busca'] = f"regional:{dom}"
                it['uf_regional'] = uf
                resultado.append(it)
    return resultado


def carregar_titulos_existentes(tentativas=3):
    """Retorna o conjunto de títulos já salvos na aba do Sheets.

    Levanta erro em vez de devolver conjunto vazio: sem os títulos, TODA notícia
    coletada vira notícia nova, e a rodada reclassifica a planilha inteira. Em
    30/07 isso aconteceu por um 400 do Sheets engolido aqui — 1369 notícias
    reclassificadas do zero, 3h25 de runner e o dinheiro de Gemini jogados fora
    (a rodada ainda morreu antes de salvar). Falhar rápido é mais barato.
    """
    erro = None
    for tentativa in range(1, tentativas + 1):
        try:
            aba = _sheets_aba()
            headers = aba.row_values(1)
            if "titulo" not in headers:
                raise RuntimeError("a aba não tem a coluna 'titulo'")
            col = headers.index("titulo") + 1
            valores = aba.col_values(col)[1:]
            return {v.strip().lower() for v in valores if v.strip()}
        except Exception as e:
            erro = e
            print(f"Tentativa {tentativa} de ler os títulos existentes falhou: {e}")
            if tentativa < tentativas:
                time.sleep(5 * tentativa)
    raise RuntimeError(
        "Não deu pra ler os títulos já salvos, então não dá pra saber o que é "
        f"notícia nova. Rodada abortada antes de gastar Gemini. Causa: {erro}")


def _safe(v):
    # neutraliza injeção de fórmula em texto que começa com = + - @
    s = str(v or "")
    return ("'" + s) if s[:1] in ("=", "+", "-", "@") else s


LOTE_RECLASSIFICACAO = 25  # linhas por batch_update


def reclassificar_pendentes(aba):
    """Reclassifica no Gemini as linhas com a coluna 'status' vazia (ex.: você
    apagou a classificação à mão pra refazer). Atualiza só as colunas de
    classificação, mantendo a notícia.

    Salva em lotes de LOTE_RECLASSIFICACAO linhas em vez de um único
    batch_update no final: com milhares de linhas pendentes (cada uma custa
    um download de artigo + uma chamada ao Gemini), o job pode ser cancelado
    pelo timeout do runner antes de terminar. Sem salvar incrementalmente,
    isso jogava fora todo o progresso já feito, porque a lista de updates só
    ia pro Sheets no final do loop inteiro.

    Cada lote é classificado em paralelo (mesmo pool da coleta) e só então
    gravado, para o progresso continuar sendo salvo de LOTE em LOTE.
    """
    headers = aba.row_values(1)
    if "titulo" not in headers or "status" not in headers:
        return

    def _letra(coluna):
        return _col_letra(headers, coluna)

    # Só as três colunas que interessam, em vez de get_all_values(): a aba tem
    # mais de 11 mil linhas e uma delas é o texto completo do artigo, ou seja,
    # dezenas de MB baixados a cada rodada só pra achar as linhas sem status.
    lidas = ("titulo", "status", "link")
    faixas = [f"{_letra(c)}2:{_letra(c)}" for c in lidas if c in headers]
    blocos = aba.batch_get(faixas)
    coluna_de = dict(zip([c for c in lidas if c in headers], blocos))

    def _valor(coluna, i):
        bloco = coluna_de.get(coluna) or []
        linha = bloco[i] if i < len(bloco) else []
        return (linha[0] if linha else "").strip()

    # "resumo" fica de fora: não é gerado aqui, é o alerta que alguém gerou e
    # decidiu salvar no painel. Reclassificar não pode sobrescrever isso.
    # "alerta_enviado_em" também fica de fora, por outro motivo: é registro do que
    # já saiu por email, não classificação. Reescrever apagaria o histórico e
    # reenviaria alerta velho.
    cols = [c for c in ("candidato", "cargo", "uf", "partido", "status", "convencao",
                        "tipo", "alerta", "alerta_tema", "instituto", "alerta_chave",
                        "confianca", "link_real", "texto_completo")
            if c in headers]
    # célula por célula, não um range candidato→confiança: se as colunas novas
    # forem adicionadas fora da ordem (ex.: no fim da planilha, depois de titulo/
    # fonte/data/link/busca), um range contíguo escreveria por cima dessas colunas.
    col_letra = {c: _letra(c) for c in cols}

    total_linhas = max((len(b) for b in blocos), default=0)
    pendentes = [
        {"linha": i + 2, "titulo": _valor("titulo", i), "link": _valor("link", i)}
        for i in range(total_linhas)
        if _valor("titulo", i) and not _valor("status", i)
    ]
    if not pendentes:
        return
    print(f"{len(pendentes)} linhas pendentes de reclassificação.")

    def _reclassificar(p):
        trecho, url_real = _ler_pagina(p["link"])
        dados = dict(classificar_com_gemini(p["titulo"], trecho),
                     texto_completo=trecho, link_real=url_real, titulo=p["titulo"])
        # a chave de fato depende de campos que só existem depois de classificar
        dados["alerta_chave"] = chave_alerta(dados)
        p["classificacao"] = dados

    total = 0
    for inicio in range(0, len(pendentes), LOTE_RECLASSIFICACAO):
        lote = pendentes[inicio:inicio + LOTE_RECLASSIFICACAO]
        _em_paralelo(lote, _reclassificar, total_rotulo="reclassificadas",
                     passo=len(lote))
        updates = [{"range": f"{col_letra[c]}{p['linha']}",
                    "values": [[_safe(p["classificacao"].get(c))]]}
                   for p in lote if p.get("classificacao") for c in cols]
        if updates:
            aba.batch_update(updates, value_input_option="USER_ENTERED")
            total += sum(1 for p in lote if p.get("classificacao"))
            print(f"[{inicio + len(lote)}/{len(pendentes)}] ({total} salvas)")
    print(f"{total} linhas reclassificadas.")


def salvar_no_sheets(noticias):
    """Adiciona as notícias novas na aba do Google Sheets."""
    if not noticias:
        print("Nenhuma notícia nova para salvar.")
        return
    aba = _sheets_aba()
    headers = aba.row_values(1)
    if not headers:
        aba.append_row(COLUNAS_PLANILHA)
        headers = COLUNAS_PLANILHA
    # mais recentes primeiro, pra entrarem no topo
    def _chave(n):
        try:
            return datetime.strptime(n.get("data", ""), "%d/%m/%Y %H:%M")
        except Exception:
            return datetime.min
    noticias = sorted(noticias, key=_chave, reverse=True)

    linhas = [[_safe(n.get(col)) for col in headers] for n in noticias]
    # insere no topo (logo abaixo do cabeçalho); USER_ENTERED deixa a URL clicável
    aba.insert_rows(linhas, row=2, value_input_option="USER_ENTERED")
    print(f"{len(noticias)} notícias inseridas no topo do Google Sheets.")


# ─── Email do alerta ──────────────────────────────────────────────────────────
# Faixas de plantão, iguais às do agente do Slack. A rodada cai por volta de
# 06h15, 11h15 e 15h15, então na prática o email da manhã vai pra dupla da manhã,
# o do meio-dia pros quatro e o da tarde pra dupla da tarde.
PLANTAO_MANHA = ("Yas", "Paulo")
PLANTAO_TARDE = ("Emilly", "Maria Eduarda")


def plantao(agora=None):
    h = (agora or datetime.now(BRT)).hour
    if 11 <= h <= 13:            # sobreposição: cai pros quatro
        return PLANTAO_MANHA + PLANTAO_TARDE
    if 13 < h < 19:
        return PLANTAO_TARDE
    return PLANTAO_MANHA         # manhã e também fora do horário comercial


ROTULO_TEMA = {
    "pesquisa-executivo": "Pesquisa eleitoral (governador ou presidente)",
    "apoio": "Apoio e aliança ao executivo",
    "rompimento": "Rompimento de aliança",
    "educação": "Educação na campanha",
}

PAINEL_URL = os.getenv("PAINEL_NOTICIAS_URL",
                       "https://painel-eleitoral-interno.streamlit.app/Notícias")


def _esc(v):
    return (str(v or "").replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;"))


def _bloco_html(a):
    ident = " · ".join(filter(None, [
        _esc(a.get("candidato")),
        _esc(a.get("partido")),
        _esc(a.get("uf")),
        _esc(a.get("instituto")),
    ]))
    link = a.get("link_real") or a.get("link") or ""
    resumo = _esc(a.get("resumo")).replace("*", "").strip()
    # O resumo guardado é o alerta inteiro, sempre nesta ordem: cabeçalho, data,
    # título, corpo e link. No email entra só o corpo (linha 3 em diante), porque
    # título, fonte e link já estão na moldura do bloco. Cortar a partir da linha
    # 2 fazia o título aparecer duas vezes, colado no começo do texto.
    linhas = [l for l in resumo.split("\n") if l.strip()]
    corpo = " ".join(l for l in linhas[3:] if not l.startswith("Link:"))
    return f"""
      <div style="border-left:3px solid {EIXO_MARINHO};padding:8px 12px;margin:0 0 14px 0;background:#f6f7fa">
        <div style="font-weight:bold;color:{EIXO_MARINHO}">{_esc(a.get('titulo'))}</div>
        <div style="color:#6b7280;font-size:12px;margin:2px 0 6px 0">
          {_esc(a.get('fonte'))} · {_esc(a.get('data'))}{(' · ' + ident) if ident else ''}
        </div>
        {f'<div style="font-size:13px;margin:0 0 6px 0">{corpo}</div>' if corpo else ''}
        {f'<a href="{_esc(link)}" style="color:{EIXO_MARINHO};font-size:12px">abrir a notícia</a>' if link.startswith("http") else ''}
      </div>"""


def _html_alertas(alertas, agora):
    por_tema = {}
    for a in alertas:
        por_tema.setdefault(a.get("alerta_tema", ""), []).append(a)
    secoes = "".join(
        f"<h3 style='margin:20px 0 8px 0;color:{EIXO_MARINHO}'>"
        f"{ROTULO_TEMA.get(tema, tema)} ({len(por_tema[tema])})</h3>"
        + "".join(_bloco_html(a) for a in por_tema[tema])
        for tema in ROTULO_TEMA if por_tema.get(tema)
    )
    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#111">
      <h2 style="margin:0 0 6px 0">Alertas de notícias</h2>
      <div style="color:#374151;margin:0 0 14px 0">
        {agora.strftime('%d/%m/%Y %H:%M')} · {len(alertas)} alerta(s) novo(s)
      </div>
      <div style="background:#eef0f6;border-left:3px solid {EIXO_MARINHO};padding:10px 12px;margin:0 0 16px 0;font-size:13px">
        <strong style="color:{EIXO_MARINHO}">Plantão agora:</strong> {', '.join(plantao(agora))}.
        O texto pronto pra WhatsApp de cada alerta está na coluna <b>resumo</b> da aba
        <b>noticias</b>, e no botão de envelope do
        <a href="{PAINEL_URL}" style="color:{EIXO_MARINHO}">painel interno</a>.
        Confira a fonte antes de disparar.
      </div>
      {secoes}
    </body></html>
    """


def enviar_alertas_pendentes(aba):
    """Manda por email os alertas ainda não enviados e carimba a hora do envio.

    O carimbo (coluna alerta_enviado_em) só é gravado depois que o envio dá
    certo, então email que falhou volta na rodada seguinte em vez de sumir. É
    esse carimbo, e não a rodada em que a notícia entrou, que define o que já foi
    avisado.

    Filtra pela data de publicação além do carimbo: sem isso, uma linha antiga
    reclassificada à mão viraria email de notícia de semanas atrás.
    """
    headers = aba.row_values(1)
    if "alerta" not in headers or "alerta_enviado_em" not in headers:
        print("Aba sem as colunas de alerta; pulando envio.")
        return
    campos = ("alerta", "alerta_enviado_em", "alerta_tema", "titulo", "fonte", "data",
              "link", "link_real", "candidato", "partido", "uf", "instituto", "resumo")
    corte = datetime.now(BRT) - timedelta(days=JANELA_DIAS + 1)
    pendentes = []
    for r in _ler_colunas(aba, headers, campos):
        if r.get("alerta") != "sim" or r.get("alerta_enviado_em"):
            continue
        dt = _data_planilha(r.get("data", ""))
        if dt and dt < corte:
            continue
        pendentes.append(r)
    if not pendentes:
        print("Nenhum alerta novo para enviar.")
        return

    agora = datetime.now(BRT)
    dests = destinatarios("DESTINATARIOS_NOTICIAS")
    assunto = f"Alertas eleições 2026 · {agora.strftime('%d/%m %H:%M')} · {len(pendentes)} novo(s)"
    print(f"{len(pendentes)} alerta(s) pendente(s); enviando para {len(dests)} destinatário(s)...")
    if not enviar_email(assunto, _html_alertas(pendentes, agora), dests):
        print("Email não saiu; os alertas ficam pendentes para a próxima rodada.")
        return

    carimbo = agora.strftime("%d/%m/%Y %H:%M")
    letra = _col_letra(headers, "alerta_enviado_em")
    aba.batch_update([{"range": f"{letra}{r['_linha']}", "values": [[carimbo]]}
                      for r in pendentes], value_input_option="USER_ENTERED")
    print(f"{len(pendentes)} alerta(s) marcado(s) como enviado(s).")


if __name__ == '__main__':
    print("Carregando títulos já salvos na planilha...")
    titulos_existentes = carregar_titulos_existentes()
    print(f"{len(titulos_existentes)} títulos já na planilha.")

    noticias = coletar()   # presidente + governador + senador (palavra-chave)

    sites = carregar_sites_regionais()
    if sites:
        print(f"{len(sites)} sites regionais; buscando via site:...")
        noticias += coletar_regionais(sites)

    # dedup por título entre as duas fontes (nacional + regional)
    vistos, unicas = set(), []
    for n in noticias:
        chave = n["titulo"].strip().lower()
        if chave and chave not in vistos:
            vistos.add(chave)
            unicas.append(n)
    noticias = unicas

    novas = [n for n in noticias if n["titulo"].strip().lower() not in titulos_existentes]
    print(f"{len(novas)} notícias novas (de {len(noticias)} coletadas)")

    if novas:
        novas = classificar_noticias(novas)
        marcar_repetidos(novas, carregar_chaves_recentes(_sheets_aba()))
        escrever_alertas(novas)
        salvar_no_sheets(novas)

    # reclassifica linhas cuja classificação foi apagada à mão (status vazio)
    reclassificar_pendentes(_sheets_aba())

    # depois de gravar, nunca antes: alerta que não chegou na planilha não pode
    # sair por email, senão ninguém acha a linha pra trabalhar em cima dela
    try:
        enviar_alertas_pendentes(_sheets_aba())
    except Exception as e:
        # a raspagem é o produto principal; email é o aviso. Falha aqui não
        # derruba a rodada, e o que não saiu volta na próxima (sem carimbo).
        print(f"Falha ao enviar os alertas por email: {e}")

    _resumo_uso_tokens("notícias", USO_TOKENS)
