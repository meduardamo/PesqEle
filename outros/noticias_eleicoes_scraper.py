"""
Alertas de notícias sobre candidaturas.

Usado na fase das convenções (julho), quando o TSE ainda não tem dado. A gente
monitora notícia por palavra-chave e usa pra montar a matriz de quem foi anunciado.
Fonte: Google Notícias (RSS). A classificação fica por conta do Gemini (ver TODO).
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
from compartilhado.relatorios_sheets_utils import (
    _col_count_atual, autorizar_com_retry as _autorizar)

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


def _extrair_texto_pagina(url: str, limite: int = 6000) -> str:
    """Devolve o texto completo do artigo por trás do link do Google Notícias.

    O <link> do RSS (news.google.com/rss/articles/...) não é o artigo: é uma
    página de redirecionamento via JavaScript. Um navegador executa o JS e
    cai no site real; um requests.get() direto só pega a casca do Google,
    sem o texto da notícia. Por isso decodifica pra URL real (gnewsdecoder,
    mesmo truque usado pra esse tipo de link) antes de extrair — mesmo padrão
    do repo googlenews, usando newspaper3k pra extração (mais robusto que
    tirar tag na unha, lida melhor com a variedade de sites de notícia).

    Falha em qualquer etapa (decodificação, paywall, bloqueio, timeout) só
    volta string vazia; quem chama cai de volta pra classificar só pela
    manchete."""
    if not url or not url.startswith("http"):
        return ""
    try:
        decoded = gnewsdecoder(url, interval=1)
        url_real = decoded.get("decoded_url") if decoded.get("status") else url
    except Exception:
        url_real = url
    try:
        art = Article(url_real, language="pt")
        art.download()
        art.parse()
        return (art.text or "").strip()[:limite]
    except Exception:
        return ""


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
TIPOS = ("agenda", "pesquisa", "debate", "aliança", "apoio", "crítica-educação", "outro")
_TIPO_ALIAS = {
    "alianca": "aliança", "aliancas": "aliança", "alianças": "aliança",
    "coligacao": "aliança", "coligação": "aliança", "federacao": "aliança",
    "federação": "aliança",
    "apoios": "apoio", "declaracao de apoio": "apoio", "declaração de apoio": "apoio",
    "critica-educacao": "crítica-educação", "critica educacao": "crítica-educação",
    "crítica educação": "crítica-educação", "critica-educação": "crítica-educação",
    "crítica-educacao": "crítica-educação", "educacao": "crítica-educação",
    "educação": "crítica-educação",
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
        '  "tipo": "agenda | pesquisa | debate | aliança | apoio | crítica-educação | outro",\n'
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
        "  - tipo='outro': não se encaixa em nenhum dos anteriores\n"
        "- confianca='alto': candidato, cargo e UF estão todos explícitos no texto\n"
        "- confianca='médio': algum campo foi inferido com boa certeza pelo contexto\n"
        "- confianca='baixo': muita ambiguidade ou faltam dois ou mais campos principais\n"
        "- Use null (não a string 'null') para campos ausentes\n"
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
    return dados


def _classificar_uma(n):
    """Baixa o artigo e classifica uma notícia, no lugar (dict mutado)."""
    trecho = _extrair_texto_pagina(n.get("link", ""))
    n["texto_completo"] = trecho
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


COLUNAS_PLANILHA = [
    "candidato", "cargo", "uf", "partido", "status", "convencao", "tipo", "resumo",
    "confianca", "titulo", "fonte", "data", "link", "busca", "texto_completo",
]


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
    from gspread.utils import rowcol_to_a1
    headers = aba.row_values(1)
    if "titulo" not in headers or "status" not in headers:
        return

    def _letra(coluna):
        return rowcol_to_a1(1, headers.index(coluna) + 1).rstrip("1")

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
    cols = [c for c in ("candidato", "cargo", "uf", "partido", "status", "convencao",
                        "tipo", "confianca", "texto_completo")
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
        trecho = _extrair_texto_pagina(p["link"])
        p["classificacao"] = dict(classificar_com_gemini(p["titulo"], trecho),
                                  texto_completo=trecho)

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
        salvar_no_sheets(novas)

    # reclassifica linhas cuja classificação foi apagada à mão (status vazio)
    reclassificar_pendentes(_sheets_aba())

    _resumo_uso_tokens("notícias", USO_TOKENS)
