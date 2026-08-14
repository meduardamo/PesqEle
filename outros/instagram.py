"""
Etapa 5 - Automatizar a coleta a partir de uma planilha de perfis.

Fluxo:
1. Baixa post(s) do Instagram a partir de um link (de post ou de perfil), usando o Apify.
   - Link de post: baixa só aquele post.
   - Link de perfil: baixa os posts mais recentes do perfil, filtrando por data
     com onlyPostsNewerThan (evita reprocessar/pagar por posts antigos).
2. Envia cada mídia (e a legenda) para o Gemini, que gera transcrição/resumo/temas.
3. Salva cada resultado como uma linha na planilha do Google Sheets.
4. Modo --perfis: lê os perfis (nome + link) da coluna B da aba "Instagram" de
   uma planilha de acompanhamento (SPREADSHEET_ID_PERFIS), roda o fluxo para
   cada um filtrando por data, pula posts já processados (pelo ID/shortCode)
   e grava os resultados na aba "Resultados" da planilha de mídia/resultados
   (SPREADSHEET_ID) — não na planilha de perfis.

A rodada diária é dividida em duas fases, desde 05/08/2026:

    --perfis   coleta na Apify, grava as linhas e manda o clipping por e-mail.
               Não chama o Gemini, então termina em cerca de 12 min.
    --analise  preenche as colunas do Gemini nas linhas que estão em branco,
               em paralelo. O que não terminar continua pendente para a rodada
               seguinte, porque o estado mora na própria planilha.

Elas rodam em workflows separados (15 - Instagram Coleta e 16 - Instagram
Análise). Juntas numa execução só, a análise post a post estourava o timeout e
levava o e-mail junto.

Antes de rodar:
    pip install apify-client google-genai gspread google-auth google-api-python-client requests

Uso:
    python instagram.py
    (vai pedir o link do post ou do perfil do Instagram)

    python instagram.py <link> [data_minima]
    Ex.: python instagram.py https://www.instagram.com/candidato/ 2026-07-13

    python instagram.py --perfis [data_minima] [limite_de_perfis]
    Ex.: python instagram.py --perfis 2026-07-13

    python instagram.py --analise [limite_de_posts]
    python instagram.py --relatorio [YYYY-MM-DD]
"""

import json
import os
import random
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime, timedelta, timezone
from urllib.parse import urlsplit, urlunsplit

import gspread
import requests
from apify_client import ApifyClient
from google import genai
from google.genai import types
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as build_google_service

# O workflow chama `python outros/instagram.py`, que põe outros/ na frente do
# sys.path, e o uso local é `python -m outros.instagram`. Os dois caminhos de
# import precisam funcionar.
try:
    from outros.instagram_relatorio import enviar_relatorio, posts_gravados_no_dia
except ImportError:
    from instagram_relatorio import enviar_relatorio, posts_gravados_no_dia


SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1piO-m19orW1i-Z-6rNeWdXnEAWqw5wneiDpdHZqOa6Y")
NOME_ABA_SHEETS = os.getenv("NOME_ABA_SHEETS", "Instagram")
RESULTS_LIMIT_PERFIL = int(os.getenv("RESULTS_LIMIT_PERFIL", "20"))
CABECALHO_SHEETS = [
    "Data/Hora",
    "Link",
    "Usuário",
    "Tipo",
    "Data de publicação",
    "Curtidas",
    "Comentários",
    "Legenda",
    "Transcrição",
    "Resumo do conteúdo",
    "Resumo da legenda",
    "Temas",
]

ATOR_INSTAGRAM_PERFIS_LOWCOST = os.getenv("ATOR_INSTAGRAM_PERFIS_LOWCOST", "sones/instagram-posts-scraper-lowcost")
POSTS_POR_PERFIL_LOWCOST = int(os.getenv("POSTS_POR_PERFIL_LOWCOST", "12"))
# Grupo de proxy do Apify passado explicitamente ao ator. Sem isso o ator escolhe
# sozinho: o build de 02/08/2026 pediu o grupo residencial, avisou que caiu para
# datacenter e mesmo assim estourou no _checkAccess nos 79 perfis. O grupo datacenter
# desta conta chama BUYPROXIES94952 (5 IPs, EUA); SHADER, o nome usado como padrão em
# exemplo do Apify, não existe aqui. Vazio devolve a escolha para o ator.
GRUPO_PROXY_APIFY = os.getenv("GRUPO_PROXY_APIFY", "BUYPROXIES94952")
# Versao fixa do ator. O autor publicou a 1.5.5 em 02/08/2026 as 17:25 UTC, 35 min
# antes da rodada daquele dia, e ela estoura no _checkAccess do proxy antes de
# buscar qualquer post, mesmo com o grupo passado explicitamente. Fixamos entao a
# 1.5.3, a ultima que coletou (01/08).
#
# Em 06/08/2026 as 12:30 UTC o autor mexeu no ator de novo: apagou a 1.5.3 e a
# 1.5.5 e apontou a tag latest de volta para a 1.5.2, de 21/07. So restaram a
# 1.5.1 e a 1.5.2, entao o pin passou a nao existir e os 79 perfis falharam com
# "Build with number 1.5.3 was not found". A 1.5.2 e a mais nova que existe, e e
# anterior a que quebrou. Vazio volta a seguir a tag "latest", hoje a propria
# 1.5.2, mas seguir a tag foi o que trouxe a 1.5.5 quebrada sem aviso.
BUILD_ATOR_LOWCOST = os.getenv("BUILD_ATOR_LOWCOST", "1.5.2")
# Fração de perfis que pode falhar na coleta antes da rodada inteira ser considerada
# quebrada (e sair com código != 0, para o Actions marcar vermelho).
LIMIAR_FALHA_PERFIS = float(os.getenv("LIMIAR_FALHA_PERFIS", "0.2"))
# A partir de quantos perfis "nenhum deles devolveu post" deixa de ser um dia parado
# e passa a ser bloqueio do Instagram. Em 31/07/2026 os 79 runs terminaram SUCCEEDED
# com "access denied" em 76 perfis, então o status do run sozinho não pega esse caso.
MINIMO_PERFIS_PARA_EXIGIR_POSTS = int(os.getenv("MINIMO_PERFIS_PARA_EXIGIR_POSTS", "10"))
# Estas duas nasceram abaixo, depois da constante que as chama, e o módulo
# quebrava no import com NameError: no nível do módulo o nome precisa existir
# antes do uso. Foi o que derrubou os workflows 15 e 16 em 07/08/2026.
def obter_nome_mes_portugues(data: datetime | None = None) -> str:
    """Retorna o nome do mês atual em português, em minúsculas."""
    referencia = data or datetime.now()
    nomes = {
        "January": "janeiro",
        "February": "fevereiro",
        "March": "março",
        "April": "abril",
        "May": "maio",
        "June": "junho",
        "July": "julho",
        "August": "agosto",
        "September": "setembro",
        "October": "outubro",
        "November": "novembro",
        "December": "dezembro",
    }
    return nomes.get(referencia.strftime("%B"), referencia.strftime("%B")).lower()


def obter_nome_aba_mensal() -> str:
    """Retorna o nome da aba para o mês atual, como 'agosto' ou 'setembro'."""
    return obter_nome_mes_portugues()


SPREADSHEET_ID_PERFIS = os.getenv("SPREADSHEET_ID_PERFIS", "1piO-m19orW1i-Z-6rNeWdXnEAWqw5wneiDpdHZqOa6Y")
ABA_PERFIS = os.getenv("ABA_PERFIS", "Instagram")
COLUNA_PERFIS = os.getenv("COLUNA_PERFIS", "B")
ABA_RESULTADOS_PERFIS = os.getenv("ABA_RESULTADOS_PERFIS") or obter_nome_aba_mensal()
# "Candidato" desde 05/08/2026: com o registro no TSE feito, pré-candidato
# deixou de ser o termo certo. A leitura aceita os dois nomes (COLUNA_CANDIDATO)
# para não depender de a planilha estar renomeada.
CABECALHO_RESULTADOS_PERFIS = [
    "Data/Hora",
    "Candidato",
    "ID do post",
    "Link",
    "Usuário",
    "Tipo",
    "Data de publicação",
    "Curtidas",
    "Comentários",
    "Legenda",
    "Transcrição",
    "Resumo do conteúdo",
    "Resumo da legenda",
    "Temas",
    # A fase 2 baixa a mídia depois, em outra execução, e o link do post não
    # serve para isso: o mp4/jpg está numa URL assinada do CDN do Instagram que
    # só vem no retorno da Apify. Por isso ela é gravada aqui na coleta.
    "URL da mídia",
]

# Colunas que a fase 1 deixa em branco e a fase 2 preenche. Contíguas de
# propósito: cada linha vira um único range no batch_update.
COLUNAS_GEMINI = ("Transcrição", "Resumo do conteúdo", "Resumo da legenda", "Temas")
# Coluna que decide se a linha está pendente de análise.
COLUNA_PENDENTE = "Resumo do conteúdo"

# A fase 2 é espera de rede quase pura (download da mídia, upload para o Gemini,
# polling do PROCESSING, geração). Em série deu 21s por post e 190 min na rodada
# de 05/08/2026, que estourou o timeout com 74 dos 79 perfis feitos.
THREADS_ANALISE = int(os.getenv("THREADS_ANALISE", "8"))
# Quantos posts analisados antes de gravar na planilha. O gspread não é
# thread-safe, então quem escreve é sempre a thread principal, em lote.
LOTE_ESCRITA_ANALISE = int(os.getenv("LOTE_ESCRITA_ANALISE", "20"))
# 503 UNAVAILABLE ("modelo sobrecarregado") derrubou 6 posts na rodada de
# 05/08/2026, sem nenhuma nova tentativa. São transitórios.
TENTATIVAS_GEMINI = int(os.getenv("TENTATIVAS_GEMINI", "4"))
# A URL do CDN do Instagram expira. Passado esse prazo a linha para de ser
# tentada de novo a cada rodada e fica marcada como não analisada.
DIAS_PARA_DESISTIR_DA_MIDIA = int(os.getenv("DIAS_PARA_DESISTIR_DA_MIDIA", "3"))
MARCA_MIDIA_EXPIRADA = "(mídia expirada, não analisado)"
# Vídeo na resolução padrão custa 258 tokens por frame a 1 FPS; na baixa, 66.
# Como vídeo é quase toda a conta de entrada, ligar isso corta o custo da rodada
# para cerca de um terço. O áudio (32 tokens/s) não muda, então a transcrição
# continua igual; o que piora é o detalhe visual do frame.
RESOLUCAO_MIDIA_BAIXA = os.getenv("RESOLUCAO_MIDIA_BAIXA", "").strip().lower() in ("1", "true", "sim")
# Tarifas do gemini-3.6-flash (ai.google.dev/gemini-api/docs/pricing, 14/08/2026),
# por 1M de tokens. Servem só para a estimativa impressa no fim da rodada. Subiram
# junto com a troca de modelo: no 2.5-flash eram 0.30 e 2.50, ou seja, a entrada
# ficou 2,5x mais cara, e entrada é quase toda a conta quando tem vídeo.
PRECO_ENTRADA_POR_MILHAO = float(os.getenv("PRECO_ENTRADA_POR_MILHAO", "0.75"))
PRECO_SAIDA_POR_MILHAO = float(os.getenv("PRECO_SAIDA_POR_MILHAO", "3.75"))

_PADRAO_SECOES = re.compile(
    r"[#\s*]*\d+\.[ \t*]*(Transcri[cç][aã]o|Resumo\s+do\s+conte[uú]do|Resumo\s+da\s+legenda|Temas?)[ \t*:]*[^\n]*\n",
    re.IGNORECASE,
)
_CHAVE_POR_PREFIXO = (
    ("transcri", "transcricao"),
    ("resumo do conte", "resumo_conteudo"),
    ("resumo da legenda", "resumo_legenda"),
    ("tema", "temas"),
)

def dividir_resultado(resultado: str) -> dict:
    """Separa o texto gerado pelo Gemini nas seções transcrição/resumos/temas."""
    secoes = {"transcricao": "", "resumo_conteudo": "", "resumo_legenda": "", "temas": ""}
    matches = list(_PADRAO_SECOES.finditer(resultado))

    for i, m in enumerate(matches):
        titulo = m.group(1).lower()
        inicio = m.end()
        fim = matches[i + 1].start() if i + 1 < len(matches) else len(resultado)
        texto = resultado[inicio:fim].strip()
        for prefixo, chave in _CHAVE_POR_PREFIXO:
            if titulo.startswith(prefixo):
                secoes[chave] = texto
                break

    if not matches:
        secoes["resumo_conteudo"] = resultado.strip()

    return secoes


def localizar_credentials(caminho: str = "credentials.json", required_keys: tuple[str, ...] = ()) -> str:
    """Procura credentials.json em alguns locais comuns do projeto."""
    base_dir = os.path.dirname(__file__)
    candidatos = []
    if os.path.isabs(caminho):
        candidatos.append(caminho)
    else:
        candidatos.extend([
            caminho,
            os.path.join(base_dir, caminho),
            os.path.join(os.getcwd(), caminho),
            os.path.join(base_dir, "..", caminho),
            os.path.join(base_dir, "..", "PNE", caminho),
            os.path.join(base_dir, "..", "..", "PNE", caminho),
            os.path.join(base_dir, "..", "..", "eixo", caminho),
        ])

    # Se houver chaves requeridas, tenta achar o arquivo que as contém.
    if required_keys:
        for item in candidatos:
            if not os.path.isfile(item):
                continue
            try:
                with open(item, encoding="utf-8") as f:
                    dados = json.load(f)
                if isinstance(dados, dict) and any(
                    isinstance(dados.get(chave), str) and dados.get(chave).strip() for chave in required_keys
                ):
                    return item
            except Exception:
                continue

    for item in candidatos:
        if os.path.isfile(item):
            return item

    return os.path.join(base_dir, caminho)


def carregar_json(caminho: str = "credentials.json", required_keys: tuple[str, ...] = ()) -> dict:
    """Carrega um arquivo JSON de credenciais."""
    caminho_arquivo = localizar_credentials(caminho, required_keys)
    with open(caminho_arquivo, encoding="utf-8") as f:
        dados = json.load(f)

    if not isinstance(dados, dict):
        raise RuntimeError("Formato inválido em credentials.json.")
    return dados


def carregar_gemini_api_key(caminho: str = "credentials.json") -> str:
    """Lê a chave do Gemini de credentials.json, preferindo o campo GEMINI_API_KEY."""
    dados = carregar_json(
        caminho,
        required_keys=("GEMINI_API_KEY", "gemini_api_key", "GOOGLE_API_KEY", "google_api_key"),
    )
    for nome in ("GEMINI_API_KEY", "gemini_api_key", "GOOGLE_API_KEY", "google_api_key"):
        valor = dados.get(nome)
        if isinstance(valor, str) and valor.strip():
            return valor.strip()

    raise RuntimeError("Não foi possível encontrar GEMINI_API_KEY em credentials.json.")


def carregar_apify_token(caminho: str = "credentials.json") -> str:
    """Lê o token do Apify de variáveis de ambiente, de apify.json ou de credentials.json."""
    for nome in ("APIFY_TOKEN", "apify_token", "APIFY_API_TOKEN", "apify_api_token"):
        valor = os.getenv(nome, "").strip()
        if valor:
            return valor

    caminho_apify_json = os.path.join(os.path.dirname(__file__), "apify.json")
    if os.path.isfile(caminho_apify_json):
        with open(caminho_apify_json, encoding="utf-8") as f:
            dados_apify = json.load(f)
        valor = dados_apify.get("APIFY_API_TOKEN", "")
        if isinstance(valor, str) and valor.strip():
            return valor.strip()

    dados = carregar_json(
        caminho,
        required_keys=("APIFY_TOKEN", "apify_token", "APIFY_API_TOKEN", "apify_api_token", "APIFY_API_KEY", "apify_api_key"),
    )
    for nome in ("APIFY_TOKEN", "apify_token", "APIFY_API_TOKEN", "apify_api_token", "APIFY_API_KEY", "apify_api_key"):
        valor = dados.get(nome)
        if isinstance(valor, str) and valor.strip():
            return valor.strip()

    return ""


def carregar_credenciais_google(caminho: str = "credentials.json") -> Credentials:
    """Carrega as credenciais da conta de serviço para as APIs do Google."""
    caminho_arquivo = localizar_credentials(caminho, required_keys=("client_email",))
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    return Credentials.from_service_account_file(caminho_arquivo, scopes=scopes)


def gs_client_from_file(caminho: str = "credentials.json") -> gspread.Client:
    """Autentica no Google Sheets usando a conta de serviço em credentials.json."""
    return gspread.authorize(carregar_credenciais_google(caminho))


def obter_aba(sh: gspread.Spreadsheet, nome_aba: str, cabecalho: list[str]) -> gspread.Worksheet:
    try:
        aba = sh.worksheet(nome_aba)
    except gspread.exceptions.WorksheetNotFound:
        aba = sh.add_worksheet(title=nome_aba, rows=100, cols=len(cabecalho))
        aba.append_row(cabecalho, value_input_option="RAW")
        return aba

    if not aba.row_values(1):
        aba.append_row(cabecalho, value_input_option="RAW")
    return aba


def garantir_colunas(aba: gspread.Worksheet, cabecalho: list[str]) -> None:
    """Acrescenta ao fim do cabeçalho as colunas novas que a aba ainda não tem.

    A aba de resultados já existe com o cabeçalho antigo, e "URL da mídia"
    entrou depois. Só acrescenta no fim: toda leitura por índice continua
    valendo, e nada é reordenado nem apagado.

    Chamada só na aba de resultados, nunca dentro de `obter_aba`: a aba
    "Instagram" da mesma planilha é a lista de perfis, e completar cabeçalho
    nela escreveria colunas em cima da planilha de acompanhamento.
    """
    atual = aba.row_values(1)
    faltando = [c for c in cabecalho if c not in atual]
    if not faltando:
        return

    if aba.col_count < len(atual) + len(faltando):
        aba.add_cols(len(atual) + len(faltando) - aba.col_count)
    inicio = _letra_coluna(len(atual))
    fim = _letra_coluna(len(atual) + len(faltando) - 1)
    aba.update(range_name=f"{inicio}1:{fim}1", values=[faltando], value_input_option="RAW")
    print(f"Coluna(s) acrescentada(s) na aba '{aba.title}': {', '.join(faltando)}.")


def _letra_coluna(indice: int) -> str:
    """Índice de coluna base 0 para letra do A1 ('O' para 14, 'AA' para 26)."""
    letra = ""
    indice += 1
    while indice:
        indice, resto = divmod(indice - 1, 26)
        letra = chr(65 + resto) + letra
    return letra


def ordenar_por_data(aba: gspread.Worksheet) -> None:
    """Reordena as linhas de dados pela 'Data de publicação', mais novo primeiro."""
    valores = aba.get_all_values()
    if len(valores) <= 2:
        return

    cabecalho, linhas = valores[0], valores[1:]
    idx_data = cabecalho.index("Data de publicação")

    def chave(linha: list[str]) -> str:
        return linha[idx_data] if idx_data < len(linha) else ""

    linhas_ordenadas = sorted(linhas, key=chave, reverse=True)
    if linhas_ordenadas != linhas:
        aba.update(range_name="A2", values=linhas_ordenadas, value_input_option="RAW")


def salvar_no_sheets(url: str, item: dict, eh_video: bool, resultado: str) -> None:
    """Adiciona uma linha com o resultado da análise na planilha do Google Sheets."""
    gc = gs_client_from_file()
    sh = gc.open_by_key(SPREADSHEET_ID)
    aba = obter_aba(sh, NOME_ABA_SHEETS, CABECALHO_SHEETS)
    secoes = dividir_resultado(resultado)

    linha = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        url,
        item.get("ownerUsername", "") or "",
        "Vídeo" if eh_video else "Foto",
        item.get("timestamp", "") or "",
        item.get("likesCount", "") if item.get("likesCount") is not None else "",
        item.get("commentsCount", "") if item.get("commentsCount") is not None else "",
        item.get("caption", "") or "",
        secoes["transcricao"],
        secoes["resumo_conteudo"],
        secoes["resumo_legenda"],
        secoes["temas"],
    ]
    aba.append_row(linha, value_input_option="RAW")
    ordenar_por_data(aba)
    print(f"Linha adicionada na planilha (aba '{NOME_ABA_SHEETS}').")


def limpar_url_instagram(url: str) -> str:
    """Remove query string e fragmento (ex.: ?utm_source=..., #...) do link.

    O ator do Apify valida a URL com uma regex que exige uma "/" logo após o
    usuário/post, então parâmetros de rastreamento colados sem "/" antes
    (comuns em links compartilhados pelo app) fazem a chamada falhar.
    """
    partes = urlsplit(url)
    if not partes.netloc:
        return url
    return urlunsplit((partes.scheme or "https", partes.netloc, partes.path, "", ""))


def eh_link_de_perfil(url: str) -> bool:
    """Distingue um link de perfil (ex.: instagram.com/candidato/) de um link de post/reel."""
    return not any(segmento in url for segmento in ("/p/", "/reel/", "/tv/"))


def dataset_id_do_run(run, contexto: str) -> str:
    """Devolve o dataset do run do Apify, levantando erro se ele não terminou em SUCCEEDED.

    O ApifyClient não levanta exceção quando o ator termina em FAILED ou ABORTED:
    devolve o run normalmente e o dataset vem vazio, o que o resto do fluxo lê como
    "nenhum post no período". Foi assim que a rodada de 02/08/2026 falhou nos 79
    perfis (proxy do Apify indisponível), gravou zero post e mesmo assim terminou
    verde no Actions.
    """
    status = (getattr(run, "status", None) or (run.get("status") if isinstance(run, dict) else None) or "").upper()
    run_id = getattr(run, "id", None) or (run.get("id") if isinstance(run, dict) else None) or "?"
    if status != "SUCCEEDED":
        raise RuntimeError(f"Run do Apify para {contexto} terminou como {status or 'STATUS DESCONHECIDO'} (runId {run_id}).")

    return getattr(run, "default_dataset_id", None) or run["defaultDatasetId"]


def coletar_itens(client: ApifyClient, urls: list[str], resultados_limit: int = 1, apenas_apos: str | None = None) -> list[dict]:
    """Roda o ator do Apify para um ou mais links (post ou perfil) e retorna os itens do dataset."""
    run_input = {
        "directUrls": urls,
        "resultsType": "posts",
        "resultsLimit": resultados_limit,
        "addParentData": False,
    }
    if apenas_apos:
        run_input["onlyPostsNewerThan"] = apenas_apos

    run = client.actor("apify/instagram-scraper").call(run_input=run_input)

    dataset_id = dataset_id_do_run(run, ", ".join(urls))
    itens = client.dataset(dataset_id).list_items().items
    if not itens:
        raise RuntimeError("Nenhum item retornado pelo Apify. Confira se o link é de um perfil/post público.")

    return itens


def filtrar_por_data(itens: list[dict], apenas_apos: str) -> list[dict]:
    """Descarta itens publicados antes de `apenas_apos` (YYYY-MM-DD).

    Necessário porque o onlyPostsNewerThan do Apify não exclui posts fixados
    (pinned) no topo do perfil, que podem ser bem mais antigos que o corte.
    """
    try:
        corte = datetime.strptime(apenas_apos, "%Y-%m-%d")
    except ValueError:
        return itens

    filtrados = []
    for item in itens:
        try:
            publicado_em = datetime.strptime((item.get("timestamp") or "")[:10], "%Y-%m-%d")
        except ValueError:
            continue
        if publicado_em >= corte:
            filtrados.append(item)
    return filtrados


def extrair_username_instagram(url: str) -> str:
    """Extrai o @username de um link de perfil (ex.: .../fulano/reels/ -> "fulano")."""
    caminho = urlsplit(url).path.strip("/")
    return caminho.split("/")[0] if caminho else ""


def normalizar_item_lowcost(item_bruto: dict) -> dict:
    """Converte um item do ator sones/instagram-posts-scraper-lowcost para o
    mesmo formato usado pelo restante do código (o formato do apify/instagram-scraper),
    para que baixar_midia, filtrar_por_data, salvar_resultado_perfil etc. funcionem sem alteração.
    """
    caption = item_bruto.get("caption") or {}
    legenda = caption.get("text", "") if isinstance(caption, dict) else str(caption or "")

    usuario = item_bruto.get("user") or {}
    taken_at = item_bruto.get("taken_at")
    timestamp = ""
    if taken_at:
        timestamp = datetime.fromtimestamp(taken_at, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.000Z")

    video_url = item_bruto.get("video_url")
    if item_bruto.get("product_type") == "carousel_container":
        tipo = "Sidecar"
    elif video_url:
        tipo = "Video"
    else:
        tipo = "Image"

    code = item_bruto.get("code", "")
    return {
        "shortCode": code,
        "id": item_bruto.get("pk") or item_bruto.get("id", ""),
        "url": item_bruto.get("post_url") or (f"https://www.instagram.com/p/{code}/" if code else ""),
        "caption": legenda,
        "ownerUsername": usuario.get("username", ""),
        "timestamp": timestamp,
        "likesCount": item_bruto.get("like_count"),
        "commentsCount": item_bruto.get("comment_count"),
        "type": tipo,
        "videoUrl": video_url,
        "displayUrl": item_bruto.get("image_url"),
    }


def coletar_itens_perfil_lowcost(
    client: ApifyClient,
    url_perfil: str,
    posts_por_perfil: int = 1,
    apenas_apos: str | None = None,
) -> list[dict]:
    """Coleta os posts recentes de UM perfil usando o ator sones/instagram-posts-scraper-lowcost.

    Bem mais barato que o apify/instagram-scraper (~US$0,30 vs ~US$2,70 por mil
    resultados no plano Free), mas só aceita @username — não faz scraping de
    um link de post único, por isso é usado apenas no modo --perfis.
    """
    username = extrair_username_instagram(url_perfil)
    if not username:
        raise RuntimeError(f"Não consegui extrair o username de {url_perfil}.")

    run_input = {
        "usernames": [username],
        "postsPerProfile": posts_por_perfil,
    }
    if apenas_apos:
        run_input["newerThan"] = apenas_apos
    if GRUPO_PROXY_APIFY:
        run_input["proxy"] = {"useApifyProxy": True, "apifyProxyGroups": [GRUPO_PROXY_APIFY]}

    ator = client.actor(ATOR_INSTAGRAM_PERFIS_LOWCOST)
    if BUILD_ATOR_LOWCOST:
        try:
            run = ator.call(run_input=run_input, build=BUILD_ATOR_LOWCOST)
        except Exception as erro:
            # Build fixada some quando o autor republica: em 06/08/2026 ele
            # apagou a 1.5.3 (e a 1.5.5, quebrada) e voltou a tag latest para a
            # 1.5.2, de 21/07. Os 79 perfis falharam com "Build with number
            # 1.5.3 was not found", um por um, e a rodada inteira se perdeu.
            # Cair para a latest coleta menos bem do que a versão escolhida,
            # mas coleta; e a build que não existe falha rápido, sem custo.
            if "was not found" not in str(erro):
                raise
            print(f"  build {BUILD_ATOR_LOWCOST} não existe mais no ator; "
                  f"rodando na latest", flush=True)
            run = ator.call(run_input=run_input)
    else:
        run = ator.call(run_input=run_input)
    dataset_id = dataset_id_do_run(run, f"@{username}")
    itens_brutos = client.dataset(dataset_id).list_items().items

    return [normalizar_item_lowcost(item) for item in itens_brutos]


def obter_perfis_instagram(
    spreadsheet_id: str = SPREADSHEET_ID_PERFIS,
    aba: str = ABA_PERFIS,
    coluna: str = COLUNA_PERFIS,
) -> list[dict]:
    """Lê nome + link de cada linha da coluna indicada (a partir da linha 2).

    Usa a API do Sheets diretamente (em vez do gspread) porque os links estão
    como hyperlink da célula (Inserir > Link), não como texto simples nem
    fórmula HYPERLINK() — o gspread só devolve o texto exibido.
    """
    service = build_google_service("sheets", "v4", credentials=carregar_credenciais_google())
    resp = service.spreadsheets().get(
        spreadsheetId=spreadsheet_id,
        ranges=[f"'{aba}'!{coluna}2:{coluna}"],
        fields="sheets.data.rowData.values(formattedValue,hyperlink)",
    ).execute()

    linhas = resp["sheets"][0]["data"][0].get("rowData", [])
    perfis = []
    for i, linha in enumerate(linhas, start=2):
        valores = linha.get("values", [])
        if not valores:
            continue
        celula = valores[0]
        nome = (celula.get("formattedValue") or "").strip()
        link = celula.get("hyperlink")
        if not nome or not link or "instagram.com" not in link:
            continue
        perfis.append({"linha": i, "nome": nome, "url": limpar_url_instagram(link)})

    return perfis


def obter_ids_processados(aba: gspread.Worksheet) -> set[str]:
    """Lê a coluna 'ID do post' já gravada na aba de resultados, para pular duplicados."""
    valores = aba.get_all_values()
    if len(valores) < 2 or "ID do post" not in valores[0]:
        return set()
    idx = valores[0].index("ID do post")
    return {linha[idx] for linha in valores[1:] if idx < len(linha) and linha[idx]}


COLUNA_CANDIDATO = ("Candidato", "Pré-candidato")


def _idx_candidato(cabecalho: list[str]) -> int:
    """Posição da coluna do candidato, com o nome antigo aceito.

    A aba foi renomeada de "Pré-candidato" para "Candidato"; abas de meses
    anteriores continuam com o nome antigo.
    """
    for nome in COLUNA_CANDIDATO:
        if nome in cabecalho:
            return cabecalho.index(nome)
    return -1


def obter_ultima_data_por_perfil(aba: gspread.Worksheet) -> dict[str, str]:
    """Retorna, por candidato, a data (YYYY-MM-DD) do post mais recente já salvo.

    Usado para avançar o onlyPostsNewerThan/newerThan por perfil a cada execução,
    para que o Apify nem chegue a buscar (e cobrar) de novo posts que já estão
    na planilha — só a data de corte por perfil, o "ID do post" continua sendo
    a rede de segurança contra duplicar no lado do Gemini/planilha.
    """
    valores = aba.get_all_values()
    if len(valores) < 2:
        return {}
    cabecalho = valores[0]
    idx_perfil = _idx_candidato(cabecalho)
    if idx_perfil < 0 or "Data de publicação" not in cabecalho:
        return {}
    idx_data = cabecalho.index("Data de publicação")

    ultima_data: dict[str, str] = {}
    for linha in valores[1:]:
        if idx_perfil >= len(linha) or idx_data >= len(linha):
            continue
        perfil = linha[idx_perfil]
        data = linha[idx_data][:10]
        if not perfil or not data:
            continue
        if perfil not in ultima_data or data > ultima_data[perfil]:
            ultima_data[perfil] = data
    return ultima_data


def montar_linha_perfil(perfil: str, item: dict) -> tuple[list, dict]:
    """Monta a linha da aba de resultados a partir do que a Apify devolveu.

    As quatro colunas do Gemini saem em branco: quem as preenche é a fase 2
    (`rodar_analise_pendentes`). A fase 1 grava e manda o clipping sem esperar
    pela análise, que é o que estourava o tempo da rodada.

    Devolve também o post em dicionário, para o relatório do fim da rodada
    montar o clipping sem reler a planilha.
    """
    eh_video = bool(item.get("videoUrl"))
    linha = [
        datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        perfil,
        item.get("shortCode", "") or str(item.get("id", "")),
        item.get("url", "") or "",
        item.get("ownerUsername", "") or "",
        "Vídeo" if eh_video else "Foto",
        item.get("timestamp", "") or "",
        item.get("likesCount", "") if item.get("likesCount") is not None else "",
        item.get("commentsCount", "") if item.get("commentsCount") is not None else "",
        item.get("caption", "") or "",
        "",  # Transcrição
        "",  # Resumo do conteúdo
        "",  # Resumo da legenda
        "",  # Temas
        item.get("videoUrl") or item.get("displayUrl") or "",
    ]
    return linha, {
        "candidato": perfil,
        "link": linha[3],
        "usuario": linha[4],
        "tipo": linha[5],
        "publicado": linha[6],
        "curtidas": linha[7],
        "comentarios": linha[8],
        "legenda": linha[9],
        "resumo_conteudo": "",
        "resumo_legenda": "",
        "temas": "",
    }


def rodar_automacao_perfis(data_minima: str, limite_perfis: int | None = None, pular_perfis: int = 0) -> None:
    """Fase 1: coleta os posts de cada perfil da planilha, grava e manda o clipping.

    Não passa pelo Gemini. A análise é a fase 2 (`rodar_analise_pendentes`), que
    lê as linhas com "Resumo do conteúdo" em branco. Separar as duas resolve o
    que quebrou em 05/08/2026: a rodada estourou o timeout de 200 min no post
    a post do Gemini, parou no perfil 74 de 79 e o e-mail nunca saiu. Aqui o
    clipping vai embora em cerca de 12 min, e o que a fase 2 não terminar fica
    pendente para a próxima, sem `pular_perfis` na mão.

    `pular_perfis` pula os N primeiros perfis da lista, para rodar em lotes
    (ex.: pular_perfis=20, limite_perfis=20 processa os perfis 21 a 40).
    """
    apify_token = carregar_apify_token()
    if not apify_token:
        raise RuntimeError("Token do Apify não encontrado. Defina APIFY_TOKEN ou adicione APIFY_TOKEN em credentials.json.")

    client = ApifyClient(apify_token)

    gc = gs_client_from_file()
    sh_resultados = gc.open_by_key(SPREADSHEET_ID)
    aba_resultados = obter_aba(sh_resultados, ABA_RESULTADOS_PERFIS, CABECALHO_RESULTADOS_PERFIS)
    garantir_colunas(aba_resultados, CABECALHO_RESULTADOS_PERFIS)
    ids_processados = obter_ids_processados(aba_resultados)
    ultima_data_por_perfil = obter_ultima_data_por_perfil(aba_resultados)

    todos_perfis = obter_perfis_instagram()
    perfis = todos_perfis[pular_perfis:]
    if limite_perfis:
        perfis = perfis[:limite_perfis]

    inicio = pular_perfis + 1
    fim = pular_perfis + len(perfis)
    print(f"{len(todos_perfis)} perfil(is) no total na coluna {COLUNA_PERFIS} da aba '{ABA_PERFIS}'; processando {inicio} a {fim}.")
    print(f"{len(ids_processados)} post(s) já processado(s) anteriormente (serão pulados).")

    total_novos = 0
    perfis_com_erro: list[str] = []
    perfis_com_post = 0
    # O que a rodada gravou, para o relatório do fim. Guardado aqui em vez de
    # relido da planilha: reler traria também o que outra rodada gravou no mesmo
    # dia, e o clipping deixaria de ser o desta execução.
    salvos_na_rodada: list[dict] = []
    for i, perfil in enumerate(perfis, start=1):
        apenas_apos_perfil = max(data_minima, ultima_data_por_perfil.get(perfil["nome"], data_minima))
        print(f"\n=== [{i}/{len(perfis)}] {perfil['nome']} ({perfil['url']}) — buscando a partir de {apenas_apos_perfil} ===")
        try:
            itens = coletar_itens_perfil_lowcost(client, perfil["url"], posts_por_perfil=POSTS_POR_PERFIL_LOWCOST, apenas_apos=apenas_apos_perfil)
            itens = filtrar_por_data(itens, apenas_apos_perfil)
        except Exception as erro:
            print(f"Aviso: falha ao coletar posts de {perfil['nome']}: {erro}")
            perfis_com_erro.append(perfil["nome"])
            continue

        if itens:
            perfis_com_post += 1
        novos = [item for item in itens if (item.get("shortCode") or str(item.get("id", ""))) not in ids_processados]
        print(f"{len(itens)} post(s) no período, {len(novos)} novo(s).")

        # Uma escrita por perfil, não uma por post: são 543 chamadas a menos na
        # API do Sheets num dia como o de 05/08/2026.
        linhas = []
        for item in novos:
            post_id = item.get("shortCode") or str(item.get("id", ""))
            linha, salvo = montar_linha_perfil(perfil["nome"], item)
            linhas.append(linha)
            salvos_na_rodada.append(salvo)
            ids_processados.add(post_id)
        if linhas:
            aba_resultados.append_rows(linhas, value_input_option="RAW")
            total_novos += len(linhas)
            print(f"{len(linhas)} linha(s) gravada(s) para {perfil['nome']}.")

    if total_novos:
        ordenar_por_data(aba_resultados)

    # Relatório da rodada: e-mail com os números por UF e por cargo e o clipping
    # em PDF anexado. Vem antes do diagnóstico de falha de propósito: mesmo numa
    # rodada que vai terminar em erro por perfil bloqueado, o que foi coletado
    # já rende clipping, e é ele que substitui o envio manual no canal.
    enviar_relatorio(salvos_na_rodada, gc, SPREADSHEET_ID_PERFIS, ABA_PERFIS,
                     SPREADSHEET_ID)

    print(f"\nColeta concluída. {total_novos} post(s) novo(s) gravado(s), aguardando a fase 2 (--analise).")
    print(f"{len(perfis)} perfil(is) percorrido(s), {len(perfis_com_erro)} com falha na coleta, {perfis_com_post} com post no período.")

    # A partir daqui é só diagnóstico da rodada: os posts que deram certo já foram
    # gravados. O erro serve para o Actions marcar vermelho em vez de esconder uma
    # coleta vazia atrás de "0 post(s) no período".
    if perfis_com_erro:
        print("Perfis com falha: " + ", ".join(perfis_com_erro))
    if len(perfis_com_erro) > len(perfis) * LIMIAR_FALHA_PERFIS:
        raise RuntimeError(
            f"Coleta falhou em {len(perfis_com_erro)} de {len(perfis)} perfis, acima do limiar de {LIMIAR_FALHA_PERFIS:.0%}."
        )
    if len(perfis) >= MINIMO_PERFIS_PARA_EXIGIR_POSTS and perfis_com_post == 0:
        raise RuntimeError(
            f"Nenhum dos {len(perfis)} perfis devolveu post no período. Provável bloqueio do Instagram ou proxy do Apify fora do ar."
        )


def linhas_pendentes(aba: gspread.Worksheet) -> list[dict]:
    """Linhas já coletadas que ainda não passaram pelo Gemini.

    Pendente é ter "Resumo do conteúdo" em branco. O estado mora na planilha,
    então a fase 2 pode morrer no meio e a rodada seguinte continua de onde
    parou, sem guardar nada entre execuções.
    """
    valores = aba.get_all_values()
    if len(valores) < 2:
        return []
    cabecalho = valores[0]
    faltando = [c for c in (COLUNA_PENDENTE, "ID do post", "URL da mídia", "Tipo", "Legenda") if c not in cabecalho]
    if faltando:
        print(f"Aviso: a aba '{aba.title}' não tem a(s) coluna(s) {', '.join(faltando)}; nada a analisar.")
        return []

    idx = {nome: cabecalho.index(nome) for nome in cabecalho}

    def celula(linha: list[str], nome: str) -> str:
        i = idx[nome]
        return linha[i].strip() if i < len(linha) else ""

    pendentes = []
    orfas = 0
    for numero, linha in enumerate(valores[1:], start=2):
        if celula(linha, COLUNA_PENDENTE):
            continue
        midia = celula(linha, "URL da mídia")
        post_id = celula(linha, "ID do post")
        if not midia or not post_id:
            # Sem URL não há o que baixar, mas essa linha também nunca mais
            # entra na fila: ela fica sem análise para sempre e não aparece
            # como erro em rodada nenhuma. Foi assim que os 638 posts de
            # 06/08/2026 ficaram parados. Contar e avisar é o que separa
            # "não dá para fazer agora" de "some daqui em silêncio".
            orfas += 1
            continue
        pendentes.append({
            "linha": numero,
            "id": post_id,
            "midia": midia,
            "eh_video": celula(linha, "Tipo") == "Vídeo",
            "legenda": celula(linha, "Legenda"),
            "publicado": celula(linha, "Data de publicação")[:10],
            "candidato": celula(linha, "Candidato") if "Candidato" in idx else "",
        })

    if orfas:
        print(f"Aviso: {orfas} linha(s) sem análise e sem URL da mídia na aba "
              f"'{aba.title}'. A fase 2 não alcança essas linhas; rode "
              f"outros/instagram_backfill.py para recoletar a mídia pelo link.")
    return pendentes


def _analisar_pendente(gem: genai.Client, pendente: dict) -> dict:
    """Baixa a mídia e roda o Gemini para uma linha. Roda dentro das threads.

    Nada de gspread aqui: o cliente não é thread-safe, então a gravação fica
    toda na thread principal.
    """
    pasta = os.path.join("download", pendente["id"])
    os.makedirs(pasta, exist_ok=True)
    extensao = "mp4" if pendente["eh_video"] else "jpg"
    caminho = os.path.join(pasta, f"{pendente['id']}.{extensao}")

    baixar_url(pendente["midia"], caminho)
    texto, uso = analisar_com_gemini(gem, pendente["eh_video"], caminho, pendente["legenda"])
    try:
        os.remove(caminho)
    except OSError:
        pass
    return {"secoes": dividir_resultado(texto), "uso": uso}


def _mapa_id_para_linha(aba: gspread.Worksheet) -> dict[str, int]:
    """ID do post para número da linha, lido na hora de gravar.

    A fase 1 roda `ordenar_por_data`, que reescreve a aba inteira. Guardar o
    número da linha lido no começo da fase 2 e escrever nele depois gravaria a
    análise na linha de outro post.
    """
    cabecalho = aba.row_values(1)
    coluna = aba.col_values(cabecalho.index("ID do post") + 1)
    return {valor: numero for numero, valor in enumerate(coluna[1:], start=2) if valor}


def gravar_analises(aba: gspread.Worksheet, prontos: list[tuple[dict, dict]]) -> None:
    """Escreve as quatro colunas do Gemini de uma vez, um range por post."""
    if not prontos:
        return
    cabecalho = aba.row_values(1)
    primeira = _letra_coluna(cabecalho.index(COLUNAS_GEMINI[0]))
    ultima = _letra_coluna(cabecalho.index(COLUNAS_GEMINI[-1]))
    por_id = _mapa_id_para_linha(aba)

    blocos = []
    for pendente, secoes in prontos:
        numero = por_id.get(pendente["id"])
        if not numero:
            print(f"Aviso: post {pendente['id']} sumiu da planilha antes da gravação, pulando.")
            continue
        blocos.append({
            "range": f"{primeira}{numero}:{ultima}{numero}",
            "values": [[secoes["transcricao"], secoes["resumo_conteudo"],
                        secoes["resumo_legenda"], secoes["temas"]]],
        })
    if blocos:
        aba.batch_update(blocos, value_input_option="RAW")
        print(f"{len(blocos)} análise(s) gravada(s) na planilha.")


def rodar_analise_pendentes(limite: int | None = None) -> None:
    """Fase 2: roda o Gemini nas linhas que a coleta deixou em branco.

    Em paralelo porque o trabalho é espera de rede: download da mídia, upload
    para o Gemini e geração. Em série foram 21s por post.
    """
    gemini_api_key = carregar_gemini_api_key()
    gem = genai.Client(api_key=gemini_api_key)

    gc = gs_client_from_file()
    aba = obter_aba(gc.open_by_key(SPREADSHEET_ID), ABA_RESULTADOS_PERFIS, CABECALHO_RESULTADOS_PERFIS)
    garantir_colunas(aba, CABECALHO_RESULTADOS_PERFIS)

    pendentes = linhas_pendentes(aba)
    print(f"{len(pendentes)} post(s) pendente(s) de análise.")

    # Passado o prazo, a URL do CDN não volta a funcionar. Marcar tira a linha
    # da fila: sem isso toda rodada gastaria download e tempo no mesmo post
    # morto, para sempre.
    hoje = datetime.now().strftime("%Y-%m-%d")
    corte = (datetime.now() - timedelta(days=DIAS_PARA_DESISTIR_DA_MIDIA)).strftime("%Y-%m-%d")
    velhas = [p for p in pendentes if p["publicado"] and p["publicado"] < corte]
    pendentes = [p for p in pendentes if p not in velhas]
    if limite:
        pendentes = pendentes[:limite]
    if not pendentes and not velhas:
        return

    prontos: list[tuple[dict, dict]] = []
    total_entrada = total_saida = 0
    falhas = expiradas = 0

    def descarregar():
        nonlocal prontos
        gravar_analises(aba, prontos)
        prontos = []

    with ThreadPoolExecutor(max_workers=THREADS_ANALISE) as executor:
        tarefas = {executor.submit(_analisar_pendente, gem, p): p for p in pendentes}
        for i, tarefa in enumerate(as_completed(tarefas), start=1):
            pendente = tarefas[tarefa]
            try:
                resultado = tarefa.result()
            except MidiaExpirada as erro:
                expiradas += 1
                # 403 do CDN em post de outro dia é URL vencida, e vencida não
                # volta: marcar aqui tira a linha da fila na hora, em vez de
                # gastar mais um download por rodada até bater o prazo de
                # DIAS_PARA_DESISTIR_DA_MIDIA. Post de hoje fica pendente, porque
                # aí o 403 pode ser bloqueio passageiro do CDN e a mídia ainda
                # está no ar.
                if pendente["publicado"] and pendente["publicado"] < hoje:
                    velhas.append(pendente)
                    print(f"[{i}/{len(pendentes)}] {pendente['id']}: mídia expirada "
                          f"({erro}), marcado como não analisado.")
                else:
                    print(f"[{i}/{len(pendentes)}] {pendente['id']}: mídia indisponível "
                          f"({erro}), fica pendente.")
                continue
            except Exception as erro:
                falhas += 1
                print(f"[{i}/{len(pendentes)}] {pendente['id']}: falhou ({str(erro)[:160]}), fica pendente.")
                continue

            prontos.append((pendente, resultado["secoes"]))
            total_entrada += resultado["uso"]["entrada"]
            total_saida += resultado["uso"]["saida"]
            print(f"[{i}/{len(pendentes)}] {pendente['id']} ({pendente['candidato']}) analisado.")
            if len(prontos) >= LOTE_ESCRITA_ANALISE:
                descarregar()

    descarregar()

    if velhas:
        gravar_analises(aba, [
            (p, {"transcricao": "", "resumo_conteudo": MARCA_MIDIA_EXPIRADA,
                 "resumo_legenda": "", "temas": ""})
            for p in velhas
        ])
        print(f"{len(velhas)} post(s) com mais de {DIAS_PARA_DESISTIR_DA_MIDIA} dias marcado(s) como não analisado(s).")

    custo = (total_entrada / 1_000_000 * PRECO_ENTRADA_POR_MILHAO
             + total_saida / 1_000_000 * PRECO_SAIDA_POR_MILHAO)
    print(f"\nAnálise concluída. {len(pendentes) - falhas - expiradas} post(s) analisado(s), "
          f"{falhas} com falha, {expiradas} sem mídia disponível.")
    print(f"Tokens: {total_entrada:,} de entrada, {total_saida:,} de saída. "
          f"Custo estimado: US$ {custo:.2f}"
          f"{' (resolução baixa)' if RESOLUCAO_MIDIA_BAIXA else ''}.")


def baixar_midia(item: dict, pasta: str = "download"):
    """Baixa a mídia de um item já coletado e retorna (eh_video, caminho_arquivo, legenda)."""
    print("Campos retornados:", list(item.keys()))

    legenda = item.get("caption", "") or ""
    video_url = item.get("videoUrl")
    image_url = item.get("displayUrl")
    print("Tipo do post:", item.get("type"))

    os.makedirs(pasta, exist_ok=True)
    nome_base = item.get("shortCode") or item.get("id") or "midia"
    if video_url:
        eh_video, caminho, origem = True, os.path.join(pasta, f"{nome_base}.mp4"), video_url
    else:
        eh_video, caminho, origem = False, os.path.join(pasta, f"{nome_base}.jpg"), image_url

    if not origem:
        raise RuntimeError("Não encontrei nem videoUrl nem displayUrl no item retornado.")

    baixar_url(origem, caminho)
    print("Mídia salva em:", caminho, "| é vídeo?", eh_video)
    return eh_video, caminho, legenda


class MidiaExpirada(RuntimeError):
    """A URL do CDN do Instagram não vale mais.

    Elas são assinadas e caducam. A fase 2 roda depois da coleta, então isso
    aparece quando uma linha fica pendente por dias, e não é erro para tentar
    de novo na mesma rodada.
    """


def baixar_url(origem: str, caminho: str) -> None:
    """Baixa a mídia, traduzindo 403/404/410 do CDN em MidiaExpirada."""
    resposta = requests.get(origem, timeout=120)
    if resposta.status_code in (403, 404, 410):
        raise MidiaExpirada(f"CDN devolveu {resposta.status_code}")
    resposta.raise_for_status()
    with open(caminho, "wb") as f:
        f.write(resposta.content)


def montar_prompt(legenda: str) -> str:
    return f"""Você analisa conteúdo de redes de candidatos para monitoramento eleitoral.
Responda em português, em seções:
1. Transcrição (só se for vídeo): fala na íntegra, sem timestamps.
2. Resumo do conteúdo: em bullets, factual.
3. Resumo da legenda.
4. Temas: palavras-chave.

Legenda:
\"\"\"{legenda}\"\"\"
"""


def _e_transitorio(erro: Exception) -> bool:
    """503 (modelo sobrecarregado), 429 (cota) e 500 voltam a funcionar sozinhos."""
    texto = str(erro)
    return any(marca in texto for marca in ("429", "500", "503", "UNAVAILABLE", "RESOURCE_EXHAUSTED", "INTERNAL"))


def com_retry(funcao, contexto: str = "", tentativas: int = TENTATIVAS_GEMINI):
    """Repete a chamada em erro transitório, com espera crescente e sorteada.

    Sem isso, os 6 posts que pegaram 503 em 05/08/2026 sumiram da rodada.
    """
    for tentativa in range(1, tentativas + 1):
        try:
            return funcao()
        except MidiaExpirada:
            raise
        except Exception as erro:
            if tentativa == tentativas or not _e_transitorio(erro):
                raise
            espera = 2 ** tentativa + random.uniform(0, 1)
            print(f"Aviso: {contexto} falhou ({str(erro)[:120]}), tentando de novo em {espera:.0f}s ({tentativa}/{tentativas - 1}).")
            time.sleep(espera)


def analisar_com_gemini(gem: genai.Client, eh_video: bool, caminho: str, legenda: str) -> tuple[str, dict]:
    """Devolve o texto da análise e o uso de tokens da chamada.

    O uso vem junto para a rodada fechar com o custo estimado no log, em vez de
    ele só aparecer na fatura.
    """
    prompt = montar_prompt(legenda)
    config = None
    if RESOLUCAO_MIDIA_BAIXA:
        config = types.GenerateContentConfig(media_resolution=types.MediaResolution.MEDIA_RESOLUTION_LOW)

    if eh_video:
        arquivo = com_retry(lambda: gem.files.upload(file=caminho), f"upload de {os.path.basename(caminho)}")
        # O polling era de 5 em 5s, e o piso valia para todo vídeo. 2s dá o
        # mesmo resultado e devolve alguns segundos por post.
        while arquivo.state.name == "PROCESSING":
            time.sleep(2)
            arquivo = gem.files.get(name=arquivo.name)
        if arquivo.state.name == "FAILED":
            raise RuntimeError(f"O Gemini não conseguiu processar o vídeo {os.path.basename(caminho)}.")
        conteudo = [arquivo, prompt]
    else:
        with open(caminho, "rb") as f:
            dados = f.read()
        conteudo = [types.Part.from_bytes(data=dados, mime_type="image/jpeg"), prompt]

    resp = com_retry(
        lambda: gem.models.generate_content(model="gemini-3.6-flash", contents=conteudo, config=config),
        f"análise de {os.path.basename(caminho)}",
    )
    uso = resp.usage_metadata
    return resp.text, {
        "entrada": getattr(uso, "prompt_token_count", 0) or 0,
        "saida": getattr(uso, "candidates_token_count", 0) or 0,
    }


def processar_item(gem: genai.Client, item: dict, pasta: str) -> None:
    """Baixa a mídia de um item, analisa com o Gemini, imprime e salva na planilha."""
    link = item.get("url") or item.get("inputUrl") or ""
    eh_video, caminho, legenda = baixar_midia(item, pasta=pasta)
    resultado, _uso = analisar_com_gemini(gem, eh_video, caminho, legenda)

    print(f"\n=== Resultado ({link}) ===\n")
    print(resultado)

    try:
        salvar_no_sheets(link, item, eh_video, resultado)
    except Exception as erro:
        print(f"\nAviso: não foi possível salvar na planilha do Google Sheets: {erro}")


def main():
    apify_token = carregar_apify_token()
    gemini_api_key = carregar_gemini_api_key()
    url = sys.argv[1].strip() if len(sys.argv) > 1 else input("Digite o link do post ou do perfil do Instagram: ").strip()

    if not url:
        print("Nenhum link do Instagram foi informado.")
        return

    url = limpar_url_instagram(url)

    if not apify_token:
        raise RuntimeError("Token do Apify não encontrado. Defina APIFY_TOKEN ou adicione APIFY_TOKEN em credentials.json.")

    client = ApifyClient(apify_token)
    gem = genai.Client(api_key=gemini_api_key)

    if eh_link_de_perfil(url):
        if len(sys.argv) > 2:
            apenas_apos = sys.argv[2].strip()
        elif os.getenv("ONLY_POSTS_NEWER_THAN", "").strip():
            apenas_apos = os.getenv("ONLY_POSTS_NEWER_THAN", "").strip()
        else:
            apenas_apos = input(
                "Link de perfil detectado. Buscar posts a partir de que data (YYYY-MM-DD, Enter para não filtrar)? "
            ).strip()

        itens = coletar_itens(client, [url], resultados_limit=RESULTS_LIMIT_PERFIL, apenas_apos=apenas_apos or None)

        if apenas_apos:
            antes = len(itens)
            itens = filtrar_por_data(itens, apenas_apos)
            descartados = antes - len(itens)
            if descartados:
                print(f"{descartados} post(s) descartado(s) por serem anteriores a {apenas_apos} (provável post fixado).")

        print(f"\n{len(itens)} post(s) encontrado(s) no perfil"
              + (f" a partir de {apenas_apos}" if apenas_apos else "") + ".")

        for i, item in enumerate(itens, start=1):
            print(f"\n--- Processando post {i}/{len(itens)} ---")
            try:
                processar_item(gem, item, pasta=os.path.join("download", item.get("shortCode") or str(i)))
            except Exception as erro:
                print(f"Aviso: falha ao processar o post {item.get('url', '')}: {erro}")
    else:
        itens = coletar_itens(client, [url], resultados_limit=1)
        processar_item(gem, itens[0], pasta="download")


def main_perfis():
    if len(sys.argv) > 2:
        data_minima = sys.argv[2].strip()
    else:
        data_minima = os.getenv("DATA_MINIMA_PERFIS", "").strip()
    if not data_minima:
        data_minima = input("Buscar posts a partir de que data (YYYY-MM-DD)? ").strip()
    if not data_minima:
        print("Data mínima não informada, cancelando (evita rodar sem filtro sobre todos os perfis).")
        return

    limite_perfis = int(sys.argv[3]) if len(sys.argv) > 3 else None
    pular_perfis = int(sys.argv[4]) if len(sys.argv) > 4 else 0
    rodar_automacao_perfis(data_minima=data_minima, limite_perfis=limite_perfis, pular_perfis=pular_perfis)


def main_relatorio():
    """Só o relatório, a partir do que já está gravado na aba de resultados.

    `python outros/instagram.py --relatorio [YYYY-MM-DD]`. Existe porque a
    coleta é a parte cara (a Apify cobra pelos 79 perfis) e refazer ou reenviar
    o clipping não precisa dela. Sem data, é o que foi gravado hoje.
    """
    dia = sys.argv[2].strip() if len(sys.argv) > 2 else datetime.now().strftime("%Y-%m-%d")
    gc = gs_client_from_file()
    salvos = posts_gravados_no_dia(gc, SPREADSHEET_ID, ABA_RESULTADOS_PERFIS, dia)
    print(f"{len(salvos)} post(s) gravado(s) em {dia}.")
    enviar_relatorio(salvos, gc, SPREADSHEET_ID_PERFIS, ABA_PERFIS, SPREADSHEET_ID)


def main_analise():
    """Fase 2, sozinha: `python outros/instagram.py --analise [limite]`.

    O limite serve para rodar um lote pequeno, por exemplo para comparar o
    resultado com RESOLUCAO_MIDIA_BAIXA ligada antes de mudar todo mundo.
    """
    limite = int(sys.argv[2]) if len(sys.argv) > 2 and sys.argv[2].strip() else None
    rodar_analise_pendentes(limite=limite)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--perfis":
        main_perfis()
    elif len(sys.argv) > 1 and sys.argv[1] == "--analise":
        main_analise()
    elif len(sys.argv) > 1 and sys.argv[1] == "--relatorio":
        main_relatorio()
    else:
        main()
