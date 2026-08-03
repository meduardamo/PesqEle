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

Antes de rodar:
    pip install apify-client google-genai gspread google-auth google-api-python-client requests

Uso:
    python instagram.py
    (vai pedir o link do post ou do perfil do Instagram)

    python instagram.py <link> [data_minima]
    Ex.: python instagram.py https://www.instagram.com/candidato/ 2026-07-13

    python instagram.py --perfis [data_minima] [limite_de_perfis]
    Ex.: python instagram.py --perfis 2026-07-13
"""

import json
import os
import re
import sys
import time
from datetime import datetime, timezone
from urllib.parse import urlsplit, urlunsplit

import gspread
import requests
from apify_client import ApifyClient
from google import genai
from google.genai import types
from google.oauth2.service_account import Credentials
from googleapiclient.discovery import build as build_google_service


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
# Fração de perfis que pode falhar na coleta antes da rodada inteira ser considerada
# quebrada (e sair com código != 0, para o Actions marcar vermelho).
LIMIAR_FALHA_PERFIS = float(os.getenv("LIMIAR_FALHA_PERFIS", "0.2"))
# A partir de quantos perfis "nenhum deles devolveu post" deixa de ser um dia parado
# e passa a ser bloqueio do Instagram. Em 31/07/2026 os 79 runs terminaram SUCCEEDED
# com "access denied" em 76 perfis, então o status do run sozinho não pega esse caso.
MINIMO_PERFIS_PARA_EXIGIR_POSTS = int(os.getenv("MINIMO_PERFIS_PARA_EXIGIR_POSTS", "10"))
SPREADSHEET_ID_PERFIS = os.getenv("SPREADSHEET_ID_PERFIS", "1piO-m19orW1i-Z-6rNeWdXnEAWqw5wneiDpdHZqOa6Y")
ABA_PERFIS = os.getenv("ABA_PERFIS", "Instagram")
COLUNA_PERFIS = os.getenv("COLUNA_PERFIS", "B")
ABA_RESULTADOS_PERFIS = os.getenv("ABA_RESULTADOS_PERFIS", "Resultados")
CABECALHO_RESULTADOS_PERFIS = [
    "Data/Hora",
    "Pré-candidato",
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
]

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

    run = client.actor(ATOR_INSTAGRAM_PERFIS_LOWCOST).call(run_input=run_input)
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


def obter_ultima_data_por_perfil(aba: gspread.Worksheet) -> dict[str, str]:
    """Retorna, por 'Pré-candidato', a data (YYYY-MM-DD) do post mais recente já salvo.

    Usado para avançar o onlyPostsNewerThan/newerThan por perfil a cada execução,
    para que o Apify nem chegue a buscar (e cobrar) de novo posts que já estão
    na planilha — só a data de corte por perfil, o "ID do post" continua sendo
    a rede de segurança contra duplicar no lado do Gemini/planilha.
    """
    valores = aba.get_all_values()
    if len(valores) < 2:
        return {}
    cabecalho = valores[0]
    if "Pré-candidato" not in cabecalho or "Data de publicação" not in cabecalho:
        return {}
    idx_perfil = cabecalho.index("Pré-candidato")
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


def salvar_resultado_perfil(aba: gspread.Worksheet, perfil: str, item: dict, eh_video: bool, resultado: str) -> None:
    """Adiciona uma linha de resultado (com ID do post e nome do pré-candidato) na aba de resultados."""
    secoes = dividir_resultado(resultado)
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
        secoes["transcricao"],
        secoes["resumo_conteudo"],
        secoes["resumo_legenda"],
        secoes["temas"],
    ]
    aba.append_row(linha, value_input_option="RAW")


def rodar_automacao_perfis(data_minima: str, limite_perfis: int | None = None, pular_perfis: int = 0) -> None:
    """Etapa 5: roda o fluxo completo para cada perfil da planilha de acompanhamento.

    `pular_perfis` pula os N primeiros perfis da lista, para rodar em lotes
    (ex.: pular_perfis=20, limite_perfis=20 processa os perfis 21 a 40).
    """
    apify_token = carregar_apify_token()
    gemini_api_key = carregar_gemini_api_key()
    if not apify_token:
        raise RuntimeError("Token do Apify não encontrado. Defina APIFY_TOKEN ou adicione APIFY_TOKEN em credentials.json.")

    client = ApifyClient(apify_token)
    gem = genai.Client(api_key=gemini_api_key)

    gc = gs_client_from_file()
    sh_resultados = gc.open_by_key(SPREADSHEET_ID)
    aba_resultados = obter_aba(sh_resultados, ABA_RESULTADOS_PERFIS, CABECALHO_RESULTADOS_PERFIS)
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

        for item in novos:
            post_id = item.get("shortCode") or str(item.get("id", ""))
            try:
                eh_video, caminho, legenda = baixar_midia(item, pasta=os.path.join("download", post_id or "midia"))
                resultado = analisar_com_gemini(gem, eh_video, caminho, legenda)
                salvar_resultado_perfil(aba_resultados, perfil["nome"], item, eh_video, resultado)
                ids_processados.add(post_id)
                total_novos += 1
                print(f"Post {post_id} processado e salvo.")
            except Exception as erro:
                print(f"Aviso: falha ao processar o post {post_id} de {perfil['nome']}: {erro}")

    if total_novos:
        ordenar_por_data(aba_resultados)
    print(f"\nAutomação concluída. {total_novos} post(s) novo(s) processado(s).")
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

    resposta = requests.get(origem)
    resposta.raise_for_status()
    with open(caminho, "wb") as f:
        f.write(resposta.content)

    print("Mídia salva em:", caminho, "| é vídeo?", eh_video)
    return eh_video, caminho, legenda


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


def analisar_com_gemini(gem: genai.Client, eh_video: bool, caminho: str, legenda: str) -> str:
    prompt = montar_prompt(legenda)

    if eh_video:
        arquivo = gem.files.upload(file=caminho)
        while arquivo.state.name == "PROCESSING":
            time.sleep(5)
            arquivo = gem.files.get(name=arquivo.name)
        conteudo = [arquivo, prompt]
    else:
        with open(caminho, "rb") as f:
            dados = f.read()
        conteudo = [types.Part.from_bytes(data=dados, mime_type="image/jpeg"), prompt]

    resp = gem.models.generate_content(model="gemini-2.5-flash", contents=conteudo)
    return resp.text


def processar_item(gem: genai.Client, item: dict, pasta: str) -> None:
    """Baixa a mídia de um item, analisa com o Gemini, imprime e salva na planilha."""
    link = item.get("url") or item.get("inputUrl") or ""
    eh_video, caminho, legenda = baixar_midia(item, pasta=pasta)
    resultado = analisar_com_gemini(gem, eh_video, caminho, legenda)

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
    rodar_automacao_perfis(data_minima=data_minima, limite_perfis=limite_perfis)


if __name__ == "__main__":
    if len(sys.argv) > 1 and sys.argv[1] == "--perfis":
        main_perfis()
    else:
        main()