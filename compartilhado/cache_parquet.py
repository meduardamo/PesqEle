"""Cache Parquet, no Drive, das abas pesadas do PollingData.

Por que existe: a aba `resultados_bi` da matriz T1 passou de 137 mil linhas e o
`get_all_values` dela leva de 5 a 12 segundos por leitura fria, 11 MB de string.
Seis painéis Streamlit leem essa aba inteira e jogam fora quase tudo em pandas.
Em Parquet a mesma tabela tem 0,3 MB e abre em centésimos de segundo.

A aba continua sendo a fonte: o Looker lê dela e ela é o fallback dos painéis.
O que este módulo publica é uma cópia *verbatim*, gerada lendo a aba de volta e
passando pelo mesmo `values_to_df` que os painéis usam. Isso é de propósito: o
DataFrame que o painel recebe pelo caminho rápido e pelo caminho lento tem que
ser o mesmo objeto, senão o fallback vira uma segunda versão da verdade.

Falhar aqui nunca pode derrubar o rebuild. Quem chama trata a exceção e segue.
"""
from __future__ import annotations

import io
import json
import os

import pandas as pd
from google.auth.transport.requests import AuthorizedSession

# Pasta "_cache_parquet_paineis", dentro de Pesquisas Eleitorais, no drive
# compartilhado "Eleições 2026". Não é segredo: quem manda no acesso é a
# permissão da pasta. Dá para trocar por variável de ambiente sem mexer no código.
PASTA_CACHE_PADRAO = "126CgkxgSAR6EBc489HwGgn128hX0Fx3I"
PASTA_CACHE_ENV = "DRIVE_PASTA_CACHE_PARQUET"

API_ARQUIVOS = "https://www.googleapis.com/drive/v3/files"
API_UPLOAD = "https://www.googleapis.com/upload/drive/v3/files"

# Abas que os painéis leem. Vale a pena publicar até as pequenas: o custo do
# Sheets é de latência, não de tamanho, e uma aba fora do cache obriga a página
# a abrir a planilha do mesmo jeito, jogando fora o ganho das outras.
# "institutos" entra não pelo tamanho, que é pequeno, mas pelo carimbo: é o
# modifiedTime do Parquet que mata o memo do painel quando a aba muda. Sem
# Parquet, o carimbo é sempre "" e a tabela velha sobrevivia o TTL inteiro.
ABAS_PADRAO = ("resultados_bi", "resultados", "pesquisas", "institutos")


def pasta_cache() -> str:
    return os.getenv(PASTA_CACHE_ENV, "").strip() or PASTA_CACHE_PADRAO


def nome_arquivo(spreadsheet_id: str, aba: str) -> str:
    """Nome determinístico: o painel monta o mesmo nome sem precisar de índice."""
    return f"{spreadsheet_id}__{aba}.parquet"


def values_to_df(values: list) -> pd.DataFrame:
    """Cópia exata do `values_to_df` de `data_loaders.py` dos painéis.

    Tem que continuar idêntica. Se as duas divergirem, o caminho rápido e o
    fallback passam a devolver DataFrames diferentes e o painel muda de
    comportamento conforme o Drive estiver no ar ou não. O teste
    `tests/test_cache_parquet.py` fixa o contrato (padding de linha curta e
    remoção de coluna inteiramente vazia).
    """
    if not values:
        return pd.DataFrame()
    header = [str(c).strip() or f"col_{i+1}" for i, c in enumerate(values[0])]
    rows = []
    for row in values[1:]:
        row = list(row)
        if len(row) < len(header):
            row += [""] * (len(header) - len(row))
        rows.append(row[: len(header)])
    df = pd.DataFrame(rows, columns=header).dropna(how="all")
    empty_cols = [c for c in df.columns if df[c].astype(str).str.strip().eq("").all()]
    return df.drop(columns=empty_cols, errors="ignore")


def credenciais_do_cliente(gc):
    """Credenciais de dentro do cliente gspread.

    O atributo mudou de lugar entre versões do gspread (6.1 expõe `auth` no
    cliente, 6.2 moveu para `http_client.auth`). Procurar nos dois lugares
    evita que um upgrade de dependência quebre a publicação do cache.
    """
    for caminho in (lambda: gc.auth,
                    lambda: gc.http_client.auth,
                    lambda: gc.session.credentials):
        try:
            creds = caminho()
        except AttributeError:
            continue
        if creds is not None:
            return creds
    raise RuntimeError("não achei as credenciais dentro do cliente gspread")


def _achar_arquivo(sessao: AuthorizedSession, pasta: str, nome: str) -> str | None:
    """ID do arquivo com esse nome na pasta, ou None."""
    nome_escapado = nome.replace("'", "\\'")
    resp = sessao.get(
        API_ARQUIVOS,
        params={
            "q": f"name='{nome_escapado}' and '{pasta}' in parents and trashed=false",
            "includeItemsFromAllDrives": "true",
            "supportsAllDrives": "true",
            "fields": "files(id)",
        },
        timeout=60,
    )
    resp.raise_for_status()
    arquivos = resp.json().get("files", [])
    return arquivos[0]["id"] if arquivos else None


def _subir(sessao: AuthorizedSession, pasta: str, nome: str, conteudo: bytes) -> str:
    """Sobe (ou sobrescreve) o arquivo e devolve o ID.

    Sobrescrever o mesmo ID em vez de criar um arquivo novo mantém o link
    estável e evita encher a pasta de versões.
    """
    existente = _achar_arquivo(sessao, pasta, nome)
    metadados: dict = {"name": nome}
    if existente is None:
        metadados["parents"] = [pasta]
    corpo, tipo = _multipart(metadados, conteudo)
    cabecalho = {"Content-Type": tipo}
    params = {"uploadType": "multipart", "supportsAllDrives": "true", "fields": "id"}
    if existente:
        resp = sessao.patch(f"{API_UPLOAD}/{existente}", params=params,
                            headers=cabecalho, data=corpo, timeout=180)
    else:
        resp = sessao.post(API_UPLOAD, params=params,
                           headers=cabecalho, data=corpo, timeout=180)
    resp.raise_for_status()
    return resp.json()["id"]


def _multipart(metadados: dict, conteudo: bytes) -> tuple[bytes, str]:
    limite = "----eixo-cache-parquet"
    partes = [
        f"--{limite}\r\nContent-Type: application/json; charset=UTF-8\r\n\r\n".encode(),
        json.dumps(metadados).encode(),
        f"\r\n--{limite}\r\nContent-Type: application/octet-stream\r\n\r\n".encode(),
        conteudo,
        f"\r\n--{limite}--\r\n".encode(),
    ]
    return b"".join(partes), f"multipart/related; boundary={limite}"


def publicar_abas(gc, spreadsheet_id: str, abas=ABAS_PADRAO) -> list[dict]:
    """Lê as abas da planilha e publica cada uma como Parquet no Drive.

    Lê a aba de volta em vez de usar o DataFrame que o rebuild tem em memória:
    o painel lê da aba, e é a aba (com a formatação que o Sheets aplicou) que
    precisa ser reproduzida bit a bit.
    """
    sessao = AuthorizedSession(credenciais_do_cliente(gc))
    pasta = pasta_cache()
    planilha = gc.open_by_key(spreadsheet_id)
    publicados = []
    for aba in abas:
        try:
            df = values_to_df(planilha.worksheet(aba).get_all_values())
        except Exception as erro:
            print(f"  [cache] aba '{aba}' não lida, sem cache: {erro}", flush=True)
            continue
        if df.empty:
            print(f"  [cache] aba '{aba}' vazia, nada a publicar", flush=True)
            continue
        buffer = io.BytesIO()
        df.to_parquet(buffer, compression="zstd", index=False)
        dados = buffer.getvalue()
        nome = nome_arquivo(spreadsheet_id, aba)
        arquivo_id = _subir(sessao, pasta, nome, dados)
        print(f"  [cache] {nome}: {len(df)} linhas, {len(dados)/1e6:.2f} MB "
              f"(drive {arquivo_id})", flush=True)
        publicados.append({"aba": aba, "arquivo_id": arquivo_id,
                           "linhas": len(df), "bytes": len(dados)})
    return publicados
