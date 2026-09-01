# -*- coding: utf-8 -*-
"""Guarda no Drive uma cópia de cada plano de governo registrado no TSE.

Por que existe: o DivulgaCand é a única fonte do PDF e passa horas fora do ar ou
respondendo 403 de WAF. Em 19/08/2026 isso travou a releitura dos 206 planos com
a taxonomia nova, porque cada releitura precisa baixar o arquivo de novo. Com a
cópia no Drive, o TSE é consultado uma vez por plano e a análise deixa de
depender dele: `analise_planos.baixar_plano` procura o espelho antes de sair
para a rede (ver `registrar_espelho`).

O segundo motivo é ter o histórico. O candidato pode registrar um plano novo no
lugar do antigo, e hoje o painel só mostra o que estiver no ar naquele dia. Aqui
o arquivo anterior não é apagado: a cópia nova entra como revisão do mesmo item
do Drive, com `keepRevisionForever`, e a planilha guarda a versão, o sha256 e a
data de cada troca.

Quando um plano é considerado novo:
  - não há linha na aba `planos_arquivos` para aquele candidato; ou
  - o LINK_PLANO da base mudou (o TSE emite outro id de documento a cada
    registro, então o link é a chave da versão); ou
  - --forcar, que rebaixa tudo e compara pelo sha256.

Uso:
    export SPREADSHEET_ID_TSE=...
    export PASTA_DRIVE_PLANOS=...        # opcional, ver PASTA_PADRAO
    python -m outros.espelhar_planos
    python -m outros.espelhar_planos --uf MS
    python -m outros.espelhar_planos --forcar --limite 5

Grava em lotes, como o processar_planos: cair no meio perde no máximo o lote.
"""

from __future__ import annotations

import argparse
import hashlib
import io
import json
import os
import re
import sys
import time
import unicodedata
from datetime import datetime, timedelta, timezone

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from analise_planos import PlanoIlegivel, PlanoIndisponivel, baixar_plano  # noqa: E402

ESCOPOS = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

ANO = "2026"
ABA_BASE = "base_dadosabertos"
ABA_ARQUIVOS = "planos_arquivos"

# Raiz do drive compartilhado "Eleições 2026". A subpasta "Planos de Governo" é
# criada na primeira execução, e dentro dela uma pasta por UF (BR para os planos
# de presidente). Sem a divisão por UF a pasta fica com 206 PDFs em lista única,
# que é onde a equipe procura à mão quando o painel não basta.
PASTA_PADRAO = "0AH-94UFLKIFPUk9PVA"
NOME_PASTA_RAIZ = "Planos de Governo"

LOTE = 10                 # candidatos entre uma gravação e outra na planilha
PAUSA_ENTRE_PLANOS = int(os.getenv("PAUSA_ENTRE_PLANOS", "4"))
TENTATIVAS_REDE = 4
ESPERA_REDE = 5

COLS = ["ano", "sq_candidato", "candidato", "partido", "uf", "cargo", "link",
        "drive_id", "drive_link", "sha256", "bytes", "versao_arquivo",
        "espelhado_em"]

FUSO = timezone(timedelta(hours=-3))


def agora() -> str:
    return datetime.now(FUSO).strftime("%d/%m/%Y %H:%M")


def log(msg: str = "") -> None:
    print(msg, flush=True)


# ── Google ────────────────────────────────────────────────────────────────────

def clientes_google(caminho_credenciais: str = ""):
    """gspread e Drive v3 a partir da mesma credencial de conta de serviço.

    Aceita o JSON inteiro em GOOGLE_SHEETS_CREDS (é assim que os workflows
    passam o secret) ou o arquivo credentials.json, que é a convenção local.
    """
    import gspread
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build

    if caminho_credenciais:
        with open(caminho_credenciais, encoding="utf-8") as f:
            info = json.load(f)
    else:
        bruto = os.getenv("GOOGLE_SHEETS_CREDS", "").strip()
        if bruto:
            info = json.loads(bruto)
        else:
            caminho = os.getenv("GOOGLE_APPLICATION_CREDENTIALS", "credentials.json")
            if not os.path.exists(caminho):
                raise SystemExit(
                    "Sem credenciais. Defina GOOGLE_SHEETS_CREDS com o JSON da conta "
                    "de serviço, ou passe --credenciais caminho/para/credentials.json"
                )
            with open(caminho, encoding="utf-8") as f:
                info = json.load(f)
    creds = Credentials.from_service_account_info(info, scopes=ESCOPOS)
    return gspread.authorize(creds), build("drive", "v3", credentials=creds)


def com_retentativa(descricao, fn):
    """Repete chamada de rede que caiu por motivo transitório."""
    for n in range(1, TENTATIVAS_REDE + 1):
        try:
            return fn()
        except Exception as e:
            texto = str(e)
            transitorio = any(t in texto for t in
                              ("500", "502", "503", "504", "429", "timed out",
                               "Connection", "rateLimitExceeded", "userRateLimitExceeded"))
            if n == TENTATIVAS_REDE or not transitorio:
                raise
            espera = ESPERA_REDE * 2 ** (n - 1)
            log(f"    {descricao} falhou ({texto[:120]}); tentativa {n + 1} em {espera}s")
            time.sleep(espera)


def ler_aba(sh, nome: str) -> pd.DataFrame:
    try:
        valores = sh.worksheet(nome).get_all_values()
    except Exception:
        return pd.DataFrame()
    if not valores:
        return pd.DataFrame()
    return pd.DataFrame(valores[1:], columns=[c.strip() for c in valores[0]])


def aba_ou_cria(sh, nome: str, colunas: list[str]):
    try:
        ws = sh.worksheet(nome)
    except Exception:
        ws = sh.add_worksheet(nome, rows=400, cols=len(colunas) + 2)
        ws.update(values=[colunas], range_name="A1")
        return ws
    # Aba criada por uma versão anterior pode estar sem coluna nova. Escrever o
    # cabeçalho de novo é barato e evita linha desalinhada.
    atual = ws.row_values(1)
    if [c.strip() for c in atual] != colunas:
        ws.update(values=[colunas], range_name="A1")
    return ws


# ── Pastas ────────────────────────────────────────────────────────────────────

def garantir_pasta(drive, nome: str, pai: str) -> str:
    """Id da subpasta `nome` dentro de `pai`, criada se ainda não existir.

    supportsAllDrives e includeItemsFromAllDrives são obrigatórios: sem eles a
    API responde 404, porque drive compartilhado não aparece na visão padrão da
    conta de serviço.
    """
    seguro = nome.replace("'", "\\'")
    q = (f"'{pai}' in parents and name='{seguro}' "
         "and mimeType='application/vnd.google-apps.folder' and trashed=false")
    achou = com_retentativa(
        f"busca da pasta '{nome}'",
        lambda: drive.files().list(
            q=q, includeItemsFromAllDrives=True, supportsAllDrives=True,
            fields="files(id)", pageSize=5,
        ).execute(),
    ).get("files", [])
    if achou:
        return achou[0]["id"]
    nova = com_retentativa(
        f"criação da pasta '{nome}'",
        lambda: drive.files().create(
            body={"name": nome, "parents": [pai],
                  "mimeType": "application/vnd.google-apps.folder"},
            supportsAllDrives=True, fields="id",
        ).execute(),
    )
    log(f"  pasta '{nome}' criada")
    return nova["id"]


# ── Nome do arquivo ───────────────────────────────────────────────────────────

def _sem_acento(t: str) -> str:
    return "".join(c for c in unicodedata.normalize("NFD", str(t or ""))
                   if unicodedata.category(c) != "Mn")


def nome_arquivo(uf: str, nome: str, partido: str, sq: str) -> str:
    """Nome legível e único. O sq entra no fim porque é a chave que casa com a
    planilha e com a análise; sem ele, dois homônimos na mesma UF colidem.
    """
    limpo = re.sub(r"[^A-Za-z0-9 .\-]", "", _sem_acento(nome)).strip()
    limpo = re.sub(r"\s+", " ", limpo)[:60] or "sem nome"
    sigla = re.sub(r"[^A-Za-z0-9]", "", _sem_acento(partido))[:12]
    return f"{uf} - {limpo} ({sigla}) - {sq}.pdf"


# ── Envio ─────────────────────────────────────────────────────────────────────

def liberar_leitura(drive, file_id: str) -> bool:
    """Deixa o PDF abrir por link, sem login.

    Por que: o painel passou a apontar para a cópia do Drive em vez do link do
    TSE. O TSE serve o arquivo com `Content-Disposition: attachment`, então o
    navegador baixa em vez de abrir e o clique parece não fazer nada; a cópia do
    Drive abre no visualizador. Só que o drive é compartilhado e a conta de
    serviço é a dona, então sem esta permissão quem abre o painel vê pedido de
    acesso.

    São documentos públicos: plano de governo registrado no TSE, que qualquer
    pessoa baixa do DivulgaCand. O que muda é de onde vem o arquivo.

    Idempotente: repetir devolve a mesma permissão `anyoneWithLink`.
    """
    try:
        com_retentativa(
            f"liberação de leitura de {file_id}",
            lambda: drive.permissions().create(
                fileId=file_id, body={"role": "reader", "type": "anyone"},
                supportsAllDrives=True, fields="id",
            ).execute(),
        )
        return True
    except Exception as e:
        log(f"    não consegui liberar leitura ({str(e)[:120]})")
        return False


def enviar_ou_atualizar(drive, dados: bytes, nome: str, pasta: str,
                        drive_id: str = "") -> dict:
    """Cria o PDF na pasta, ou grava uma revisão nova do que já está lá.

    Atualizar em vez de criar outro item é o que preserva o link: o painel e a
    planilha guardam o id do Drive, e trocar o id a cada registro novo quebraria
    todo link já divulgado. O histórico não se perde porque cada atualização
    vira uma revisão, marcada com keepRevisionForever (sem isso o Drive descarta
    revisão antiga sozinho depois de 30 dias).
    """
    from googleapiclient.http import MediaIoBaseUpload

    midia = MediaIoBaseUpload(io.BytesIO(dados), mimetype="application/pdf",
                              resumable=len(dados) > 5_000_000)
    if drive_id:
        arq = com_retentativa(
            f"atualização de {nome}",
            lambda: drive.files().update(
                fileId=drive_id, media_body=midia, body={"name": nome},
                supportsAllDrives=True, keepRevisionForever=True,
                fields="id,webViewLink",
            ).execute(),
        )
    else:
        arq = com_retentativa(
            f"envio de {nome}",
            lambda: drive.files().create(
                body={"name": nome, "parents": [pasta]}, media_body=midia,
                supportsAllDrives=True, keepRevisionForever=True,
                fields="id,webViewLink",
            ).execute(),
        )
    liberar_leitura(drive, arq["id"])
    return arq


def achar_por_nome(drive, nome: str, pasta: str) -> str:
    """Id de um PDF já enviado com esse nome, para não duplicar quando a
    planilha perdeu a linha mas o arquivo está lá."""
    seguro = nome.replace("'", "\\'")
    achou = com_retentativa(
        f"busca de '{nome}'",
        lambda: drive.files().list(
            q=f"'{pasta}' in parents and name='{seguro}' and trashed=false",
            includeItemsFromAllDrives=True, supportsAllDrives=True,
            fields="files(id)", pageSize=5,
        ).execute(),
    ).get("files", [])
    return achou[0]["id"] if achou else ""


# ── Fila ──────────────────────────────────────────────────────────────────────

def montar_fila(base: pd.DataFrame, salvos: pd.DataFrame, cargo: str,
                uf: str, sq: str, forcar: bool) -> pd.DataFrame:
    """Quem precisa ser baixado nesta execução."""
    b = base.copy()
    b["SQ_CANDIDATO"] = b["SQ_CANDIDATO"].astype(str).str.strip()
    b["LINK_PLANO"] = b["LINK_PLANO"].astype(str).str.strip()
    b = b[b["LINK_PLANO"].str.startswith("http")]
    if "ANO_ELEICAO" in b.columns:
        b = b[b["ANO_ELEICAO"].astype(str).str.strip() == ANO]

    cargos = b["DS_CARGO"].astype(str).str.strip().str.upper()
    if cargo.upper() == "GOVERNADOR":
        b = b[cargos == "GOVERNADOR"]
    elif cargo.upper() == "PRESIDENTE":
        b = b[cargos == "PRESIDENTE"]
    else:
        # VICE-GOVERNADOR entra porque o processar_planos com --cargo TODOS não
        # filtra cargo nenhum: sem ele o espelho parava em 203 dos 207 planos e
        # os 4 vices (todos do MISSÃO, número 14, em ES, CE, MA e PE) ficavam
        # dependendo do DivulgaCand. O documento deles é outro id de arquivo, e
        # não o mesmo PDF do titular da chapa.
        b = b[cargos.isin(["GOVERNADOR", "PRESIDENTE", "VICE-GOVERNADOR"])]

    if uf:
        b = b[b["SG_UF"].astype(str).str.strip().str.upper() == uf.upper()]
    if sq:
        b = b[b["SQ_CANDIDATO"] == sq.strip()]

    # Um registro por candidato. A base repete a linha quando o TSE reemite o
    # cadastro, e o que interessa é o último link.
    b = b.drop_duplicates("SQ_CANDIDATO", keep="last")

    if forcar or salvos.empty:
        return b

    link_salvo = {str(r["sq_candidato"]).strip(): str(r.get("link", "")).strip()
                  for _, r in salvos.iterrows()}
    tem_id = {str(r["sq_candidato"]).strip(): bool(str(r.get("drive_id", "")).strip())
              for _, r in salvos.iterrows()}
    pendente = b["SQ_CANDIDATO"].map(
        lambda s: link_salvo.get(s, "") == "" or not tem_id.get(s, False)
        or link_salvo.get(s) != b.loc[b["SQ_CANDIDATO"] == s, "LINK_PLANO"].iloc[0]
    )
    return b[pendente]


# ── Uso pelo analisador ───────────────────────────────────────────────────────

def registrar_espelho(sh=None, drive=None, caminho_credenciais: str = "") -> int:
    """Liga o espelho como primeira fonte de `analise_planos.baixar_plano`.

    Chamado pelo processar_planos antes de percorrer a fila. Devolve quantos
    planos o espelho cobre, para o log dizer de quanto o TSE deixou de ser
    necessário. Falha silenciosa de propósito: se a aba não existir ou o Drive
    recusar, o analisador segue baixando do TSE como antes.
    """
    import analise_planos

    try:
        if sh is None or drive is None:
            gc, drive = clientes_google(caminho_credenciais)
            sh = gc.open_by_key(os.environ["SPREADSHEET_ID_TSE"])
        salvos = ler_aba(sh, ABA_ARQUIVOS)
        if salvos.empty or "link" not in salvos.columns:
            return 0
        por_link = {}
        for _, r in salvos.iterrows():
            link = str(r.get("link", "")).strip()
            fid = str(r.get("drive_id", "")).strip()
            if link and fid:
                por_link[link] = fid
    except Exception as e:
        log(f"espelho indisponível ({str(e)[:120]}); seguindo direto no TSE")
        return 0

    def _do_drive(url: str) -> bytes | None:
        fid = por_link.get(str(url).strip())
        if not fid:
            return None
        try:
            return com_retentativa(
                f"leitura do espelho {fid}",
                lambda: drive.files().get_media(fileId=fid,
                                                supportsAllDrives=True).execute(),
            )
        except Exception as e:
            log(f"    espelho {fid} não abriu ({str(e)[:100]}); tentando o TSE")
            return None

    # O módulo entra em sys.modules com dois nomes: quem põe outros/ no
    # sys.path e importa por caminho (processar_planos, auditar_planos) recebe
    # "analise_planos", e quem importa como pacote (aferir_acao) recebe
    # "outros.analise_planos". São dois objetos com globais separados, então
    # marcar um só deixa o outro baixando do TSE. Foi o que fez a aferição de
    # 01/09/2026 cair nos 19 planos em 403 com o espelho ligado no log.
    for nome in ("analise_planos", "outros.analise_planos"):
        mod = sys.modules.get(nome)
        if mod is not None:
            mod.FONTE_ESPELHO = _do_drive
    return len(por_link)


# ── Execução ──────────────────────────────────────────────────────────────────

def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--cargo", default="TODOS",
                    choices=["TODOS", "GOVERNADOR", "PRESIDENTE"])
    ap.add_argument("--uf", default="", help="restringe a uma UF")
    ap.add_argument("--sq", default="", help="restringe a um SQ_CANDIDATO")
    ap.add_argument("--limite", type=int, default=0)
    ap.add_argument("--forcar", action="store_true",
                    help="rebaixa tudo e compara pelo sha256")
    ap.add_argument("--publicar", action="store_true",
                    help="só garante leitura por link em tudo o que já está no Drive")
    ap.add_argument("--credenciais", default="")
    args = ap.parse_args()

    sheet_id = os.getenv("SPREADSHEET_ID_TSE", "").strip()
    if not sheet_id:
        raise SystemExit("Defina SPREADSHEET_ID_TSE.")
    raiz = os.getenv("PASTA_DRIVE_PLANOS", "").strip() or PASTA_PADRAO

    gc, drive = clientes_google(args.credenciais)
    sh = gc.open_by_key(sheet_id)

    base = ler_aba(sh, ABA_BASE)
    if base.empty or "LINK_PLANO" not in base.columns:
        raise SystemExit(f"A aba {ABA_BASE} está vazia ou sem LINK_PLANO.")
    salvos = ler_aba(sh, ABA_ARQUIVOS)
    ws = aba_ou_cria(sh, ABA_ARQUIVOS, COLS)

    if args.publicar:
        # Conserto de quem foi espelhado antes de a liberação existir. É
        # idempotente: a API devolve a mesma permissão `anyoneWithLink` quando
        # ela já está lá, então rodar de novo não custa nada além das chamadas.
        if salvos.empty:
            log("nada espelhado ainda.")
            return 0
        ids = [str(r.get("drive_id", "")).strip() for _, r in salvos.iterrows()]
        ids = [i for i in ids if i]
        ok = 0
        for n, fid in enumerate(ids, start=1):
            if liberar_leitura(drive, fid):
                ok += 1
            if n % 25 == 0:
                log(f"  {n}/{len(ids)}")
        log(f"leitura por link garantida em {ok} de {len(ids)}.")
        return 0

    fila = montar_fila(base, salvos, args.cargo, args.uf, args.sq, args.forcar)
    if args.limite:
        fila = fila.head(args.limite)

    já = 0 if salvos.empty else int(salvos["sq_candidato"].nunique())
    log(f"espelho em {NOME_PASTA_RAIZ} · {já} planos já copiados · "
        f"{len(fila)} na fila desta rodada")
    if fila.empty:
        return 0

    # Estado atual por candidato, para saber a versão anterior e o item do Drive
    # que deve receber a revisão nova.
    anterior = {}
    if not salvos.empty:
        for _, r in salvos.iterrows():
            anterior[str(r["sq_candidato"]).strip()] = dict(r)

    pasta_raiz = garantir_pasta(drive, NOME_PASTA_RAIZ, raiz)
    pastas_uf: dict[str, str] = {}

    novas: dict[str, dict] = {}
    erros: list[str] = []
    total = len(fila)

    def gravar():
        """Reescreve a aba com o estado consolidado. A aba é pequena (uma linha
        por candidato) e reescrever inteiro evita a dedup por posição, que já deu
        problema nas outras abas quando a ordem das linhas mudou."""
        if not novas:
            return
        consolidado = dict(anterior)
        consolidado.update(novas)
        linhas = [[str(consolidado[s].get(c, "")) for c in COLS]
                  for s in sorted(consolidado, key=lambda s: (
                      consolidado[s].get("uf", ""), consolidado[s].get("candidato", "")))]
        com_retentativa("gravação da aba", lambda: ws.batch_clear(["A2:Z"]))
        if linhas:
            com_retentativa("gravação da aba",
                            lambda: ws.update(values=linhas, range_name="A2"))
        anterior.update(novas)
        novas.clear()

    for i, (_, r) in enumerate(fila.iterrows(), start=1):
        sq = str(r["SQ_CANDIDATO"]).strip()
        nome = str(r.get("NM_URNA_CANDIDATO") or r.get("NM_CANDIDATO") or "").strip()
        partido = str(r.get("SG_PARTIDO", "")).strip()
        cargo_c = str(r.get("DS_CARGO", "")).strip().upper()
        uf_c = str(r.get("SG_UF", "")).strip().upper()
        if cargo_c == "PRESIDENTE":
            uf_c = "BR"
        link = str(r["LINK_PLANO"]).strip()

        log(f"[{i}/{total}] {uf_c} · {nome} ({partido})...")
        try:
            dados = baixar_plano(link)
        except (PlanoIndisponivel, PlanoIlegivel) as e:
            erros.append(f"{uf_c} · {nome}: {e}")
            log(f"    não baixou: {e}")
            time.sleep(PAUSA_ENTRE_PLANOS)
            continue
        except Exception as e:
            erros.append(f"{uf_c} · {nome}: {type(e).__name__}: {e}")
            log(f"    erro: {type(e).__name__}: {e}")
            time.sleep(PAUSA_ENTRE_PLANOS)
            continue

        if not dados or not dados[:1024].lstrip().startswith(b"%PDF"):
            erros.append(f"{uf_c} · {nome}: resposta não é PDF ({len(dados)} bytes)")
            log("    resposta não é PDF (bloqueio do TSE); fica para a próxima rodada")
            time.sleep(PAUSA_ENTRE_PLANOS)
            continue

        sha = hashlib.sha256(dados).hexdigest()
        ant = anterior.get(sq, {})
        if sha == str(ant.get("sha256", "")).strip() and str(ant.get("drive_id", "")).strip():
            # Acontece com --forcar e quando o TSE reemite o link sem trocar o
            # arquivo. Atualiza só o link, sem gastar uma revisão do Drive.
            novas[sq] = dict(ant, link=link, espelhado_em=agora())
            log("    conteúdo idêntico ao que já está no Drive; só atualizei o link")
            time.sleep(PAUSA_ENTRE_PLANOS)
            continue

        if uf_c not in pastas_uf:
            pastas_uf[uf_c] = garantir_pasta(drive, uf_c, pasta_raiz)
        arquivo = nome_arquivo(uf_c, nome, partido, sq)
        fid = str(ant.get("drive_id", "")).strip() or achar_por_nome(
            drive, arquivo, pastas_uf[uf_c])

        try:
            enviado = enviar_ou_atualizar(drive, dados, arquivo, pastas_uf[uf_c], fid)
        except Exception as e:
            erros.append(f"{uf_c} · {nome}: envio ao Drive: {type(e).__name__}: {e}")
            log(f"    envio ao Drive falhou: {type(e).__name__}: {e}")
            time.sleep(PAUSA_ENTRE_PLANOS)
            continue

        versao = int(str(ant.get("versao_arquivo", "0") or 0) or 0) + 1
        novas[sq] = {
            "ano": ANO, "sq_candidato": sq, "candidato": nome, "partido": partido,
            "uf": uf_c, "cargo": cargo_c, "link": link,
            "drive_id": enviado["id"], "drive_link": enviado.get("webViewLink", ""),
            "sha256": sha, "bytes": str(len(dados)),
            "versao_arquivo": str(versao), "espelhado_em": agora(),
        }
        log(f"    {'revisão ' + str(versao) if fid else 'copiado'} · "
            f"{len(dados) / 1e6:.1f} MB")

        if len(novas) >= LOTE:
            gravar()
        time.sleep(PAUSA_ENTRE_PLANOS)

    gravar()
    log(f"\nfim. {total - len(erros)} de {total} no Drive.")
    if erros:
        log(f"{len(erros)} ficaram para a próxima rodada:")
        for e in erros[:40]:
            log(f"  - {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
