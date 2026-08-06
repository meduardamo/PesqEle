"""
Coleta dados do PollingData pela API do portal, em vez de raspar a pagina.

Contrato extraido do bundle do proprio site (flex.pollingdata.com.br):
  base   https://api.pollingdata.com.br/api/pro/v1
  auth   header "Authorization: Bearer <token>"; o portal guarda esse token em
         localStorage.authToken depois do login
  rotas  /auth/me
         /polls/scenarios-overview   ano, cargo, turno, uf
         /polls/resultado            ano, cargo, turno, uf [, cenario, url]
         /polls/pesquisas            ano [, ...]
         /polls/candidates
         /pdvoto/upcoming-polls

NAO FAZ LOGIN DE PROPOSITO. O portal mantem uma chave "session_replaced", sinal
de que so aceita uma sessao por conta: um robo logando a cada rodada derrubaria
a sessao do navegador da pessoa varias vezes por dia. O token vem pronto por
variavel de ambiente; quando expirar, o script avisa em vez de tentar renovar.

Uso:
  python -m outros.polling_automatizado --explorar            # mostra o formato da resposta
  python -m outros.polling_automatizado --explorar --uf PE --cargo Governador
  python -m outros.polling_automatizado                       # coleta e grava na planilha
  python -m outros.polling_automatizado --dry-run

Secrets/env:
  POLLINGDATA_TOKEN            token do portal (obrigatorio)
  GOOGLE_CREDENTIALS_JSON      credencial da service account
  SPREADSHEET_ID_POLLING_API   planilha de destino
  POLLING_API_ABA              aba de destino (padrao "pollingdata_api")
"""

import argparse
import json
import os
import sys
import time
from datetime import datetime, timedelta, timezone

import requests

BRT = timezone(timedelta(hours=-3))
BASE = os.getenv("POLLINGDATA_API_BASE", "https://api.pollingdata.com.br/api/pro/v1")
ABA_PADRAO = "pollingdata_api"
ANO = int(os.getenv("POLLING_API_ANO", "2026"))

# O portal usa esses rotulos nos parametros (vistos nas chamadas do bundle).
CARGOS = ["Presidente", "Governador", "Senador"]
UFS = ["AC", "AL", "AP", "AM", "BA", "CE", "DF", "ES", "GO", "MA", "MT", "MS", "MG",
       "PA", "PB", "PR", "PE", "PI", "RJ", "RN", "RS", "RO", "RR", "SC", "SP", "SE", "TO"]

PAUSA = float(os.getenv("POLLING_API_PAUSA", "0.4"))   # respeita o serviço deles

# O que coletar. Senador nao entra no 2o turno: no Brasil a eleicao pro Senado e
# majoritaria simples, decidida numa unica votacao. Presidente vem em BR (nacional)
# e tambem por UF, porque instituto testa presidenciavel dentro do estado.
COLETA = [
    ("Governador", UFS, [1, 2]),
    ("Senador", UFS, [1]),
    ("Presidente", ["BR"] + UFS, [1, 2]),
]


def _sessao():
    token = os.getenv("POLLINGDATA_TOKEN", "").strip()
    if not token:
        raise SystemExit(
            "POLLINGDATA_TOKEN ausente.\n"
            "Pegue o valor em localStorage.authToken no portal logado e guarde como\n"
            "secret do GitHub. Não coloque a senha: o script não faz login de propósito."
        )
    s = requests.Session()
    s.headers.update({
        "Authorization": f"Bearer {token}",
        "Accept": "application/json",
        "User-Agent": "eixo-eleicoes/1.0 (coleta interna)",
    })
    return s


def _get(s, caminho, **params):
    url = f"{BASE}{caminho}"
    for tentativa in range(1, 4):
        r = s.get(url, params={k: v for k, v in params.items() if v is not None}, timeout=40)
        if r.status_code == 401:
            raise SystemExit(
                "401 do PollingData: o token expirou ou foi invalidado.\n"
                "Abra o portal logada, copie de novo localStorage.authToken e atualize o secret."
            )
        if r.status_code == 429 or r.status_code >= 500:
            espera = 2 * tentativa
            print(f"  HTTP {r.status_code} em {caminho}; nova tentativa em {espera}s")
            time.sleep(espera)
            continue
        r.raise_for_status()
        return r.json()
    print(f"  [erro] {caminho} não respondeu após 3 tentativas")
    return None


def _formato(valor, prefixo="", profundidade=0):
    """Descreve o formato do JSON sem despejar o conteudo inteiro."""
    ident = "  " * (profundidade + 1)
    if isinstance(valor, dict):
        print(f"{ident}{prefixo}objeto com {len(valor)} chave(s): {', '.join(list(valor)[:12])}")
        if profundidade < 2:
            for k, v in list(valor.items())[:6]:
                _formato(v, f"{k}: ", profundidade + 1)
    elif isinstance(valor, list):
        print(f"{ident}{prefixo}lista com {len(valor)} item(ns)")
        if valor and profundidade < 2:
            _formato(valor[0], "[0] ", profundidade + 1)
    else:
        amostra = str(valor)
        print(f"{ident}{prefixo}{type(valor).__name__} = {amostra[:70]}")


def explorar(uf, cargo, turno):
    """Mostra o formato de cada rota, pra modelar as colunas com dado real na mao."""
    s = _sessao()
    print("=== /auth/me ===")
    _formato(_get(s, "/auth/me"))
    for caminho, params in (
        ("/polls/scenarios-overview", {"ano": ANO, "cargo": cargo, "turno": turno, "uf": uf}),
        ("/polls/resultado", {"ano": ANO, "cargo": cargo, "turno": turno, "uf": uf}),
        ("/polls/pesquisas", {"ano": ANO}),
        ("/pdvoto/upcoming-polls", {}),
    ):
        print(f"\n=== {caminho}  {params} ===")
        try:
            _formato(_get(s, caminho, **params))
        except SystemExit:
            raise
        except Exception as e:
            print(f"  [erro] {str(e)[:160]}")
        time.sleep(PAUSA)


def _achatar(obj, prefixo=""):
    """Transforma dicionario aninhado em colunas planas."""
    saida = {}
    for k, v in (obj or {}).items():
        chave = f"{prefixo}{k}"
        if isinstance(v, dict):
            saida.update(_achatar(v, f"{chave}."))
        elif isinstance(v, list):
            saida[chave] = json.dumps(v, ensure_ascii=False)[:400]
        else:
            saida[chave] = v
    return saida


def _linhas_de(resposta, contexto):
    """Aceita lista de objetos, objeto com lista dentro, ou objeto unico."""
    if resposta is None:
        return []
    dados = resposta
    if isinstance(dados, dict):
        for chave in ("data", "results", "items", "pesquisas", "resultados", "scenarios"):
            if isinstance(dados.get(chave), list):
                dados = dados[chave]
                break
        else:
            dados = [dados]
    if not isinstance(dados, list):
        return []
    return [{**contexto, **_achatar(d)} for d in dados if isinstance(d, dict)]


def _cenarios_de(s, ano, cargo, uf, turno):
    """Lista os cenarios daquela combinacao, em vez de adivinhar nomes.

    O portal expoe cenario tanto por slug na URL ("t1_lula-flavio-sem-bolsonaros")
    quanto por identificador interno, e a rota /polls/resultado aceita os dois
    (parametros "url" e "cenario"). Aqui a gente pega o que vier e repassa igual.
    """
    resp = _get(s, "/polls/scenarios-overview", ano=ano, cargo=cargo, turno=turno, uf=uf)
    if resp is None:
        return []
    itens = resp
    if isinstance(resp, dict):
        for chave in ("data", "scenarios", "cenarios", "results", "items"):
            if isinstance(resp.get(chave), list):
                itens = resp[chave]
                break
        else:
            itens = [resp]
    cenarios = []
    for it in itens if isinstance(itens, list) else []:
        if not isinstance(it, dict):
            continue
        slug = it.get("url") or it.get("slug") or it.get("cenario_url")
        ident = it.get("cenario") or it.get("id") or it.get("scenario_id") or it.get("nome")
        if slug or ident:
            cenarios.append({"url": slug, "cenario": ident,
                             "rotulo": it.get("nome") or it.get("titulo") or it.get("label") or ident or slug})
    return cenarios


def coletar(ano=ANO, so_uf=None, plano=None):
    s = _sessao()
    if _get(s, "/auth/me"):
        print("token válido.\n")
    linhas, sem_cenario, sem_dado = [], 0, 0

    for cargo, ufs_cargo, turnos in (plano or COLETA):
        alvos = [so_uf.upper()] if so_uf else ufs_cargo
        for uf in alvos:
            for turno in turnos:
                cenarios = _cenarios_de(s, ano, cargo, uf, turno)
                time.sleep(PAUSA)
                if not cenarios:
                    # Sem cenario listado, tenta a combinacao "crua": e assim que o
                    # portal responde quando ha um cenario unico e implicito.
                    cenarios = [{"url": None, "cenario": None, "rotulo": "(único)"}]
                    sem_cenario += 1
                achou = 0
                for c in cenarios:
                    ctx = {"ano": ano, "cargo": cargo, "uf": uf, "turno": turno,
                           "cenario": c.get("rotulo") or "",
                           "cenario_url": c.get("url") or "",
                           "coletado_em": datetime.now(BRT).strftime("%Y-%m-%d %H:%M")}
                    resp = _get(s, "/polls/resultado", ano=ano, cargo=cargo, turno=turno,
                                uf=uf, cenario=c.get("cenario"), url=c.get("url"))
                    novas = _linhas_de(resp, ctx)
                    linhas.extend(novas)
                    achou += len(novas)
                    time.sleep(PAUSA)
                if not achou:
                    sem_dado += 1
                print(f"  {cargo:<11} {uf:<3} t{turno}: {len(cenarios):>2} cenário(s), {achou:>3} linha(s)")

    print(f"\ntotal: {len(linhas)} linha(s)")
    print(f"  combinações sem cenário listado: {sem_cenario}")
    print(f"  combinações sem dado nenhum: {sem_dado}")
    return linhas


def salvar(linhas, dry_run=False):
    if not linhas:
        print("nada a gravar.")
        return
    colunas = []
    for l in linhas:
        for k in l:
            if k not in colunas:
                colunas.append(k)
    print(f"{len(colunas)} coluna(s): {', '.join(colunas[:12])}{'...' if len(colunas) > 12 else ''}")
    if dry_run:
        print("[dry-run] nada gravado. Amostra:")
        for l in linhas[:3]:
            print("  ", {k: l.get(k) for k in colunas[:8]})
        return

    import gspread
    from google.oauth2.service_account import Credentials
    from compartilhado.relatorios_sheets_utils import (autorizar_com_retry,
                                                       reescrever_aba)

    sheet_id = os.getenv("SPREADSHEET_ID_POLLING_API", "").strip()
    if not sheet_id:
        raise SystemExit("Defina SPREADSHEET_ID_POLLING_API.")
    info = json.loads(os.environ["GOOGLE_CREDENTIALS_JSON"])
    creds = Credentials.from_service_account_info(
        info, scopes=["https://www.googleapis.com/auth/spreadsheets"])
    sh = autorizar_com_retry(creds).open_by_key(sheet_id)
    nome = os.getenv("POLLING_API_ABA", ABA_PADRAO)
    try:
        ws = sh.worksheet(nome)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=nome, rows=len(linhas) + 10, cols=len(colunas))
    reescrever_aba(
        ws, [colunas] + [[str(l.get(c, "")) for c in colunas] for l in linhas],
        nome, value_input_option="USER_ENTERED")
    ws.freeze(rows=1)
    print(f"aba '{nome}' atualizada com {len(linhas)} linha(s).")


def main():
    p = argparse.ArgumentParser()
    p.add_argument("--explorar", action="store_true", help="mostra o formato das respostas")
    p.add_argument("--uf", default="AC")
    p.add_argument("--cargo", default="Governador", choices=CARGOS)
    p.add_argument("--turno", type=int, default=1)
    p.add_argument("--so-uf", help="coletar uma UF só")
    p.add_argument("--dry-run", action="store_true")
    args = p.parse_args()

    if args.explorar:
        explorar(args.uf, args.cargo, args.turno)
        return
    linhas = coletar(so_uf=args.so_uf)
    salvar(linhas, dry_run=args.dry_run)


if __name__ == "__main__":
    main()
