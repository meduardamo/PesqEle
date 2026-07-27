"""
Coleta de candidaturas e planos de governo no TSE.

Três fontes:
  - base oficial (CSV consulta_cand), atualizada 1x/dia
  - candidaturas em tempo real pela API do DivulgaCand (atualiza a cada 60 min)
  - PDFs dos planos de governo (só presidente e governador entregam)

Salva as tabelas no Google Sheets se SPREADSHEET_ID_TSE estiver definido.
Autenticação pelo arquivo credentials.json (service account).
"""

import io
import os
import time
import zipfile
from pathlib import Path

import gspread
import requests
import pandas as pd

ANO = 2026

API = "https://divulgacandcontas.tse.jus.br/divulga/rest/v1"
DOC = "https://divulgacandcontas.tse.jus.br/divulga/rest/arquivo/doc"
CDN = "https://cdn.tse.jus.br/estatistica/sead/odsele/consulta_cand"

HEADERS = {"User-Agent": "Mozilla/5.0"}
PAUSA = 0.3   # o TSE bloqueia se as requisições vierem rápido demais
PASTA = Path("dados_tse")

UFS = ['AC','AL','AP','AM','BA','CE','DF','ES','GO','MA','MT','MS','MG','PA','PB',
       'PR','PE','PI','RJ','RN','RS','RO','RR','SC','SP','SE','TO']

# códigos de cargo do TSE
CARGOS = {1: 'PRESIDENTE', 2: 'VICE-PRESIDENTE', 3: 'GOVERNADOR', 4: 'VICE-GOVERNADOR',
          5: 'SENADOR', 6: 'DEPUTADO FEDERAL', 7: 'DEPUTADO ESTADUAL',
          8: 'DEPUTADO DISTRITAL', 9: '1º SUPLENTE', 10: '2º SUPLENTE'}

# O vice NÃO vem aninhado no titular na listagem: é candidatura própria, com código
# de cargo separado. Buscando só 1/3/5 a gente traria Dr. Furlan e perderia Luciana
# Gurgel, que é a outra metade da mesma chapa. O mesmo vale pros suplentes de senador.
CARGOS_TITULARES = (1, 3, 5)
CARGOS_VINCULADOS = (2, 4, 9, 10)
CARGOS_PROPORCIONAIS = (6, 7, 8)   # deputado federal, estadual, distrital
CARGOS_PADRAO = CARGOS_TITULARES + CARGOS_VINCULADOS + CARGOS_PROPORCIONAIS


def _get(url):
    # 5 tentativas com backoff: a API do TSE tem blips curtos (2 rodadas
    # falharam em 22/07 com 3 tentativas de 1s; a rodada seguinte passava).
    for tentativa in range(1, 6):
        try:
            r = requests.get(url, headers=HEADERS, timeout=40)
            r.raise_for_status()
            time.sleep(PAUSA)
            return r.json()
        except Exception:
            time.sleep(2 * tentativa)
    return None


def cod_eleicao(ano, abrangencia='F'):
    # cada eleição tem um código próprio (muda a cada ano); 'F' geral, 'M' municipal
    eleicoes = _get(f"{API}/eleicao/ordinarias")
    if eleicoes is None:
        # API fora do ar mesmo após as 5 tentativas (blip longo do TSE, como
        # em 23/07): sai limpo, a próxima rodada do schedule cobre. Erro de
        # verdade é só quando a API responde SEM a eleição (ramo abaixo).
        print("API do TSE indisponível; encerrando sem erro, próxima rodada cobre")
        raise SystemExit(0)
    for e in eleicoes:
        if e.get('ano') == ano and e.get('tipoAbrangencia') == abrangencia:
            return e['id']
    raise ValueError(f"Eleição {ano}/{abrangencia} não encontrada")


def baixar_base_oficial(ano=ANO):
    """Baixa o consulta_cand (zip) e extrai o CSV nacional. Só rebaixa se mudou.

    Devolve None quando o arquivo ainda não existe. O TSE só publica essa base
    depois que o período de registro avança; no começo do período a API já tem
    candidatura e o CSV ainda não saiu. Antes isso derrubava a rodada inteira e
    a coleta pela API, que estava funcionando, se perdia junto.
    """
    PASTA.mkdir(parents=True, exist_ok=True)
    url = f"{CDN}/consulta_cand_{ano}.zip"
    estado = PASTA / f"_ultimo_{ano}.txt"

    # O CDN do TSE derruba a conexao (RemoteDisconnected) em vez de responder 404
    # quando o arquivo do ano ainda nao existe, entao tentativa unica nao distingue
    # "nao publicado" de "instabilidade". Tenta algumas vezes antes de desistir.
    atual = None
    for tentativa in range(1, 4):
        try:
            head = requests.head(url, headers=HEADERS, timeout=30)
            if head.status_code == 200:
                atual = head.headers.get("Last-Modified")
                break
            print(f"base oficial: HTTP {head.status_code} (tentativa {tentativa}/3)")
        except requests.RequestException as e:
            print(f"base oficial: {str(e)[:70]} (tentativa {tentativa}/3)")
        time.sleep(2 * tentativa)
    else:
        print(f"consulta_cand_{ano}.zip ainda não publicado pelo TSE; seguindo só com a API")
        return None
    if estado.exists() and estado.read_text() == (atual or ""):
        print("base sem alteração desde o último download")
        return PASTA / f"consulta_cand_{ano}_BRASIL.csv"

    try:
        r = requests.get(url, headers=HEADERS, timeout=120)
        r.raise_for_status()
    except requests.RequestException as e:
        print(f"falha ao baixar a base oficial ({str(e)[:80]}); seguindo só com a API")
        return None
    alvo = f"consulta_cand_{ano}_BRASIL.csv"
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extract(alvo, PASTA)
    estado.write_text(atual or "")
    print(f"baixado: {alvo}")
    return PASTA / alvo


def carregar_base(csv_path):
    # o CSV vem com todos os cargos — mantemos todos (majoritários + proporcionais)
    return pd.read_csv(csv_path, encoding="ISO-8859-1", sep=";", low_memory=False)


def consolidar(df_api, csv_path):
    """Base-mãe: junta o CSV oficial (perfil completo) com a tabela da API
    (link do plano + situação em tempo real), pelo SQ_CANDIDATO.
    O CSV é a espinha; a API só acrescenta o que ele não tem."""
    base = carregar_base(csv_path)
    _extra_cols = [c for c in ("sq_candidato", "link_plano", "situacao", "foto_url")
                   if c in df_api.columns]
    extra = (df_api[_extra_cols]
             .rename(columns={"sq_candidato": "SQ_CANDIDATO",
                              "link_plano": "LINK_PLANO",
                              "situacao": "SITUACAO_TEMPO_REAL",
                              "foto_url": "FOTO_URL"}))
    return base.merge(extra, on="SQ_CANDIDATO", how="left")


def extrair_candidaturas(ano=ANO, cargos=CARGOS_PADRAO, enriquecer=True):
    """Lista candidaturas pela API. enriquecer busca o detalhe (link do plano de
    governo) de cada um — uma chamada por candidato, mais lento. Vale a pena só no
    majoritário; os proporcionais (deputados) NÃO são enriquecidos aqui (seriam
    milhares de chamadas por rodada) — e o perfil completo (partido, gênero, raça)
    de todo mundo vem da base_dadosabertos na consolidação."""
    eleicao = cod_eleicao(ano)
    linhas = []
    for cargo in cargos:
        # Presidente e vice-presidente são nacionais (UE = BR); dep. distrital só no DF;
        # o resto é por UF.
        ues = ['BR'] if cargo in (1, 2) else (['DF'] if cargo == 8 else UFS)
        for ue in ues:
            d = _get(f"{API}/candidatura/listar/{ano}/{ue}/{eleicao}/{cargo}/candidatos")
            cands = (d or {}).get('candidatos', [])
            for c in cands:
                row = {"ano": ano, "cargo": CARGOS[cargo], "ue": ue,
                       "sq_candidato": c['id'], "numero": c.get('numero'),
                       "nome_urna": c.get('nomeUrna'),
                       "nome_completo": c.get('nomeCompleto'),
                       "partido_listagem": (c.get('partido') or {}).get('sigla'),
                       "situacao": c.get('descricaoSituacao'),
                       "totalizacao": c.get('descricaoTotalizacao'),
                       "coligacao_federacao": c.get('nomeColigacao')}
                if enriquecer and cargo not in CARGOS_PROPORCIONAIS:
                    # O detalhe também traz partido/gênero/raça, mas esses
                    # microdados já vêm completos na base_dadosabertos; daqui
                    # só interessa o que ela não tem: o link do plano.
                    det = _get(f"{API}/candidatura/buscar/{ano}/{ue}/{eleicao}/candidato/{c['id']}")
                    if det:
                        # link do plano de governo (só presidente/governador têm)
                        idarq = _id_plano(det)
                        row["link_plano"] = f"{DOC}/{idarq}" if idarq else None
                        # A foto sai do MESMO detalhe, sem chamada extra. O TSE
                        # marca fotoUrlPublicavel quando pode ser exibida.
                        row["foto_url"] = (det.get("fotoUrl") or ""
                                           if det.get("fotoUrlPublicavel") else "")
                linhas.append(row)
            print(f"{CARGOS[cargo]} {ue}: {len(cands)}")
    return pd.DataFrame(linhas)


def extrair_chapas(ano=ANO):
    """Uma linha por chapa majoritária registrada: titular + cada vice/suplente.

    O vínculo entre titular e vice só é confiável DE CIMA PRA BAIXO: o detalhe do
    titular traz a lista `vices`, enquanto a listagem do vice vem com
    idCandidatoSuperior nulo (conferido na chapa do AP em 21/07/2026, a primeira
    registrada do país). Por isso percorre titular por titular.

    Os campos do vice usam outra convenção de nome (nm_URNA, sg_PARTIDO,
    sq_CANDIDATO) porque vêm de outra tabela do TSE, não do mesmo serializer.
    """
    eleicao = cod_eleicao(ano)
    linhas = []
    for cargo in CARGOS_TITULARES:
        ues = ['BR'] if cargo in (1, 2) else UFS
        for ue in ues:
            d = _get(f"{API}/candidatura/listar/{ano}/{ue}/{eleicao}/{cargo}/candidatos")
            for c in (d or {}).get('candidatos', []):
                det = _get(f"{API}/candidatura/buscar/{ano}/{ue}/{eleicao}/candidato/{c['id']}") or {}
                base = {
                    "ano": ano, "uf": ue, "cargo": CARGOS[cargo],
                    "titular": c.get('nomeUrna'),
                    "titular_nome_completo": c.get('nomeCompleto'),
                    "titular_partido": (c.get('partido') or {}).get('sigla'),
                    "numero": c.get('numero'),
                    "coligacao_federacao": c.get('nomeColigacao'),
                    "situacao": c.get('descricaoSituacao'),
                    "totalizacao": c.get('descricaoTotalizacao'),
                    "sq_titular": c.get('id'),
                    "link_plano": (lambda i: f"{DOC}/{i}" if i else None)(_id_plano(det)),
                }
                vices = det.get('vices') or []
                if not vices:
                    # Chapa registrada sem vice ainda, ou cargo que não tem vice
                    # (senador tem suplente, que aparece aqui do mesmo jeito).
                    linhas.append({**base, "vinculado": None, "vinculado_cargo": None,
                                   "vinculado_partido": None, "sq_vinculado": None,
                                   "vinculado_situacao": None})
                    continue
                for v in vices:
                    linhas.append({**base,
                                   "vinculado": v.get('nm_URNA') or v.get('nm_CANDIDATO'),
                                   "vinculado_cargo": v.get('ds_CARGO'),
                                   "vinculado_partido": v.get('sg_PARTIDO'),
                                   "sq_vinculado": v.get('sq_CANDIDATO'),
                                   "vinculado_situacao": v.get('descricaoTotalizacao')})
            print(f"chapas {CARGOS[cargo]} {ue}: {len([l for l in linhas if l['uf']==ue and l['cargo']==CARGOS[cargo]])}")
    return pd.DataFrame(linhas)


def _id_plano(detalhe):
    # codTipo 5 = proposta de governo (os outros arquivos são certidões, bens etc.)
    for f in detalhe.get('arquivos') or []:
        if str(f.get('codTipo')) == "5":
            return f['idArquivo']
    return None


CREDS_FILE = Path("credentials.json")
SHEETS_ID  = os.getenv("SPREADSHEET_ID_TSE", "")


def salvar_no_sheets(df, aba):
    """Sobrescreve a aba no Google Sheets com os dados do DataFrame."""
    gc = gspread.service_account(filename=str(CREDS_FILE))
    sh = gc.open_by_key(SHEETS_ID)
    try:
        ws = sh.worksheet(aba)
    except gspread.WorksheetNotFound:
        ws = sh.add_worksheet(title=aba, rows=len(df) + 1, cols=len(df.columns))
    ws.clear()
    # converte NaN para string vazia para evitar erros de serialização
    valores = df.where(df.notna(), "").astype(str)
    ws.update([valores.columns.tolist()] + valores.values.tolist())
    print(f"Sheets atualizado: aba '{aba}' ({len(df)} linhas)")


if __name__ == '__main__':
    df = extrair_candidaturas()
    print(f"candidaturas: {len(df)} linha(s)")
    if len(df):
        salvar_no_sheets(df, "candidaturas_divulgacand")

    chapas = extrair_chapas()
    print(f"chapas: {len(chapas)} linha(s)")
    if len(chapas):
        salvar_no_sheets(chapas, "chapas_divulgacand")

    # A base oficial só sai depois que o período de registro avança. Enquanto não
    # existe, a coleta pela API já vale por si e não faz sentido derrubar a rodada.
    csv = baixar_base_oficial()
    if csv and len(df):
        base = consolidar(df, csv)
        print(f"base consolidada: {base.shape[0]} linhas")
        salvar_no_sheets(base, "base_dadosabertos")
    else:
        print("base_dadosabertos não gerada nesta rodada")
