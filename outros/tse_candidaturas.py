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
import re
import time
import unicodedata
import zipfile
from pathlib import Path

import gspread
import requests
import pandas as pd

from compartilhado.relatorios_sheets_utils import reescrever_aba

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
    if isinstance(csv_path, pd.DataFrame):
        return csv_path.copy()
    return pd.read_csv(csv_path, encoding="ISO-8859-1", sep=";", low_memory=False)


def consolidar(df_api, csv_path, df_existente=None):
    """Base-mãe: junta o CSV oficial (perfil completo) com a tabela da API
    (link do plano + situação em tempo real), pelo SQ_CANDIDATO.
    O CSV é a espinha; a API só acrescenta o que ele não tem.

    Se df_existente tiver LINK_PLANO já preenchido na aba e a API não trouxer
    link para aquele candidato, preserva o link existente. Quando a API passar
    a devolver o link oficial, o da API tem prioridade.
    """
    base = carregar_base(csv_path)
    _extra_cols = [c for c in ("sq_candidato", "link_plano", "situacao", "foto_url")
                   if c in df_api.columns]
    extra = (df_api[_extra_cols]
             .rename(columns={"sq_candidato": "SQ_CANDIDATO",
                              "link_plano": "LINK_PLANO",
                              "situacao": "SITUACAO_TEMPO_REAL",
                              "foto_url": "FOTO_URL"}))
    res = base.merge(extra, on="SQ_CANDIDATO", how="left")

    if df_existente is not None and not df_existente.empty:
        if "SQ_CANDIDATO" in df_existente.columns and "LINK_PLANO" in df_existente.columns:
            existentes = df_existente[["SQ_CANDIDATO", "LINK_PLANO"]].copy()
            existentes["SQ_CANDIDATO"] = existentes["SQ_CANDIDATO"].astype(str).str.strip()
            existentes["LINK_PLANO"] = existentes["LINK_PLANO"].astype(str).str.strip()
            existentes = existentes[~existentes["LINK_PLANO"].isin(["", "None", "nan", "<NA>"])]
            mapa_existente = dict(zip(existentes["SQ_CANDIDATO"], existentes["LINK_PLANO"]))

            sem_link = (res["LINK_PLANO"].isna() |
                        res["LINK_PLANO"].astype(str).str.strip().isin(["", "None", "nan", "<NA>"]))
            sqs = res["SQ_CANDIDATO"].astype(str).str.strip()
            res.loc[sem_link, "LINK_PLANO"] = sqs[sem_link].map(mapa_existente).fillna("")
    return res


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
    """ID do plano de governo MAIS RECENTE.

    codTipo 5 = proposta de governo (os outros arquivos são certidões, bens etc.).

    O candidato pode substituir o plano durante o registro, e aí ficam dois
    arquivos tipo 5 (Lula e Pablo Marçal em 2022, por exemplo). Pegar o
    primeiro da lista dava certo só porque a API vinha ordenada; o idArquivo é
    sequencial, então o maior é o último enviado — isso não depende da ordem.
    """
    planos = [f for f in (detalhe.get('arquivos') or [])
              if str(f.get('codTipo')) == "5" and f.get('idArquivo')]
    if not planos:
        return None
    
    if len(planos) > 1:
        bad_words = ["requerimento", "manifesta", "peti", "juntada"]
        planos_bons = [f for f in planos if not any(w in f.get('nome', '').lower() for w in bad_words)]
        if planos_bons:
            planos = planos_bons

    return max(planos, key=lambda f: int(f['idArquivo']))['idArquivo']


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
    # converte NaN para string vazia para evitar erros de serialização
    valores = df.where(df.notna(), "").astype(str)
    reescrever_aba(ws, [valores.columns.tolist()] + valores.values.tolist(), aba)
    print(f"Sheets atualizado: aba '{aba}' ({len(df)} linhas)")


def ler_aba(aba):
    """Lê uma aba existente do Sheets como DataFrame (vazio se não existir)."""
    if not SHEETS_ID or not CREDS_FILE.exists():
        return pd.DataFrame()
    try:
        gc = gspread.service_account(filename=str(CREDS_FILE))
        sh = gc.open_by_key(SHEETS_ID)
        ws = sh.worksheet(aba)
        valores = ws.get_all_values()
        if not valores:
            return pd.DataFrame()
        return pd.DataFrame(valores[1:], columns=[c.strip() for c in valores[0]])
    except Exception as e:
        print(f"aviso: não foi possível ler aba '{aba}' ({str(e)[:80]})")
        return pd.DataFrame()


# ── Aba de deputado federal na planilha "Eleições 2026 - Nomes competitivos" ──
# Planilha de trabalho da equipe (não é a de coleta): a mesma que já tem as abas
# de governador/senador e de senadores com mandato até 2031. Aqui entra a lista
# de candidatos a deputado federal saída do DivulgaCand, com marcação de quem
# está tentando a reeleição.
#
# Os dois IDs vêm de secret, como o resto das planilhas do repo. Secret que não
# existe chega como string vazia no Actions, e é isso que o .strip() abaixo trata:
# sem ID a exportação avisa e sai, sem derrubar a coleta.
PAINEL_NOMES_ID = os.getenv("SPREADSHEET_ID_COMPETITIVOS", "").strip()
PAINEL_NOMES_ABA = "Candidatos Deputados Federais"

# Legislatura atual (57ª), de onde sai quem já é deputado federal hoje.
DEPUTADOS_ATUAIS_ID = os.getenv("SPREADSHEET_ID_DEPUTADOS_ATUAIS", "").strip()
DEPUTADOS_ATUAIS_ABA = "deputados_completo"

COLUNAS_PAINEL = ["Cargo", "Disputa", "UF", "Partido", "Candidato", "Status",
                  "Situação Atual"]

# Partido como a casa publica: por extenso quando é nome, sigla quando é sigla.
# Mesma regra do gerador-de-envios (alerta_pesquisa_core.rotulo_partido_alerta) e
# da aba de governador/senador desta planilha, que já traz "Republicanos".
PARTIDO_PUBLICADO = {
    "agir": "Agir", "avante": "Avante",
    "cid": "Cidadania", "cidadania": "Cidadania",
    "dem": "Democrata", "democratas": "Democrata",
    "missao": "Missão",
    "mob": "Mobiliza", "mobiliza": "Mobiliza",
    "novo": "Novo",
    "pode": "Podemos", "podemos": "Podemos",
    "rede": "Rede",
    "rep": "Republicanos", "republicanos": "Republicanos",
    "sd": "Solidariedade", "solidariedade": "Solidariedade",
    "uniao": "União", "uniao brasil": "União",
    "progressistas": "PP", "psol": "PSOL",
    # A API escreve PCDOB; a grafia do partido tem caixa própria.
    "pcdob": "PCdoB", "pc do b": "PCdoB",
}

# Título de urna abreviado, também como no gerador-de-envios.
TITULO_URNA = {
    "professor": "Prof.", "prof": "Prof.",
    "professora": "Profa.", "profa": "Profa.",
    "doutor": "Dr.", "dr": "Dr.",
    "doutora": "Dra.", "dra": "Dra.",
    "delegado": "Del.", "del": "Del.",
    "delegada": "Dela.", "dela": "Dela.",
    "coronel": "Cel.", "cel": "Cel.", "coronela": "Cel.",
}

PARTICULAS = {"de", "da", "do", "das", "dos", "e"}


def _chave(texto):
    """'PROFESSOR ÁLVARO' -> 'professor alvaro'. Sem acento e sem caixa, que é
    como dá pra cruzar nome do TSE com nome parlamentar da Câmara."""
    txt = unicodedata.normalize("NFKD", str(texto or ""))
    txt = "".join(c for c in txt if not unicodedata.combining(c)).lower()
    return re.sub(r"[^a-z0-9]+", " ", txt).strip()


def _partido_publicado(sigla):
    bruto = str(sigla or "").strip()
    return PARTIDO_PUBLICADO.get(_chave(bruto), bruto.upper())


def _nome_publicado(nome):
    """'PROFESSOR ÁLVARO DOMINGUES' -> 'Prof. Álvaro Domingues'.

    O nome de urna vem em caixa alta na API. Palavra entre parênteses fica como
    está: é apelido em sigla ('PAULO CÉSAR (PC)'), que Title Case estragaria.
    """
    texto = re.sub(r"\s+", " ", str(nome or "")).strip()
    if not texto:
        return ""
    palavras = []
    for i, palavra in enumerate(texto.split(" ")):
        if palavra.startswith("(") and palavra.endswith(")"):
            palavras.append(palavra)
            continue
        if i > 0 and palavra.lower() in PARTICULAS:
            palavras.append(palavra.lower())
            continue
        partes = re.split(r"([-'])", palavra)
        palavras.append("".join(
            (p[:1].upper() + p[1:].lower()) if p and p not in ("-", "'") else p
            for p in partes))
    if len(palavras) > 1:
        abreviado = TITULO_URNA.get(_chave(palavras[0]))
        if abreviado:
            palavras[0] = abreviado
    return " ".join(palavras)


def deputados_em_exercicio(gc=None):
    """{UF: {nome normalizado}} da legislatura atual, pra dizer quem está
    tentando a reeleição. Casa por UF junto com o nome: homônimo entre estados
    diferentes é comum o bastante pra não confiar só no nome."""
    gc = gc or gspread.service_account(filename=str(CREDS_FILE))
    linhas = (gc.open_by_key(DEPUTADOS_ATUAIS_ID)
                .worksheet(DEPUTADOS_ATUAIS_ABA).get_all_records())
    atuais = {}
    for linha in linhas:
        nome = _chave(linha.get("nome"))
        uf = str(linha.get("siglaUf") or "").strip().upper()
        if nome and uf:
            atuais.setdefault(uf, set()).add(nome)
    return atuais


def _preenchido_a_mao(ws):
    """O que a equipe já escreveu na aba, por (UF, candidato).

    Status é coluna de trabalho: quem acompanha a disputa é que escreve ali. Como
    a rodada seguinte reescreve a aba inteira, o que foi digitado tem que voltar
    junto — e o mesmo vale para qualquer coluna que a equipe acrescente à direita.

    Devolve (colunas extras, {(uf, nome): {coluna: valor}}). Cabeçalho fora do
    padrão devolve vazio: melhor não adivinhar de qual coluna o dado veio.
    """
    valores = ws.get_all_values()
    if not valores:
        return [], {}
    cabecalho = [c.strip() for c in valores[0]]
    if "UF" not in cabecalho or "Candidato" not in cabecalho:
        return [], {}
    i_uf, i_nome = cabecalho.index("UF"), cabecalho.index("Candidato")
    extras = [c for c in cabecalho if c and c not in COLUNAS_PAINEL]
    guardadas = {}
    for linha in valores[1:]:
        if len(linha) <= max(i_uf, i_nome):
            continue
        uf = linha[i_uf].strip().upper()
        nome = _chave(linha[i_nome])
        if uf and nome:
            guardadas[(uf, nome)] = dict(zip(cabecalho, linha))
    return extras, guardadas


def montar_deputados_federais(df, atuais, guardadas=None, extras=()):
    """Uma linha por candidatura a deputado federal, nas colunas da planilha de
    trabalho. Ordena por UF, partido e nome, que é como a equipe lê a lista.

    Status sai vazio na candidatura nova: é campo de preenchimento da equipe, não
    da coleta. Candidatura que já estava na aba mantém o que foi escrito lá.
    """
    guardadas = guardadas or {}
    dep = df[df["cargo"] == CARGOS[6]] if len(df) else df
    linhas = []
    for _, c in dep.iterrows():
        uf = str(c.get("ue") or "").strip().upper()
        nome = _nome_publicado(c.get("nome_urna"))
        anterior = guardadas.get((uf, _chave(nome)), {})
        nomes = {_chave(c.get("nome_urna")), _chave(c.get("nome_completo"))}
        reeleicao = bool(nomes & atuais.get(uf, set()))
        linha = {
            "Cargo": "Deputado Federal",
            "Disputa": f"Deputado Federal - {uf}",
            "UF": uf,
            "Partido": _partido_publicado(c.get("partido_listagem")),
            "Candidato": nome,
            "Status": anterior.get("Status", ""),
            "Situação Atual": "Reeleição" if reeleicao else "Novo",
        }
        for coluna in extras:
            linha[coluna] = anterior.get(coluna, "")
        linhas.append(linha)
    saida = pd.DataFrame(linhas, columns=COLUNAS_PAINEL + list(extras))
    if len(saida):
        saida = saida.sort_values(["UF", "Partido", "Candidato"],
                                  key=lambda s: s.map(_chave))
    return saida


def exportar_deputados_federais(df):
    """Reescreve a aba de deputado federal na planilha de trabalho da equipe.

    Sobrescreve a lista mesmo: a aba é espelho do DivulgaCand, e candidatura
    indeferida ou substituída tem que sumir daqui igual sumiu de lá. O que a
    equipe escreveu volta pela chave UF + candidato.
    """
    faltando = [nome for nome, valor in
                (("SPREADSHEET_ID_COMPETITIVOS", PAINEL_NOMES_ID),
                 ("SPREADSHEET_ID_DEPUTADOS_ATUAIS", DEPUTADOS_ATUAIS_ID))
                if not valor]
    if faltando:
        print(f"aba de deputado federal pulada: falta {', '.join(faltando)}")
        return pd.DataFrame(columns=COLUNAS_PAINEL)

    gc = gspread.service_account(filename=str(CREDS_FILE))
    sh = gc.open_by_key(PAINEL_NOMES_ID)
    try:
        ws = sh.worksheet(PAINEL_NOMES_ABA)
        extras, guardadas = _preenchido_a_mao(ws)
    except gspread.WorksheetNotFound:
        ws, extras, guardadas = None, [], {}

    saida = montar_deputados_federais(df, deputados_em_exercicio(gc),
                                      guardadas, extras)
    if not len(saida):
        print("nenhum deputado federal registrado ainda; aba mantida como está")
        return saida
    if ws is None:
        ws = sh.add_worksheet(title=PAINEL_NOMES_ABA, rows=len(saida) + 1,
                              cols=len(saida.columns))
    reescrever_aba(ws, [list(saida.columns)] + saida.astype(str).values.tolist(),
                    PAINEL_NOMES_ABA)
    reeleicao = int((saida["Situação Atual"] == "Reeleição").sum())
    mantidos = int((saida["Status"].astype(str).str.strip() != "").sum())
    print(f"'{PAINEL_NOMES_ABA}': {len(saida)} candidaturas, "
          f"{reeleicao} tentando reeleição, {mantidos} com status preenchido")
    return saida


if __name__ == '__main__':
    df = extrair_candidaturas()
    print(f"candidaturas: {len(df)} linha(s)")
    if len(df):
        salvar_no_sheets(df, "candidaturas_divulgacand")
        # Espelho da lista de deputado federal na planilha de trabalho da
        # equipe, com a marcação de reeleição. Falha aqui não pode derrubar a
        # coleta, que é o que alimenta todo o resto.
        try:
            exportar_deputados_federais(df)
        except Exception as e:
            print(f"aba de deputado federal não atualizada: {str(e)[:120]}")

    chapas = extrair_chapas()
    print(f"chapas: {len(chapas)} linha(s)")
    if len(chapas):
        salvar_no_sheets(chapas, "chapas_divulgacand")

    # A base oficial só sai depois que o período de registro avança. Enquanto não
    # existe, a coleta pela API já vale por si e não faz sentido derrubar a rodada.
    csv = baixar_base_oficial()
    if csv and len(df):
        existente = ler_aba("base_dadosabertos")
        base = consolidar(df, csv, existente)
        print(f"base consolidada: {base.shape[0]} linhas")
        salvar_no_sheets(base, "base_dadosabertos")
    else:
        print("base_dadosabertos não gerada nesta rodada")
