"""
Helpers de manutenção de planilha compartilhados entre relatorios_extracao_segmentos.py
(workflow 04, extrai segmentos/rejeição/aprovação dos PDFs) e relatorios_busca_fontes.py
(workflow 03, busca o link do relatório faltante).

Os dois arquivos leem e escrevem na MESMA aba 'relatorios', e por muito tempo
cada um tinha sua própria cópia colada destes helpers. Isso já causou bug real
duas vezes (14-16/07/2026): um fix de _row_count_atual foi aplicado só numa
cópia, e _resetar_validacoes_relatorios ficou sem 2 chamadas de coloração na
outra. Consolidado aqui pra qualquer correção valer pros dois workflows de
uma vez.

Não colocar aqui nada que dependa de estado GLOBAL mutável específico de cada
arquivo (ex.: USO_TOKENS, GEMINI_MODEL) - cada workflow rastreia seu próprio
custo de Gemini separadamente. _registrar_uso por isso recebe o dicionário de
uso como parâmetro explícito em vez de mexer numa variável global do módulo.
"""

import json
import os
import re
import time
import unicodedata
from datetime import timedelta, timezone

import gspread
import requests
from google.oauth2.service_account import Credentials

RELATORIOS_COLUNAS = [
    ("registro", "Registro TSE"),
    ("cargo", "Cargo"),
    ("uf", "UF"),
    ("instituto", "Instituto"),
    ("data_divulgacao", "Data de divulgação"),
    ("url_original", "Link na internet"),
    ("link", "PDF salvo no Drive"),
    ("nivel_conferencia", "Situação da fonte"),
    ("conferido", "Conferido?"),
    ("segmentos_extraido", "Segmentos extraídos?"),
    ("segmentos_data_extracao", "Data da extração de segmentos"),
    ("segmentos_erro", "Erro na extração de segmentos"),
    ("segmentos_tentativas", "Tentativas de segmentos"),
    ("link_pollingdata", "Link PollingData"),
    ("topline_extraido", "Voto cadastrado?"),
    ("topline_data_extracao", "Data do registro"),
]

REL_COL = dict(RELATORIOS_COLUNAS)

CABECALHO_RELATORIOS = [rotulo for _, rotulo in RELATORIOS_COLUNAS]

# A extração automática de TOPLINE (só a parte de topline, não a de
# segmentos/rejeição/aprovação) foi desativada em 16/07/2026 - o cadastro de
# topline passou a ser 100% manual via Polling Manual (gerador-de-envios).
# Segmentos/rejeição/aprovação continuam extraídos automaticamente
# (relatorios_extracao_segmentos.py extrair, workflow 04), sem mudança. "Topline
# extraída?" mudou de sentido: não indica mais se o Gemini extraiu do PDF,
# indica se o registro+cargo já está na Matriz T1/T2
# (marcar_topline_extraida_manual, no Polling Manual, vira "sim" quando ela
# salva por lá). O alias legado "Data da extração de topline" continua
# mapeado aqui só pra não perder dado de linha antiga que ainda não passou
# pela migração de cabeçalho.
ALIASES_RELATORIOS = {
    "Registro TSE": ["registro", "registro_tse"],
    "Cargo": ["cargo"],
    "UF": ["uf"],
    "Instituto": ["instituto"],
    "Data de divulgação": ["data_divulgacao", "data_divulgacao_pesqele"],
    "Link na internet": ["url_original"],
    "PDF salvo no Drive": ["link", "Link do relatório"],
    # "Origem do link" foi removida em 17/07/2026 (a narrativa de busca deixou de
    # ser gravada na planilha - Situação da fonte volta a ser só o status limpo,
    # pra manter a lista suspensa/dropdown de verdade). _nivel_de tolera célula
    # antiga no formato composto ("token — narrativa") de uma migração anterior.
    "Situação da fonte": ["nivel_conferencia", "Nível de conferência"],
    "Conferido?": ["conferido"],
    "Segmentos extraídos?": ["segmentos_extraido", "extraido"],
    "Data da extração de segmentos": ["segmentos_data_extracao", "data_extracao"],
    "Erro na extração de segmentos": ["segmentos_erro", "extracao_erro"],
    "Tentativas de segmentos": ["segmentos_tentativas", "extracao_tentativas"],
    "Link PollingData": ["link_pollingdata"],
    "Voto cadastrado?": ["topline_extraido", "Topline extraída?", "Intenção de voto cadastrada?"],
    "Data do registro": ["topline_data_extracao", "Data da extração de topline", "Data do registro manual"],
}

REL_KEY = {rotulo: chave for chave, rotulo in RELATORIOS_COLUNAS}
for chave, rotulo in RELATORIOS_COLUNAS:
    REL_KEY[chave] = chave
    for alias in ALIASES_RELATORIOS.get(rotulo, []):
        REL_KEY[alias] = chave

STATUS_TOPLINE_MANUAL = "⚠️ REGISTRE NO POLLING MANUAL"

POLLING_MANUAL_URL = "https://eixoestrategiapolitica.streamlit.app/Polling_Manual"


def link_status_topline_manual() -> str:
    """Fórmula HYPERLINK clicável pro estado 'ainda não cadastrado na Matriz
    T1/T2' de 'Topline extraída?' - usada tanto pra linha nova na fila quanto
    pra linha recém-separada de um registro multicargo."""
    return f'=HYPERLINK("{POLLING_MANUAL_URL}";"{STATUS_TOPLINE_MANUAL}")'

CARGOS_MONITORADOS = ("presidente", "governador", "senador")

CARGO_ROTULO = {
    "presidente": "Presidente",
    "governador": "Governador",
    "senador": "Senador",
}

MAPA_UF = {
    "ACRE": "AC", "ALAGOAS": "AL", "AMAPÁ": "AP", "AMAZONAS": "AM", "BAHIA": "BA",
    "CEARÁ": "CE", "DISTRITO FEDERAL": "DF", "ESPÍRITO SANTO": "ES", "GOIÁS": "GO",
    "MARANHÃO": "MA", "MATO GROSSO": "MT", "MATO GROSSO DO SUL": "MS", "MINAS GERAIS": "MG",
    "PARÁ": "PA", "PARAÍBA": "PB", "PARANÁ": "PR", "PERNAMBUCO": "PE", "PIAUÍ": "PI",
    "RIO DE JANEIRO": "RJ", "RIO GRANDE DO NORTE": "RN", "RIO GRANDE DO SUL": "RS",
    "RONDÔNIA": "RO", "RORAIMA": "RR", "SANTA CATARINA": "SC", "SÃO PAULO": "SP",
    "SERGIPE": "SE", "TOCANTINS": "TO", "BRASIL": "BR",
}

BRT = timezone(timedelta(hours=-3))

REGISTRO_TSE_RE = re.compile(
    r"\b(?:BR|AC|AL|AP|AM|BA|CE|DF|ES|GO|MA|MT|MS|MG|PA|PB|PR|PE|PI|RJ|RN|RS|RO|RR|SC|SP|SE|TO)"
    r"[\s-]*\d{5}[/-]2026\b",
    flags=re.I,
)


def autorizar_com_retry(creds, tentativas: int = 4):
    """Cliente gspread que reencaminha a chamada quando o Google engasga.

    O adapter e montado na sessao HTTP que o gspread usa, entao vale pra TODA
    chamada ao Sheets, nao so pra abertura da planilha. Sem isso, um soluco de
    rede derruba o workflow inteiro mesmo que o segundo seguinte funcionasse:
    em 21/07/2026 dois workflows cairam assim, um com
    ConnectionResetError(104) no open_by_key e outro com APIError 503 num
    col_values, os dois na PRIMEIRA leitura, antes de processar qualquer linha.

    allowed_methods=None inclui POST/PUT no retry. E seguro aqui porque toda
    escrita e de valor determinado (update_cells com celulas ja calculadas):
    repetir grava o mesmo conteudo, nao acumula.
    """
    from requests.adapters import HTTPAdapter
    from urllib3.util import Retry

    gc = gspread.authorize(creds)
    retry = Retry(
        total=tentativas,
        backoff_factor=1.5,          # espera 1,5s, 3s, 6s, 12s
        status_forcelist=[429, 500, 502, 503, 504],
        allowed_methods=None,
        raise_on_status=False,
    )
    sessao = getattr(getattr(gc, "http_client", None), "session", None) or getattr(gc, "session", None)
    if sessao is not None:
        sessao.mount("https://", HTTPAdapter(max_retries=retry))
    return gc


def reescrever_aba(ws, linhas, rotulo: str = "", tentativas: int = 3,
                   antes=None, **kwargs) -> None:
    """Limpa a aba e grava tudo de novo, repetindo em falha transitória.

    O par clear + update não é atômico: se o update falha, a aba fica VAZIA até
    alguém rodar de novo. Em 06/08/2026 o workflow 10 gravava as 6.569
    candidaturas quando a API devolveu 409 ('The operation was aborted', o erro
    de edição concorrente do Sheets, que qualquer escrita simultânea na mesma
    planilha dispara, inclusive alguém digitando na aba). A rodada morreu com a
    aba limpa e o dado só voltou na hora seguinte.

    O 409 não entra no retry de `autorizar_com_retry`, e não deve mesmo: lá o
    adapter repete qualquer POST/PUT, e repetir um append_rows duplicaria
    linhas. Aqui é seguro porque a operação é a reescrita inteira, idempotente
    por construção: o clear roda de novo antes de cada tentativa.

    `antes` é um callable opcional que roda entre o clear e o update (o
    resize da aba, no caso do pollingdata). kwargs vão para o update, ex.:
    value_input_option="RAW".
    """
    for tentativa in range(1, tentativas + 1):
        try:
            ws.clear()
            if antes is not None:
                antes()
            ws.update(linhas, **kwargs)
            return
        except gspread.exceptions.APIError as erro:
            if tentativa == tentativas:
                raise
            espera = tentativa * 5
            print(f"  [{rotulo or getattr(ws, 'title', 'aba')}] Sheets recusou a "
                  f"escrita (tentativa {tentativa}/{tentativas}, {str(erro)[:60]}); "
                  f"tentando de novo em {espera}s", flush=True)
            time.sleep(espera)


def _sem_acento(valor):
    return unicodedata.normalize("NFKD", str(valor or "")).encode("ascii", "ignore").decode()


def _rel_display(nome):
    return REL_COL.get(nome, nome)


SEPARADOR_SITUACAO = " — "


def _nivel_de(valor):
    """Extrai o token de status (ex.: 'ok', 'suspensa') da célula fundida de
    'Situação da fonte', que pode ser só o token ou 'token — narrativa'."""
    txt = str(valor or "").strip()
    if not txt:
        return ""
    return txt.split(SEPARADOR_SITUACAO, 1)[0].strip()



def _rel_key(nome):
    return REL_KEY.get(nome, nome)


def _rel_record(row):
    return {_rel_key(k): v for k, v in row.items()}


def _rel_records(ws):
    return [_rel_record(r) for r in ws.get_all_records()]


def _cargo_norm(valor):
    s = _sem_acento(valor).strip().lower()
    for cargo in CARGOS_MONITORADOS:
        if cargo in s:
            return cargo
    return s


def _cargos_monitorados(valor):
    s = _sem_acento(valor).lower()
    cargos = []
    for cargo in CARGOS_MONITORADOS:
        if cargo in s and cargo not in cargos:
            cargos.append(cargo)
    return [CARGO_ROTULO[c] for c in cargos]


def _chave_fila(registro, cargo, uf):
    return (
        re.sub(r"[^A-Z0-9]", "", str(registro or "").upper()),
        _cargo_norm(cargo),
        _sem_acento(uf).strip().upper(),
    )


def _ultima_linha_com_dados(ws):
    """Última linha com algum valor, incluindo o cabeçalho.

    A grade do Google Sheets pode ter centenas de linhas vazias pré-criadas;
    uma validação de checkbox também pode fazer ``get_all_values`` devolver linhas
    vazias. Por isso, contamos apenas linhas com conteúdo de verdade.
    """
    ultima = 1
    for linha, valores in enumerate(ws.get_all_values(), start=1):
        # O Sheets materializa checkbox vazio como FALSE. Uma linha que só tem
        # esse FALSE é sobra da validação, não é uma linha de dado.
        if any(str(valor).strip() and str(valor).strip().upper() != "FALSE"
               for valor in valores):
            ultima = linha
    return ultima


def _ultima_linha_com_registro(ws):
    col_a = ws.col_values(1)
    ultima = 1
    for idx, val in enumerate(col_a, start=1):
        if idx > 1 and str(val).strip():
            ultima = idx
    return ultima


def _row_count_atual(ws):
    """Linhas da grade direto da API, sem confiar no cache local do gspread.

    ws.row_count é lido de ws._properties, setado na hora que o Worksheet foi
    aberto (ou na última chamada de resize()/add_rows() NESSE MESMO objeto).
    Se outra chamada de API mudou a grade nesse meio-tempo (ex.: append_rows
    expande a grade sozinho além do que add_rows pediu), o cache local fica
    errado e deleteDimension recebe um endIndex que não bate mais com a
    grade real — foi o que gerou 'Cannot delete a row that doesn't exist'."""
    meta = ws.spreadsheet.fetch_sheet_metadata()
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("sheetId") == ws.id:
            return props.get("gridProperties", {}).get("rowCount", ws.row_count)
    return ws.row_count


def _col_count_atual(ws):
    """Colunas da grade direto da API. Mesmo motivo do _row_count_atual: escrever
    numa coluna além da largura da grade devolve 400 'exceeds grid limits'."""
    meta = ws.spreadsheet.fetch_sheet_metadata()
    for sheet in meta.get("sheets", []):
        props = sheet.get("properties", {})
        if props.get("sheetId") == ws.id:
            return props.get("gridProperties", {}).get("columnCount", ws.col_count)
    return ws.col_count


def _encolher_linhas_vazias(ws):
    """Remove somente as linhas vazias que sobram abaixo dos dados.

    Roda logo depois de add_rows()+append_rows(). add_rows(N) às vezes
    expande a grade em blocos maiores que N por conta própria, e um fetch de
    metadata (_row_count_atual) imediatamente depois pode devolver um
    total_atual desatualizado (ainda não propagado), igual a 'ultima' - o
    early-return antigo confiava nessa ÚNICA leitura e pulava a limpeza
    achando que não sobrava nada. A linha extra fica com a validação de
    checkbox aplicada (herdada da coluna 'liberado'), então o Sheets ainda
    conta ela como "usada" pro auto-append da PRÓXIMA rodada - o resultado é
    um buraco permanente entre o fim de uma execução do workflow e o começo
    da seguinte (achado em topline_pesquisas: linhas 83-107 e 133-158, cada
    uma bem na fronteira entre duas rodadas em dias diferentes). Por isso
    confere de novo, com uma pequena pausa, antes de desistir.
    """
    ultima = _ultima_linha_com_dados(ws)
    total_atual = _row_count_atual(ws)
    if total_atual <= ultima:
        time.sleep(1.5)
        total_atual = _row_count_atual(ws)
        if total_atual <= ultima:
            return
    for tentativa in range(2):
        try:
            ws.spreadsheet.batch_update({
                "requests": [{
                    "deleteDimension": {
                        "range": {
                            "sheetId": ws.id,
                            "dimension": "ROWS",
                            "startIndex": ultima,
                            "endIndex": total_atual,
                        }
                    }
                }]
            })
            return
        except Exception as e:
            if tentativa == 1:
                print(f"[AVISO] não deu pra remover linhas vazias da aba '{ws.title}': {e}")
                return
            # endIndex pode ter ficado desatualizado NO SENTIDO CONTRÁRIO
            # (grade real menor que o total_atual lido) - relê e tenta de novo.
            time.sleep(1.5)
            total_atual = _row_count_atual(ws)
            if total_atual <= ultima:
                return


def _append_rows_compacto(ws, linhas, value_input_option="RAW"):
    """Acrescenta linhas e deixa a grade exatamente no tamanho dos dados.

    Antes de gravar, expande apenas o necessário; depois remove qualquer sobra.
    Assim as abas crescem junto com a entrada de dados, sem um bloco permanente
    de linhas vazias.
    """
    if not linhas:
        return
    ultima = _ultima_linha_com_dados(ws)
    necessario = ultima + len(linhas)
    if ws.row_count < necessario:
        ws.add_rows(necessario - ws.row_count)
    ws.append_rows(linhas, value_input_option=value_input_option)
    _encolher_linhas_vazias(ws)


def _garantir_coluna(ws, header, nome):
    """Retorna o índice (1-based) da coluna 'nome'. Se não existir, cria (expandindo
    a grade se preciso, pra não estourar o limite de colunas)."""
    if nome in header:
        return header.index(nome) + 1
    novo = len(header) + 1
    if ws.col_count < novo:
        ws.add_cols(novo - ws.col_count)
    ws.update_cell(1, novo, nome)
    header.append(nome)
    return novo


def _garantir_coluna_relatorios(ws, header, nome):
    display = _rel_display(nome)
    for candidato in [display, nome] + ALIASES_RELATORIOS.get(display, []):
        if candidato in header:
            return header.index(candidato) + 1
    return _garantir_coluna(ws, header, display)


def _remover_colunas_sobrando(ws, total_colunas):
    """Remove colunas antigas/duplicadas que ficaram à direita após migrar cabeçalho."""
    if ws.col_count <= total_colunas:
        return
    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": total_colunas,
                        "endIndex": ws.col_count,
                    }
                }
            }]
        })
    except Exception as e:
        print(f"[AVISO] não deu pra remover colunas antigas à direita: {e}")


def _normalizar_booleanos_coluna(ws, col_i, ate_linha):
    """Converte texto literal 'TRUE'/'FALSE' (sobra de reescrita com value_input_option
    RAW, que nunca interpreta string como booleano) em booleano de verdade. Com
    validação BOOLEAN estrita, uma célula com o TEXTO 'TRUE' não conta como valor
    válido de checkbox e a caixinha não marca, mesmo com a validação certa aplicada."""
    try:
        valores = ws.col_values(col_i)
    except Exception as e:
        print(f"[AVISO] não deu pra ler a coluna pra normalizar booleanos: {e}")
        return
    requests = []
    for i in range(2, ate_linha + 1):
        v = valores[i - 1].strip().upper() if i - 1 < len(valores) else ""
        if v in ("TRUE", "FALSE"):
            requests.append({
                "updateCells": {
                    "range": {"sheetId": ws.id, "startRowIndex": i - 1, "endRowIndex": i,
                               "startColumnIndex": col_i - 1, "endColumnIndex": col_i},
                    "rows": [{"values": [{"userEnteredValue": {"boolValue": v == "TRUE"}}]}],
                    "fields": "userEnteredValue",
                }
            })
    if not requests:
        return
    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"[AVISO] não deu pra normalizar booleanos da coluna: {e}")


def _ativar_checkbox(ws, coluna, header, ate_linha):
    """Transforma a coluna em checkbox real do Sheets (TRUE/FALSE), da linha 2 até
    ate_linha (última linha COM pesquisa). Sem linha de dados, não faz nada, pra não
    espalhar checkbox vazio nas linhas de baixo."""
    if coluna not in header or ate_linha < 2:
        return
    col_i = header.index(coluna) + 1
    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": ate_linha,
                        "startColumnIndex": col_i - 1,
                        "endColumnIndex": col_i,
                    },
                    "rule": {
                        "condition": {"type": "BOOLEAN"},
                        "strict": True,
                        "showCustomUi": True,
                    },
                }
            }]
        })
    except Exception as e:
        print(f"[AVISO] não deu pra criar o checkbox de '{coluna}': {e}")
    _normalizar_booleanos_coluna(ws, col_i, ate_linha)


def _ativar_dropdown(ws, coluna, header, ate_linha, opcoes):
    """Transforma a coluna em lista suspensa (ONE_OF_LIST) com as 'opcoes', da linha 2
    até ate_linha. strict=True + showCustomUi=True: mostra a setinha e só aceita um
    valor da lista (blank continua permitido)."""
    if coluna not in header or ate_linha < 2:
        return
    col_i = header.index(coluna) + 1
    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "setDataValidation": {
                    "range": {
                        "sheetId": ws.id,
                        "startRowIndex": 1,
                        "endRowIndex": ate_linha,
                        "startColumnIndex": col_i - 1,
                        "endColumnIndex": col_i,
                    },
                    "rule": {
                        "condition": {
                            "type": "ONE_OF_LIST",
                            "values": [{"userEnteredValue": str(o)} for o in opcoes],
                        },
                        "strict": True,
                        "showCustomUi": True,
                    },
                }
            }]
        })
    except Exception as e:
        print(f"[AVISO] não deu pra criar a lista suspensa de '{coluna}': {e}")


def _colorir_por_valor(ws, coluna, header, ate_linha, cores):
    """Formatação condicional: pinta o fundo da coluna conforme o valor (ex.: relatório
    verde, notícia laranja, N/A cinza). 'cores' = {valor: (r,g,b)} com r,g,b em 0-1.
    Idempotente: remove as regras que já cobrem essa coluna antes de recriar, pra não
    acumular a cada rodada de manutenção. A cor persiste e vale pras linhas novas também."""
    if coluna not in header or ate_linha < 2:
        return
    col = header.index(coluna)
    try:
        meta = ws.spreadsheet.fetch_sheet_metadata(
            params={"fields": "sheets(properties(sheetId),conditionalFormats)"})
        reqs = []
        for s in meta.get("sheets", []):
            if s.get("properties", {}).get("sheetId") != ws.id:
                continue
            cfs = s.get("conditionalFormats", [])
            for idx in range(len(cfs) - 1, -1, -1):   # de trás pra frente: índice não desloca
                rngs = cfs[idx].get("ranges", [])
                if any(r.get("startColumnIndex") == col and r.get("endColumnIndex") == col + 1 for r in rngs):
                    reqs.append({"deleteConditionalFormatRule": {"sheetId": ws.id, "index": idx}})
        faixa = {"sheetId": ws.id, "startRowIndex": 1, "endRowIndex": max(ate_linha, ws.row_count),
                 "startColumnIndex": col, "endColumnIndex": col + 1}
        for valor, (r, g, b) in cores.items():
            reqs.append({"addConditionalFormatRule": {"index": 0, "rule": {
                "ranges": [faixa],
                "booleanRule": {
                    "condition": {"type": "TEXT_EQ", "values": [{"userEnteredValue": valor}]},
                    "format": {"backgroundColor": {"red": r, "green": g, "blue": b}},
                }}}})
        if reqs:
            ws.spreadsheet.batch_update({"requests": reqs})
    except Exception as e:
        print(f"[AVISO] não deu pra colorir a coluna '{coluna}': {e}")




def _colorir_cabecalhos_relatorios(ws, header):
    """Dá leitura visual imediata aos três blocos operacionais da fila."""
    grupos = [
        # Identificação da pesquisa: cinza azulado.
        (["registro", "cargo", "uf", "instituto", "data_divulgacao"],
         (0.85, 0.90, 0.95)),
        # Fonte e revisão humana: verde muito claro.
        (["url_original", "link", "nivel_conferencia", "conferido"],
         (0.88, 0.94, 0.86)),
        # Resultado da extração demográfica: azul claro.
        (["segmentos_extraido", "segmentos_data_extracao", "segmentos_erro", "segmentos_tentativas"],
         (0.82, 0.91, 0.97)),
        # Registro de topline (agora manual, via Polling Manual): roxo claro.
        (["topline_extraido", "topline_data_extracao"],
         (0.89, 0.84, 0.95)),
    ]
    requests = []
    for chaves, (r, g, b) in grupos:
        indices = [header.index(_rel_display(chave)) for chave in chaves if _rel_display(chave) in header]
        if not indices:
            continue
        # Os campos de cada bloco são contíguos no cabeçalho canônico.
        requests.append({"repeatCell": {
            "range": {
                "sheetId": ws.id,
                "startRowIndex": 0,
                "endRowIndex": 1,
                "startColumnIndex": min(indices),
                "endColumnIndex": max(indices) + 1,
            },
            "cell": {"userEnteredFormat": {"backgroundColor": {"red": r, "green": g, "blue": b}}},
            "fields": "userEnteredFormat.backgroundColor",
        }})
    if not requests:
        return
    try:
        ws.spreadsheet.batch_update({"requests": requests})
    except Exception as e:
        print(f"[AVISO] não deu pra colorir cabeçalhos da aba relatorios: {e}")


def _situacao_do_texto_solto(valor):
    """Reconhece o status que a equipe às vezes digita À MÃO na coluna do PDF
    em vez de um link (ex.: 'Pesquisa suspensa pelo TSE', 'Suspensa pelo TRE',
    'divulgada há mais de 5 dias'). Devolve o valor canônico de 'Situação da
    fonte' pra jogar o texto pro lugar certo, ou '' se a célula for link/fórmula/
    vazia (aí não mexe)."""
    txt = str(valor or "").strip()
    if not txt or txt.lower().startswith("http") or txt.startswith("="):
        return ""
    t = _sem_acento(txt).lower()
    if "suspens" in t or "barrad" in t:
        return "suspensa"
    if ("mais de" in t and "dia" in t) or "fora da janela" in t:
        return "fora_da_janela"
    return ""


def _migrar_status_texto_pdf(ws, header, ate_linha):
    """Tira texto de status escrito à mão na coluna do PDF, jogando pra 'Situação
    da fonte' quando ela ainda está vazia. Idempotente: lê tudo uma vez e só
    grava as células que mudaram."""
    if ate_linha < 2:
        return
    pos = {chave: header.index(_rel_display(chave))
           for chave in ("link", "nivel_conferencia")
           if _rel_display(chave) in header}
    if "link" not in pos or "nivel_conferencia" not in pos:
        return
    try:
        valores = ws.get_all_values()
    except Exception as e:
        print(f"[AVISO] não deu pra ler valores pra limpar status do PDF: {e}")
        return
    if len(valores) < 2:
        return

    celulas = []
    for r in range(1, min(ate_linha, len(valores))):
        row = valores[r]
        # Status digitado na coluna do PDF migra pra Situação da fonte. Só grava
        # a Situação por cima de célula vazia ou "N/A" (placeholder, não é
        # revisão de verdade); se já houver uma Situação com sentido, respeita e
        # só tira o texto solto do PDF, que ali é redundante.
        link_val = row[pos["link"]] if pos["link"] < len(row) else ""
        situ = _situacao_do_texto_solto(link_val)
        if situ:
            nivel_atual = str(row[pos["nivel_conferencia"]]).strip() if pos["nivel_conferencia"] < len(row) else ""
            if nivel_atual.upper() in ("", "N/A", "NA"):
                celulas.append(gspread.Cell(r + 1, pos["nivel_conferencia"] + 1, situ))
            celulas.append(gspread.Cell(r + 1, pos["link"] + 1, ""))

    if celulas:
        try:
            ws.update_cells(celulas, value_input_option="USER_ENTERED")
        except Exception as e:
            print(f"[AVISO] não deu pra gravar limpeza do PDF: {e}")


def _sinalizar_voto_pendente(ws, header, ate_linha):
    """Sempre que Conferido?='sim' e Voto cadastrado? ainda está vazio, avisa que
    falta lançar a pesquisa no Polling Manual. Extração automática de topline foi
    aposentada em 16/07/2026 (cadastro de voto é 100% manual agora) - sem isso,
    'Voto cadastrado?' nunca recebia sinal nenhum de pendência.
    marcar_topline_extraida_manual (Polling Manual, gerador-de-envios) troca esse
    aviso por 'sim' quando alguém lança a pesquisa por lá. Idempotente: só grava
    onde ainda está vazio, nunca sobrescreve 'sim' nem 'N/A'."""
    if ate_linha < 2:
        return
    col_conf = _rel_display("conferido")
    col_topl = _rel_display("topline_extraido")
    if col_conf not in header or col_topl not in header:
        return
    i_conf = header.index(col_conf)
    i_topl = header.index(col_topl)
    try:
        valores = ws.get_all_values()
    except Exception as e:
        print(f"[AVISO] não deu pra ler valores pra sinalizar voto pendente: {e}")
        return
    celulas = []
    for r in range(1, min(ate_linha, len(valores))):
        row = valores[r]
        conf = str(row[i_conf]).strip().lower() if i_conf < len(row) else ""
        topl = str(row[i_topl]).strip() if i_topl < len(row) else ""
        if conf == "sim" and not topl:
            celulas.append(gspread.Cell(r + 1, i_topl + 1, link_status_topline_manual()))
    if celulas:
        try:
            ws.update_cells(celulas, value_input_option="USER_ENTERED")
        except Exception as e:
            print(f"[AVISO] não deu pra gravar aviso de Polling Manual: {e}")


def _resetar_validacoes_relatorios(ws, header, ate_linha):
    """Remove checkboxes/validações acidentais nas OUTRAS colunas e garante a de
    Conferido?. NUNCA limpa a validação de Conferido? antes de recriar: isso roda em
    TODA chamada de _aba() pra 'relatorios' (extrair, topline...), e limpar+recriar em
    duas chamadas separadas deixa uma janela em que, se a segunda falhar (rede, limite
    de taxa), o checkbox de Conferido? some da planilha até a próxima rodada consertar.
    setDataValidation é idempotente, então só (re)aplicar a de Conferido? já resolve
    sem esse risco."""
    if ate_linha < 2:
        return
    # Limpa status digitado à mão na coluna do PDF e sinaliza pendência de Polling
    # Manual antes de (re)aplicar cores/validações, pra elas já refletirem os
    # valores corrigidos.
    _migrar_status_texto_pdf(ws, header, ate_linha)
    _sinalizar_voto_pendente(ws, header, ate_linha)
    col_conferido = _rel_display("conferido")
    # colunas com validação PRÓPRIA que não podem ser limpas junto (senão o checkbox do
    # Conferido? some a cada rodada de manutenção).
    protegidas = sorted({header.index(_rel_display(n)) for n in
                         ("conferido", "segmentos_extraido", "nivel_conferencia", "topline_extraido")
                         if _rel_display(n) in header})
    faixas, ini = [], 0
    for pc in protegidas:
        if pc > ini:
            faixas.append((ini, pc))
        ini = pc + 1
    if ini < len(header):
        faixas.append((ini, len(header)))
    if faixas:
        try:
            ws.spreadsheet.batch_update({
                "requests": [{
                    "repeatCell": {
                        "range": {
                            "sheetId": ws.id,
                            "startRowIndex": 1,
                            "endRowIndex": ate_linha,
                            "startColumnIndex": ini,
                            "endColumnIndex": fim,
                        },
                        "cell": {"dataValidation": None},
                        "fields": "dataValidation",
                    }
                } for ini, fim in faixas]
            })
        except Exception as e:
            print(f"[AVISO] não deu pra limpar validações antigas da aba relatorios: {e}")
    CINZA_NA = (0.85, 0.85, 0.85)  # cinza único pra todo "não se aplica" (N/A, fora_da_janela, imagem)
    # Conferido?: virou lista suspensa (era checkbox) pra caber um terceiro estado -
    # N/A pra linha que não tem nem fonte pra conferir (fora_da_janela/suspensa).
    # Sem "não": ou já foi conferida (sim), ou não se aplica (N/A), ou ainda não
    # foi revisada (em branco) - não existe um "não" permanente aqui.
    _ativar_dropdown(ws, col_conferido, header, ate_linha, ["sim", "N/A"])
    _colorir_por_valor(ws, col_conferido, header, ate_linha, {
        "sim": (0.82, 0.93, 0.82),
        "N/A": CINZA_NA,
    })
    # Voto cadastrado?: lista suspensa também - "sim" (lançado no Polling Manual),
    # o aviso de pendência (grava sozinho quando Conferido?=sim, ver
    # _sinalizar_voto_pendente) ou N/A (linha sem fonte, nunca vai ter voto).
    _ativar_dropdown(ws, _rel_display("topline_extraido"), header, ate_linha,
                      ["sim", STATUS_TOPLINE_MANUAL, "N/A"])
    _colorir_por_valor(ws, _rel_display("topline_extraido"), header, ate_linha, {
        "sim": (0.82, 0.93, 0.82),
        STATUS_TOPLINE_MANUAL: (1.0, 0.82, 0.68),  # laranja: ação manual necessária
        "N/A": CINZA_NA,
    })
    # Segmentos extraídos?: "sim"/"não" são as duas conclusões que o pipeline grava;
    # N/A cobre a linha sem fonte nenhuma pra extrair (mesmo caso do Conferido?).
    _ativar_dropdown(ws, _rel_display("segmentos_extraido"), header, ate_linha, ["sim", "não", "N/A"])
    _colorir_por_valor(ws, _rel_display("segmentos_extraido"), header, ate_linha, {
        "sim": (0.82, 0.93, 0.82),
        "não": (0.96, 0.80, 0.80),  # vermelho pastel: relatório sem quebra de segmento
        "N/A": CINZA_NA,
    })
    # Situação da fonte: lista suspensa de verdade (ONE_OF_LIST) - célula guarda
    # só o status limpo, sem narrativa colada (Sheets não mostra dropdown em
    # célula de texto composto). Cor por valor exato, junto com o dropdown.
    _ativar_dropdown(ws, _rel_display("nivel_conferencia"), header, ate_linha, [
        "ok", "nao", "provavel", "teaser", "paywall", "bloqueado",
        "erro_chrome", "erro_tecnico", "imagem", "fora_da_janela", "link_existente",
        "suspensa", "N/A",
    ])
    _colorir_por_valor(ws, _rel_display("nivel_conferencia"), header, ate_linha, {
        "ok": (0.82, 0.93, 0.82),             # verde: fonte confirmada
        "link_existente": (0.88, 0.95, 0.88),  # verde claro: link já veio pronto
        "nao": (0.96, 0.80, 0.80),             # vermelho pastel: ainda não confirmada
        "provavel": (1.0, 0.95, 0.70),         # amarelo: confirmação fraca
        "teaser": (0.85, 0.90, 0.95),          # azul acinzentado: só anuncia divulgação futura
        "paywall": (1.0, 0.87, 0.70),          # laranja: bloqueado por paywall
        "bloqueado": (0.95, 0.75, 0.75),       # vermelho mais forte: acesso bloqueado
        "erro_chrome": (0.95, 0.75, 0.75),
        "erro_tecnico": (0.95, 0.75, 0.75),
        "imagem": CINZA_NA,                    # não dá pra conferir por texto
        "suspensa": CINZA_NA,                  # suspensa pela Justiça Eleitoral: nunca vai ter fonte
        "N/A": CINZA_NA,
        # fora_da_janela: pesquisa divulgada há mais de MAX_DIAS_BUSCA dias, não
        # vale mais tentar achar o relatório - só visível, sem ação pendente real.
        "fora_da_janela": CINZA_NA,
    })
    _colorir_cabecalhos_relatorios(ws, header)


def _limpar_status_extracao(row):
    for coluna in (
        "link", "nivel_conferencia", "conferido",
        "segmentos_extraido", "segmentos_data_extracao", "segmentos_erro", "segmentos_tentativas",
        "topline_extraido", "topline_data_extracao",
    ):
        row[_rel_display(coluna)] = ""
        row[coluna] = ""
    nota = "separado de linha multicargo; buscar fonte específica"
    row[_rel_display("nivel_conferencia")] = nota
    row["nivel_conferencia"] = nota
    return row


def _separar_linhas_multicargo(ws, header):
    """Migra a fila para uma linha por registro+cargo+UF.

    Quando uma linha antiga tinha "Governador, Senador", a original fica com o
    primeiro cargo monitorado e as demais viram novas linhas sem link. Isso evita
    que uma matéria parcial de governador cubra senador por engano.
    """
    if _rel_display("cargo") not in header or _rel_display("registro") not in header:
        return

    registros = _rel_records(ws)
    existentes = {
        _chave_fila(r.get("registro"), r.get("cargo"), r.get("uf"))
        for r in registros
        if str(r.get("registro", "")).strip()
    }
    col_cargo = header.index(_rel_display("cargo")) + 1
    updates, novas = [], []

    for row_i, r in enumerate(registros, start=2):
        cargos = _cargos_monitorados(r.get("cargo"))
        if not cargos:
            continue

        primeiro = cargos[0]
        if str(r.get("cargo", "")).strip() != primeiro:
            updates.append(gspread.Cell(row_i, col_cargo, primeiro))
        if len(cargos) <= 1:
            continue
        existentes.add(_chave_fila(r.get("registro"), primeiro, r.get("uf")))

        for cargo in cargos[1:]:
            chave = _chave_fila(r.get("registro"), cargo, r.get("uf"))
            if chave in existentes:
                continue
            novo = {c: r.get(_rel_key(c), "") for c in header}
            novo[_rel_display("cargo")] = cargo
            _limpar_status_extracao(novo)
            novas.append([novo.get(c, "") for c in header])
            existentes.add(chave)

    if updates:
        ws.update_cells(updates, value_input_option="RAW")
    if novas:
        _append_rows_compacto(ws, novas)
        print(f"{len(novas)} linha(s) multicargo separada(s) na fila de relatórios.", flush=True)


def _extrair_json_objeto(texto):
    bruto = (texto or "").strip()
    bruto = re.sub(r"^```json\s*", "", bruto, flags=re.I)
    bruto = re.sub(r"^```\s*", "", bruto)
    bruto = re.sub(r"\s*```$", "", bruto)
    decoder = json.JSONDecoder()
    for match in re.finditer(r"\{", bruto):
        try:
            obj, _ = decoder.raw_decode(bruto[match.start():])
            if isinstance(obj, dict):
                return obj
        except json.JSONDecodeError:
            continue
    raise RuntimeError("JSON não encontrado na resposta do Gemini")


def _custo_estimado(entrada, saida, pensamento):
    # preço aproximado da faixa "flash" (~$0,30/1M tokens de entrada, ~$2,50/1M de
    # saída, saída e pensamento cobram na mesma tabela). Ajuste se trocar de modelo
    # (GEMINI_MODEL) ou se o preço mudar. Estimativa, não fatura oficial; confira o
    # console de billing do Google pro valor exato.
    return (entrada / 1_000_000 * 0.30) + ((saida + pensamento) / 1_000_000 * 2.50)


def _registrar_uso(resp, uso):
    meta = getattr(resp, "usage_metadata", None)
    if not meta:
        return
    uso["chamadas"] += 1
    uso["entrada"] += getattr(meta, "prompt_token_count", 0) or 0
    uso["saida"] += getattr(meta, "candidates_token_count", 0) or 0
    uso["pensamento"] += getattr(meta, "thoughts_token_count", 0) or 0


def _resumo_uso_tokens(rotulo, uso):
    if not uso["chamadas"]:
        return
    custo = _custo_estimado(uso["entrada"], uso["saida"], uso["pensamento"])
    print(f"\nGemini ({rotulo}): {uso['chamadas']} chamada(s) · "
          f"{uso['entrada']:,} tokens entrada · {uso['saida']:,} saída · "
          f"{uso['pensamento']:,} pensamento · custo estimado ${custo:.4f}")


# ─────────────────────────────────────────────────────────────────────────
# Helpers de baixo nível compartilhados entre relatorios_extracao_segmentos.py
# (ativo) e relatorios_extracao_topline_aposentado.py (aposentado) - download
# de PDF, fatiamento em blocos, validação de registro TSE no texto, acesso a
# credenciais/planilha. Nada aqui referencia PROMPT/GEMINI_MODEL/USO_TOKENS
# porque cada um dos dois pipelines usa seu próprio prompt e contabiliza seu
# próprio custo de Gemini separadamente (ver docstring do topo do arquivo).
# ─────────────────────────────────────────────────────────────────────────

HEADERS = {"User-Agent": "Mozilla/5.0"}


def _creds_info():
    raw = os.getenv("GOOGLE_CREDENTIALS_JSON", "")
    return json.loads(raw) if raw else json.load(open("credentials.json", encoding="utf-8"))


def _sheets():
    scopes = ["https://www.googleapis.com/auth/spreadsheets"]
    return autorizar_com_retry(Credentials.from_service_account_info(_creds_info(), scopes=scopes))


def _rel_row(valores, header):
    return [valores.get(_rel_key(c), valores.get(c, "")) for c in header]


def _normalizar_cabecalho(ws, cabecalho, remover_sobras=False):
    """Garante a ordem canônica sem perder dados de colunas já existentes."""
    valores = ws.get_all_values()
    if not valores:
        ws.update(range_name="A1", values=[cabecalho])
        # sem dados ainda: o checkbox nasce junto com as pesquisas (no atualiza_relatorios)
        return cabecalho[:]

    atual = valores[0]
    aliases_antigos = {a for aliases in ALIASES_RELATORIOS.values() for a in aliases}
    aliases_antigos.update(REL_COL.keys())
    extras = [c for c in atual if c and c not in cabecalho and c not in aliases_antigos]
    alvo = cabecalho + extras
    if atual == alvo:
        return atual

    idx = {nome: pos for pos, nome in enumerate(atual) if nome}

    def _valor(row, coluna):
        candidatos = [coluna] + ALIASES_RELATORIOS.get(coluna, [])
        for c in candidatos:
            pos = idx.get(c)
            if pos is not None and pos < len(row) and row[pos] != "":
                return row[pos]
        return ""

    novos = [alvo]
    for row in valores[1:]:
        novos.append([_valor(row, c) for c in alvo])

    if ws.col_count < len(alvo):
        ws.add_cols(len(alvo) - ws.col_count)
    ws.update(range_name="A1", values=novos, value_input_option="RAW")
    # coluna conferido recém-criada: aplica a lista suspensa só nas linhas que já têm pesquisa
    if _rel_display("conferido") in alvo and _rel_display("conferido") not in idx:
        _ativar_dropdown(ws, _rel_display("conferido"), alvo, len(valores), ["sim", "N/A"])
    if remover_sobras:
        _remover_colunas_sobrando(ws, len(alvo))
    return alvo


def _verdadeiro(v):
    return str(v).strip().lower() in ("sim", "true", "verdadeiro", "1", "x")


def _int0(v):
    """Inteiro tolerante: '', texto ou lixo viram 0 (célula editada à mão não derruba o run)."""
    try:
        return int(float(str(v).strip() or 0))
    except Exception:
        return 0


def _data_iso(valor):
    """'01/07/2026' (padrão BR, dia primeiro) ou '2026-07-01' -> '2026-07-01'."""
    s = str(valor or "").strip()
    m = re.fullmatch(r"(\d{2})/(\d{2})/(\d{4})", s)
    if m:
        return f"{m.group(3)}-{m.group(2)}-{m.group(1)}"
    return s[:10]


def _drive_id(link):
    import re
    m = re.search(r"/d/([A-Za-z0-9_-]+)", link) or re.search(r"[?&]id=([A-Za-z0-9_-]+)", link)
    return m.group(1) if m else None


def _pdf_cache_path(link):
    """Caminho de cache pro PDF do link, num diretório temporário estável do runner.
    Serve pra baixar UMA vez por link e reusar entre cmd_extrair e cmd_topline (mesmo
    job) e entre tentativas, em vez de rebaixar o mesmo PDF várias vezes."""
    import hashlib, tempfile
    if not link:
        return None
    d = os.path.join(tempfile.gettempdir(), "eixo_pdf_cache")
    try:
        os.makedirs(d, exist_ok=True)
    except OSError:
        return None
    h = hashlib.sha1(str(link).encode("utf-8", "ignore")).hexdigest()[:20]
    return os.path.join(d, f"{h}.pdf")


def _baixar_pdf(link):
    """Baixa o PDF do link (com cache em disco pra não rebaixar o mesmo arquivo).
    Se for link do Google Drive, usa a API do Drive com a conta de serviço (a pasta
    precisa estar compartilhada com ela). Senão, download direto."""
    cache = _pdf_cache_path(link)
    if cache and os.path.exists(cache) and os.path.getsize(cache) > 1000:
        with open(cache, "rb") as f:
            return f.read()
    conteudo = _baixar_pdf_raw(link)
    if cache and conteudo:
        try:
            with open(cache, "wb") as f:
                f.write(conteudo)
        except OSError:
            pass
    return conteudo


def _baixar_pdf_raw(link):
    fid = _drive_id(link)
    if not fid:
        return requests.get(link, headers=HEADERS, timeout=60).content
    from google.auth.transport.requests import Request
    creds = Credentials.from_service_account_info(
        _creds_info(), scopes=["https://www.googleapis.com/auth/drive.readonly"])
    creds.refresh(Request())
    r = requests.get(
        f"https://www.googleapis.com/drive/v3/files/{fid}",
        params={"alt": "media", "supportsAllDrives": "true"},
        headers={"Authorization": f"Bearer {creds.token}"}, timeout=120)
    r.raise_for_status()
    if r.content[:4] != b"%PDF":
        raise RuntimeError("conteúdo não é PDF (a conta de serviço tem acesso ao arquivo?)")
    return r.content


PAGINAS_POR_BLOCO = 5


def _norm_registro(valor):
    return re.sub(r"[^A-Z0-9]", "", str(valor or "").upper())


def _registros_tse_texto(texto):
    return {_norm_registro(m.group(0)) for m in REGISTRO_TSE_RE.finditer(texto or "")}


def _texto_pdf_bytes(pdf_bytes, max_paginas=None):
    """Texto do PDF para validação e fallback. Vazio em PDF só imagem."""
    try:
        import fitz  # PyMuPDF
        partes = []
        with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
            total = doc.page_count if max_paginas is None else min(doc.page_count, max_paginas)
            for i in range(total):
                raw = doc.load_page(i).get_text("text") or ""
                raw = raw.replace("-\n", "").replace("\n", " ")
                raw = re.sub(r"\s{2,}", " ", raw).strip()
                if raw:
                    partes.append(raw)
        return "\n".join(partes)
    except Exception:
        return ""


def _sufixo_registro(registro_norm):
    """Número + ano, sem o prefixo de UF (últimos 9 chars: 5 dígitos + 2026).

    Pesquisa presidencial fatiada por estado às vezes é registrada só sob o
    protocolo estadual (ex.: GO-02402/2026), mesmo quando a fila guarda o
    registro com prefixo BR- e o mesmo número de sequência (BR-02402/2026).
    Todo prefixo de UF (inclusive BR) tem 2 letras, então os últimos 9
    caracteres isolam número+ano de forma confiável.
    """
    return registro_norm[-9:] if len(registro_norm) >= 9 else registro_norm


def _validar_registro_pdf(pdf_bytes, registro):
    """Retorna mensagem de erro se o PDF cita registros TSE e nenhum é o da fila."""
    registro_norm = _norm_registro(registro)
    if not registro_norm:
        return ""
    texto = _texto_pdf_bytes(pdf_bytes, max_paginas=30)
    encontrados = _registros_tse_texto(texto)
    if not encontrados or registro_norm in encontrados:
        return ""
    sufixo = _sufixo_registro(registro_norm)
    if any(_sufixo_registro(e) == sufixo for e in encontrados):
        return ""
    regs = ", ".join(sorted(encontrados))
    return f"registro da fila não aparece no PDF; registros encontrados: {regs}"


LIMITE_BYTES_BLOCO = 15_000_000   # a API do Gemini rejeita requisição inline grande demais


def _blocos_pdf(pdf_bytes, tamanho=PAGINAS_POR_BLOCO):
    """Fatia o PDF em blocos de páginas. Cada página é um slide autocontido,
    então a tabela nunca se parte entre blocos. Reduz o tamanho do bloco quando o PDF
    tem poucas páginas mas é gigante em bytes (scan em resolução muito alta, ex.: 17
    páginas / 85MB): um bloco de 5 páginas nesse caso ainda estoura o limite de
    requisição inline do Gemini (400 INVALID_ARGUMENT), mesmo já sendo "só um bloco"."""
    import io
    from pypdf import PdfReader, PdfWriter
    reader = PdfReader(io.BytesIO(pdf_bytes))
    n = len(reader.pages)
    if n:
        bytes_por_pagina = len(pdf_bytes) / n
        if bytes_por_pagina > 0:
            tamanho = max(1, min(tamanho, int(LIMITE_BYTES_BLOCO // bytes_por_pagina)))
    for ini in range(0, n, tamanho):
        writer = PdfWriter()
        for i in range(ini, min(ini + tamanho, n)):
            writer.add_page(reader.pages[i])
        buf = io.BytesIO()
        writer.write(buf)
        yield buf.getvalue()


def _migrar_topline_sem_conferida(ws):
    """Transfere a marca legada para `origem` e remove `conferida` do staging.

    A exclusão usa a API estrutural do Sheets, preservando as validações e a
    formatação das demais colunas, inclusive o checkbox `liberado`. Usada só
    pelas abas topline_pesquisas/topline_resultados (via _aba), então só
    entra em jogo se relatorios_extracao_topline_aposentado.py for reativado.
    """
    valores = ws.get_all_values()
    if not valores or "conferida" not in valores[0]:
        return

    header = valores[0]
    if "origem" not in header:
        _garantir_coluna(ws, header, "origem")
        valores = ws.get_all_values()
        header = valores[0]

    i_conferida = header.index("conferida")
    i_origem = header.index("origem")
    atualizacoes = []
    for linha, row in enumerate(valores[1:], start=2):
        origem = row[i_origem].strip() if len(row) > i_origem else ""
        legado = row[i_conferida].strip().lower() if len(row) > i_conferida else ""
        if origem:
            continue
        if "manual" in legado:
            origem = "polling_manual"
        elif legado:
            origem = "PDF (relatório do instituto)"
        else:
            # Esta aba recebe exclusivamente a extração de relatórios.
            origem = "PDF (relatório do instituto)"
        atualizacoes.append(gspread.Cell(linha, i_origem + 1, origem))

    if atualizacoes:
        ws.update_cells(atualizacoes, value_input_option="RAW")

    try:
        ws.spreadsheet.batch_update({
            "requests": [{
                "deleteDimension": {
                    "range": {
                        "sheetId": ws.id,
                        "dimension": "COLUMNS",
                        "startIndex": i_conferida,
                        "endIndex": i_conferida + 1,
                    }
                }
            }]
        })
        print(f"[migracao] {ws.title}: coluna 'conferida' removida")
    except Exception as e:
        print(f"[AVISO] não foi possível remover 'conferida' de {ws.title}: {e}")


def _aba(sh, nome, cabecalhos, manutencao=True):
    """Garante a aba e o cabeçalho. Cria a aba se não existir; escreve o
    cabeçalho se a primeira linha estiver vazia. Não mexe em dados existentes.

    ``cabecalhos`` é o dict {nome_aba: [colunas]} do chamador - cada um dos
    dois pipelines (segmentos ativo, topline aposentado) só conhece as abas
    que usa, então não dá pra fixar um dict global aqui.

    manutencao=False pula a manutenção pesada da aba 'relatorios' (split de
    linhas multicargo + reset das validações + remoção de colunas sobrando).
    Comandos que só LEEM a fila (extrair, topline) passam False pra não refazer,
    a cada invocação, um trabalho de planilha que só precisa rodar quando linhas
    novas entram (isso acontece no passo que adiciona pesquisas e no busca_fontes)."""
    header = cabecalhos[nome]
    try:
        ws = sh.worksheet(nome)
    except gspread.exceptions.WorksheetNotFound:
        ws = sh.add_worksheet(title=nome, rows=1, cols=len(header))
    if not ws.row_values(1):
        ws.update(range_name="A1", values=[header])
    elif nome == "relatorios":
        header = _normalizar_cabecalho(ws, header, remover_sobras=manutencao)
        if manutencao:
            _separar_linhas_multicargo(ws, header)
            _resetar_validacoes_relatorios(ws, header, _ultima_linha_com_registro(ws))
    elif nome in ("topline_pesquisas", "topline_resultados"):
        _normalizar_cabecalho(ws, header)
        _migrar_topline_sem_conferida(ws)
    return ws


# palavras que indicam o cargo no corpo do texto (matérias de site costumam falar
# "vaga ao Senado"/"disputa pelo Senado" em vez de "senador" literalmente) - usado
# tanto por relatorios_extracao_segmentos.py (checar se um relatório sem segmentos
# no cargo da linha realmente não tem quebra demográfica) quanto por
# relatorios_extracao_topline_aposentado.py (achar a seção do cargo num PDF grande).
PALAVRAS_CARGO = {
    "presidente": ["presiden"],
    "governador": ["governad", "governo do estado"],
    "senador": ["senador", "senado"],
}


def _blocos_ativos_cargo(pdf, cargo):
    """Gera (bloco_bytes, texto_bloco) só dos blocos de 5 páginas que pertencem à
    seção do cargo pedido. Um PDF grande organiza os cargos em seções contínuas, mas
    nem toda página de uma seção repete a palavra do cargo (tabela/gráfico sem
    legenda, rodapé de página sem relação, bloco intermediário sem título). Fica ativo
    a partir do bloco onde o cargo pedido aparece, e continua ativo enquanto os blocos
    seguintes não mencionarem OUTRO cargo monitorado; só desativa quando um outro
    cargo assume claramente a seção. Bloco inicial começa ativo (cobre PDF que já abre
    no cargo certo)."""
    from compartilhado.relatorios_topline_core import extrair_texto_pdf_bytes
    palavras_alvo = PALAVRAS_CARGO.get(str(cargo or "").lower(), [str(cargo or "").lower()])
    outros_cargos = [p for c, ps in PALAVRAS_CARGO.items() if c != cargo for p in ps]
    ativo = True
    for bloco in _blocos_pdf(pdf, tamanho=5):
        try:
            txt_bloco = extrair_texto_pdf_bytes(bloco)
        except Exception:
            txt_bloco = ""
        low_bloco = txt_bloco.lower()
        if any(p in low_bloco for p in palavras_alvo):
            ativo = True
        elif any(p in low_bloco for p in outros_cargos):
            ativo = False
        # bloco sem nenhuma palavra de cargo (imagem, rodapé, texto neutro): mantém o
        # estado do bloco anterior, não desativa nem ativa.
        if ativo:
            yield bloco, txt_bloco


def _n_paginas_pdf(pdf_bytes):
    try:
        import io
        from pypdf import PdfReader
        return len(PdfReader(io.BytesIO(pdf_bytes)).pages)
    except Exception:
        return 0
