"""
Extração headless do topline (voto estimulado por candidato) de PDFs de pesquisa.

Reaproveita a lógica da página Streamlit `5_Polling_Manual.py` (gerador-de-envios),
sem depender de Streamlit. Produz DataFrames no formato do `scraper_polling`
(pesquisas + resultados), distinguindo relatórios de instituto de lançamentos
feitos no Polling Manual.
"""

import hashlib
import json
import os
import re
import time
import unicodedata
from datetime import datetime

# fitz (PyMuPDF) e pandas são importados dentro das funções que os usam, pra este
# módulo poder ser importado só pelos helpers (sigla_uf, instituto_canonico) em
# ambientes sem essas libs.

# Este módulo é independente do pollingdata_scraper.py de propósito: ele fica
# intocado. As 3 funções abaixo são cópias pequenas do que era usado de lá.

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")
TIPOS_RESULTADO = ["candidato", "nao_valido"]

# Marca a procedência: estes dados vêm dos PDFs dos relatórios, NÃO do PollingData.
ORIGEM = "PDF (relatório do instituto)"

# Alguns registros do PesqEle usam nomes legais ou grafias históricas. Os
# aliases são aplicados antes de consultar a lista canônica e as fichas, para
# que extração e publicação usem a mesma forma das matrizes.
ALIASES_INSTITUTO_CANONICO = {
    "atlasintel": "AtlasIntel",
    "atlas intel": "AtlasIntel",
    "atlasintel bloomberg": "AtlasIntel",
    "atlas intel bloomberg": "AtlasIntel",
    "atlasintel meio norte": "AtlasIntel",
    "atlas intel meio norte": "AtlasIntel",
    "midia inteligencia em pesquisa": "Ideia Inteligência",
    # 100 Cidades é o projeto/razão exibida no PesqEle; a pesquisa é da Futura.
    "100 cidades": "Futura",
    "100 cidades participacoes ltda 100 cidades": "Futura",
    "ipespe": "Ipespe",
    "ipems": "IPEMS",
    "instituto opiniao pi": "Instituto Opinião (PI)",
    # ECM — nome legal do TSE corresponde à marca ECM Pesquisas.
    "ecm edicao comunicacao marketing ltda ecm pesquisas": "ECM Pesquisas",
    "ecm pesquisas": "ECM Pesquisas",
    # IMAPE — nome legal do TSE.
    "imape inst majoritario de pesquisas e estatisticas": "IMAPE",
}


def _slug(s: str) -> str:
    s = normalizar_texto_simples(s).lower()
    return re.sub(r"[^a-z0-9]+", "-", s).strip("-")


DISPUTA_CANDIDATO_ALIASES = {
    "luiz inacio lula da silva": "lula",
    "lula": "lula",
    "jair messias bolsonaro": "bolsonaro",
    "jair bolsonaro": "bolsonaro",
    "bolsonaro": "bolsonaro",
    "flavio bolsonaro": "flavio",
    "flavio": "flavio",
    "michelle bolsonaro": "michelle",
    "michelle": "michelle",
    "geraldo alckmin": "alckmin",
    "alckmin": "alckmin",
    "fernando haddad": "haddad",
    "haddad": "haddad",
    "ronaldo caiado": "caiado",
    "caiado": "caiado",
    "romeu zema": "zema",
    "zema": "zema",
    "joaquim barbosa": "joaquim",
    "joaquim": "joaquim",
    "tarcisio de freitas": "tarcisio",
    "tarcisio": "tarcisio",
    "ratinho junior": "ratinho",
    "ratinho jr": "ratinho",
    "ratinho": "ratinho",
}


def _norm_ascii(s: str) -> str:
    s = unicodedata.normalize("NFKD", normalizar_texto_simples(s))
    s = "".join(ch for ch in s if not unicodedata.combining(ch)).lower()
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", s)).strip()


def slug_candidato_disputa(nome: str) -> str:
    chave = _norm_ascii(nome)
    if not chave:
        return ""
    if chave in DISPUTA_CANDIDATO_ALIASES:
        return DISPUTA_CANDIDATO_ALIASES[chave]
    partes = [p for p in chave.split() if p not in {"de", "da", "do", "dos", "das", "e"}]
    if not partes:
        return _slug(chave)
    if partes[0] in {"lula", "ciro", "datena", "tabata", "marina", "simone", "michelle", "flavio"}:
        return partes[0]
    return partes[-1] if len(partes) > 1 else partes[0]


def normalizar_disputa_t2(valor: str, itens: list | None = None) -> str:
    """Gera a chave de disputa do t2 (ex.: 't2_bolsonaro-lula'), sempre com os dois
    nomes em ordem alfabética. Isso é essencial: sem ordem canônica, 'Lula x Bolsonaro'
    e 'Bolsonaro x Lula' (mesmo confronto, ordem diferente na fonte ou no texto que o
    Gemini devolveu) virariam duas disputas distintas, e a série quebraria no gráfico
    (pollingdata_scraper.py agrupa média móvel e dedup por disputa). Prioriza os candidatos
    estruturados de 'itens' (passam por slug_candidato_disputa, mais estável que confiar
    no texto livre do Gemini) e só cai pro campo 'disputa' bruto se não der pra montar
    dos itens.
    """
    nomes = []
    for item in itens or []:
        candidato = normalizar_texto_simples(item.get("candidato"))
        if not candidato or classificar_tipo_resultado(candidato, item.get("tipo", "")) == "nao_valido":
            continue
        slug = slug_candidato_disputa(candidato)
        if slug and slug not in nomes:
            nomes.append(slug)
        if len(nomes) == 2:
            break
    if len(nomes) == 2:
        a, b = sorted(nomes)
        return f"t2_{a}-{b}"

    valor = normalizar_texto_simples(valor)
    if not valor:
        return ""
    bruto = valor.lower()
    slug = _slug(bruto[3:] if bruto.startswith("t2_") else bruto)
    partes = slug.split("-")
    if len(partes) == 2:
        a, b = sorted(partes)
        return f"t2_{a}-{b}"
    return f"t2_{slug}"


def gerar_poll_id(uf, instituto, id_pesquisa, data_campo, cargo, turno, raw_block_hash, disputa=""):
    uf = str(uf).upper()
    data_campo = normalizar_texto_simples(data_campo)
    turno_key = disputa if disputa else turno
    if id_pesquisa and str(id_pesquisa).lower() not in ("sem registro", "sem_registro", "semregistro", "nan", ""):
        return f"{uf}|{cargo}|{turno_key}|{id_pesquisa}|{data_campo}"
    return f"{uf}|{cargo}|{turno_key}|{_slug(instituto)}|{data_campo}"


def gerar_scenario_id(poll_id, scenario_label):
    return f"{poll_id}|{normalizar_texto_simples(scenario_label)}"


def normalizar_data_campo_segura(valor) -> str:
    """Aceita YYYY-MM-DD ou M/D/YYYY (nunca D/M/YYYY) e devolve YYYY-MM-DD."""
    s = normalizar_texto_simples(valor)
    if not s:
        return ""
    if re.fullmatch(r"\d{4}-\d{2}-\d{2}", s):
        try:
            return datetime.strptime(s, "%Y-%m-%d").strftime("%Y-%m-%d")
        except ValueError:
            return s
    if re.fullmatch(r"\d{1,2}/\d{1,2}/\d{4}", s):
        try:
            return datetime.strptime(s, "%m/%d/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return s
    return s


MESES_PT = {
    "janeiro": 1,
    "fevereiro": 2,
    "marco": 3,
    "abril": 4,
    "maio": 5,
    "junho": 6,
    "julho": 7,
    "agosto": 8,
    "setembro": 9,
    "outubro": 10,
    "novembro": 11,
    "dezembro": 12,
}


def _data_iso_componentes(dia, mes, ano) -> str:
    """Monta uma data ISO somente quando os componentes forem válidos."""
    try:
        return datetime(int(ano), int(mes), int(dia)).strftime("%Y-%m-%d")
    except (TypeError, ValueError):
        return ""


def _data_iso_divulgacao(valor) -> str:
    """Aceita a data de divulgação brasileira (DD/MM/AAAA) ou ISO."""
    s = normalizar_texto_simples(valor)
    if re.fullmatch(r"\d{2}/\d{2}/\d{4}", s):
        try:
            return datetime.strptime(s, "%d/%m/%Y").strftime("%Y-%m-%d")
        except ValueError:
            return ""
    normalizada = normalizar_data_campo_segura(s)
    if not re.fullmatch(r"\d{4}-\d{2}-\d{2}", normalizada):
        return ""
    try:
        return datetime.strptime(normalizada, "%Y-%m-%d").strftime("%Y-%m-%d")
    except ValueError:
        return ""


def _texto_data_pdf(texto) -> str:
    texto = unicodedata.normalize("NFKD", str(texto or ""))
    texto = "".join(ch for ch in texto if not unicodedata.combining(ch))
    return re.sub(r"\s+", " ", texto).lower()


def extrair_data_campo_pdf(texto_pdf: str) -> str:
    """Extrai deterministicamente o último dia do período de campo em um PDF.

    O modelo continua responsável pela leitura das tabelas. Para a data, porém,
    a regra usa somente o trecho de metodologia/coleta do PDF: datas de capa,
    comparativos ou divulgação não podem deslocar a série temporal.
    """
    texto = _texto_data_pdf(texto_pdf)
    if not texto:
        return ""

    # Restringe a busca ao contexto de coleta. Assim "julho de 2026" no cabeçalho
    # ou em gráficos comparativos não é confundido com data de campo.
    contextos = re.findall(
        r"(?:coleta\s+de\s+dados|periodo\s+de\s+campo|trabalho\s+de\s+campo|"
        r"data\s+de\s+campo|entrevistas?\s+(?:foram\s+)?realizadas?)[^.]{0,360}",
        texto,
    )
    if not contextos:
        return ""

    meses = "|".join(MESES_PT)
    padroes = [
        # "27 e 30 de junho de 2026" ou "27 a 30 de junho 2026" (o "de" antes do
        # ano é opcional - alguns institutos, ex. Real Time Mídia, omitem).
        re.compile(
            rf"\b(?P<inicio>\d{{1,2}})\s*(?:a|ate|e|-)\s*(?P<fim>\d{{1,2}})\s+"
            rf"de\s+(?P<mes>{meses})\s+(?:de\s+)?(?P<ano>20\d{{2}})\b"
        ),
        # "29 de junho e 01 de julho de 2026".
        re.compile(
            rf"\b(?P<inicio>\d{{1,2}})\s+de\s+(?P<mes_inicio>{meses})\s*"
            rf"(?:a|ate|e|-)\s*(?P<fim>\d{{1,2}})\s+de\s+(?P<mes>{meses})\s+"
            rf"(?:de\s+)?(?P<ano>20\d{{2}})\b"
        ),
        # "27/06/2026 a 30/06/2026" ou "27 a 30/06/2026".
        re.compile(
            r"\b(?:\d{1,2}[/-])?(?:\d{1,2}[/-])?(?:20\d{2}\s*)?"
            r"(?:a|ate|e|-)\s*(?P<fim>\d{1,2})[/-](?P<mes_num>\d{1,2})[/-](?P<ano>20\d{2})\b"
        ),
    ]
    for contexto in contextos:
        for padrao in padroes:
            match = padrao.search(contexto)
            if not match:
                continue
            dados = match.groupdict()
            mes = dados.get("mes_num") or MESES_PT.get(dados.get("mes", ""))
            data = _data_iso_componentes(dados.get("fim"), mes, dados.get("ano"))
            if data:
                return data
    return ""


def resolver_data_campo_deterministica(
    data_modelo, texto_pdf: str, data_divulgacao="", data_referencia=None,
) -> tuple[str, str]:
    """Resolve a data de campo e impede a gravação de datas futuras.

    Prioridade: período explicitamente declarado no PDF, data válida devolvida
    pelo modelo e, por último, data de divulgação. A data final nunca pode ser
    posterior à data de referência nem à divulgação do próprio relatório.
    """
    data_pdf = extrair_data_campo_pdf(texto_pdf)
    data_modelo_iso = _data_iso_divulgacao(data_modelo)
    data_divulgacao_iso = _data_iso_divulgacao(data_divulgacao)

    if data_referencia is None:
        data_referencia_iso = datetime.now().strftime("%Y-%m-%d")
    elif hasattr(data_referencia, "strftime"):
        data_referencia_iso = data_referencia.strftime("%Y-%m-%d")
    else:
        data_referencia_iso = _data_iso_divulgacao(data_referencia)

    data = data_pdf or data_modelo_iso or data_divulgacao_iso
    if not data:
        return "", "data_campo ausente ou inválida; cenário retido"
    # Data futura/pós-divulgação NUNCA pode ser gravada, mas o cenário em si não é
    # lixo só porque o modelo (ou o regex de período do PDF) leu a data errada -
    # o resultado do voto continua válido. Antes isso devolvia "" e o chamador
    # descartava o cenário inteiro (raise ValueError em relatorios_extracao_topline_aposentado.py),
    # apesar do texto do aviso dizer "cenário retido" (retido = mantido, não
    # descartado). Corrigido: cai pra data de divulgação (sempre <= hoje, por
    # definição já aconteceu) em vez de jogar o cenário fora. Achado com
    # BR-07181/2026 (Quaest) zerando topline inteiro 2 rodadas seguidas por causa
    # disso (data_campo lida como 2026-07-31, futura).
    if data_referencia_iso and data > data_referencia_iso:
        if data_divulgacao_iso and data_divulgacao_iso <= data_referencia_iso:
            return data_divulgacao_iso, (
                f"data_campo {data} é futura; usada divulgação: {data_divulgacao_iso}"
            )
        return "", f"data_campo {data} é futura; cenário descartado (sem divulgação válida como alternativa)"
    if data_divulgacao_iso and data > data_divulgacao_iso:
        return data_divulgacao_iso, (
            f"data_campo {data} é posterior à divulgação {data_divulgacao_iso}; "
            f"usada divulgação: {data_divulgacao_iso}"
        )
    if data_pdf and data_pdf != data_modelo_iso:
        return data_pdf, f"data_campo corrigida pelo período do PDF: {data_pdf}"
    if not data_modelo_iso and data_divulgacao_iso:
        return data_divulgacao_iso, f"data_campo sem período; usada divulgação: {data_divulgacao_iso}"
    return data, ""


# ─────────────────────────── normalizadores ───────────────────────────

def normalizar_texto_simples(valor) -> str:
    return re.sub(r"\s+", " ", str(valor or "")).strip()


def normalizar_percentual_simples(valor):
    s = normalizar_texto_simples(valor).replace("%", "").replace(",", ".")
    if not s:
        return None
    try:
        return float(s)
    except Exception:
        return None


def normalizar_percentual_resultado(valor):
    """Normaliza percentual de candidato/nao_valido.

    O Gemini às vezes apaga a vírgula decimal quando lê tabela brasileira:
    26,1 vira 261; 64,01 vira 6401. Só corrigimos inteiros acima de 100.
    Decimais acima de 100 seguem inválidos para a validação pegar mistura de
    bases (ex.: Porcentagem válida + inválidos).
    """
    if valor is None:
        return None
    texto = str(valor).strip()
    if not texto:
        return None
    limpo = texto.replace("%", "").replace(" ", "").replace(",", ".")
    try:
        numero = float(limpo)
    except Exception:
        return None
    if numero > 100:
        inteiro_sem_decimal = re.fullmatch(r"\d+", texto.replace("%", "").replace(" ", "")) is not None
        if not inteiro_sem_decimal:
            return None
        digitos = re.sub(r"\D", "", texto)
        if not digitos:
            return None
        if int(digitos) == 1000:
            numero = 100.0
        elif len(digitos) >= 4:
            numero = float(digitos) / 100
        else:
            numero = float(digitos) / 10
    if numero < 0 or numero > 100:
        return None
    return round(numero, 2)


def normalizar_inteiro_simples(valor):
    s = re.sub(r"[^\d]", "", normalizar_texto_simples(valor))
    if not s:
        return None
    try:
        return int(s)
    except Exception:
        return None


def classificar_tipo_resultado(nome: str, tipo_informado: str = "") -> str:
    tipo = normalizar_texto_simples(tipo_informado).lower()
    if tipo in TIPOS_RESULTADO:
        return tipo
    nome_norm = normalizar_texto_simples(nome).lower()
    marcadores = ["branco", "nulo", "nulos", "ns/nr", "nsnr", "não sabe", "nao sabe",
                  "indeciso", "indecisos", "nenhum", "não válido", "nao valido",
                  "não valido", "nao válido", "não respond", "nao respond"]
    if any(tag in nome_norm for tag in marcadores):
        return "nao_valido"
    return "candidato"


def extrair_json_de_texto_bruto(texto: str) -> dict:
    bruto = (texto or "").strip()
    if not bruto:
        raise RuntimeError("O Gemini não retornou JSON.")
    bruto = re.sub(r"^```json\s*", "", bruto, flags=re.IGNORECASE)
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
    raise RuntimeError("Não localizei um objeto JSON válido na resposta do Gemini.")


# ─────────────────────────── leitura do PDF ───────────────────────────

def extrair_texto_pdf_bytes(pdf_bytes, page_indices=None) -> str:
    import fitz  # PyMuPDF
    partes = []
    with fitz.open(stream=pdf_bytes, filetype="pdf") as doc:
        pages = page_indices if page_indices is not None else list(range(doc.page_count))
        for idx in pages:
            if idx < 0 or idx >= doc.page_count:
                continue
            raw = doc.load_page(idx).get_text("text") or ""
            raw = raw.replace("-\n", "").replace("\n", " ")
            raw = re.sub(r"\s{2,}", " ", raw).strip()
            if raw:
                partes.append(raw)
    return " ".join(partes).strip()


# ─────────────────────────── Gemini ───────────────────────────

_CLIENT = None


def _gemini_client():
    global _CLIENT
    if _CLIENT is None:
        from google import genai
        key = os.environ.get("GEMINI_API_KEY", "")
        if not key:
            raise RuntimeError("GEMINI_API_KEY não definido.")
        _CLIENT = genai.Client(api_key=key)
    return _CLIENT


# Uso acumulado de tokens do Gemini nesta execução (processo novo a cada rodada do
# workflow, então não precisa resetar entre chamadas de cmd_topline).
USO_TOKENS = {"chamadas": 0, "entrada": 0, "saida": 0, "pensamento": 0}


def _registrar_uso(resp):
    meta = getattr(resp, "usage_metadata", None)
    if not meta:
        return
    USO_TOKENS["chamadas"] += 1
    USO_TOKENS["entrada"] += getattr(meta, "prompt_token_count", 0) or 0
    USO_TOKENS["saida"] += getattr(meta, "candidates_token_count", 0) or 0
    USO_TOKENS["pensamento"] += getattr(meta, "thoughts_token_count", 0) or 0


def gerar_conteudo_gemini(contents, tentativas: int = 3, backoff: float = 1.5):
    from google.genai import types
    client = _gemini_client()
    ultimo = None
    for t in range(1, tentativas + 1):
        try:
            try:
                cfg = types.GenerateContentConfig(
                    response_mime_type="application/json",
                    temperature=0,
                    thinking_config=types.ThinkingConfig(thinking_budget=4000),
                    # sem timeout a chamada pode travar indefinidamente numa falha de
                    # rede silenciosa, sem lançar exceção pro retry abaixo tratar
                    http_options=types.HttpOptions(timeout=120_000))
                resp = client.models.generate_content(model=GEMINI_MODEL, contents=contents, config=cfg)
            except Exception:
                resp = client.models.generate_content(
                    model=GEMINI_MODEL, contents=contents,
                    config=types.GenerateContentConfig(http_options=types.HttpOptions(timeout=120_000)))
            if getattr(resp, "text", None):
                _registrar_uso(resp)
                return resp
            ultimo = RuntimeError("resposta vazia")
        except Exception as e:
            ultimo = e
        if t < tentativas:
            time.sleep(backoff * (2 ** (t - 1)))
    raise RuntimeError(f"Gemini falhou após {tentativas} tentativas: {ultimo}")


NOME_UF = {
    "brasil": "BR", "acre": "AC", "alagoas": "AL", "amapá": "AP", "amapa": "AP",
    "amazonas": "AM", "bahia": "BA", "ceará": "CE", "ceara": "CE",
    "distrito federal": "DF", "espírito santo": "ES", "espirito santo": "ES",
    "goiás": "GO", "goias": "GO", "maranhão": "MA", "maranhao": "MA",
    "mato grosso": "MT", "mato grosso do sul": "MS", "minas gerais": "MG",
    "pará": "PA", "para": "PA", "paraíba": "PB", "paraiba": "PB", "paraná": "PR",
    "parana": "PR", "pernambuco": "PE", "piauí": "PI", "piaui": "PI",
    "rio de janeiro": "RJ", "rio grande do norte": "RN", "rio grande do sul": "RS",
    "rondônia": "RO", "rondonia": "RO", "roraima": "RR", "santa catarina": "SC",
    "são paulo": "SP", "sao paulo": "SP", "sergipe": "SE", "tocantins": "TO",
}


def sigla_uf(valor) -> str:
    v = normalizar_texto_simples(valor)
    if len(v) == 2:
        return v.upper()
    return NOME_UF.get(v.lower(), v.upper())


_CANONICO = None


def _canonico() -> dict:
    global _CANONICO
    if _CANONICO is None:
        caminho = "canonico.json"  # sempre relativo à raiz do repo (cwd do workflow)
        try:
            with open(caminho, encoding="utf-8") as f:
                _CANONICO = json.load(f)
        except Exception:
            _CANONICO = {"institutos": [], "presidente": [], "governador": {}, "senador": {}}
    return _CANONICO


def _referencia(cargo, uf):
    """Retorna (lista de candidatos canônicos do cargo/UF, lista de institutos canônicos)."""
    c = _canonico()
    cargo = (cargo or "").lower()
    if cargo == "presidente":
        cands = c.get("presidente", [])
    elif cargo in ("governador", "senador"):
        cands = c.get(cargo, {}).get(sigla_uf(uf), [])
    else:
        cands = []
    return cands, c.get("institutos", [])


# palavras genéricas ignoradas ao casar o nome legal do PesqEle com o canônico
_STOP_INST = {
    "de", "da", "do", "e", "ltda", "instituto", "pesquisas", "pesquisa", "consultoria",
    "me", "eireli", "opiniao", "publica", "analise", "tecnologia", "comunicacao",
    "marketing", "associados", "midia", "publicidade", "estatistica", "informacao",
    "consumidor", "dados", "eventos", "agencia",
}


def _tokens_inst(s):
    s = unicodedata.normalize("NFKD", normalizar_texto_simples(s))
    s = "".join(c for c in s if not unicodedata.combining(c)).lower()
    return [t for t in re.sub(r"[^a-z0-9 ]", " ", s).split() if t]


def instituto_canonico(nome):
    """Mapeia o nome legal do PesqEle (ex: 'REAL TIME MIDIA LTDA / REAL TIME BIG DATA')
    para o nome canônico do PollingData (ex: 'Real Time Big Data'). Se não achar, devolve
    a marca (parte após '/') em Title Case."""
    nome = normalizar_texto_simples(nome)
    if not nome:
        return ""
    alias = ALIASES_INSTITUTO_CANONICO.get(_norm_ascii(nome))
    if alias:
        return alias
    alvo = set(_tokens_inst(nome))
    melhor, tam = None, 0
    for canon in _canonico().get("institutos", []):
        sig = [t for t in _tokens_inst(canon) if t not in _STOP_INST] or _tokens_inst(canon)
        if sig and all(t in alvo for t in sig) and len(canon) > tam:
            melhor, tam = canon, len(canon)   # prefere o canônico mais específico
    return melhor or (nome.split("/")[-1].strip() or nome).title()


_FICHAS = None


def _fichas_institutos():
    """Carrega institutos_fichas.json uma vez. Vazio se o arquivo não existir."""
    global _FICHAS
    if _FICHAS is None:
        try:
            caminho = "institutos_fichas.json"  # sempre relativo à raiz do repo (cwd do workflow)
            with open(caminho, encoding="utf-8") as f:
                _FICHAS = json.load(f).get("institutos", [])
        except Exception:
            _FICHAS = []
    return _FICHAS


def ficha_instituto(nome):
    """Se o instituto casa com uma ficha conhecida, devolve o bloco de texto pro prompt
    (estrutura específica daquele instituto). Senão, ''. Casa por palavras: todas as
    palavras de 'match' precisam aparecer no nome normalizado (minúsculo, sem acento)."""
    alvo = _norm_ascii(instituto_canonico(nome))
    if not alvo:
        return ""
    for f in _fichas_institutos():
        termos = [str(t).lower().strip() for t in f.get("match", []) if str(t).strip()]
        excluir = [str(t).lower().strip() for t in f.get("exclude", []) if str(t).strip()]
        if termos and all(t in alvo for t in termos) and not any(x in alvo for x in excluir):
            return (f"ESTRUTURA CONHECIDA DESTE INSTITUTO ({f.get('nome','')}) — SIGA À RISCA:\n"
                    f"{f.get('ficha','')}\n\n")
    return ""


def extrair_dados_polling_gemini(texto_fonte: str, url_original: str = "",
                                 escopo: dict | None = None,
                                 pdf_bytes: bytes | None = None) -> dict:
    """Extrai via Gemini.

    Quando o texto extraído já é suficiente, envia só texto para evitar payload
    pesado de PDF/notícia. O PDF visual fica restrito a material escaneado ou com
    texto insuficiente.
    """
    escopo = escopo or {}
    # instituto NÃO entra como restrição (é sempre um só no relatório); serve só pra
    # puxar a ficha de estrutura conhecida daquele instituto.
    bloco_ficha = ficha_instituto(escopo.get("instituto"))
    restricoes = []
    for chave, rotulo in (("cargo", "cargo"), ("uf", "uf"), ("turno", "turno")):
        val = normalizar_texto_simples(escopo.get(chave))
        if val:
            restricoes.append(f"- {rotulo} = {val}")
    bloco = ""
    if restricoes:
        bloco = ("FOCO DA EXTRAÇÃO (restrições obrigatórias):\n" + "\n".join(restricoes) +
                 "\nExtraia APENAS o bloco que casa com essas restrições. Ignore outros estados, "
                 "cargos ou turnos que apareçam no material.\nSe não houver bloco que case, "
                 "retorne cenarios=[].\n\n")

    # O mesmo relatório pode trazer, por exemplo, o registro estadual no TRE e o
    # nacional no TSE. A linha da fila já separa cargo/registro; deixar isso explícito
    # evita que o modelo trate os dois códigos como pesquisas duplicadas ou misture
    # cenários destinados a linhas diferentes.
    registro_fila = normalizar_texto_simples(escopo.get("registro_tse"))
    if registro_fila:
        bloco += (f"REGISTRO DE DESTINO DA FILA: {registro_fila}. Um mesmo PDF pode listar "
                  "mais de um registro (por exemplo, TRE estadual e TSE nacional). Isso NÃO "
                  "significa que você deve criar cópias nem trocar este registro pelo outro: "
                  "extraia apenas o cargo/turno do FOCO e mantenha o registro de destino.\n\n")

    # uf_referencia: só contexto, NÃO é restrição (diferente de "uf" acima), e NÃO é
    # confiável sozinha. Institutos costumam registrar uma amostra estadual sob um
    # protocolo com prefixo BR-, e a fila herda essa classificação (por prefixo do
    # registro, não pela amostra real) do PesqEle/TSE. Por isso o documento sempre
    # manda mais que essa referência.
    uf_ref = normalizar_texto_simples(escopo.get("uf_referencia"))
    if uf_ref:
        bloco += (f"CONTEXTO (não é filtro, e pode estar ERRADO): a fila classifica esta pesquisa "
                   f"com abrangência '{uf_ref}'. Essa classificação vem do prefixo do registro TSE "
                   "(BR- não garante amostra nacional; institutos frequentemente registram pesquisa "
                   "de UM estado sob protocolo BR-), então NÃO confie nela sozinha. Para decidir a UF "
                   "de cada cenário de presidente, leia o que o PRÓPRIO documento diz sobre o universo "
                   "pesquisado/amostra (ex.: 'entrevistas realizadas em [estado]', 'universo: eleitores "
                   "de [estado]'). Se o documento disser que a amostra é de um estado específico, use a "
                   "UF desse estado nesse cenário, mesmo que a referência acima diga BRASIL. Só use BR "
                   "quando o próprio documento indicar amostra nacional.\n\n")

    cands, institutos = _referencia(escopo.get("cargo"), escopo.get("uf"))
    bloco_ref = ""
    if cands:
        bloco_ref += ("CANDIDATOS CANÔNICOS deste cargo/UF (quando o candidato do relatório for "
                      "um destes, use EXATAMENTE o mesmo nome curto e a mesma sigla; o texto entre "
                      "parênteses é o partido):\n" + "\n".join(cands) + "\n\n")
    if institutos:
        bloco_ref += ("INSTITUTOS CANÔNICOS (quando o instituto do relatório corresponder a um "
                      "destes, use EXATAMENTE este nome):\n" + ", ".join(institutos) + "\n\n")

    prompt = f"""
Você recebe o texto completo de um PDF de uma pesquisa eleitoral brasileira.
Extraia os dados estruturados para inserção em planilha.

{bloco}{bloco_ficha}{bloco_ref}REGRAS:
- Responda somente com JSON válido.
- Não invente dados ausentes. Use string vazia ou null.
- Datas devem sair em YYYY-MM-DD quando possível.
- data_campo: se o relatório trouxer um PERÍODO de campo (ex.: '11 a 24 de junho de 2026'),
  use o ÚLTIMO dia do período (2026-06-24), não o primeiro nem a data de divulgação. Só use
  a data de divulgação se não houver período de campo declarado. Essa é a mesma convenção
  usada no resto do sistema (extrair_ultima_data); usar outra data quebra a posição do ponto
  na série temporal da média móvel. Exemplo: '01 a 05/07/2026' gera 2026-07-05, mesmo que o
  PDF tenha sido divulgado ou extraído no dia 06.
- cargo deve ser governador, senador ou presidente.
- turno deve ser t1 ou t2.
- uf deve estar em caixa alta. Para presidente nacional use BR. Para presidente medido só
  entre os eleitores de um estado específico (não uma amostra nacional), use a UF desse
  estado, não BR. Decida pelo que o documento diz sobre o universo/amostra pesquisado,
  NUNCA só pelo prefixo do registro TSE (um registro BR- pode ser amostra de um único
  estado; veja CONTEXTO acima quando houver).
- percentual deve ser numérico, sem %. Preserve os números EXATAMENTE como no material;
  não arredonde, não recalcule, não normalize para somar 100. Converta vírgula decimal
  para ponto: 26,1% -> 26.1; 50,3% -> 50.3; 64,01% -> 64.01. NUNCA remova a vírgula
  transformando 26,1 em 261 ou 50,3 em 503.
- Quando uma tabela trouxer as colunas "Porcentual" e "Porcentagem válida", escolha UMA
  base só. Como o JSON consolida branco/nulo/NS/NR em "Não válido", use a coluna
  "Porcentual" para candidatos E inválidos. Não misture "Porcentagem válida" dos
  candidatos com "Porcentual" dos inválidos. Só use "Porcentagem válida" se não houver
  nenhum item "Não válido" no cenário.
- CLASSIFICAÇÃO DE TURNO: use t2 quando a própria pergunta, título ou cabeçalho da
  tabela/gráfico disser explicitamente 'segundo turno', '2º turno', '2° turno' ou equivalente
  inequívoco. Além disso, EXCLUSIVAMENTE para presidente e governador (únicos cargos com 2º
  turno de verdade no sistema eleitoral brasileiro), uma tabela estimulada com EXATAMENTE dois
  nomes de candidato (mais NH/BR/NULO e NS/NR, sem nenhum outro candidato) é t2 mesmo sem menção
  explícita — é a forma mais comum de reportar simulação de 2º turno. NÃO aplique essa contagem
  para senador (nunca tem 2º turno, ver regra própria abaixo) nem para uma tabela de
  rejeição/aprovação/comparação entre dois nomes que não seja pergunta de intenção de voto.
- O FOCO DE TURNO acima é obrigatório: no foco t1, inclua os cenários estimulados de primeiro
  turno, inclusive confrontos de dois nomes que não tenham menção explícita a segundo turno; no
  foco t2, retorne cenarios=[] se o documento não trouxer uma simulação explicitamente chamada
  de segundo turno para aquele cargo. Não use inferência eleitoral para completar confrontos.
- PROIBIDO INVENTAR CENÁRIO OU CONFRONTO: cada cenário/confronto que você devolver tem que
  corresponder a UMA tabela ou gráfico específico e localizável no material, com todo
  candidato e todo percentual lidos DAQUELE gráfico - nunca complete, deduza ou "lembre" de
  outro cenário a partir de conhecimento geral sobre política brasileira (nomes de eleições
  passadas, pesquisas de outros institutos, senso comum sobre quem tende a enfrentar quem).
  Se o material só trouxer 1 confronto de 2º turno pro cargo, devolva 1 cenário - NUNCA
  multiplique isso em vários confrontos hipotéticos contra outros nomes só porque são
  presidenciáveis conhecidos. Isso já causou erro grave e confirmado: um relatório com um
  único confronto real "Lula x Flávio Bolsonaro" (24%/67%) gerou também confrontos
  fictícios contra Ciro Gomes, Fernando Haddad, Simone Tebet e outros nomes que SEQUER
  APARECEM no documento - o elenco inteiro da eleição de 2022, vindo da memória do
  modelo, não do PDF.
- ARMADILHA "BOLSONARO": Jair Bolsonaro está INELEGÍVEL para 2026 (condenado, banido até
  2030) e não é candidato em nenhuma pesquisa deste ciclo. Se o material mencionar um
  "Bolsonaro" candidato de verdade, quase sempre é Flávio Bolsonaro ou Michelle Bolsonaro
  (nunca Jair) - confira o nome completo na tabela/gráfico da pergunta de voto antes de
  gravar, nunca assuma "Bolsonaro" = Jair Bolsonaro. Uma matéria sobre um desentendimento
  familiar entre Flávio e Michelle Bolsonaro (ex.: "vídeo pedindo desculpas") é conteúdo
  temático/reativo, NÃO é pergunta de voto - não vira cenário nem confronto de jeito nenhum.
- Para t2, cada confronto direto deve ser um cenário separado e deve preencher 'disputa'
  no formato t2_candidato1-candidato2, em minúsculas, sem acento, usando nomes curtos,
  SEMPRE em ordem alfabética dos dois nomes (não pela ordem que aparecem no relatório)
  (ex.: t2_flavio-lula, t2_lula-zema, t2_caiado-lula). Isso é obrigatório: o mesmo
  confronto relatado como "Lula x Flávio" ou "Flávio x Lula" tem que gerar a
  MESMA disputa, senão o gráfico de segundo turno quebra em duas séries.
- tipo deve ser candidato ou nao_valido.
- PADRONIZAÇÃO DE NOMES: se o candidato corresponder a um da lista canônica acima, use o nome
  curto e a sigla EXATAMENTE como lá (campo 'candidato' = a parte antes do parêntese; 'partido' =
  a sigla dentro do parêntese). Se não estiver na lista, use o nome curto usual + a sigla do partido.
- 'partido' é SEMPRE a SIGLA curta em caixa alta (PT, PL, MDB, REP, UNIAO, PSD...), nunca o nome
  por extenso (use REP, não 'Republicanos'; UNIAO, não 'União Brasil'). Sem partido: 'SEM PARTIDO'.
- INSTITUTO: se corresponder a um da lista canônica, use o nome exato de lá.
- INVÁLIDOS: consolide TODOS os votos inválidos (branco, nulo, indeciso, não sabe, não respondeu,
  ns/nr) em UM ÚNICO item por cenário: candidato='Não válido', partido='', tipo='nao_valido',
  percentual = a soma deles.
- modo é o método de coleta (ex.: Presencial, Telefônica (CATI), Online, Misto). Vazio se não houver.
- metodologia é a descrição da metodologia conforme reportada (plano amostral, universo, técnica). Texto livre.
- Extraia SOMENTE cenários de voto ESTIMULADO (aqueles em que os nomes dos candidatos são
  apresentados ao entrevistado). NÃO inclua voto ESPONTÂNEO (sem lista de nomes), nem rejeição,
  nem avaliação/aprovação de governo. Cada cenário estimulado vira um item de "cenarios".
- IGNORE páginas de SÍNTESE/RESUMO/DESTAQUE (capa de capítulo, "principais leituras",
  "síntese", cards de highlight com 1 ou 2 números grandes tipo "36% × 36%"): elas repetem
  números de cenários que já aparecem completos em outra página, e extraí-las cria cenários
  fragmentados/duplicados. Só extraia da tabela ou gráfico COMPLETO, com a lista de candidatos.
- LISTA SEM CONTEXTO NÃO É CENÁRIO: se aparecer uma lista de nomes+percentuais SEM pergunta ou
  título identificável (ex.: continuação de uma tabela que começou fora do trecho que você
  recebeu), NÃO invente um cenário pra ela - pode ser o final de uma tabela de REJEIÇÃO ou de
  outra pergunta. Ignore listas órfãs; só extraia quando conseguir identificar qual pergunta a
  tabela responde.
- Se o relatório trouxer uma pergunta filtro como "O candidato que você votaria é um desses
  nomes ou outro candidato?" e também uma pergunta formal como "Se a eleição fosse hoje...",
  extraia a pergunta formal de intenção de voto. A pergunta filtro só entra se for a única
  medição estimulada daquele cargo/turno.
- votos_por_entrevistado: use 2 quando o relatório indicar que o entrevistado podia citar/votar
  em até 2 nomes naquele cenário (comum para senador em eleição com 2 vagas; procure notas como
  'cada entrevistado poderia citar até 2 candidatos'), E os percentuais daquele cenário somarem
  perto de 200% (cada nome citado conta separado, sem dividir por tabela de 1º/2º voto). Caso
  contrário, use 1.
- SENADOR COM 1º/2º VOTO SEPARADOS: se o relatório trouxer tabelas separadas para senador de
  2 vagas ("1º voto", "2º voto" e/ou "média do 1º e 2º voto"), crie um cenário SEPARADO para
  cada uma que existir, preservando o dado bruto exatamente como publicado. Use scenario_label/
  descricao que deixe claro qual é qual (ex.: "Senado 1º voto", "Senado 2º voto", "Senado média
  1º/2º"), votos_por_entrevistado=1 em cada (cada tabela soma ~100% sozinha). NÃO calcule média,
  NÃO some, NÃO junte as tabelas, e NUNCA trate "2º voto para senador" como segundo turno (turno
  é sempre t1 nesses casos; 2º voto de senado NÃO é 2º turno).
- SENADOR NUNCA TEM SEGUNDO TURNO: eleição de senador no Brasil não tem 2º turno (decide por
  maioria simples no 1º turno). Mesmo que o relatório traga uma pergunta chamada "simulação de
  2º turno para Senador" ou um confronto direto de dois nomes para o cargo de senador, IGNORE
  essa pergunta por completo, não crie cenário nenhum para ela. Isso vale só para senador; para
  presidente e governador, confronto direto de 2º turno continua sendo t2 normalmente.

FORMATO:
{{
  "cargo": "", "turno": "", "uf": "", "instituto": "", "registro_tse": "",
  "data_campo": "", "amostra": null, "margem_erro": null, "confianca": null,
  "modo": "", "metodologia": "", "fonte_url_original": "{url_original}",
  "observacoes": "", "pendencias": [],
  "cenarios": [
    {{ "scenario_label": "", "descricao": "", "disputa": "", "votos_por_entrevistado": 1,
       "itens": [ {{ "candidato": "", "partido": "", "percentual": null, "tipo": "candidato" }} ] }}
  ]
}}
""".strip()

    texto_fonte = (texto_fonte or "").strip()
    # Nunca decide "só texto basta" só pelo tamanho: relatório com muita imagem/gráfico
    # tem texto extraído comprido (rodapé legal repetido em toda página) mas ZERO dado
    # de verdade (candidato/percentual só existe no gráfico); e tabela cruzada (região x
    # candidato) vira uma sequência linear de números no texto, sem estrutura de
    # linha/coluna, que o modelo pode desalinhar. Mandar o PDF junto sempre que tiver
    # (o modelo cruza texto com o visual) resolve os dois casos; só cai pra texto puro
    # quando não há pdf_bytes.
    if pdf_bytes:
        from google.genai import types
        if texto_fonte:
            texto_msg = (f"\n\nTEXTO EXTRAÍDO DO PDF/PÁGINA:\n{texto_fonte}\n\n"
                         "PDF ANEXO: confira o visual (tabelas e gráficos). O texto extraído pode "
                         "não conter os números (relatório com dado só em gráfico) ou desalinhar "
                         "tabela cruzada (linha x coluna); nesses casos, confie no PDF, não no texto.")
        else:
            texto_msg = "\n\nO material é o PDF anexo (leia as páginas, inclusive tabelas e gráficos)."
        contents = [prompt + texto_msg, types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")]
    elif texto_fonte:
        contents = prompt + f"\n\nTEXTO FONTE:\n{texto_fonte}"
    else:
        raise RuntimeError("Passe texto_fonte ou pdf_bytes.")

    resp = gerar_conteudo_gemini(contents)
    payload = extrair_json_de_texto_bruto(getattr(resp, "text", "") or "")
    return normalizar_payload_polling(payload)


def normalizar_payload_polling(payload: dict) -> dict:
    payload = payload or {}
    cenarios_norm = []
    for idx, cenario in enumerate(payload.get("cenarios") or [], start=1):
        label = normalizar_texto_simples(cenario.get("scenario_label") or cenario.get("cenario") or idx)
        itens_norm = []
        for item in (cenario.get("itens") or cenario.get("resultados") or []):
            candidato = normalizar_texto_simples(
                item.get("candidato") or item.get("nome") or item.get("opcao") or item.get("candidato_partido"))
            partido = normalizar_texto_simples(item.get("partido"))
            percentual = normalizar_percentual_resultado(item.get("percentual"))
            tipo = classificar_tipo_resultado(candidato, item.get("tipo", ""))
            if not candidato and percentual is None:
                continue
            itens_norm.append({"candidato": candidato, "partido": partido,
                               "percentual": percentual, "tipo": tipo})
        votos = normalizar_inteiro_simples(cenario.get("votos_por_entrevistado")) or 1
        cenarios_norm.append({"scenario_label": label or str(idx),
                              "descricao": normalizar_texto_simples(cenario.get("descricao") or cenario.get("titulo")),
                              "disputa": normalizar_texto_simples(cenario.get("disputa")),
                              "votos_por_entrevistado": max(1, min(votos, 3)),
                              "itens": itens_norm})
    return {
        # Sem fallback pra "governador": um cargo vazio tem que continuar vazio, senão
        # cai num default arbitrário e o cenário é gravado sob o cargo errado (ex.: um
        # payload de presidente/senador em que o Gemini deixou "cargo" em branco virava
        # "governador" silenciosamente). cmd_topline valida cargo contra o esperado da
        # linha da fila e descarta o que não bater.
        "cargo": normalizar_texto_simples(payload.get("cargo")).lower(),
        "turno": normalizar_texto_simples(payload.get("turno")).lower() or "t1",
        "uf": normalizar_texto_simples(payload.get("uf")).upper() or "BR",
        "instituto": normalizar_texto_simples(payload.get("instituto")),
        "registro_tse": normalizar_texto_simples(payload.get("registro_tse")),
        "data_campo": normalizar_texto_simples(payload.get("data_campo")),
        "amostra": normalizar_inteiro_simples(payload.get("amostra")),
        "margem_erro": normalizar_percentual_simples(payload.get("margem_erro")),
        "confianca": normalizar_inteiro_simples(payload.get("confianca")),
        "modo": normalizar_texto_simples(payload.get("modo")),
        "metodologia": normalizar_texto_simples(payload.get("metodologia")),
        "fonte_url_original": normalizar_texto_simples(payload.get("fonte_url_original")),
        "observacoes": normalizar_texto_simples(payload.get("observacoes")),
        "pendencias": payload.get("pendencias") or [],
        "cenarios": cenarios_norm or [{"scenario_label": "1", "descricao": "", "itens": []}],
    }


# ─────────────────────── payload -> DataFrames ───────────────────────

def montar_dataframes_polling(payload: dict, fonte_url: str, fonte_url_original: str = "",
                              instituto_fonte: str = "") -> tuple:
    import pandas as pd
    cargo = normalizar_texto_simples(payload.get("cargo")).lower()
    turno = normalizar_texto_simples(payload.get("turno")).lower()
    uf = sigla_uf(payload.get("uf"))   # PesqEle manda "ALAGOAS"; polling usa "AL"
    # instituto vem do PesqEle (fila), normalizado pro canônico; alguns relatórios só
    # trazem o nome no logo, então não dá pra confiar no que o Gemini leu do PDF.
    instituto = instituto_canonico(instituto_fonte or payload.get("instituto"))
    registro_tse = normalizar_texto_simples(payload.get("registro_tse"))
    data_campo = normalizar_data_campo_segura(normalizar_texto_simples(payload.get("data_campo")))
    amostra = normalizar_inteiro_simples(payload.get("amostra"))
    margem_erro = normalizar_percentual_simples(payload.get("margem_erro"))
    confianca = normalizar_inteiro_simples(payload.get("confianca"))
    horario_raspagem = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    ano_calc = datetime.now().year
    if data_campo and len(data_campo) >= 4 and data_campo[:4].isdigit():
        try:
            ano_calc = int(data_campo[:4])
        except ValueError:
            pass

    modo_payload = normalizar_texto_simples(payload.get("modo"))
    metodologia = normalizar_texto_simples(payload.get("metodologia"))

    registro_base = registro_tse
    if registro_base.lower() in {"sem registro", "sem_registro", "semregistro"}:
        registro_base = ""

    block_hash = hashlib.sha1(
        f"manual|{uf}|{cargo}|{turno}|{instituto}|{registro_tse}|{data_campo}".encode("utf-8", errors="ignore")
    ).hexdigest()[:10]

    fonte_url_original_final = normalizar_texto_simples(fonte_url_original) or normalizar_texto_simples(
        payload.get("fonte_url_original"))
    classificacao = ""   # preenchida no roteamento (parte 2), a partir do scraper_polling

    pesquisas_rows, resultados_rows = [], []
    for idx, cenario in enumerate(payload.get("cenarios") or [], start=1):
        disputa = ""
        if turno == "t2":
            disputa = normalizar_disputa_t2(cenario.get("disputa"), cenario.get("itens"))
        poll_id = gerar_poll_id(uf, instituto, registro_base, data_campo, cargo, turno, block_hash, disputa=disputa)
        fonte_url_final = normalizar_texto_simples(fonte_url) or f"pdf://relatorio/{poll_id}"
        # No t2, a disputa já diferencia cada confronto; o PollingData usa NA como
        # cenário. No t1, mantém cenários numéricos para média entre cenários.
        scenario_label = "NA" if turno == "t2" and disputa else str(idx)
        # prioriza a descricao (texto que diz QUAL cenário é) sobre o scenario_label
        # (frequentemente só "1"/"2"); a ordem invertida enchia a planilha de
        # descricao="1", inútil pra conferência humana. Descarta rótulo puramente
        # numérico como descricao.
        desc_label = normalizar_texto_simples(cenario.get("scenario_label"))
        descricao = normalizar_texto_simples(cenario.get("descricao")) or \
            (desc_label if not desc_label.isdigit() else "")
        scenario_id = gerar_scenario_id(poll_id, scenario_label)

        pesquisas_rows.append({
            "origem": ORIGEM,
            "scenario_id": scenario_id, "poll_id": poll_id, "ano": ano_calc, "uf": uf,
            "cargo": cargo, "turno": turno, "disputa": disputa, "instituto": instituto,
            "classificacao_instituto": classificacao, "registro_tse": registro_tse,
            "data_campo": data_campo, "modo": modo_payload, "amostra": amostra,
            "margem_erro": margem_erro, "confianca": confianca,
            "scenario_label": scenario_label, "descricao": descricao,
            "votos_por_entrevistado": cenario.get("votos_por_entrevistado", 1),
            "fonte_url": fonte_url_final, "fonte_url_original": fonte_url_original_final,
            "horario_raspagem": horario_raspagem,
            "metodologia": metodologia,
        })

        def _linha_resultado(candidato, partido, candidato_partido, tipo, percentual):
            resultados_rows.append({
                "origem": ORIGEM,
                "scenario_id": scenario_id, "poll_id": poll_id, "ano": ano_calc, "uf": uf,
                "cargo": cargo, "turno": turno, "disputa": disputa, "data_campo": data_campo, "instituto": instituto,
                "classificacao_instituto": classificacao, "registro_tse": registro_tse,
                "scenario_label": scenario_label, "candidato": candidato, "partido": partido,
                "candidato_partido": candidato_partido, "tipo": tipo, "percentual": percentual,
                "fonte_url": fonte_url_final, "horario_raspagem": horario_raspagem,
            })

        invalido, tem_invalido = 0.0, False
        for item in cenario.get("itens") or []:
            candidato = normalizar_texto_simples(item.get("candidato"))
            partido = normalizar_texto_simples(item.get("partido"))
            percentual = normalizar_percentual_resultado(item.get("percentual"))
            if percentual is None:
                continue
            if classificar_tipo_resultado(candidato, item.get("tipo", "")) == "nao_valido":
                invalido += percentual          # consolida todos os inválidos
                tem_invalido = True
                continue
            if not candidato:
                continue
            cp = f"{candidato} ({partido})" if partido else candidato
            _linha_resultado(candidato, partido, cp, "candidato", percentual)
        if tem_invalido:
            _linha_resultado("Não válido", "", "Não válido", "nao_valido", round(invalido, 1))

    return pd.DataFrame(pesquisas_rows), pd.DataFrame(resultados_rows)
