"""
Transcrição de debate eleitoral com identificação de falante, via Gemini.

Whisper e afins separam voz mas devolvem SPEAKER_00, SPEAKER_01, e alguém
precisa dizer depois qual é qual. Aqui o modelo lê o conteúdo do debate e
nomeia direto: o mediador chama o candidato pelo nome, os candidatos se
dirigem um ao outro, cada um cita o próprio histórico. O preço é que ele
erra na troca rápida de turno, e por isso a saída traz DESCONHECIDO em vez
de chute, e o log mostra bloco a bloco onde a identificação afrouxou.

O áudio vai em blocos de 10 min e não inteiro: em arquivo longo o modelo
começa a resumir em vez de transcrever, e o corte é o que segura isso.

A fila vem da planilha "Mapeamento de Debates", no drive compartilhado
Eleições 2026. Quem quiser transcrever um debate cola o link do YouTube lá e
põe o status em 'pendente'; o script devolve os links da transcrição na
própria linha.

No Drive cada debate cai em Debates/UF/ano-mês/dia, com transcrição, csv,
bruto e mp3 juntos:

    Debates/SP/2026-08/09/2026-band-sp-gov-t1.txt

As colunas de eixo, tema e termos não vêm do modelo: são dicionário de termos
(outros/assuntos_debates.py). Quando o dicionário muda, o --remarcar reaplica
sobre o CSV que já está no Drive, no mesmo arquivo e sem custo de API. Só a
transcrição em si custa dinheiro.

Uso:
    python -m outros.transcricao_debates --fila
    python -m outros.transcricao_debates --fila --id 2026-band-sp-gov-t1
    python -m outros.transcricao_debates --remarcar --id 2026-band-sp-gov-t1
    python -m outros.transcricao_debates --remarcar --id 2026-band-mg-gov-t1 --cortar-antes 00:50:00
    python -m outros.transcricao_debates --url "https://youtube.com/watch?v=..."
    python -m outros.transcricao_debates --audio debate.mp3 --inicio 00:30:00 --duracao 00:10:00
"""

import argparse
import csv
import http.client
import os
import re
import shutil
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

from google import genai
from google.genai import types

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-3.6-flash")

# Drive compartilhado "Eleições 2026" > Debates, e a planilha de mapeamento que
# fica dentro dela. Sem valor padrão no código: este repo é público, e o resto
# dele já guarda id de planilha em secret.
PASTA_DRIVE = os.getenv("PASTA_DRIVE_DEBATES", "").strip()
PLANILHA = os.getenv("SPREADSHEET_ID_DEBATES", "").strip()
ABA = "debates"

ESCOPOS = [
    "https://www.googleapis.com/auth/drive",
    "https://www.googleapis.com/auth/spreadsheets",
]

# Aba 'Debates' da planilha [interno] ELEIÇÕES 2026, mantida pelo monitoramento.
# É de lá que vem o calendário; aqui ninguém digita debate na mão.
FONTE_PLANILHA = os.getenv("SPREADSHEET_ID_INTERNO", "").strip()
FONTE_ABA = os.getenv("ABA_FONTE_DEBATES", "Debates")

# Ordem das colunas da aba 'debates'. Mexeu na planilha, mexe aqui.
COL = {
    "id": 0, "data": 1, "horario": 2, "cargo": 3, "uf": 4, "turno": 5,
    "emissora": 6, "url_youtube": 7, "mediador": 8, "participantes": 9,
    "status": 10, "link_resumo": 11, "link_transcricao": 12, "link_csv": 13,
    "link_audio": 14, "processado_em": 15, "observacoes": 16, "id_fonte": 17,
}
ORDEM_COLUNAS_LOGICA = list(COL.keys())
PASTA_MIME = "application/vnd.google-apps.folder"
# Onde o script escreve de volta: link_resumo até observacoes.
FAIXA_SAIDA = "L{i}:Q{i}"

# Campos que vêm da fonte, na ordem em que ficam na nossa planilha (B até J).
DA_FONTE = ["data", "horario", "cargo", "uf", "turno", "emissora",
            "url_youtube", "mediador", "participantes"]
FAIXA_FONTE = "B{i}:J{i}"

BLOCO_SEG = 600          # 10 min de conteúdo por bloco
SOBREPOSICAO_SEG = 20    # o bloco vai 20s além, para não cortar frase na borda
TENTATIVAS = 5
ESPERA_BASE = 15         # 1a espera, dobrando a cada tentativa
ESPERA_MAX = 120
# Pausa antes da repescagem dos blocos vazios. A onda de 503 dura minutos, e
# repescar na hora cai nela de novo.
ESPERA_REPESCAGEM = 90

# Chamada de Drive e de planilha também cai sozinha, e cai perto do fim: no run
# de 14/08 a transcrição inteira ficou pronta e o upload morreu com "EOF
# occurred in violation of protocol", com a escrita do status logo depois
# levando "Connection reset by peer". Repetir isso é barato, ao contrário do
# bloco do modelo, então a espera é curta e a conta é outra.
TENTATIVAS_REDE = 4
ESPERA_REDE = 5

# MINIMAL, LOW, MEDIUM, HIGH, um número de tokens de orçamento (0 desliga) ou
# vazio para deixar o padrão do modelo. O 3.6-flash aceitou MINIMAL e ignorou:
# gastou ~48 mil tokens de raciocínio por bloco do mesmo jeito. Fica nas duas
# formas porque orçamento e nível são campos diferentes na API.
RACIOCINIO = os.getenv("GEMINI_THINKING", "MINIMAL").strip().upper()
# Vira True se o modelo recusar o campo, para não repetir 400 em cada bloco.
SEM_THINKING_CONFIG = False

# Palavras por minuto abaixo disso é sinal de que o modelo resumiu em vez de
# transcrever. Fala corrida em português fica entre 130 e 160; medido no debate
# Band SP de 09/08, num trecho de 10 min, deu 185, que é ritmo de bate-boca.
#
# A primeira versão contava turnos por minuto e disparava alarme falso: naquele
# mesmo trecho deram 2,6 turnos/min, porque réplica e tréplica são longas. Ritmo
# de fala não depende do formato do bloco; contagem de turnos depende.
PALAVRAS_POR_MIN_SUSPEITO = 80.0

# Preço da API por 1 milhão de tokens, em dólar, para o modelo pago. Debate de
# 2h dá ~12 blocos e cada bloco reenvia o áudio inteiro do trecho, então a
# entrada é quase toda áudio: sem essa conta ninguém sabe quanto custou até a
# fatura chegar. Modelo fora da tabela transcreve igual, só não estima gasto.
PRECO_POR_MILHAO = {
    "gemini-3.6-flash": (0.75, 3.75),
    "gemini-2.5-flash": (0.30, 2.50),
}
# Só para dar a ordem de grandeza em real no log. Não é câmbio do dia nem a
# taxa que o Google usou na fatura; o número em dólar é o que vale.
USD_BRL = float(os.getenv("USD_BRL", "5.40"))


def custo(uso):
    """Devolve (dólar, texto) do uso acumulado. Dólar é None se o modelo
    não estiver na tabela de preço."""
    preco = PRECO_POR_MILHAO.get(GEMINI_MODEL)
    # O ponto de milhar sai do format e é trocado em cada número, e não na
    # frase pronta: no texto inteiro o replace comeria a vírgula da lista.
    milhar = lambda v: f"{v:,}".replace(",", ".")
    tokens = f"{milhar(uso['in'])} tokens de entrada, {milhar(uso['out'])} de saída"
    if not preco:
        return None, f"{tokens} (sem preço em tabela para {GEMINI_MODEL})"
    dolar = uso["in"] / 1e6 * preco[0] + uso["out"] / 1e6 * preco[1]
    return dolar, f"US$ {dolar:.2f} (~R$ {dolar * USD_BRL:.2f}), {tokens}"

CONTEXTO_PADRAO = """\
Debate eleitoral brasileiro das eleições de 2026.

Participantes: mediador, candidatos e jornalistas da bancada.
Marque jornalista da bancada como JORNALISTA quando não der para saber o nome."""

PROMPT = """Você está transcrevendo o áudio de um debate eleitoral brasileiro.

{contexto}

Transcreva TODO o áudio, do início ao fim, palavra por palavra. Não resuma, não
parafraseie, não pule trecho nenhum. Se um trecho estiver inaudível, escreva
[inaudível] naquele ponto e siga em frente.

Formato de cada linha, sem nenhum texto fora disso:
[MM:SS] NOME DO FALANTE: fala

Regras:
- NOME DO FALANTE em caixa alta, usando a lista de participantes acima.
- Identifique quem fala pelo conteúdo: o mediador chama o candidato pelo nome,
  os candidatos se dirigem um ao outro, cada um cita o próprio histórico.
- Se não tiver certeza de quem é, escreva DESCONHECIDO. Não chute.
- Fala sobreposta: uma linha por pessoa, na ordem em que começam.
- Nova linha a cada troca de falante.
- Mantenha número, percentual, nome próprio e sigla de partido exatamente como
  foram ditos.
- O timestamp é relativo ao início DESTE áudio, começando em 00:00."""

# O centésimo depois do segundo é opcional: o bloco 10 do debate de SP de
# 09/08 voltou inteiro como "[01:56.00] FERNANDO HADDAD: ...", e as 96 linhas
# foram descartadas por causa do ".00". Eram 10 minutos de debate.
RE_LINHA = re.compile(
    r"^\[(\d{1,2}):(\d{2})(?::(\d{2}))?(?:\.\d+)?\]\s*([^:]{1,60}?)\s*:\s*(.+)$")

_T0 = time.time()


# ---------------------------------------------------------------- log

def log(msg=""):
    """Print com relógio, para acompanhar rodada longa no log do Actions."""
    if msg == "":
        print(flush=True)
        return
    m, s = divmod(int(time.time() - _T0), 60)
    print(f"[{m:02d}:{s:02d}] {msg}", flush=True)


def secao(titulo):
    log()
    log("=" * 68)
    log(titulo)
    log("=" * 68)


def hhmmss(seg):
    h, r = divmod(int(seg), 3600)
    m, s = divmod(r, 60)
    return f"{h:02d}:{m:02d}:{s:02d}"


def em_segundos(hms):
    """HH:MM:SS, MM:SS ou número de segundos -> segundos. Vazio vira 0."""
    if not hms:
        return 0
    partes = str(hms).strip().split(":")
    if len(partes) == 1:
        return int(float(partes[0]))
    seg = 0
    for p in partes:
        seg = seg * 60 + int(float(p or 0))
    return seg


def agora_brt():
    return datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M")


def exige(binario):
    if not shutil.which(binario):
        sys.exit(f"'{binario}' não encontrado no PATH.")


# ------------------------------------------------- rede do Drive e da planilha

def transitorio(e):
    """Erro que vale repetir: queda de conexão ou API pedindo para voltar depois.

    Socket cortado no meio da chamada chega sempre embrulhado em OSError, seja
    como ssl.SSLError, ConnectionResetError ou timeout, e o requests herda de
    OSError também. Erro de permissão ou de id errado não entra aqui: repetir
    404 quatro vezes só atrasa o log.
    """
    if isinstance(e, (OSError, http.client.HTTPException)):
        return True
    # googleapiclient guarda o status em .resp; gspread, em .response.
    codigo = getattr(getattr(e, "resp", None), "status", None)
    if codigo is None:
        codigo = getattr(getattr(e, "response", None), "status_code", None)
    return codigo in (429, 500, 502, 503, 504)


def com_retentativa(descricao, fn):
    """Repete uma chamada de rede que caiu por motivo transitório."""
    for n in range(1, TENTATIVAS_REDE + 1):
        try:
            return fn()
        except Exception as e:
            if n == TENTATIVAS_REDE or not transitorio(e):
                raise
            espera = ESPERA_REDE * 2 ** (n - 1)
            log(f"    {descricao} falhou ({e}); tentativa {n + 1}/{TENTATIVAS_REDE} em {espera}s")
            time.sleep(espera)


def escrever_celula(ws, linha, coluna, valor):
    com_retentativa(
        f"escrita na linha {linha}",
        lambda: ws.update_cell(linha, coluna, valor),
    )


# ---------------------------------------------------------------- áudio

def baixar_audio(url, destino):
    exige("yt-dlp")
    destino.mkdir(parents=True, exist_ok=True)
    log(f"baixando áudio de {url}")
    cmd = [
        "yt-dlp", "-x", "--audio-format", "mp3", "--audio-quality", "5",
        "--geo-bypass-country", "BR",
        "--extractor-args", "youtube:player_client=android,ios,web",
        "--no-playlist", "--newline", "-o", str(destino / "debate.%(ext)s"), url,
    ]

    proxy = os.getenv("YTDLP_PROXY", "").strip() or os.getenv("HTTP_PROXY", "").strip()
    if proxy:
        cmd[1:1] = ["--proxy", proxy]
        log("usando proxy para download")

    # O YouTube exige resolver um desafio em JavaScript para liberar os
    # formatos. O yt-dlp não baixa o solucionador por padrão, e sem ele o erro
    # que aparece é "Requested format is not available", que não diz nada sobre
    # a causa. Precisa também de um runtime JS (deno) no PATH.
    ajuda = subprocess.run(["yt-dlp", "--help"], capture_output=True, text=True).stdout
    if "--remote-components" in ajuda:
        cmd[1:1] = ["--remote-components", "ejs:github"]
        log("solucionador de desafio JS habilitado")
    # O runner do Actions cai no anti-bot do YouTube com alguma frequência. O
    # cookie resolve, e sem ele o download simplesmente volta erro.
    cookies = os.getenv("YTDLP_COOKIES", "").strip()
    if cookies:
        arq = destino / "cookies.txt"
        arq.write_text(cookies, encoding="utf-8")
        cmd = cmd[:1] + ["--cookies", str(arq)] + cmd[1:]
        log("usando cookies do YTDLP_COOKIES")

    proc = subprocess.run(cmd, capture_output=True, text=True)
    for linha in (proc.stdout or "").splitlines():
        if "[download]" in linha and ("%" in linha or "Destination" in linha):
            log(f"  {linha.strip()}")
    if proc.returncode != 0:
        log((proc.stderr or "").strip()[-1500:])
        raise RuntimeError("yt-dlp falhou (se for bloqueio do YouTube, popule o secret YTDLP_COOKIES)")

    mp3 = destino / "debate.mp3"
    if not mp3.exists():
        raise RuntimeError("yt-dlp terminou sem gerar debate.mp3")
    log(f"áudio salvo: {mp3.name} ({mp3.stat().st_size / 1e6:.1f} MB)")
    return mp3


def duracao(caminho):
    exige("ffprobe")
    r = subprocess.run(
        ["ffprobe", "-v", "error", "-show_entries", "format=duration",
         "-of", "default=noprint_wrappers=1:nokey=1", str(caminho)],
        capture_output=True, text=True, check=True,
    )
    return float(r.stdout.strip())


def recortar(origem, destino, inicio, dur):
    """Reencoda em mono 16 kHz: é o que o modelo usa e derruba o upload."""
    exige("ffmpeg")
    cmd = ["ffmpeg", "-y", "-loglevel", "error", "-ss", str(inicio)]
    if dur is not None:
        cmd += ["-t", str(dur)]
    cmd += ["-i", str(origem), "-vn", "-ac", "1", "-ar", "16000", "-b:a", "64k", str(destino)]
    subprocess.run(cmd, check=True)
    return destino


# ---------------------------------------------------------------- gemini

def subir_gemini(client, caminho):
    arq = client.files.upload(file=str(caminho))
    esperou = 0
    while arq.state.name == "PROCESSING":
        time.sleep(2)
        esperou += 2
        arq = client.files.get(name=arq.name)
    if arq.state.name != "ACTIVE":
        raise RuntimeError(f"upload terminou em {arq.state.name}")
    return arq, esperou


def contar(resp, uso):
    """Soma o uso desta resposta no acumulador e devolve o texto do log.

    Tentativa que voltou vazia também entrou na conta do Google, então o
    acumulador é alimentado antes de decidir se o texto serve.
    """
    u = getattr(resp, "usage_metadata", None)
    if not u:
        uso["chamadas"] += 1
        return "sem contagem"
    entrada = getattr(u, "prompt_token_count", 0) or 0
    # thoughts_token_count é cobrado como saída e não vem dentro do
    # candidates_token_count: sem somar, a conta sai menor que a fatura.
    pensamento = getattr(u, "thoughts_token_count", 0) or 0
    saida = (getattr(u, "candidates_token_count", 0) or 0) + pensamento
    uso["in"] += entrada
    uso["out"] += saida
    uso["chamadas"] += 1
    detalhe = f"{entrada} in / {saida} out"
    if pensamento:
        detalhe += f" ({pensamento} de raciocínio)"
    return detalhe


def montar_config():
    """Config da chamada. Transcrever não é raciocinar: o modelo tem o áudio e
    o formato de saída, e no debate Band SP de 2h ele queimou ~52 mil tokens de
    raciocínio por bloco para devolver ~2,4 mil de transcrição, 96% da saída
    paga. Com o raciocínio no mínimo o gasto cai na mesma proporção."""
    cfg = dict(temperature=0.0, max_output_tokens=32000)
    if RACIOCINIO not in ("", "PADRAO") and not SEM_THINKING_CONFIG:
        if RACIOCINIO.lstrip("-").isdigit():
            pensa = types.ThinkingConfig(thinking_budget=int(RACIOCINIO))
        else:
            pensa = types.ThinkingConfig(thinking_level=RACIOCINIO)
        cfg["thinking_config"] = pensa
    return types.GenerateContentConfig(**cfg)


def transcrever(client, arq, contexto, uso):
    global SEM_THINKING_CONFIG
    prompt = PROMPT.format(contexto=contexto.strip())
    for n in range(1, TENTATIVAS + 1):
        t = time.time()
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[prompt, arq],
                config=montar_config(),
            )
            texto = (resp.text or "").strip()
            tokens = contar(resp, uso)
            if texto:
                log(f"    resposta em {time.time() - t:.1f}s, {tokens}")
                return texto
            log(f"    tentativa {n}/{TENTATIVAS}: resposta vazia ({tokens})")
        except Exception as e:
            # Modelo que não aceita thinking_level/thinking_budget responde 400 e
            # responderia 400 em todos os blocos: desliga o campo e segue caro,
            # em vez de devolver debate vazio.
            if not SEM_THINKING_CONFIG and ("thinking" in str(e).lower() or "invalid_argument" in str(e).lower() or "400" in str(e)):
                SEM_THINKING_CONFIG = True
                log(f"    {GEMINI_MODEL} recusou thinking_config, seguindo sem ele: {e}")
                continue
            log(f"    tentativa {n}/{TENTATIVAS} falhou em {time.time() - t:.1f}s: {e}")
        if n < TENTATIVAS:
            # O 503 de "high demand" é onda, não fila: espera curta cai na
            # mesma onda. No debate de 14/08 quatro blocos se perderam com
            # 5s e 10s de espera, com o modelo respondendo normal 40s depois.
            espera = min(ESPERA_BASE * 2 ** (n - 1), ESPERA_MAX)
            log(f"    aguardando {espera}s")
            time.sleep(espera)
    return ""


# Marcador de fala no meio da linha, para reparar bloco que voltou sem quebra
# de linha nenhuma. Exige o nome e os dois pontos depois do tempo, senão um
# "[00:15]" citado dentro da fala também viraria troca de turno.
RE_MARCADOR = re.compile(
    r"(?<!^)\s*(\[\d{1,2}:\d{2}(?::\d{2})?(?:\.\d+)?\]\s*[^:\n]{1,60}?\s*:)")

# Variante sem colchete, "02:08 - MATEUS SIMÕES:", às vezes com a fala só na
# linha seguinte e entre aspas. Foi como voltou o bloco 11 do debate de MG de
# 16/08: 86 linhas, 3.718 palavras, nenhuma aproveitada.
RE_TRAVESSAO = re.compile(
    r"^(\d{1,2}:\d{2}(?::\d{2})?)(?:\.\d+)?\s*[-–—]\s*([^:\n]{1,60}?)\s*:\s*(.*)$")


def normalizar_formato(texto):
    """Põe a saída do modelo no formato "[tempo] NOME: fala", linha a linha.

    O prompt pede um formato só e o modelo entrega outro de vez em quando. Não
    é erro de transcrição: o conteúdo está lá, íntegro, e só o enfeite muda.
    Descartar por isso é jogar fora bloco pago, e foi o que aconteceu com três
    blocos dos dois primeiros debates.

    O que esta função aceita, e nada além disso: o travessão no lugar do
    colchete, a fala na linha seguinte ao nome, e a aspa em volta da fala.
    Linha que não vira marcador continua exatamente como veio.
    """
    linhas, pendente = [], None
    for bruta in (texto or "").splitlines():
        crua = bruta.strip()
        m = RE_TRAVESSAO.match(crua)
        if m:
            if pendente:
                linhas.append(pendente)
            tempo, falante, fala = m.groups()
            cabeca = f"[{tempo}] {falante}:"
            if fala.strip():
                linhas.append(f"{cabeca} {fala.strip()}")
                pendente = None
            else:
                # Nome sozinho na linha: a fala vem na próxima não vazia.
                pendente = cabeca
            continue
        if pendente:
            if crua:
                linhas.append(f"{pendente} {crua}")
                pendente = None
            continue
        linhas.append(bruta)
    if pendente:
        linhas.append(pendente)

    limpas = []
    for l in linhas:
        m = RE_LINHA.match(l.strip())
        if m and len(m.group(5)) > 1 and m.group(5)[0] in "\"“" and m.group(5)[-1] in "\"”":
            l = l[:l.index(m.group(5))] + m.group(5)[1:-1]
        limpas.append(l)
    return "\n".join(limpas)


def parsear(texto, offset, limite):
    """Converte a saída do modelo em linhas com tempo absoluto.

    Descarta o que caiu na sobreposição: o bloco seguinte cobre esse trecho e
    manter os dois duplica fala no meio da transcrição.

    Antes de dividir por linha, reinsere a quebra em marcador que veio no meio
    do texto. No debate de MG de 16/08 o bloco 1 voltou inteiro numa única
    linha: o regex casou o primeiro turno e os outros 10 minutos foram para
    dentro daquela fala, numa célula de 1.891 palavras com "[00:04] MATEUS
    SIMÕES:" escrito no meio. Um bloco perdido é visível; este não era.
    """
    texto = normalizar_formato(RE_MARCADOR.sub(r"\n\1", texto))
    linhas, ignoradas, fora = [], 0, 0
    for bruta in texto.splitlines():
        bruta = bruta.strip()
        if not bruta or bruta.startswith(("#", "```")):
            continue
        m = RE_LINHA.match(bruta)
        if not m:
            ignoradas += 1
            continue
        a, b, c, falante, fala = m.groups()
        rel = int(a) * 3600 + int(b) * 60 + int(c) if c else int(a) * 60 + int(b)
        if limite is not None and rel >= limite:
            fora += 1
            continue
        linhas.append({
            "segundos": rel + offset,
            "tempo": hhmmss(rel + offset),
            "falante": re.sub(r"\s+", " ", falante).strip().upper(),
            "fala": fala.strip(),
        })
    return linhas, ignoradas, fora


# ---------------------------------------------------------------- google

def clientes_google():
    """gspread e Drive v3 a partir da credencial da conta de serviço."""
    from google.oauth2.service_account import Credentials
    from googleapiclient.discovery import build
    import gspread

    caminho = os.getenv("GOOGLE_CREDENTIALS_FILE", "credentials.json")
    if not Path(caminho).exists():
        sys.exit(f"credencial não encontrada em '{caminho}'.")
    creds = Credentials.from_service_account_file(caminho, scopes=ESCOPOS)
    return gspread.authorize(creds), build("drive", "v3", credentials=creds)


def extrair_id_drive(link_ou_id: str) -> str:
    m = re.search(r"/d/([a-zA-Z0-9_-]+)", link_ou_id)
    if m:
        return m.group(1)
    m = re.search(r"id=([a-zA-Z0-9_-]+)", link_ou_id)
    if m:
        return m.group(1)
    return link_ou_id.strip()


def baixar_audio_drive(drive, link_ou_id: str, destino: Path) -> Path:
    """Baixa o áudio MP3 diretamente do Google Drive quando o YouTube estiver geobloqueado."""
    from googleapiclient.http import MediaIoBaseDownload
    import io

    file_id = extrair_id_drive(link_ou_id)
    destino.mkdir(parents=True, exist_ok=True)
    mp3 = destino / "debate.mp3"
    log(f"baixando áudio do Google Drive (id: {file_id})")

    request = drive.files().get_media(fileId=file_id, supportsAllDrives=True)
    with io.FileIO(str(mp3), "wb") as fh:
        downloader = MediaIoBaseDownload(fh, request)
        done = False
        while not done:
            # O next_chunk retoma de onde parou, então repetir um pedaço que
            # caiu não rebaixa os 70 MB inteiros.
            status, done = com_retentativa(
                "download do áudio no Drive", downloader.next_chunk
            )
            if status:
                log(f"  download drive: {int(status.progress() * 100)}%")

    log(f"áudio salvo do Drive: {mp3.name} ({mp3.stat().st_size / 1e6:.1f} MB)")
    return mp3


def enviar_drive(drive, caminho, nome, mime_destino=None, pasta=None):
    """Sobe um arquivo para a pasta Debates do drive compartilhado.

    supportsAllDrives é obrigatório: sem ele a API responde 404 na pasta,
    porque drive compartilhado não aparece na visão padrão da conta.
    """
    from googleapiclient.http import MediaFileUpload

    corpo = {"name": nome, "parents": [pasta or PASTA_DRIVE]}
    if mime_destino:
        corpo["mimeType"] = mime_destino
    # mp3 de debate passa dos 60 MB, e upload de uma vez só nesse tamanho é o
    # que estoura em conexão instável. Acima de 10 MB vai em pedaços.
    grande = Path(caminho).stat().st_size > 10e6
    midia = MediaFileUpload(str(caminho), resumable=grande)
    arq = com_retentativa(
        f"upload de {nome}",
        lambda: drive.files().create(
            body=corpo, media_body=midia,
            supportsAllDrives=True, fields="id,webViewLink",
        ).execute(),
    )
    return arq["webViewLink"]


def garantir_pasta(drive, nome, pai):
    """Id da subpasta 'nome' dentro de 'pai', criada se ainda não existir.

    A aspa simples é o único caractere que quebra a query da API, e nome de
    mês ou de UF não tem nenhuma, mas o escape fica porque o nome vem da
    planilha e planilha aceita qualquer coisa.
    """
    seguro = nome.replace("'", "\\'")
    q = (f"'{pai}' in parents and name='{seguro}' "
         "and mimeType='application/vnd.google-apps.folder' and trashed=false")
    achou = com_retentativa(
        f"busca da pasta '{nome}'",
        lambda: drive.files().list(
            q=q, includeItemsFromAllDrives=True, supportsAllDrives=True,
            fields="files(id)",
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
    log(f"pasta '{nome}' criada no Drive")
    return nova["id"]


def caminho_do_debate(uf, data, ident=None):
    """Os quatro níveis do debate no Drive: UF, mês, dia e subpasta do debate.

    A data vem da planilha em ISO ('2026-08-09'), que é de onde o id do debate
    já tira o ano, mas quem preenche na mão escreve '09/08/2026', e as duas
    formas entram. O mês fica como '2026-08', e não como 'agosto': o Drive
    ordena pasta por nome, e nome de mês por extenso põe abril antes de agosto.
    Debate de presidente não tem UF na planilha e cai em 'BR'.
    Dentro de cada dia, uma subpasta para cada debate específico (ex: 2026-band-sp-gov-t1).
    """
    estado = (uf or "").strip().upper() or "BR"
    texto = (data or "").strip()

    m = re.match(r"(\d{4})-(\d{2})-(\d{2})", texto)
    if m:
        ano, mes, dia = m.groups()
    else:
        m = re.match(r"(\d{2})/(\d{2})/(\d{4})", texto)
        if not m:
            trilha = [estado, "sem-data"]
            if ident:
                trilha.append(ident.strip())
            return trilha
        dia, mes, ano = m.groups()

    trilha = [estado, f"{ano}-{mes}", dia]
    if ident:
        trilha.append(ident.strip())
    return trilha


def pasta_do_debate(drive, uf, data, ident=None, cache=None):
    """Desce UF > mês > dia > debate a partir da pasta Debates, criando o que faltar.

    O cache evita refazer a mesma busca a cada arquivo do mesmo debate: são
    quatro chamadas por nível, e a reorganização passa por dezenas de arquivos.
    """
    trilha = caminho_do_debate(uf, data, ident)
    if cache is not None and tuple(trilha) in cache:
        return cache[tuple(trilha)]
    pai = PASTA_DRIVE
    for nivel in trilha:
        pai = garantir_pasta(drive, nivel, pai)
    if cache is not None:
        cache[tuple(trilha)] = pai
    return pai


def listar_filhos(drive, pasta):
    """Arquivos e subpastas direto dentro de uma pasta, sem descer mais."""
    itens, token = [], None
    while True:
        pagina = com_retentativa(
            "listagem de pasta do Drive",
            lambda: drive.files().list(
                q=f"'{pasta}' in parents and trashed=false",
                includeItemsFromAllDrives=True, supportsAllDrives=True,
                fields="nextPageToken, files(id,name,mimeType,parents)",
                pageSize=1000, pageToken=token,
            ).execute(),
        )
        itens.extend(pagina.get("files", []))
        token = pagina.get("nextPageToken")
        if not token:
            return itens


def reorganizar_drive(args):
    """Move o que já está no Drive para a estrutura UF/ano-mês/dia.

    Só arruma o passado: a fila normal já grava no lugar certo. Mover no Drive
    preserva o id do arquivo, então os links que a planilha guarda continuam
    valendo, e rodar duas vezes não faz mal, porque o que já está no destino
    é ignorado.
    """
    if not PLANILHA or not PASTA_DRIVE:
        sys.exit("defina SPREADSHEET_ID_DEBATES e PASTA_DRIVE_DEBATES (secrets do repo).")
    gc, drive = clientes_google()
    ws = com_retentativa(
        "abertura da planilha de debates",
        lambda: gc.open_by_key(PLANILHA).worksheet(ABA),
    )
    todas = com_retentativa("leitura da planilha", ws.get_all_values)

    # De qual debate é cada arquivo: o nome sempre começa pelo id.
    debates = {}
    for linha in todas[1:]:
        if not linha:
            continue
        ident = linha[COL["id"]].strip()
        if not ident:
            continue
        def pega(nome, l=linha):
            return (l[COL[nome]] if COL[nome] < len(l) else "").strip()
        debates[ident] = (pega("uf"), pega("data"))
    log(f"planilha: {len(debates)} debate(s) com id")

    # A raiz e a antiga subpasta 'audios', que é onde os mp3 foram parar.
    na_raiz = listar_filhos(drive, PASTA_DRIVE)
    pastas = {f["name"]: f["id"] for f in na_raiz if f["mimeType"] == PASTA_MIME}
    soltos = [f for f in na_raiz if f["mimeType"] != PASTA_MIME]
    if "audios" in pastas:
        soltos += listar_filhos(drive, pastas["audios"])
        log("subpasta 'audios' entrou na varredura")
    log(f"arquivos fora da estrutura nova: {len(soltos)}")

    cache, movidos, sem_dono = {}, 0, []
    for arq in soltos:
        # Id mais longo primeiro: '...-gov-t1-2' também começa com '...-gov-t1'.
        dono = max((d for d in debates if arq["name"].startswith(d)),
                   key=len, default=None)
        if not dono:
            sem_dono.append(arq["name"])
            continue

        uf, data = debates[dono]
        trilha = " / ".join(caminho_do_debate(uf, data))
        if args.simular:
            log(f"  {arq['name']}  ->  {trilha}")
            movidos += 1
            continue

        destino = pasta_do_debate(drive, uf, data, cache)
        antigos = ",".join(arq.get("parents", []))
        if destino in arq.get("parents", []):
            continue
        com_retentativa(
            f"mover {arq['name']}",
            lambda: drive.files().update(
                fileId=arq["id"], addParents=destino, removeParents=antigos,
                supportsAllDrives=True, fields="id",
            ).execute(),
        )
        log(f"  {arq['name']}  ->  {trilha}")
        movidos += 1

    secao("RESULTADO")
    log(f"{movidos} arquivo(s) {'seriam movidos' if args.simular else 'movidos'}")
    if sem_dono:
        # Sem id casando, mexer no arquivo é chute: some da vista de quem
        # deixou ali e não dá para desfazer sozinho.
        log(f"{len(sem_dono)} arquivo(s) sem debate correspondente, deixados onde estão:")
        for nome in sem_dono[:20]:
            log(f"  {nome}")
        if len(sem_dono) > 20:
            log(f"  ... e mais {len(sem_dono) - 20}")
    if args.simular:
        log("simulação: nada foi movido. Rode sem --simular para valer.")


def contexto_da_linha(linha):
    """Monta o bloco de participantes que vai no prompt, a partir da planilha."""
    def campo(nome):
        i = COL[nome]
        return (linha[i] if i < len(linha) else "").strip()

    cargo, uf, turno = campo("cargo"), campo("uf"), campo("turno")
    emissora, mediador = campo("emissora"), campo("mediador")

    cabecalho = "Debate eleitoral brasileiro das eleições de 2026"
    if cargo:
        cabecalho = f"Debate ao cargo de {cargo}"
        if uf:
            cabecalho += f" por {uf}"
    if emissora:
        cabecalho += f", {emissora}"
    if turno:
        cabecalho += f", {turno}º turno"
    cabecalho += ", eleições de 2026."

    pessoas = []
    if mediador:
        pessoas.append(f"- {mediador}, mediador do debate")
    for p in campo("participantes").split(";"):
        if p.strip():
            pessoas.append(f"- {p.strip()}")
    if not pessoas:
        return CONTEXTO_PADRAO

    return (
        f"{cabecalho}\n\nParticipantes:\n" + "\n".join(pessoas)
        + "\n- Jornalistas da bancada, que fazem perguntas aos candidatos\n\n"
        "Marque jornalista da bancada como JORNALISTA quando não der para saber o nome."
    )


# ---------------------------------------------------------------- núcleo

CAMPOS_CSV = ["segundos", "tempo", "falante", "fala",
              "eixo", "tema", "termos", "herdado_do_anterior"]


def unificar_falantes(linhas):
    """Junta o mesmo falante escrito com e sem acento, na forma mais frequente.

    O modelo escreve o nome como ouviu, bloco a bloco, e no MG de 16/08 saíram
    'LUCAS CATTA PRÊTA' com 59 turnos e 'LUCAS CATTA PRETA' com 5. Duas linhas
    no resumo por falante para a mesma pessoa é erro que passa batido e divide
    a contagem de quem for somar por candidato.

    A forma vencedora é a mais frequente em palavras, não em turnos: variante
    que aparece só num aparte não pode virar o nome canônico do candidato.
    """
    import unicodedata
    from collections import Counter

    def chave(nome):
        t = unicodedata.normalize("NFKD", nome or "")
        return re.sub(r"\s+", " ", t.encode("ascii", "ignore").decode()).strip().upper()

    peso = {}
    for r in linhas:
        peso.setdefault(chave(r["falante"]), Counter())[r["falante"]] += \
            len(r["fala"].split())

    trocas = {}
    for k, formas in peso.items():
        if len(formas) < 2:
            continue
        vencedora = formas.most_common(1)[0][0]
        for f in formas:
            if f != vencedora:
                trocas[f] = vencedora
    if not trocas:
        return
    for r in linhas:
        if r["falante"] in trocas:
            r["falante"] = trocas[r["falante"]]
    for de, para in sorted(trocas.items()):
        log(f"falante unificado: {de!r} -> {para!r}")


def marcar_assuntos(linhas):
    """Acrescenta eixo, tema e termos a cada fala, pelo dicionário de assuntos.

    Não é o modelo que classifica: é varredura por lista de termos, sem chamada
    de API e sem custo. Rodar de novo no mesmo texto dá o mesmo resultado.

    São duas varreduras, uma por nível, e não uma com o eixo derivado do tema:
    termo que diz o eixo e não diz o tema ('escola' é educação, mas não diz a
    etapa) vale no nível eixo e não vale no nível tema. Por isso a coluna eixo
    pode ter rótulo que a coluna tema não tem.

    Envolvido em try de propósito. A transcrição custa dinheiro por rodada e a
    marcação não vale perder uma: se o dicionário faltar ou quebrar, as colunas
    saem vazias e a transcrição sai igual.
    """
    for r in linhas:
        r.update({c: "" for c in CAMPOS_CSV[4:]})
    try:
        from outros.assuntos_debates import (assuntos_da_fala, compilar,
                                             janela, montar_termos)
    except Exception as e:
        log(f"marcação de assuntos indisponível ({type(e).__name__}: {e})")
        log("a transcrição sai sem as colunas de eixo e tema")
        return

    comp_eixo = compilar(montar_termos(por_tema=False))
    comp_tema = compilar(montar_termos(por_tema=True))
    for i, r in enumerate(linhas):
        if not r["fala"].strip():
            continue
        texto = " ".join(x["fala"] for x in janela(linhas, i, 1))
        eixos = assuntos_da_fala(texto, comp_eixo, por_tema=False)
        temas = assuntos_da_fala(texto, comp_tema, por_tema=True)
        proprios = assuntos_da_fala(r["fala"], comp_eixo, por_tema=False)
        proprios_tema = assuntos_da_fala(r["fala"], comp_tema, por_tema=True)
        achados = temas or eixos
        r["eixo"] = "; ".join(sorted(eixos))
        r["tema"] = "; ".join(sorted(temas))
        r["termos"] = "; ".join(f"{k}: {', '.join(v)}"
                                for k, v in sorted(achados.items()))
        # Marcação que só existe por causa do turno anterior. Quem for citar
        # fala isolada em entrega precisa olhar estas antes.
        #
        # Os dois níveis, e não só o eixo: no debate de SP de 09/08, 51 falas
        # citam pelo menos um termo que veio do turno anterior e o aviso saía
        # em 44. As 7 de fora eram fala cujo eixo era próprio e o tema é que
        # foi herdado, e é o tema que aparece na entrega.
        r["herdado_do_anterior"] = "; ".join(sorted(
            (set(eixos) - set(proprios)) | (set(temas) - set(proprios_tema))))

    com = sum(1 for r in linhas if r["eixo"])
    herd = sum(1 for r in linhas if r["herdado_do_anterior"])
    log(f"assuntos: {com} de {len(linhas)} falas marcadas, {herd} com marcação "
        f"herdada do turno anterior")


def processar(origem, contexto, saida, nome, inicio=None, dur=None):
    """Transcreve um áudio inteiro e grava txt, csv e bruto. Devolve os caminhos."""
    trabalho = saida / "_trabalho"
    trabalho.mkdir(parents=True, exist_ok=True)

    total_bruto = duracao(origem)
    log(f"duração original: {hhmmss(total_bruto)}")
    # Deslocamento do recorte, somado em todo timestamp da saída. Sem isto a
    # transcrição recortada começa em 00:00:00 e não casa mais com o vídeo: o
    # debate de MG de 16/08 começa em 00:50:00 do arquivo da Band, e quem
    # conferir a fala no YouTube pelo tempo da planilha erra por 50 minutos.
    offset = em_segundos(inicio)
    if inicio or dur:
        log(f"recortando de {inicio or '00:00:00'}" + (f" por {dur}" if dur else " até o fim"))
        origem = recortar(origem, trabalho / "recorte.mp3", inicio or "00:00:00", dur)
        if offset:
            log(f"timestamps deslocados em +{hhmmss(offset)}, para casar com o vídeo")

    total = duracao(origem)
    n_blocos = max(1, -(-int(total) // BLOCO_SEG))
    log(f"duração a processar: {hhmmss(total)} em {n_blocos} bloco(s)")

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    linhas, textos, suspeitos = [], {}, []
    # Quantas falas cada bloco pôs na transcrição. Texto não vazio não garante
    # fala nenhuma: no MG de 16/08 o bloco 10 voltou com 57 linhas fora do
    # formato e zero fala, e como o texto existia a repescagem não o pegou.
    parseadas = {}
    uso = {"in": 0, "out": 0, "chamadas": 0}

    # Recorte de cada bloco, guardado para a repescagem saber refazer o que
    # voltou vazio sem ter que recalcular posição.
    cortes = []
    pos, n = 0, 0
    while pos < total - 1:
        n += 1
        d = min(BLOCO_SEG + SOBREPOSICAO_SEG, total - pos)
        limite = None if pos + BLOCO_SEG >= total else BLOCO_SEG
        cortes.append({"n": n, "pos": pos, "d": d, "limite": limite})
        pos += BLOCO_SEG

    def rodar(c):
        """Transcreve um bloco e guarda o resultado. Devolve o texto do modelo."""
        pos, d, limite, n = c["pos"], c["d"], c["limite"], c["n"]
        log(f"bloco {n}/{n_blocos}  {hhmmss(pos)} a {hhmmss(pos + d)}")

        arq_bloco = recortar(origem, trabalho / f"bloco_{n:03d}.mp3", pos, int(d))
        log(f"    cortado: {arq_bloco.stat().st_size / 1e6:.2f} MB")

        subido, esperou = subir_gemini(client, arq_bloco)
        log(f"    enviado ({esperou}s de processamento no servidor)")

        texto = transcrever(client, subido, contexto, uso)
        try:
            client.files.delete(name=subido.name)
        except Exception:
            pass

        textos[n] = texto
        if not texto:
            parseadas[n] = 0
            log(f"    bloco {n} voltou vazio depois de {TENTATIVAS} tentativas, segue")
            return texto

        novas, ignoradas, fora = parsear(texto, pos + offset, limite)
        parseadas[n] = len(novas)
        linhas.extend(novas)

        minutos = (limite or d) / 60
        falantes = sorted({r["falante"] for r in novas})
        desconhecidas = sum(1 for r in novas if r["falante"] == "DESCONHECIDO")
        palavras = sum(len(r["fala"].split()) for r in novas)
        ritmo = palavras / minutos if minutos else 0
        turnos_min = len(novas) / minutos if minutos else 0

        log(f"    {len(novas)} falas, {palavras} palavras, "
            f"{ritmo:.0f} palavras/min, {turnos_min:.1f} turnos/min")
        log(f"    falantes: {', '.join(falantes) or 'nenhum'}")
        if desconhecidas:
            log(f"    DESCONHECIDO em {desconhecidas} fala(s) ({desconhecidas / len(novas):.0%})")
        if ignoradas:
            log(f"    {ignoradas} linha(s) fora do formato, descartadas")
        if fora:
            log(f"    {fora} linha(s) da sobreposição, cobertas pelo bloco seguinte")
        if ritmo < PALAVRAS_POR_MIN_SUSPEITO and n not in suspeitos:
            suspeitos.append(n)
            log(f"    ATENÇÃO: {ritmo:.0f} palavras/min, o modelo pode ter resumido este bloco")
        for r in novas[:2]:
            log(f"    > [{r['tempo']}] {r['falante']}: {r['fala'][:90]}")
        return texto

    secao("TRANSCRIÇÃO")
    for c in cortes:
        rodar(c)

    # Repescagem: bloco perdido é trecho que some da transcrição, e no debate
    # Band SP de 14/08 foram 4 de 12, todos por 503 de "high demand" que passou
    # minutos depois. Sai antes de gravar arquivo, então a saída já vem inteira.
    #
    # O critério é fala nenhuma na transcrição, e não texto vazio: no MG de
    # 16/08 os blocos 5 e 10 responderam, o parse não aproveitou nada (57
    # linhas fora do formato no bloco 10), e como havia texto a repescagem
    # deixou passar. Deu 20 minutos de debate faltando num run que terminou
    # verde. Repescar aqui não duplica fala: bloco sem fala não pôs nada em
    # `linhas`.
    vazios = [c["n"] for c in cortes if not parseadas.get(c["n"])]
    if vazios:
        secao("REPESCAGEM DOS BLOCOS SEM FALA")
        log(f"sem fala na primeira passada: {vazios}")
        log(f"esperando {ESPERA_REPESCAGEM}s para a onda de erro passar")
        time.sleep(ESPERA_REPESCAGEM)
        for c in cortes:
            if c["n"] in vazios:
                rodar(c)

    vazios = [c["n"] for c in cortes if not parseadas.get(c["n"])]
    if not linhas:
        raise RuntimeError("nenhuma fala transcrita")

    secao("SAÍDA")
    linhas.sort(key=lambda r: r["segundos"])
    unificar_falantes(linhas)
    base = saida / nome

    with open(f"{base}.txt", "w", encoding="utf-8") as f:
        for r in linhas:
            f.write(f"[{r['tempo']}] {r['falante']}: {r['fala']}\n")
    marcar_assuntos(linhas)
    with open(f"{base}.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=CAMPOS_CSV)
        w.writeheader()
        w.writerows(linhas)
    with open(f"{base}_bruto.md", "w", encoding="utf-8") as f:
        f.write("\n\n".join(
            f"### bloco {c['n']} ({hhmmss(c['pos'])} a {hhmmss(c['pos'] + c['d'])})"
            f"\n\n{textos.get(c['n'], '')}"
            for c in cortes
        ))

    caminhos = [Path(f"{base}.txt"), Path(f"{base}.csv"), Path(f"{base}_bruto.md")]
    for p in caminhos:
        log(f"{p}  ({p.stat().st_size / 1024:.0f} KB)")

    secao("RESUMO POR FALANTE")
    agg = {}
    for r in linhas:
        d_ = agg.setdefault(r["falante"], {"falas": 0, "palavras": 0})
        d_["falas"] += 1
        d_["palavras"] += len(r["fala"].split())
    tot = sum(v["palavras"] for v in agg.values()) or 1

    log(f"{'falante':<26} {'falas':>7} {'palavras':>10} {'share':>7}")
    log("-" * 54)
    for f_, v in sorted(agg.items(), key=lambda x: -x[1]["palavras"]):
        log(f"{f_:<26} {v['falas']:>7} {v['palavras']:>10} {v['palavras'] / tot:>6.1%}")
    log("-" * 54)
    log(f"{'TOTAL':<26} {len(linhas):>7} {tot:>10}")

    secao("GASTO NA API")
    dolar, resumo_custo = custo(uso)
    log(f"modelo   : {GEMINI_MODEL}")
    log("raciocínio: " + ("padrão do modelo"
                          if SEM_THINKING_CONFIG or RACIOCINIO in ("", "PADRAO")
                          else RACIOCINIO))
    log(f"chamadas : {uso['chamadas']} em {n_blocos} bloco(s) "
        f"(só as que responderam; erro de API não é cobrado)")
    log(f"gasto    : {resumo_custo}")
    if dolar is not None and total:
        log(f"por hora de áudio: US$ {dolar / (total / 3600):.2f}")

    secao("O QUE CONFERIR ANTES DE USAR")
    desc = agg.get("DESCONHECIDO", {}).get("palavras", 0) / tot
    log(f"cobertura: {hhmmss(linhas[-1]['segundos'] - offset)} de {hhmmss(total)} de áudio"
        + (f"  (timestamps somam +{hhmmss(offset)})" if offset else ""))
    log(f"DESCONHECIDO: {desc:.1%} das palavras")
    if vazios:
        log(f"blocos sem fala: {vazios} (esses trechos NÃO estão na transcrição)")
    if suspeitos:
        log(f"blocos com ritmo de fala baixo: {suspeitos} (confira contra o vídeo)")
    if not vazios and not suspeitos and desc < 0.05:
        log("nenhum bloco sem fala, nenhum bloco suspeito de resumo")
    log("a identificação de falante vem do conteúdo, não da voz: confira as")
    log("trocas de bloco e os apartes contra o vídeo antes de citar em entrega")

    shutil.rmtree(trabalho, ignore_errors=True)
    # O gasto entra em observacoes junto com os avisos: é por linha da planilha
    # que dá para somar quanto custou cada debate depois.
    avisos = [f"gasto: {resumo_custo}"]
    if vazios:
        avisos.append(f"blocos sem fala: {vazios}")
    if suspeitos:
        avisos.append(f"ritmo de fala baixo: {suspeitos}")
    if desc >= 0.05:
        avisos.append(f"DESCONHECIDO em {desc:.0%} das palavras")
    return caminhos, "; ".join(avisos)


# ---------------------------------------------------------------- fila

def slug(texto, tamanho=16):
    import unicodedata
    t = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]", "", t.lower())[:tamanho] or "sem"


def gerar_id(linha_fonte, usados):
    """Rótulo legível do debate. A chave de verdade é o id_fonte; este aqui só
    nomeia o arquivo no Drive, então pode repetir sufixo sem drama."""
    def c(nome):
        i = COL_FONTE[nome]
        return (linha_fonte[i] if i < len(linha_fonte) else "").strip()

    emissora = c("emissora")
    # Consórcio de emissoras não cabe no rótulo, e o nome de uma só engana.
    marca = "consorcio" if emissora.count("/") >= 2 else slug(emissora.split("/")[0])
    cargo = {"presidente": "pres", "governador": "gov", "senador": "sen"}.get(
        c("cargo").lower(), slug(c("cargo"), 6))
    ano = (c("data") or "2026")[:4]
    base = f"{ano}-{marca}-{slug(c('uf'), 8) or 'br'}-{cargo}-t{c('turno') or '1'}"

    ident, n = base, 1
    while ident in usados:
        n += 1
        ident = f"{base}-{n}"
    usados.add(ident)
    return ident


# Ordem das colunas da planilha do monitoramento, como ela está hoje. Serve de
# ponto de partida: o sync confere o cabeçalho de lá antes de ler qualquer
# linha e refaz esse mapa a partir do que achar.
COL_FONTE_PADRAO = {
    "id_debate": 0, "data": 1, "horario": 2, "cargo": 3, "uf": 4, "turno": 5,
    "emissora": 6, "url_youtube": 7, "mediador": 8, "participantes": 9,
    "observacoes": 10,
}
COL_FONTE = dict(COL_FONTE_PADRAO)

# Nomes de cabeçalho aceitos para cada campo, já normalizados (sem acento, tudo
# minúsculo, separador virando '_'). A planilha é de outra equipe, então cabe
# mais de um nome por campo.
NOMES_FONTE = {
    "id_debate": ("id_debate", "id", "id_do_debate"),
    "data": ("data", "data_do_debate"),
    "horario": ("horario", "hora"),
    "cargo": ("cargo",),
    "uf": ("uf", "estado"),
    "turno": ("turno",),
    "emissora": ("emissora", "emissoras", "veiculo"),
    "url_youtube": ("url_youtube", "url", "link", "link_youtube", "youtube", "url_do_video"),
    "mediador": ("mediador", "mediadores", "mediacao"),
    "participantes": ("participantes", "candidatos"),
    "observacoes": ("observacoes", "observacao", "obs"),
}


def normalizar_titulo(texto):
    import unicodedata
    t = unicodedata.normalize("NFKD", texto or "").encode("ascii", "ignore").decode()
    return re.sub(r"[^a-z0-9]+", "_", t.lower()).strip("_")


def mapear_fonte(cabecalho):
    """Em que coluna da planilha do monitoramento está cada campo.

    Ler por posição fixa era o buraco: uma coluna inserida no meio de lá fazia
    o sync gravar participante no lugar de mediador, sem erro nenhum, e só
    aparecia quando alguém lesse a transcrição. Aqui a posição sai do
    cabeçalho, e campo que não for encontrado para o run em vez de chutar.
    """
    titulos = [normalizar_titulo(c) for c in cabecalho]
    achados = {}
    for campo, aceitos in NOMES_FONTE.items():
        for aceito in aceitos:
            if aceito in titulos:
                achados[campo] = titulos.index(aceito)
                break

    faltando = [c for c in NOMES_FONTE if c not in achados]
    if faltando:
        sys.exit(
            f"cabeçalho da aba '{FONTE_ABA}' mudou: não achei coluna para "
            f"{', '.join(faltando)}.\n"
            f"cabeçalho lido: {', '.join(t for t in titulos if t) or '(vazio)'}\n"
            "Ajuste NOMES_FONTE em outros/transcricao_debates.py, ou peça para "
            "a coluna voltar ao nome antigo."
        )
    return achados


def sincronizar(gc, ws):
    """Traz o calendário da aba do monitoramento para a nossa planilha.

    Casa por id_fonte, que é a coluna estável de lá. Debate novo entra; debate
    já sincronizado só recebe campo que veio preenchido, para não apagar o que
    alguém completou na mão. Linha em 'processando' ou 'pronto' não tem o
    status mexido: transcrição já feita não volta para a fila.
    """
    global COL_FONTE

    if not FONTE_PLANILHA:
        log("SPREADSHEET_ID_INTERNO não definido, sync pulado")
        return

    secao("SYNC COM O MONITORAMENTO")
    fonte = com_retentativa(
        "abertura da planilha do monitoramento",
        lambda: gc.open_by_key(FONTE_PLANILHA).worksheet(FONTE_ABA),
    )
    tudo_de_la = com_retentativa("leitura da fonte", fonte.get_all_values)
    if not tudo_de_la:
        log(f"aba '{FONTE_ABA}' vazia, sync pulado")
        return

    # Antes de ler linha, confere onde está cada coluna lá.
    COL_FONTE = mapear_fonte(tudo_de_la[0])
    mudadas = [c for c, pos in COL_FONTE.items() if COL_FONTE_PADRAO[c] != pos]
    if mudadas:
        log("colunas fora da posição de sempre, lidas pelo cabeçalho:")
        for c in mudadas:
            log(f"  {c}: coluna {COL_FONTE_PADRAO[c] + 1} -> {COL_FONTE[c] + 1}")

    de_la = tudo_de_la[1:]
    nossas = com_retentativa("leitura da nossa planilha", ws.get_all_values)
    cab_atual = [c.strip().lower() for c in (nossas[0] if nossas else [])]

    if cab_atual and cab_atual != ORDEM_COLUNAS_LOGICA:
        log("reordenando colunas da planilha para a ordem lógica padrão...")
        pos_antiga = {col: idx for idx, col in enumerate(cab_atual)}
        novas_linhas = [ORDEM_COLUNAS_LOGICA]
        for linha in nossas[1:]:
            nova_linha = []
            for c in ORDEM_COLUNAS_LOGICA:
                idx = pos_antiga.get(c)
                val = (linha[idx] if idx is not None and idx < len(linha) else "").strip()
                nova_linha.append(val)
            novas_linhas.append(nova_linha)

        if len(ORDEM_COLUNAS_LOGICA) > ws.col_count:
            ws.add_cols(len(ORDEM_COLUNAS_LOGICA) - ws.col_count)

        com_retentativa("reordenação das colunas", lambda: ws.update(values=novas_linhas, range_name="A1"))
        log(f"planilha reordenada com sucesso ({len(ORDEM_COLUNAS_LOGICA)} colunas)!")
        nossas = novas_linhas

    log(f"fonte '{FONTE_ABA}': {len(de_la)} linha(s) | nossa: {len(nossas) - 1} linha(s)")

    # Mapear debates na fonte por ident_esperado -> idf
    mapa_fonte_esperado = {}
    for l in de_la:
        idf = (l[COL_FONTE["id_debate"]] if l else "").strip()
        if idf:
            ident_esp = gerar_id(l, set())
            mapa_fonte_esperado[ident_esp] = idf

    # Identificar linhas prontas e linhas duplicadas indevidas
    linhas_prontas = {}
    duplicadas_para_apagar = []
    restauracoes_id_fonte = []

    for idx, l in enumerate(nossas[1:], start=2):
        ident = (l[COL["id"]] if COL["id"] < len(l) else "").strip()
        ident_base = re.sub(r"-\d+$", "", ident)
        status = (l[COL["status"]] if COL["status"] < len(l) else "").strip().lower()
        idf_atual = (l[COL["id_fonte"]] if COL["id_fonte"] < len(l) else "").strip()

        if status == "pronto":
            linhas_prontas[ident_base] = idx
            if ident_base in mapa_fonte_esperado and not idf_atual.startswith("D"):
                idf_correto = mapa_fonte_esperado[ident_base]
                restauracoes_id_fonte.append({"range": f"R{idx}", "values": [[idf_correto]]})
                log(f"restaurando id_fonte {idf_correto} na linha {idx} ({ident_base})")
        else:
            if ident_base in linhas_prontas:
                duplicadas_para_apagar.append(idx)
                log(f"identificada duplicata para apagar: linha {idx} ({ident})")

    if restauracoes_id_fonte:
        com_retentativa("restauração de id_fonte", lambda: ws.batch_update(restauracoes_id_fonte))

    if duplicadas_para_apagar:
        for idx in sorted(duplicadas_para_apagar, reverse=True):
            log(f"apagando linha duplicada {idx}...")
            com_retentativa(f"apagar linha {idx}", lambda i=idx: ws.delete_rows(i))
        nossas = com_retentativa("releitura pós-limpeza", ws.get_all_values)

    por_fonte = {}
    usados = set()
    for i, l in enumerate(nossas[1:], start=2):
        ident = (l[COL["id"]] if COL["id"] < len(l) else "").strip()
        idf = (l[COL["id_fonte"]] if COL["id_fonte"] < len(l) else "").strip()
        if ident:
            usados.add(ident)
        if idf:
            por_fonte[idf] = (i, l)

    novas, edicoes, promovidas = [], [], []
    for l in de_la:
        idf = (l[COL_FONTE["id_debate"]] if l else "").strip()
        if not idf:
            continue
        campos = [(l[COL_FONTE[c]] if COL_FONTE[c] < len(l) else "").strip() for c in DA_FONTE]
        tem_url = bool(campos[DA_FONTE.index("url_youtube")])

        if idf not in por_fonte:
            ident = gerar_id(l, usados)
            linha = [""] * len(COL)
            linha[COL["id"]] = ident
            for nome, valor in zip(DA_FONTE, campos):
                linha[COL[nome]] = valor
            linha[COL["status"]] = "pendente" if tem_url else "agendado"
            linha[COL["observacoes"]] = (l[COL_FONTE["observacoes"]]
                                         if COL_FONTE["observacoes"] < len(l) else "")
            linha[COL["id_fonte"]] = idf
            novas.append(linha)
            log(f"  novo   {idf} -> {ident} ({linha[COL['status']]})")
            continue

        i, atual = por_fonte[idf]
        status = (atual[COL["status"]] if COL["status"] < len(atual) else "").strip().lower()
        merge = [
            novo or (atual[COL[nome]] if COL[nome] < len(atual) else "")
            for nome, novo in zip(DA_FONTE, campos)
        ]
        if merge != [(atual[COL[n]] if COL[n] < len(atual) else "") for n in DA_FONTE]:
            edicoes.append({"range": FAIXA_FONTE.format(i=i), "values": [merge]})
            log(f"  atualiza {idf} (linha {i})")
        if tem_url and status == "agendado":
            promovidas.append({"range": f"K{i}", "values": [["pendente"]]})
            log(f"  {idf} ganhou link -> pendente")

    if edicoes or promovidas:
        com_retentativa("sync das edições", lambda: ws.batch_update(edicoes + promovidas))
    if novas:
        com_retentativa("sync das linhas novas", lambda: ws.append_rows(novas, table_range="A1"))

    log(f"sync: {len(novas)} novo(s), {len(edicoes)} atualizado(s), "
        f"{len(promovidas)} promovido(s) para 'pendente', {len(duplicadas_para_apagar)} duplicada(s) apagada(s)")


RE_CABECA_BRUTO = re.compile(
    r"^### bloco (\d+) \((\d{1,2}:\d{2}:\d{2}) a (\d{1,2}:\d{2}:\d{2})\)$", re.M)


def falas_do_bruto(caminho):
    """Refaz a transcrição a partir do _bruto.md, sem chamar a API.

    O bruto guarda a resposta crua do modelo bloco a bloco, e é o que permite
    consertar um parse ruim sem pagar a transcrição de novo. Ele sai junto com
    o csv em toda rodada e fica 30 dias no artefato do run.

    O corte e o limite de sobreposição são remontados do cabeçalho de cada
    bloco, do mesmo jeito que o processar monta: só o último bloco vai até o
    fim, os outros param em BLOCO_SEG para não duplicar a borda.
    """
    texto = Path(caminho).read_text(encoding="utf-8")
    partes = RE_CABECA_BRUTO.split(texto)
    cabecas = [(int(partes[i]), partes[i + 1], partes[i + 3])
               for i in range(1, len(partes), 4)]
    if not cabecas:
        raise RuntimeError(f"{caminho} não tem cabeçalho de bloco")

    linhas, por_bloco = [], {}
    for k, (n, ini, corpo) in enumerate(cabecas):
        pos = em_segundos(ini)
        limite = None if k == len(cabecas) - 1 else BLOCO_SEG
        novas, ignoradas, _ = parsear(corpo, pos, limite)
        por_bloco[n] = (len(novas), sum(len(r["fala"].split()) for r in novas),
                        ignoradas)
        linhas.extend(novas)
    linhas.sort(key=lambda r: r["segundos"])
    unificar_falantes(linhas)
    return linhas, por_bloco


def reescrever_doc(drive, link, linhas):
    """Regrava o Doc da transcrição a partir das falas, no mesmo arquivo.

    Sobe text/plain por cima do id que já existe: o Drive converte e o link
    continua o mesmo. Subir com enviar_drive criaria um segundo Doc de mesmo
    nome na pasta do debate, e quem já tem o link ficaria com o antigo.
    """
    import io
    from googleapiclient.http import MediaIoBaseUpload

    conteudo = "".join(f"[{r['tempo']}] {r['falante']}: {r['fala']}\n"
                       for r in linhas)
    midia = MediaIoBaseUpload(io.BytesIO(conteudo.encode("utf-8")),
                              mimetype="text/plain", resumable=False)
    com_retentativa(
        "regravação do Doc da transcrição",
        lambda: drive.files().update(
            fileId=extrair_id_drive(link), media_body=midia,
            supportsAllDrives=True, fields="id",
        ).execute(),
    )


def remarcar(args):
    """Refaz eixo, tema, termos e herdado nos CSVs já publicados.

    Existe porque as quatro colunas de assunto saem de dicionário de termos, e
    não do modelo: quando o dicionário muda, o certo é reaplicar sobre a
    transcrição que já está lá, e não pagar a transcrição de novo. O custo é
    zero e a fala, o falante e o tempo não são tocados.

    Com --cortar-antes o trecho anterior ao horário sai da transcrição, e aí a
    fala é tocada: é o jeito de tirar o que não é debate sem pagar a
    transcrição de novo. O áudio da Band de MG de 16/08 tem 50 minutos de
    pré-programa antes do debate, com apresentador e comentaristas de estúdio,
    e essas 69 falas entravam em toda contagem por candidato.

    Escreve no mesmo arquivo do link_csv, em vez de subir outro: o link já foi
    distribuído, e o enviar_drive cria arquivo novo em cada chamada, o que
    deixaria duas planilhas com o mesmo nome na pasta do debate.
    """
    if not PLANILHA:
        sys.exit("defina SPREADSHEET_ID_DEBATES (secret do repo).")
    corte = em_segundos(args.cortar_antes)
    if (corte or args.refazer_do_bruto) and not args.id:
        sys.exit("--cortar-antes e --refazer-do-bruto valem para um debate por "
                 "vez: informe também --id.")
    if args.refazer_do_bruto and not Path(args.refazer_do_bruto).exists():
        sys.exit(f"não encontrei {args.refazer_do_bruto}")
    gc, drive = clientes_google()
    ws = com_retentativa(
        "abertura da planilha de debates",
        lambda: gc.open_by_key(PLANILHA).worksheet(ABA),
    )

    secao("FILA DE REMARCAÇÃO")
    todas = com_retentativa("leitura da fila", ws.get_all_values)
    fila = []
    for i, linha in enumerate(todas[1:], start=2):
        if not linha:
            continue
        def campo(nome, l=linha):
            j = COL[nome]
            return (l[j] if j < len(l) else "").strip()
        if args.id and campo("id") != args.id:
            continue
        if not args.id and campo("status").lower() != "pronto":
            continue
        if not campo("link_csv"):
            log(f"linha {i} ({campo('id')}) não tem link_csv, pulando")
            continue
        fila.append((i, linha))

    if not fila:
        log("nada para remarcar.")
        return
    log(f"{len(fila)} debate(s): "
        f"{', '.join(l[COL['id']] or f'linha {i}' for i, l in fila)}")

    falhas = []
    for i, linha in fila:
        ident = linha[COL["id"]].strip() or f"linha{i}"
        link = linha[COL["link_csv"]].strip()
        secao(f"REMARCANDO {ident}  (linha {i})")
        try:
            csv_id = extrair_id_drive(link)
            aba = com_retentativa(
                f"abertura do csv de {ident}",
                lambda: gc.open_by_key(csv_id).sheet1,
            )
            valores = com_retentativa("leitura do csv", aba.get_all_values)
            if not valores:
                raise RuntimeError("csv vazio")
            cab = [c.strip() for c in valores[0]]
            faltando = [c for c in CAMPOS_CSV[:4] if c not in cab]
            if faltando:
                raise RuntimeError(f"csv sem as colunas {faltando}")
            pos = {c: cab.index(c) for c in cab}
            linhas = [
                {c: (l[pos[c]] if pos[c] < len(l) else "") for c in CAMPOS_CSV[:4]}
                for l in valores[1:]
            ]
            log(f"{len(linhas)} fala(s) lidas de {link}")

            if args.refazer_do_bruto:
                antigas = len(linhas)
                linhas, por_bloco = falas_do_bruto(args.refazer_do_bruto)
                log(f"refeito de {args.refazer_do_bruto}: {antigas} -> {len(linhas)} fala(s)")
                for n, (q, pal, ign) in sorted(por_bloco.items()):
                    aviso = "  <- SEM FALA" if not q else ""
                    log(f"  bloco {n:>2}: {q:>3} falas, {pal:>5} palavras, "
                        f"{ign:>3} ignoradas{aviso}")
                vazios = [n for n, (q, _, _) in por_bloco.items() if not q]
                if vazios:
                    log(f"blocos que continuam sem fala: {vazios} "
                        f"(o bruto não tem o que aproveitar neles)")

            cortadas = []
            if corte:
                def seg(l):
                    return em_segundos(l["segundos"] or l["tempo"])
                cortadas = [l for l in linhas if seg(l) < corte]
                linhas = [l for l in linhas if seg(l) >= corte]
                if not linhas:
                    raise RuntimeError(
                        f"o corte em {args.cortar_antes} não deixaria fala nenhuma")
                pal = sum(len(l["fala"].split()) for l in cortadas)
                log(f"corte em {hhmmss(corte)}: saem {len(cortadas)} fala(s) e "
                    f"{pal} palavra(s); ficam {len(linhas)}")
                quem = sorted({l["falante"] for l in cortadas})
                log(f"falantes que saem: {', '.join(quem) or 'nenhum'}")
                for l in cortadas[:3]:
                    log(f"  - [{l['tempo']}] {l['falante']}: {l['fala'][:70]}")

            antes = sum(1 for l in valores[1:]
                        if pos.get("eixo", 999) < len(l) and l[pos["eixo"]].strip())
            marcar_assuntos(linhas)
            depois = sum(1 for l in linhas if l["eixo"])
            log(f"falas com eixo: {antes} antes, {depois} depois")

            if args.simular:
                log("--simular: não escrevi nada")
                continue

            corpo = [CAMPOS_CSV] + [[l[c] for c in CAMPOS_CSV] for l in linhas]
            com_retentativa("limpeza do csv", aba.clear)
            com_retentativa(
                "escrita do csv",
                lambda: aba.update(values=corpo, range_name="A1"),
            )
            doc = (linha[COL["link_transcricao"]]
                   if COL["link_transcricao"] < len(linha) else "").strip()
            if (cortadas or args.refazer_do_bruto) and doc:
                # O Doc é a mesma transcrição em texto. Cortar um e não o
                # outro deixa os dois discordando, e é o Doc que a equipe lê.
                reescrever_doc(drive, doc, linhas)
                log("Doc da transcrição regravado no mesmo link")

            obs = (linha[COL["observacoes"]] if COL["observacoes"] < len(linha) else "").strip()
            nota = f"remarcado em {agora_brt()}: {depois} falas com eixo (era {antes})"
            if args.refazer_do_bruto:
                nota += f"; transcrição refeita do bruto, {len(linhas)} falas"
            if cortadas:
                nota += (f"; cortadas {len(cortadas)} fala(s) antes de "
                         f"{hhmmss(corte)}, que não eram do debate")
            com_retentativa(
                f"escrita da nota na linha {i}",
                lambda: ws.update(
                    values=[[agora_brt(), f"{obs}; {nota}" if obs else nota]],
                    range_name=f"N{i}:O{i}",
                ),
            )
            log("csv reescrito no mesmo link e nota gravada na planilha")
        except Exception as e:
            log(f"ERRO em {ident}: {e}")
            falhas.append(ident)

    if falhas:
        sys.exit(f"{len(falhas)} debate(s) com erro: {', '.join(falhas)}")


def rodar_fila(args):
    if not PLANILHA or not PASTA_DRIVE:
        sys.exit("defina SPREADSHEET_ID_DEBATES e PASTA_DRIVE_DEBATES (secrets do repo).")
    gc, drive = clientes_google()
    ws = com_retentativa(
        "abertura da planilha de debates",
        lambda: gc.open_by_key(PLANILHA).worksheet(ABA),
    )

    if not args.sem_sync:
        sincronizar(gc, ws)
    if args.so_sync:
        log("--so-sync: parando antes da transcrição")
        return

    secao("FILA")
    todas = com_retentativa("leitura da fila", ws.get_all_values)
    log(f"planilha: {len(todas) - 1} linha(s) na aba '{ABA}'")

    fila = []
    for i, linha in enumerate(todas[1:], start=2):
        if not linha or not linha[COL["url_youtube"]].strip():
            continue
        status = (linha[COL["status"]] if COL["status"] < len(linha) else "").strip().lower()
        ident = linha[COL["id"]].strip()
        if args.id and ident != args.id:
            continue
        if not args.id and status != "pendente":
            continue
        fila.append((i, linha))

    if not fila:
        log("nada pendente. Ponha o status de uma linha em 'pendente' e rode de novo.")
        return

    log(f"fila: {len(fila)} debate(s) -> {', '.join(l[COL['id']] or f'linha {i}' for i, l in fila)}")

    falhas = []
    for i, linha in fila:
        ident = linha[COL["id"]].strip() or f"linha{i}"
        url = linha[COL["url_youtube"]].strip()
        secao(f"DEBATE {ident}  (linha {i})")

        escrever_celula(ws, i, COL["status"] + 1, "processando")
        saida = Path(args.saida) / ident
        saida.mkdir(parents=True, exist_ok=True)
        try:
            contexto = contexto_da_linha(linha)
            log("contexto montado a partir da planilha:")
            for l_ in contexto.splitlines():
                log(f"  | {l_}")

            def campo(nome):
                j = COL[nome]
                return (linha[j] if j < len(linha) else "").strip()

            # Uma pasta por dia de debate, dentro do mês, dentro da UF, com subpasta para o debate.
            trilha = caminho_do_debate(campo("uf"), campo("data"), ident=ident)
            destino_drive = pasta_do_debate(drive, campo("uf"), campo("data"), ident=ident)
            log(f"destino no Drive: {' / '.join(trilha)}")

            link_audio_existente = linha[COL["link_audio"]].strip() if COL.get("link_audio", 999) < len(linha) else ""
            if link_audio_existente:
                log(f"link_audio já disponível no Drive: {link_audio_existente}")
                try:
                    audio = baixar_audio_drive(drive, link_audio_existente, saida / "_trabalho")
                except Exception as e_drive:
                    log(f"falha ao baixar do Drive ({e_drive}), tentando YouTube...")
                    audio = baixar_audio(url, saida / "_trabalho")
            else:
                try:
                    audio = baixar_audio(url, saida / "_trabalho")
                except Exception as e_yt:
                    if link_audio_existente:
                        audio = baixar_audio_drive(drive, link_audio_existente, saida / "_trabalho")
                    else:
                        raise e_yt

            # O mp3 sobe antes de transcrever, e não depois: se a transcrição
            # falhar, o áudio já está guardado e a segunda tentativa não
            # precisa baixar de novo do YouTube, que é a parte que trava.
            if not link_audio_existente:
                secao("ÁUDIO NO DRIVE")
                link_audio = enviar_drive(
                    drive, audio, f"{ident}.mp3", pasta=destino_drive
                )
                escrever_celula(ws, i, COL["link_audio"] + 1, link_audio)
                log(f"{ident}.mp3 ({audio.stat().st_size / 1e6:.0f} MB) -> {link_audio}")

            caminhos, avisos = processar(
                audio, contexto, saida, ident, args.inicio, args.duracao
            )

            secao("RESUMO EXECUTIVO TIMBRADO")
            link_resumo = ""
            try:
                from outros.resumo_debates import gerar as gerar_resumo, meta_da_linha, criar_docx_timbrado, titulo as titulo_resumo
                meta = meta_da_linha(linha, COL, todas)
                with open(saida / f"{ident}.csv", "r", encoding="utf-8") as f_csv:
                    falas = list(csv.DictReader(f_csv))
                md_resumo = gerar_resumo(falas, meta)
                arq_md = saida / f"{ident}_resumo.md"
                arq_md.write_text(md_resumo, encoding="utf-8")

                template_docx = Path(__file__).parent / "templates" / "timbrado_eleicoes.docx"
                arq_upload_resumo = arq_md
                if template_docx.exists():
                    arq_docx = saida / f"{ident}_resumo.docx"
                    if criar_docx_timbrado(md_resumo, titulo_resumo(meta), "Eleições 2026 • Monitoramento de Debates", template_docx, arq_docx):
                        arq_upload_resumo = arq_docx
                        log(f"DOCX Timbrado gerado: {arq_docx.name} (fonte Montserrat)")

                link_resumo = enviar_drive(drive, arq_upload_resumo, f"{ident}_resumo",
                                           "application/vnd.google-apps.document", destino_drive)
                log(f"resumo timbrado no Drive: {link_resumo}")
            except Exception as e_resumo:
                log(f"aviso: falha ao gerar resumo automático ({e_resumo})")
                link_resumo = ""

            secao("DRIVE")
            links = {}
            for p in caminhos:
                # txt vira Doc e csv vira Sheets: é como a equipe lê e cruza
                # depois. O bruto fica como arquivo, que é material de conferência.
                destino = {".txt": "application/vnd.google-apps.document",
                           ".csv": "application/vnd.google-apps.spreadsheet"}.get(p.suffix)
                links[p.suffix] = enviar_drive(drive, p, p.name, destino, destino_drive)
                log(f"{p.name} -> {links[p.suffix]}")

            com_retentativa(
                f"escrita da saída na linha {i}",
                lambda: ws.update(
                    values=[[link_resumo, links.get(".txt", ""), links.get(".csv", ""),
                             link_audio, agora_brt(), avisos]],
                    range_name=FAIXA_SAIDA.format(i=i),
                ),
            )
            escrever_celula(ws, i, COL["status"] + 1, "pronto")
            log(f"linha {i} marcada como 'pronto'")
        except Exception as e:
            log(f"ERRO em {ident}: {e}")
            falhas.append(ident)
            # Marcar 'erro' não pode derrubar a fila. Em 14/08 essa escrita
            # caiu junto com a rede, o processo morreu aqui, e a linha ficou
            # presa em 'processando', que nenhum run seguinte pega de volta.
            try:
                escrever_celula(ws, i, COL["status"] + 1, "erro")
                escrever_celula(ws, i, COL["observacoes"] + 1, str(e)[:400])
            except Exception as e_status:
                log(f"não deu para marcar 'erro' na linha {i}: {e_status}")
                log(f"ponha o status da linha {i} na mão, senão ela fica em 'processando'")
        finally:
            shutil.rmtree(saida / "_trabalho", ignore_errors=True)

    # Um debate com erro não pode parar os outros da fila, mas o run tem que
    # ficar vermelho: verde com transcrição faltando é o que passa despercebido.
    if falhas:
        sys.exit(f"{len(falhas)} debate(s) com erro: {', '.join(falhas)}")


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="Transcreve debate com quem falou o quê")
    fonte = ap.add_mutually_exclusive_group(required=True)
    fonte.add_argument("--fila", action="store_true", help="processa os 'pendente' da planilha")
    fonte.add_argument("--url", help="URL avulsa do vídeo (YouTube)")
    fonte.add_argument("--audio", help="arquivo de áudio local")
    fonte.add_argument("--reorganizar-drive", action="store_true",
                       help="move o que já está no Drive para UF/ano-mês/dia e sai")
    fonte.add_argument("--remarcar", action="store_true",
                       help="refaz eixo, tema e termos nos CSVs já prontos, sem chamar a API")
    ap.add_argument("--simular", action="store_true",
                    help="com --reorganizar-drive ou --remarcar, só mostra o que faria")
    ap.add_argument("--id", default=None, help="com --fila, roda só esse id (ignora o status)")
    ap.add_argument("--sem-sync", action="store_true",
                    help="não puxa o calendário do monitoramento antes da fila")
    ap.add_argument("--so-sync", action="store_true",
                    help="só sincroniza o calendário e sai, sem transcrever nada")
    ap.add_argument("--cortar-antes", default=None,
                    help="com --remarcar e --id, tira da transcrição as falas "
                         "anteriores a esse HH:MM:SS (pré-programa da emissora)")
    ap.add_argument("--refazer-do-bruto", default=None,
                    help="com --remarcar e --id, reparseia o _bruto.md do run e "
                         "reescreve a transcrição, sem chamar a API")
    ap.add_argument("--inicio", default=None, help="HH:MM:SS, recorta o áudio antes de processar")
    ap.add_argument("--duracao", default=None, help="HH:MM:SS, recorta o áudio antes de processar")
    ap.add_argument("--contexto", default=None, help="participantes; só nos modos avulsos")
    ap.add_argument("--saida", default="transcricoes", help="pasta de saída local")
    ap.add_argument("--nome", default="debate", help="prefixo dos arquivos, nos modos avulsos")
    args = ap.parse_args()

    # Arrumar pasta não chama o modelo e não depende de chave nenhuma.
    if args.reorganizar_drive:
        secao("REORGANIZAR O DRIVE" + (" (simulação)" if args.simular else ""))
        reorganizar_drive(args)
        return

    # Remarcar é dicionário de termos, não modelo: roda sem chave e sem custo.
    if args.remarcar:
        secao("REMARCAR ASSUNTOS" + (" (simulação)" if args.simular else ""))
        log(f"planilha : {PLANILHA}")
        log(f"alvo     : {args.id or 'todos os debates prontos'}")
        log(f"corte    : {args.cortar_antes or 'nenhum, só refaz os assuntos'}")
        remarcar(args)
        return

    # --so-sync só mexe em planilha, não chama o modelo.
    if not args.so_sync and not os.getenv("GEMINI_API_KEY", "").strip():
        sys.exit("GEMINI_API_KEY não definido.")

    secao("CONFIGURAÇÃO")
    log(f"modelo   : {GEMINI_MODEL}")
    log(f"bloco    : {BLOCO_SEG}s + {SOBREPOSICAO_SEG}s de sobreposição")
    log(f"modo     : {'fila' if args.fila else ('url' if args.url else 'áudio local')}")
    log(f"recorte  : {args.inicio or 'do início'} por {args.duracao or 'tudo'}")
    log(f"saída    : {Path(args.saida).resolve()}")
    if args.fila:
        log(f"planilha : {PLANILHA}")
        log(f"pasta    : {PASTA_DRIVE}")
        rodar_fila(args)
        return

    # Modo avulso: não toca na planilha nem no Drive, serve para teste rápido.
    contexto = (args.contexto or os.getenv("DEBATE_CONTEXTO", "")).strip() or CONTEXTO_PADRAO
    log("contexto informado ao modelo:")
    for l_ in contexto.splitlines():
        log(f"  | {l_}")

    saida = Path(args.saida)
    saida.mkdir(parents=True, exist_ok=True)

    secao("ÁUDIO")
    if args.url:
        trabalho = saida / "_trabalho"
        trabalho.mkdir(exist_ok=True)
        origem = baixar_audio(args.url, trabalho)
    else:
        origem = Path(args.audio).resolve()
        if not origem.exists():
            sys.exit(f"arquivo não encontrado: {origem}")

    processar(origem, contexto, saida, args.nome, args.inicio, args.duracao)


if __name__ == "__main__":
    main()
