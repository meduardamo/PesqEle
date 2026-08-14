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

Uso:
    python -m outros.transcricao_debates --fila
    python -m outros.transcricao_debates --fila --id 2026-band-sp-gov-t1
    python -m outros.transcricao_debates --url "https://youtube.com/watch?v=..."
    python -m outros.transcricao_debates --audio debate.mp3 --inicio 00:30:00 --duracao 00:10:00
"""

import argparse
import csv
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
    "status": 10, "link_transcricao": 11, "link_csv": 12,
    "processado_em": 13, "observacoes": 14, "id_fonte": 15, "link_audio": 16,
}
# Subpasta de Debates onde o mp3 fica guardado. Procurada pelo nome em vez de
# ir para outro secret: é filha de uma pasta que já está em secret.
SUBPASTA_AUDIOS = "audios"
# Onde o script escreve de volta: link_transcricao até observacoes.
FAIXA_SAIDA = "L{i}:O{i}"

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

RE_LINHA = re.compile(r"^\[(\d{1,2}):(\d{2})(?::(\d{2}))?\]\s*([^:]{1,60}?)\s*:\s*(.+)$")

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


def agora_brt():
    return datetime.now(timezone(timedelta(hours=-3))).strftime("%Y-%m-%d %H:%M")


def exige(binario):
    if not shutil.which(binario):
        sys.exit(f"'{binario}' não encontrado no PATH.")


# ---------------------------------------------------------------- áudio

def baixar_audio(url, destino):
    exige("yt-dlp")
    log(f"baixando áudio de {url}")
    cmd = [
        "yt-dlp", "-x", "--audio-format", "mp3", "--audio-quality", "5",
        "--no-playlist", "--newline", "-o", str(destino / "debate.%(ext)s"), url,
    ]

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


def parsear(texto, offset, limite):
    """Converte a saída do modelo em linhas com tempo absoluto.

    Descarta o que caiu na sobreposição: o bloco seguinte cobre esse trecho e
    manter os dois duplica fala no meio da transcrição.
    """
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
    arq = drive.files().create(
        body=corpo, media_body=midia,
        supportsAllDrives=True, fields="id,webViewLink",
    ).execute()
    return arq["webViewLink"]


def pasta_audios(drive):
    """Id da subpasta 'audios', criada na primeira vez que for preciso."""
    q = (f"'{PASTA_DRIVE}' in parents and name='{SUBPASTA_AUDIOS}' "
         "and mimeType='application/vnd.google-apps.folder' and trashed=false")
    achou = drive.files().list(
        q=q, includeItemsFromAllDrives=True, supportsAllDrives=True,
        fields="files(id)",
    ).execute().get("files", [])
    if achou:
        return achou[0]["id"]
    nova = drive.files().create(
        body={"name": SUBPASTA_AUDIOS, "parents": [PASTA_DRIVE],
              "mimeType": "application/vnd.google-apps.folder"},
        supportsAllDrives=True, fields="id",
    ).execute()
    log(f"subpasta '{SUBPASTA_AUDIOS}' criada no Drive")
    return nova["id"]


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

def processar(origem, contexto, saida, nome, inicio=None, dur=None):
    """Transcreve um áudio inteiro e grava txt, csv e bruto. Devolve os caminhos."""
    trabalho = saida / "_trabalho"
    trabalho.mkdir(parents=True, exist_ok=True)

    total_bruto = duracao(origem)
    log(f"duração original: {hhmmss(total_bruto)}")
    if inicio or dur:
        log(f"recortando de {inicio or '00:00:00'}" + (f" por {dur}" if dur else " até o fim"))
        origem = recortar(origem, trabalho / "recorte.mp3", inicio or "00:00:00", dur)

    total = duracao(origem)
    n_blocos = max(1, -(-int(total) // BLOCO_SEG))
    log(f"duração a processar: {hhmmss(total)} em {n_blocos} bloco(s)")

    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    linhas, textos, suspeitos = [], {}, []
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
            log(f"    bloco {n} voltou vazio depois de {TENTATIVAS} tentativas, segue")
            return texto

        novas, ignoradas, fora = parsear(texto, pos, limite)
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
    vazios = [c["n"] for c in cortes if not textos.get(c["n"], "").strip()]
    if vazios:
        secao("REPESCAGEM DOS BLOCOS VAZIOS")
        log(f"vazios na primeira passada: {vazios}")
        log(f"esperando {ESPERA_REPESCAGEM}s para a onda de erro passar")
        time.sleep(ESPERA_REPESCAGEM)
        for c in cortes:
            if c["n"] in vazios:
                rodar(c)

    vazios = [c["n"] for c in cortes if not textos.get(c["n"], "").strip()]
    if not linhas:
        raise RuntimeError("nenhuma fala transcrita")

    secao("SAÍDA")
    linhas.sort(key=lambda r: r["segundos"])
    base = saida / nome

    with open(f"{base}.txt", "w", encoding="utf-8") as f:
        for r in linhas:
            f.write(f"[{r['tempo']}] {r['falante']}: {r['fala']}\n")
    with open(f"{base}.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["segundos", "tempo", "falante", "fala"])
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
    log(f"cobertura: {hhmmss(linhas[-1]['segundos'])} de {hhmmss(total)} de áudio")
    log(f"DESCONHECIDO: {desc:.1%} das palavras")
    if vazios:
        log(f"blocos vazios: {vazios} (esses trechos NÃO estão na transcrição)")
    if suspeitos:
        log(f"blocos com ritmo de fala baixo: {suspeitos} (confira contra o vídeo)")
    if not vazios and not suspeitos and desc < 0.05:
        log("nenhum bloco vazio, nenhum bloco suspeito de resumo")
    log("a identificação de falante vem do conteúdo, não da voz: confira as")
    log("trocas de bloco e os apartes contra o vídeo antes de citar em entrega")

    shutil.rmtree(trabalho, ignore_errors=True)
    # O gasto entra em observacoes junto com os avisos: é por linha da planilha
    # que dá para somar quanto custou cada debate depois.
    avisos = [f"gasto: {resumo_custo}"]
    if vazios:
        avisos.append(f"blocos vazios: {vazios}")
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


COL_FONTE = {
    "id_debate": 0, "data": 1, "horario": 2, "cargo": 3, "uf": 4, "turno": 5,
    "emissora": 6, "url_youtube": 7, "mediador": 8, "participantes": 9,
    "observacoes": 10,
}


def sincronizar(gc, ws):
    """Traz o calendário da aba do monitoramento para a nossa planilha.

    Casa por id_fonte, que é a coluna estável de lá. Debate novo entra; debate
    já sincronizado só recebe campo que veio preenchido, para não apagar o que
    alguém completou na mão. Linha em 'processando' ou 'pronto' não tem o
    status mexido: transcrição já feita não volta para a fila.
    """
    if not FONTE_PLANILHA:
        log("SPREADSHEET_ID_INTERNO não definido, sync pulado")
        return

    secao("SYNC COM O MONITORAMENTO")
    fonte = gc.open_by_key(FONTE_PLANILHA).worksheet(FONTE_ABA)
    de_la = fonte.get_all_values()[1:]
    nossas = ws.get_all_values()
    log(f"fonte '{FONTE_ABA}': {len(de_la)} linha(s) | nossa: {len(nossas) - 1} linha(s)")

    por_fonte = {}
    usados = set()
    for i, l in enumerate(nossas[1:], start=2):
        if l and l[COL["id"]].strip():
            usados.add(l[COL["id"]].strip())
        idf = (l[COL["id_fonte"]] if COL["id_fonte"] < len(l) else "").strip()
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
        # Campo vazio na fonte não apaga o que já está preenchido aqui.
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
        ws.batch_update(edicoes + promovidas)
    if novas:
        ws.append_rows(novas, table_range="A1")

    log(f"sync: {len(novas)} novo(s), {len(edicoes)} atualizado(s), "
        f"{len(promovidas)} promovido(s) para 'pendente'")


def rodar_fila(args):
    if not PLANILHA or not PASTA_DRIVE:
        sys.exit("defina SPREADSHEET_ID_DEBATES e PASTA_DRIVE_DEBATES (secrets do repo).")
    gc, drive = clientes_google()
    ws = gc.open_by_key(PLANILHA).worksheet(ABA)

    if not args.sem_sync:
        sincronizar(gc, ws)
    if args.so_sync:
        log("--so-sync: parando antes da transcrição")
        return

    secao("FILA")
    todas = ws.get_all_values()
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

    for i, linha in fila:
        ident = linha[COL["id"]].strip() or f"linha{i}"
        url = linha[COL["url_youtube"]].strip()
        secao(f"DEBATE {ident}  (linha {i})")

        ws.update_cell(i, COL["status"] + 1, "processando")
        saida = Path(args.saida) / ident
        saida.mkdir(parents=True, exist_ok=True)
        try:
            contexto = contexto_da_linha(linha)
            log("contexto montado a partir da planilha:")
            for l_ in contexto.splitlines():
                log(f"  | {l_}")

            audio = baixar_audio(url, saida / "_trabalho")

            # O mp3 sobe antes de transcrever, e não depois: se a transcrição
            # falhar, o áudio já está guardado e a segunda tentativa não
            # precisa baixar de novo do YouTube, que é a parte que trava.
            secao("ÁUDIO NO DRIVE")
            link_audio = enviar_drive(
                drive, audio, f"{ident}.mp3", pasta=pasta_audios(drive)
            )
            ws.update_cell(i, COL["link_audio"] + 1, link_audio)
            log(f"{ident}.mp3 ({audio.stat().st_size / 1e6:.0f} MB) -> {link_audio}")

            caminhos, avisos = processar(
                audio, contexto, saida, ident, args.inicio, args.duracao
            )

            secao("DRIVE")
            links = {}
            for p in caminhos:
                # txt vira Doc e csv vira Sheets: é como a equipe lê e cruza
                # depois. O bruto fica como arquivo, que é material de conferência.
                destino = {".txt": "application/vnd.google-apps.document",
                           ".csv": "application/vnd.google-apps.spreadsheet"}.get(p.suffix)
                links[p.suffix] = enviar_drive(drive, p, p.name, destino)
                log(f"{p.name} -> {links[p.suffix]}")

            ws.update(
                values=[[links.get(".txt", ""), links.get(".csv", ""),
                         agora_brt(), avisos]],
                range_name=FAIXA_SAIDA.format(i=i),
            )
            ws.update_cell(i, COL["status"] + 1, "pronto")
            log(f"linha {i} marcada como 'pronto'")
        except Exception as e:
            log(f"ERRO em {ident}: {e}")
            ws.update_cell(i, COL["status"] + 1, "erro")
            ws.update_cell(i, COL["observacoes"] + 1, str(e)[:400])
        finally:
            shutil.rmtree(saida / "_trabalho", ignore_errors=True)


# ---------------------------------------------------------------- main

def main():
    ap = argparse.ArgumentParser(description="Transcreve debate com quem falou o quê")
    fonte = ap.add_mutually_exclusive_group(required=True)
    fonte.add_argument("--fila", action="store_true", help="processa os 'pendente' da planilha")
    fonte.add_argument("--url", help="URL avulsa do vídeo (YouTube)")
    fonte.add_argument("--audio", help="arquivo de áudio local")
    ap.add_argument("--id", default=None, help="com --fila, roda só esse id (ignora o status)")
    ap.add_argument("--sem-sync", action="store_true",
                    help="não puxa o calendário do monitoramento antes da fila")
    ap.add_argument("--so-sync", action="store_true",
                    help="só sincroniza o calendário e sai, sem transcrever nada")
    ap.add_argument("--inicio", default=None, help="HH:MM:SS, recorta o áudio antes de processar")
    ap.add_argument("--duracao", default=None, help="HH:MM:SS, recorta o áudio antes de processar")
    ap.add_argument("--contexto", default=None, help="participantes; só nos modos avulsos")
    ap.add_argument("--saida", default="transcricoes", help="pasta de saída local")
    ap.add_argument("--nome", default="debate", help="prefixo dos arquivos, nos modos avulsos")
    args = ap.parse_args()

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
