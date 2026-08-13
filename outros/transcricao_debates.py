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

Uso:
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
from pathlib import Path

from google import genai
from google.genai import types

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")

BLOCO_SEG = 600          # 10 min de conteúdo por bloco
SOBREPOSICAO_SEG = 20    # o bloco vai 20s além, para não cortar frase na borda
TENTATIVAS = 3

# Linhas por minuto abaixo disso é sinal de que o modelo resumiu em vez de
# transcrever. Medido no debate Band SP: bloco normal fica entre 8 e 20.
LINHAS_POR_MIN_SUSPEITO = 4.0

CONTEXTO_PADRAO = """\
Debate ao governo de São Paulo, TV Bandeirantes, eleições de 2026.

Participantes:
- Rodolfo Schneider, mediador do debate
- Tarcísio de Freitas (REPUBLICANOS)
- Fernando Haddad (PT)
- Jornalistas da bancada, que fazem perguntas aos candidatos

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


def exige(binario):
    if not shutil.which(binario):
        sys.exit(f"'{binario}' não encontrado no PATH.")


def baixar_audio(url, destino):
    exige("yt-dlp")
    log(f"baixando áudio de {url}")
    saida = destino / "debate.%(ext)s"
    cmd = [
        "yt-dlp", "-x", "--audio-format", "mp3", "--audio-quality", "5",
        "--no-playlist", "--newline", "-o", str(saida), url,
    ]
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
        sys.exit("yt-dlp falhou. Se for bloqueio do YouTube, popule o secret YTDLP_COOKIES.")

    mp3 = destino / "debate.mp3"
    if not mp3.exists():
        sys.exit("yt-dlp terminou sem gerar debate.mp3.")
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


def subir(client, caminho):
    arq = client.files.upload(file=str(caminho))
    esperou = 0
    while arq.state.name == "PROCESSING":
        time.sleep(2)
        esperou += 2
        arq = client.files.get(name=arq.name)
    if arq.state.name != "ACTIVE":
        raise RuntimeError(f"upload terminou em {arq.state.name}")
    return arq, esperou


def transcrever(client, arq, contexto):
    prompt = PROMPT.format(contexto=contexto.strip())
    for n in range(1, TENTATIVAS + 1):
        t = time.time()
        try:
            resp = client.models.generate_content(
                model=GEMINI_MODEL,
                contents=[prompt, arq],
                config=types.GenerateContentConfig(temperature=0.0, max_output_tokens=32000),
            )
            texto = (resp.text or "").strip()
            u = getattr(resp, "usage_metadata", None)
            tokens = (
                f"{getattr(u, 'prompt_token_count', 0) or 0} in / "
                f"{getattr(u, 'candidates_token_count', 0) or 0} out"
            ) if u else "sem contagem"
            if texto:
                log(f"    resposta em {time.time() - t:.1f}s, {tokens}")
                return texto
            log(f"    tentativa {n}/{TENTATIVAS}: resposta vazia ({tokens})")
        except Exception as e:
            log(f"    tentativa {n}/{TENTATIVAS} falhou em {time.time() - t:.1f}s: {e}")
        if n < TENTATIVAS:
            espera = 5 * n
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


def main():
    ap = argparse.ArgumentParser(description="Transcreve debate com quem falou o quê")
    fonte = ap.add_mutually_exclusive_group(required=True)
    fonte.add_argument("--url", help="URL do vídeo (YouTube)")
    fonte.add_argument("--audio", help="arquivo de áudio local")
    ap.add_argument("--inicio", default=None, help="HH:MM:SS, recorta o áudio antes de processar")
    ap.add_argument("--duracao", default=None, help="HH:MM:SS, recorta o áudio antes de processar")
    ap.add_argument("--contexto", default=None, help="participantes do debate; sobrescreve o padrão")
    ap.add_argument("--saida", default="transcricoes", help="pasta de saída")
    ap.add_argument("--nome", default="debate", help="prefixo dos arquivos gerados")
    args = ap.parse_args()

    chave = os.getenv("GEMINI_API_KEY", "").strip()
    if not chave:
        sys.exit("GEMINI_API_KEY não definido.")

    # O workflow passa por env var, não por argumento: contexto tem quebra de
    # linha, e no shell isso vira barra-n literal dentro do prompt.
    contexto = (args.contexto or os.getenv("DEBATE_CONTEXTO", "")).strip() or CONTEXTO_PADRAO
    saida = Path(args.saida)
    saida.mkdir(parents=True, exist_ok=True)
    trabalho = saida / "_trabalho"
    trabalho.mkdir(exist_ok=True)

    secao("CONFIGURAÇÃO")
    log(f"modelo          : {GEMINI_MODEL}")
    log(f"bloco           : {BLOCO_SEG}s + {SOBREPOSICAO_SEG}s de sobreposição")
    log(f"fonte           : {args.url or args.audio}")
    log(f"recorte         : {args.inicio or 'do início'} por {args.duracao or 'tudo'}")
    log(f"saída           : {saida.resolve()}")
    log("contexto informado ao modelo:")
    for linha in contexto.splitlines():
        log(f"  | {linha}")

    secao("ÁUDIO")
    origem = baixar_audio(args.url, trabalho) if args.url else Path(args.audio).resolve()
    if not origem.exists():
        sys.exit(f"arquivo não encontrado: {origem}")

    total_bruto = duracao(origem)
    log(f"duração original: {hhmmss(total_bruto)}")

    if args.inicio or args.duracao:
        ini = args.inicio or "00:00:00"
        log(f"recortando de {ini}" + (f" por {args.duracao}" if args.duracao else " até o fim"))
        origem = recortar(origem, trabalho / "recorte.mp3", ini, args.duracao)

    total = duracao(origem)
    n_blocos = max(1, -(-int(total) // BLOCO_SEG))
    log(f"duração a processar: {hhmmss(total)} em {n_blocos} bloco(s)")

    client = genai.Client(api_key=chave)
    linhas, bruto, vazios, suspeitos = [], [], [], []

    secao("TRANSCRIÇÃO")
    inicio = 0
    n = 0
    while inicio < total - 1:
        n += 1
        ultimo = inicio + BLOCO_SEG >= total
        dur = min(BLOCO_SEG + SOBREPOSICAO_SEG, total - inicio)
        limite = None if ultimo else BLOCO_SEG
        log(f"bloco {n}/{n_blocos}  {hhmmss(inicio)} a {hhmmss(inicio + dur)}")

        arq_bloco = recortar(origem, trabalho / f"bloco_{n:03d}.mp3", inicio, int(dur))
        log(f"    cortado: {arq_bloco.stat().st_size / 1e6:.2f} MB")

        subido, esperou = subir(client, arq_bloco)
        log(f"    enviado ({esperou}s de processamento no servidor)")

        texto = transcrever(client, subido, contexto)
        try:
            client.files.delete(name=subido.name)
        except Exception:
            pass

        bruto.append(f"### bloco {n} ({hhmmss(inicio)} a {hhmmss(inicio + dur)})\n\n{texto}")

        if not texto:
            vazios.append(n)
            log(f"    bloco {n} voltou vazio depois de {TENTATIVAS} tentativas, segue")
            inicio += BLOCO_SEG
            continue

        novas, ignoradas, fora = parsear(texto, inicio, limite)
        linhas.extend(novas)

        minutos = (limite or dur) / 60
        densidade = len(novas) / minutos if minutos else 0
        falantes = sorted({r["falante"] for r in novas})
        desconhecidas = sum(1 for r in novas if r["falante"] == "DESCONHECIDO")
        palavras = sum(len(r["fala"].split()) for r in novas)

        log(f"    {len(novas)} falas, {palavras} palavras, {densidade:.1f} falas/min")
        log(f"    falantes: {', '.join(falantes) or 'nenhum'}")
        if desconhecidas:
            log(f"    DESCONHECIDO em {desconhecidas} fala(s) ({desconhecidas / len(novas):.0%})")
        if ignoradas:
            log(f"    {ignoradas} linha(s) fora do formato, descartadas")
        if fora:
            log(f"    {fora} linha(s) da sobreposição, cobertas pelo bloco seguinte")
        if densidade < LINHAS_POR_MIN_SUSPEITO:
            suspeitos.append(n)
            log(f"    ATENÇÃO: densidade baixa, o modelo pode ter resumido este bloco")
        for r in novas[:2]:
            log(f"    > [{r['tempo']}] {r['falante']}: {r['fala'][:90]}")

        inicio += BLOCO_SEG

    if not linhas:
        sys.exit("nenhuma fala transcrita.")

    secao("SAÍDA")
    linhas.sort(key=lambda r: r["segundos"])
    base = saida / args.nome

    with open(f"{base}.txt", "w", encoding="utf-8") as f:
        for r in linhas:
            f.write(f"[{r['tempo']}] {r['falante']}: {r['fala']}\n")

    with open(f"{base}.csv", "w", encoding="utf-8", newline="") as f:
        w = csv.DictWriter(f, fieldnames=["segundos", "tempo", "falante", "fala"])
        w.writeheader()
        w.writerows(linhas)

    with open(f"{base}_bruto.md", "w", encoding="utf-8") as f:
        f.write("\n\n".join(bruto))

    for p in (f"{base}.txt", f"{base}.csv", f"{base}_bruto.md"):
        log(f"{p}  ({Path(p).stat().st_size / 1024:.0f} KB)")

    secao("RESUMO POR FALANTE")
    agg = {}
    for r in linhas:
        d = agg.setdefault(r["falante"], {"falas": 0, "palavras": 0})
        d["falas"] += 1
        d["palavras"] += len(r["fala"].split())
    tot_palavras = sum(d["palavras"] for d in agg.values()) or 1

    log(f"{'falante':<26} {'falas':>7} {'palavras':>10} {'share':>7}")
    log("-" * 54)
    for nome, d in sorted(agg.items(), key=lambda x: -x[1]["palavras"]):
        log(f"{nome:<26} {d['falas']:>7} {d['palavras']:>10} {d['palavras'] / tot_palavras:>6.1%}")
    log("-" * 54)
    log(f"{'TOTAL':<26} {len(linhas):>7} {tot_palavras:>10}")

    secao("O QUE CONFERIR ANTES DE USAR")
    desc = agg.get("DESCONHECIDO", {}).get("palavras", 0) / tot_palavras
    log(f"cobertura: {hhmmss(linhas[-1]['segundos'])} de {hhmmss(total)} de áudio")
    log(f"DESCONHECIDO: {desc:.1%} das palavras")
    if vazios:
        log(f"blocos vazios: {vazios} (esses trechos NÃO estão na transcrição)")
    if suspeitos:
        log(f"blocos com densidade baixa: {suspeitos} (confira contra o vídeo)")
    if not vazios and not suspeitos and desc < 0.05:
        log("nenhum bloco vazio, nenhum bloco suspeito de resumo")
    log("a identificação de falante vem do conteúdo, não da voz: confira as")
    log("trocas de bloco e os apartes contra o vídeo antes de citar em entrega")

    shutil.rmtree(trabalho, ignore_errors=True)


if __name__ == "__main__":
    main()
