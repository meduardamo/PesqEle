# -*- coding: utf-8 -*-
"""Monta o site estático que serve os planos de governo em PDF.

Por que existe: não há link que abra o plano na página do trecho. Medido nos
seis endpoints em 19/08/2026, o TSE responde com `Content-Disposition:
attachment` e o navegador baixa em vez de mostrar; o visualizador do Drive é um
leitor em JavaScript e ignora a âncora `#page=N`; e `uc?export=view` do Drive
também vem como attachment. Para a âncora funcionar o arquivo precisa chegar
como `application/pdf` sem attachment, e o GitHub Pages faz exatamente isso:

    content-type: application/pdf
    content-disposition: (nenhum)
    accept-ranges: bytes

O `accept-ranges` importa tanto quanto o resto: com ele o Chrome busca só o
pedaço do arquivo onde está a página pedida, em vez de baixar 10 MB para mostrar
a página 42.

O que este script faz: lê a aba `planos_arquivos`, baixa cada PDF do espelho no
Drive (e não do TSE, que bloqueia) e escreve a árvore do site em disco:

    <destino>/
      .nojekyll          o Jekyll do Pages ignora pasta começada por _ e mexe
                         em nome de arquivo; com este arquivo ele nem roda
      index.html         lista por UF, para quem chega pela raiz
      MS/120002536582.pdf

O nome do arquivo é o sq_candidato, e não o nome da pessoa: é a chave estável
que o painel já usa, e sai sem espaço, acento ou parêntese para escapar na URL.

Uso:
    export SPREADSHEET_ID_TSE=...
    python -m outros.publicar_planos_pages --destino ../planos-governo-2026
    python -m outros.publicar_planos_pages --destino ... --uf MS

Não commita nem empurra nada: escreve os arquivos e para. O repositório é
público e quem publica decide quando.
"""

from __future__ import annotations

import argparse
import html
import os
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pandas as pd

sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from outros.espelhar_planos import (  # noqa: E402
    ABA_ARQUIVOS, clientes_google, com_retentativa, ler_aba, log,
)

FUSO = timezone(timedelta(hours=-3))

# Ordem de leitura do índice: as UFs em ordem alfabética, com os planos de
# presidente (BR) em cima, que é como o painel também apresenta.
def _ordem_uf(uf: str) -> tuple:
    return (0 if uf == "BR" else 1, uf)


def baixar_do_drive(drive, file_id: str) -> bytes:
    return com_retentativa(
        f"leitura de {file_id}",
        lambda: drive.files().get_media(fileId=file_id,
                                        supportsAllDrives=True).execute(),
    )


def escrever_indice(destino: Path, linhas: list[dict], base_url: str) -> None:
    """Uma página de entrada simples, agrupada por UF.

    Existe porque a raiz de um Pages sem index devolve 404, e alguém que receba
    o link de um plano vai, mais cedo ou mais tarde, apagar o final da URL para
    ver o que tem ali.
    """
    por_uf: dict[str, list[dict]] = {}
    for r in linhas:
        por_uf.setdefault(r["uf"], []).append(r)

    blocos = []
    for uf in sorted(por_uf, key=_ordem_uf):
        itens = "".join(
            f'<li><a href="{html.escape(r["arquivo"])}">'
            f'{html.escape(r["candidato"].title())}</a> '
            f'<span>({html.escape(r["partido"].upper())})</span></li>'
            for r in sorted(por_uf[uf], key=lambda x: x["candidato"])
        )
        titulo = "Presidência" if uf == "BR" else uf
        blocos.append(f"<section><h2>{titulo}</h2><ul>{itens}</ul></section>")

    gerado = datetime.now(FUSO).strftime("%d/%m/%Y às %H:%M")
    (destino / "index.html").write_text(f"""<!doctype html>
<html lang="pt-br"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Planos de governo 2026</title>
<style>
  :root {{ color-scheme: light; }}
  body {{ font-family: -apple-system, "Segoe UI", Helvetica, Arial, sans-serif;
         max-width: 900px; margin: 0 auto; padding: 32px 20px; color: #192D4E;
         line-height: 1.55; background: #fff; }}
  h1 {{ font-size: 26px; margin: 0 0 6px; }}
  p.sub {{ color: #5A6B85; font-size: 14px; margin: 0 0 28px; }}
  h2 {{ font-size: 13px; text-transform: uppercase; letter-spacing: .08em;
        color: #962E4D; margin: 26px 0 8px; }}
  ul {{ list-style: none; padding: 0; margin: 0;
        display: grid; grid-template-columns: repeat(auto-fill, minmax(260px, 1fr));
        gap: 4px 18px; }}
  li {{ font-size: 14px; }}
  a {{ color: #192D4E; }}
  span {{ color: #5A6B85; font-size: 12px; }}
  footer {{ margin-top: 36px; padding-top: 14px; border-top: 1px solid #E3E0DA;
            color: #5A6B85; font-size: 12px; }}
</style></head><body>
<h1>Planos de governo 2026</h1>
<p class="sub">Cópia dos planos de governo registrados no TSE pelas candidaturas
a governador e à Presidência. Os arquivos são os mesmos do DivulgaCand; estão
aqui para poder abrir direto na página citada, o que o site do TSE não permite.</p>
{''.join(blocos)}
<footer>{len(linhas)} planos · atualizado em {gerado} · Eixo Políticas Públicas.
Fonte: DivulgaCand/TSE.</footer>
</body></html>""", encoding="utf-8")


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__)
    ap.add_argument("--destino", required=True,
                    help="pasta do repositório do site (será preenchida)")
    ap.add_argument("--uf", default="", help="restringe a uma UF")
    ap.add_argument("--credenciais", default="")
    ap.add_argument("--base-url", default="",
                    help="só para o índice; não afeta os arquivos")
    args = ap.parse_args()

    sheet_id = os.getenv("SPREADSHEET_ID_TSE", "").strip()
    if not sheet_id:
        raise SystemExit("Defina SPREADSHEET_ID_TSE.")

    gc, drive = clientes_google(args.credenciais)
    sh = gc.open_by_key(sheet_id)
    salvos = ler_aba(sh, ABA_ARQUIVOS)
    if salvos.empty:
        raise SystemExit(f"A aba {ABA_ARQUIVOS} está vazia. Rode o espelho antes.")
    if args.uf:
        salvos = salvos[salvos["uf"].astype(str).str.upper() == args.uf.upper()]

    destino = Path(args.destino).expanduser().resolve()
    destino.mkdir(parents=True, exist_ok=True)
    # Sem .nojekyll o Pages roda o Jekyll, que ignora pasta iniciada por _ e
    # pode reescrever caminho. Não há nada para o Jekyll fazer aqui.
    (destino / ".nojekyll").write_text("", encoding="utf-8")

    linhas, erros = [], []
    total = len(salvos)
    for i, (_, r) in enumerate(salvos.iterrows(), start=1):
        uf = str(r.get("uf", "")).strip().upper() or "BR"
        sq = str(r.get("sq_candidato", "")).strip()
        fid = str(r.get("drive_id", "")).strip()
        nome = str(r.get("candidato", "")).strip()
        if not sq or not fid:
            continue
        rel = f"{uf}/{sq}.pdf"
        arq = destino / rel
        arq.parent.mkdir(parents=True, exist_ok=True)

        # sha256 já está na planilha, mas comparar exige ler o arquivo local
        # inteiro; o tamanho pega a troca de plano igual e custa um stat.
        esperado = str(r.get("bytes", "")).strip()
        if arq.exists() and esperado.isdigit() and arq.stat().st_size == int(esperado):
            linhas.append({"uf": uf, "candidato": nome,
                           "partido": str(r.get("partido", "")), "arquivo": rel})
            continue

        try:
            dados = baixar_do_drive(drive, fid)
        except Exception as e:
            erros.append(f"{uf} · {nome}: {type(e).__name__}: {e}")
            log(f"[{i}/{total}] {uf} · {nome}: falhou ({str(e)[:80]})")
            continue
        if not dados[:4] == b"%PDF":
            erros.append(f"{uf} · {nome}: o que veio do Drive não é PDF")
            continue
        arq.write_bytes(dados)
        linhas.append({"uf": uf, "candidato": nome,
                       "partido": str(r.get("partido", "")), "arquivo": rel})
        log(f"[{i}/{total}] {uf} · {nome} · {len(dados) / 1e6:.1f} MB")
        time.sleep(0.2)

    escrever_indice(destino, linhas, args.base_url)
    _mb = sum((destino / l["arquivo"]).stat().st_size for l in linhas) / 1e6
    log(f"\n{len(linhas)} planos em {destino} · {_mb:.0f} MB")
    if erros:
        log(f"{len(erros)} falharam:")
        for e in erros[:30]:
            log(f"  - {e}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
