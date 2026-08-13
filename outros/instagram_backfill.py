"""
Tapa as linhas que a fase 1 gravou sem "URL da mídia".

A fase 2 (`rodar_analise_pendentes`) só olha para linha que tem a URL do CDN
gravada: sem ela não há o que baixar, então `linhas_pendentes` pula a linha e
segue. O efeito é que uma linha nessa situação nunca mais entra na fila, sem
aparecer como erro em rodada nenhuma.

Foi o que aconteceu na rodada de 06/08/2026, a primeira depois da separação
entre coleta (15) e análise (16): 638 dos 643 posts do dia foram gravados sem
URL da mídia e ficaram sem transcrição, resumo e temas.

O link do post continua na planilha, então dá para pedir o post de novo à Apify,
pegar uma URL de CDN nova e rodar a mesma análise da fase 2. É o que este
script faz. Não substitui o 16: serve para o passivo que ele não enxerga.

Uso:
    python outros/instagram_backfill.py                  # todas as linhas órfãs da aba do mês
    python outros/instagram_backfill.py --aba agosto     # aba específica
    python outros/instagram_backfill.py --limite 5       # piloto antes de soltar o resto
    python outros/instagram_backfill.py --so-listar      # diagnóstico, não escreve nada
"""

from __future__ import annotations

import argparse
import os
from concurrent.futures import ThreadPoolExecutor, as_completed

import gspread
from apify_client import ApifyClient
from google import genai

from instagram import (
    COLUNAS_GEMINI,
    COLUNA_PENDENTE,
    LOTE_ESCRITA_ANALISE,
    PRECO_ENTRADA_POR_MILHAO,
    PRECO_SAIDA_POR_MILHAO,
    SPREADSHEET_ID,
    THREADS_ANALISE,
    MidiaExpirada,
    _analisar_pendente,
    _letra_coluna,
    carregar_apify_token,
    carregar_gemini_api_key,
    coletar_itens,
    dividir_resultado,
    gravar_analises,
    gs_client_from_file,
    obter_nome_aba_mensal,
)

# A Apify cobra por resultado, não por chamada, então o tamanho do lote não
# muda a conta. Serve para a rodada ir gravando: se cair no meio, o que já foi
# analisado está na planilha e a próxima execução pega só o que sobrou.
POSTS_POR_LOTE = int(os.getenv("POSTS_POR_LOTE_BACKFILL", "100"))

# Post apagado ou fechado depois da coleta não volta. Marcar tira a linha da
# fila, senão toda rodada futura paga a consulta à Apify de novo pelo mesmo
# post morto. Mesma ideia do MARCA_MIDIA_EXPIRADA da fase 2.
MARCA_POST_INDISPONIVEL = "(post fora do ar, não analisado)"


def linhas_orfas(aba: gspread.Worksheet) -> list[dict]:
    """Linhas sem análise e sem URL da mídia, que é o que a fase 2 não enxerga.

    Linha sem análise mas COM URL fica de fora de propósito: essa o workflow 16
    pega sozinho na próxima rodada, e reprocessar aqui pagaria duas vezes.
    """
    valores = aba.get_all_values()
    if len(valores) < 2:
        return []

    cabecalho = valores[0]
    faltando = [c for c in (COLUNA_PENDENTE, "ID do post", "Link", "Legenda", "URL da mídia")
                if c not in cabecalho]
    if faltando:
        raise RuntimeError(f"A aba '{aba.title}' não tem a(s) coluna(s) {', '.join(faltando)}.")

    idx = {nome: i for i, nome in enumerate(cabecalho)}

    def celula(linha: list[str], nome: str) -> str:
        i = idx[nome]
        return linha[i].strip() if i < len(linha) else ""

    orfas = []
    for numero, linha in enumerate(valores[1:], start=2):
        if celula(linha, COLUNA_PENDENTE) or celula(linha, "URL da mídia"):
            continue
        link = celula(linha, "Link")
        post_id = celula(linha, "ID do post")
        if not link or not post_id:
            continue
        orfas.append({
            "linha": numero,
            "id": post_id,
            "link": link,
            "legenda": celula(linha, "Legenda"),
            "publicado": celula(linha, "Data de publicação")[:10],
            "candidato": celula(linha, "Candidato") if "Candidato" in idx else "",
        })
    return orfas


def recoletar(client: ApifyClient, orfas: list[dict]) -> dict[str, dict]:
    """Pede os posts de novo à Apify e devolve {ID do post: item}.

    A URL de CDN que veio na coleta original já venceu, e URL vencida não
    volta. A única forma de ter a mídia é pedir o post outra vez.
    """
    urls = [o["link"] for o in orfas]
    itens = coletar_itens(client, urls, resultados_limit=1)

    por_id: dict[str, dict] = {}
    for item in itens:
        chave = item.get("shortCode") or str(item.get("id", ""))
        if chave:
            por_id[chave] = item
    return por_id


def midia_do_item(item: dict) -> str:
    return item.get("videoUrl") or item.get("displayUrl") or ""


def gravar_midia(aba: gspread.Worksheet, encontrados: list[tuple[dict, str]]) -> None:
    """Grava a URL de CDN nova antes de analisar.

    Se a análise cair no meio, a linha passa a ter mídia e o workflow 16 termina
    o serviço sozinho na rodada seguinte, sem precisar rodar este script de novo.
    """
    if not encontrados:
        return
    cabecalho = aba.row_values(1)
    letra = _letra_coluna(cabecalho.index("URL da mídia"))
    aba.batch_update(
        [{"range": f"{letra}{o['linha']}", "values": [[url]]} for o, url in encontrados],
        value_input_option="RAW",
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--aba", default=os.getenv("ABA_BACKFILL") or obter_nome_aba_mensal())
    parser.add_argument("--limite", type=int, default=None,
                        help="máximo de posts nesta rodada (use para o piloto)")
    parser.add_argument("--so-listar", action="store_true",
                        help="mostra o que seria feito e sai, sem escrever na planilha")
    args = parser.parse_args()

    gc = gs_client_from_file()
    aba = gc.open_by_key(SPREADSHEET_ID).worksheet(args.aba)

    orfas = linhas_orfas(aba)
    print(f"{len(orfas)} linha(s) órfã(s) na aba '{args.aba}' "
          f"(sem análise e sem URL da mídia).")
    if args.limite:
        orfas = orfas[:args.limite]
        print(f"Rodando só as {len(orfas)} primeiras.")
    if not orfas:
        return

    if args.so_listar:
        por_candidato: dict[str, int] = {}
        for o in orfas:
            por_candidato[o["candidato"]] = por_candidato.get(o["candidato"], 0) + 1
        for candidato, n in sorted(por_candidato.items(), key=lambda kv: -kv[1])[:15]:
            print(f"  {n:>4}  {candidato}")
        return

    client = ApifyClient(carregar_apify_token())
    gem = genai.Client(api_key=carregar_gemini_api_key())

    total_entrada = total_saida = 0
    analisados = falhas = sumidos = 0

    for inicio in range(0, len(orfas), POSTS_POR_LOTE):
        lote = orfas[inicio:inicio + POSTS_POR_LOTE]
        print(f"\n=== Lote {inicio // POSTS_POR_LOTE + 1}: {len(lote)} post(s) ===")

        por_id = recoletar(client, lote)

        encontrados, perdidos = [], []
        for orfa in lote:
            item = por_id.get(orfa["id"])
            url = midia_do_item(item) if item else ""
            (encontrados if url else perdidos).append((orfa, url) if url else orfa)

        if perdidos:
            sumidos += len(perdidos)
            print(f"{len(perdidos)} post(s) não voltaram da Apify (apagados ou perfil fechado).")
            gravar_analises(aba, [
                (p, {"transcricao": "", "resumo_conteudo": MARCA_POST_INDISPONIVEL,
                     "resumo_legenda": "", "temas": ""})
                for p in perdidos
            ])

        if not encontrados:
            continue
        gravar_midia(aba, encontrados)

        pendentes = [{**orfa,
                      "midia": url,
                      "eh_video": url.split("?")[0].endswith(".mp4")}
                     for orfa, url in encontrados]

        prontos: list[tuple[dict, dict]] = []
        with ThreadPoolExecutor(max_workers=THREADS_ANALISE) as executor:
            tarefas = {executor.submit(_analisar_pendente, gem, p): p for p in pendentes}
            for i, tarefa in enumerate(as_completed(tarefas), start=1):
                pendente = tarefas[tarefa]
                try:
                    resultado = tarefa.result()
                except MidiaExpirada as erro:
                    falhas += 1
                    print(f"[{i}/{len(pendentes)}] {pendente['id']}: mídia caiu de novo ({erro}).")
                    continue
                except Exception as erro:
                    falhas += 1
                    print(f"[{i}/{len(pendentes)}] {pendente['id']}: falhou ({str(erro)[:160]}).")
                    continue

                prontos.append((pendente, resultado["secoes"]))
                total_entrada += resultado["uso"]["entrada"]
                total_saida += resultado["uso"]["saida"]
                analisados += 1
                print(f"[{i}/{len(pendentes)}] {pendente['id']} ({pendente['candidato']}) analisado.")
                if len(prontos) >= LOTE_ESCRITA_ANALISE:
                    gravar_analises(aba, prontos)
                    prontos = []

        gravar_analises(aba, prontos)

    custo = (total_entrada / 1_000_000 * PRECO_ENTRADA_POR_MILHAO
             + total_saida / 1_000_000 * PRECO_SAIDA_POR_MILHAO)
    print(f"\nBackfill concluído. {analisados} analisado(s), {falhas} com falha, "
          f"{sumidos} fora do ar.")
    print(f"Tokens: {total_entrada:,} de entrada, {total_saida:,} de saída. "
          f"Custo estimado: US$ {custo:.2f}.")


if __name__ == "__main__":
    main()
