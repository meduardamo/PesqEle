"""
Geração automatizada de Resumo Executivo de Debates Eleitorais (Gemini).

Lê a transcrição e a classificação temática do debate, calcula tempos
e percentuais de forma determinística (Python) e gera o resumo executivo
para clientes seguindo a estrutura padrão solicitada:
  1. Abertura (participantes, veículo, mediador, tempo de fala e tom)
  2. Ranking dos principais temas debatidos (com tempo e %)
  3. Parágrafos por tema (**Tema** em negrito, posições, dados e confronto)
  4. Fechamento (avaliação geral e clima de encerramento)
  + Lista dos 5 números/trechos-chave com timestamps para conferência.

Também inclui funções de higienização de falas procedimentais do mediador
para evitar classificação indevida de temas substantivos em transições.

Uso:
    python -m outros.resumo_debate --csv debate_temas.csv
    python -m outros.resumo_debate --csv debate_temas.csv --contexto "1º Debate SP - Band 09/08/2026"
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import re
import sys
from collections import defaultdict
from pathlib import Path
from typing import Any, Dict, List, Tuple

try:
    from google import genai
    from google.genai import types
    GENAI_DISPONIVEL = True
except ImportError:
    GENAI_DISPONIVEL = False

GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")


# ==============================================================================
# 1. TRATAMENTO DE FALAS PROCEDIMENTAIS DO MEDIADOR (CORREÇÃO DE CLASSIFICAÇÃO)
# ==============================================================================

PADROES_PROCEDIMENTAIS = [
    r"muito obrigado",
    r"obrigad[oa]",
    r"\d+\s*minuto[s]?",
    r"\d+\s*segundo[s]?",
    r"tempo na tela",
    r"no cron[oô]metro",
    r"boa noite",
    r"boa tarde",
    r"candidat[oa],\s*(o senhor|a senhora|voc[eê]|seu tempo)",
    r"agora o candidat[oa]",
    r"agora a resposta",
    r"passo a palavra",
    r"vamos para o (primeiro|segundo|terceiro|quarto|\d+[ºo]) bloco",
    r"intervalo comercial",
    r"comerciais",
    r"regras do debate",
    r"direito de resposta",
    r"banco de tempo",
    r"pela ordem sorteada",
    r"dois minutos",
    r"tr[eê]s minutos",
    r"um minuto e meio",
    r"palavra com o senhor",
    r"palavra com a senhora",
    r"sua vez",
]

RE_PROCEDIMENTAL = re.compile(
    r"(" + "|".join(PADROES_PROCEDIMENTAIS) + r")", re.IGNORECASE
)

# Palavras que indicam conteúdo substantivo/pergunta temática
PALAVRAS_SUBSTANTIVAS = [
    "proposta", "propostas", "plano", "governo", "saúde", "educação",
    "segurança", "economia", "imposto", "dívida", "cracolândia", "escola",
    "hospital", "polícia", "tarifa", "orçamento", "reforma", "investimento",
    "fundo", "índice", "meta", "feminicídio", "privatização", "sabesp",
]


def eh_fala_procedimental(falante: str, fala: str, palavras: int = 0) -> bool:
    """Identifica se uma fala é estritamente procedimental/condução de debate.
    
    Evita que transições do mediador (ex: 'Muito obrigado. Agora o candidato
    Mateus Simões do PSD, candidato, 1 minuto e meio na tela, boa noite pro senhor.')
    sejam classificadas erroneamente com temas substantivos.
    """
    falante_upper = (falante or "").strip().upper()
    fala_limpa = (fala or "").strip()
    fala_lower = fala_limpa.lower()
    qtd_palavras = palavras or len(fala_limpa.split())

    if "MÚSICA" in falante_upper or "MUSICA" in falante_upper:
        return True

    eh_mediacao = any(m in falante_upper for m in ["MEDIADOR", "SCHNEIDER", "APRESENTADOR", "JORNALISTA", "LOCUTOR"])
    
    if eh_mediacao:
        # Se tem padrão claro de mediação/tempo/passagem de palavra
        tem_marcador_procedimental = bool(RE_PROCEDIMENTAL.search(fala_limpa))
        tem_conteudo_substantivo = any(w in fala_lower for w in PALAVRAS_SUBSTANTIVAS)

        if tem_marcador_procedimental and not tem_conteudo_substantivo:
            return True
        if qtd_palavras <= 25 and tem_marcador_procedimental:
            return True
        if qtd_palavras <= 10 and not tem_conteudo_substantivo:
            return True

    return False


def limpar_classificacao_procedimental(linhas: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """Aplica limpeza determinística em falas procedimentais de mediação."""
    linhas_limpas = []
    for r in linhas:
        r_copia = dict(r)
        falante = r_copia.get("falante", "")
        fala = r_copia.get("fala", "")
        palavras = int(r_copia.get("palavras") or len(fala.split()))

        if eh_fala_procedimental(falante, fala, palavras):
            r_copia["tipo"] = "Procedimental"
            r_copia["tema"] = "Sem tema"
            r_copia["eixo"] = "Sem tema"
            if "assuntos" in r_copia:
                r_copia["assuntos"] = ""
            if "proprios" in r_copia:
                r_copia["proprios"] = ""
        linhas_limpas.append(r_copia)
    return linhas_limpas


# ==============================================================================
# 2. CÁLCULO DETERMINÍSTICO DE MÉTRICAS (PYTHON)
# ==============================================================================

def calcular_metricas_debate(linhas: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Calcula durações, tempos de fala por candidato e minutagem por tema."""
    total_linhas = len(linhas)
    if total_linhas == 0:
        return {}

    tempos_falante_seg = defaultdict(int)
    palavras_falante = defaultdict(int)
    tempos_eixo_seg = defaultdict(int)
    falas_eixo_qtd = defaultdict(int)
    trechos_por_eixo = defaultdict(list)

    for i, r in enumerate(linhas):
        seg_atual = int(r.get("segundos", 0))
        if i + 1 < total_linhas:
            seg_prox = int(linhas[i + 1].get("segundos", seg_atual + 60))
            dur_seg = max(1, min(seg_prox - seg_atual, 300))
        else:
            dur_seg = 60

        falante = r.get("falante", "DESCONHECIDO").strip()
        fala = r.get("fala", "").strip()
        palavras = int(r.get("palavras") or len(fala.split()))
        eixo_str = r.get("eixo") or r.get("tema") or "Sem tema"

        tempos_falante_seg[falante] += dur_seg
        palavras_falante[falante] += palavras

        if eixo_str and eixo_str != "Sem tema" and not eh_fala_procedimental(falante, fala, palavras):
            eixos = [e.strip() for e in eixo_str.split(";") if e.strip() and e.strip() != "Sem tema"]
            for e in eixos:
                tempos_eixo_seg[e] += dur_seg
                falas_eixo_qtd[e] += 1
                if len(fala) > 40:
                    trechos_por_eixo[e].append({
                        "tempo": r.get("tempo", "00:00:00"),
                        "falante": falante,
                        "fala": fala,
                    })

    duracao_total_seg = sum(tempos_falante_seg.values())
    duracao_total_min = duracao_total_seg / 60.0

    falantes_nao_candidatos = {"MÚSICA", "MUSICA", "DESCONHECIDO", "JORNALISTA"}
    candidatos_stats = []
    for f, seg in sorted(tempos_falante_seg.items(), key=lambda x: -x[1]):
        if f in falantes_nao_candidatos or "SCHNEIDER" in f or "MEDIADOR" in f:
            continue
        minutos = seg / 60.0
        candidatos_stats.append({
            "nome": f,
            "minutos": round(minutos, 1),
            "palavras": palavras_falante[f],
        })

    total_min_candidatos = sum(c["minutos"] for c in candidatos_stats) or 1.0
    for c in candidatos_stats:
        c["percentual"] = round((c["minutos"] / total_min_candidatos) * 100, 1)

    ranking_eixos = []
    for e, seg in sorted(tempos_eixo_seg.items(), key=lambda x: -x[1]):
        minutos = round(seg / 60.0, 1)
        ranking_eixos.append({
            "tema": e,
            "minutos": minutos,
            "qtd_falas": falas_eixo_qtd[e],
            "trechos": trechos_por_eixo[e][:5],
        })

    return {
        "duracao_total_min": round(duracao_total_min, 1),
        "candidatos": candidatos_stats,
        "ranking_temas": ranking_eixos,
        "total_linhas": total_linhas,
    }


# ==============================================================================
# 3. PROMPT E GERAÇÃO DO RESUMO EXECUTIVO (GEMINI)
# ==============================================================================

PROMPT_RESUMO_EXECUTIVO = """Você é um analista político sênior responsável por redigir resumos executivos de eventos eleitorais para envio direto a clientes corporativos e institucionais.

Use as informações consolidadas e a transcrição fornecida para redigir um RESUMO EXECUTIVO do evento.

DIRETRIZES DE FORMATO E ESTILO:
- Escreva em português, em texto corrido (NÃO utilize bullets, listas, travessões de tópicos ou marcadores).
- Tamanho aproximado: entre 500 e 700 palavras.
- Tom analítico, factual, sóbrio, objetivo e equilibrado.
- Siga ESTRITAMENTE a seguinte estrutura de 4 seções:

1. Parágrafo de abertura:
   - Quem participou (nome completo e candidatura/cargo).
   - Data do evento, veículo/emissora e nome do(s) entrevistador(es)/mediador(es).
   - Duração total{tempo_candidato}.
   - Tom geral do evento.

2. Parágrafo com o ranking dos 5 a 8 temas mais abordados:
   - Cite os principais temas em ordem decrescente de tempo/menções, mencionando a minutagem estimada de cada um (utilize os números da medição fornecida).

3. Um parágrafo para cada um dos principais temas abordados (do mais para o menos abordado):
   - Comece o parágrafo com o nome do tema em negrito (exemplo: **Segurança pública —** texto...).
   - Explique a posição e os principais argumentos expostos sobre esse tema, incluindo números, dados ou programas citados.
{confronto_candidatos}

4. Parágrafo de fechamento:
   - Avaliação geral: o evento trouxe propostas novas ou foi dominado por defesa de históricos de gestão e respostas defensivas?
   - Como foi o encerramento?

DIRETRIZES ANTI-CLICHÊ DE IA (GUIA PROF. RAFAEL SAMPAIO):
- NÃO utilize adjetivos inflados/vagos: proibido "crucial", "fundamental", "essencial", "significativo", "robusto", "estratégico", "disruptivo", "valioso", "emblemático", "meticuloso", "notável", "profundo", "inovador".
- NÃO utilize verbos vazios/metafóricos: proibido "destacar", "ressaltar", "enfatizar", "evidenciar", "moldar", "refletir", "simbolizar", "mergulhar", "navegar", "potencializar", "revolucionar", "sacramentar".
- NÃO utilize substantivos abstratos e metáforas: proibido "cenário", "panorama", "mosaico", "tapeçaria", "marco", "legado duradouro", "testemunho", "ponto focal", "esfera", "horizonte".
- NÃO utilize jargões corporativos/marqueteiros: proibido "insights", "sinergia", "multifacetado", "mudança de jogo", "chave para", "motor de crescimento", "aliado estratégico".
- NÃO utilize gerúndios conclusivos vagos: proibido terminar frases com "...garantindo que", "...destacando sua importância", "...refletindo a relevância", "...contribuindo para", "...simbolizando o compromisso".
- NÃO editorialize nem use chavões: proibido "é importante notar", "vale a pena mencionar", "no cenário atual", "como se sabe", "neste contexto".
- NÃO use fórmulas artificiais: proibido paralelismos negativos ("não apenas X, mas também Y"), listas mecânicas de 3 adjetivos ou falsos intervalos ("do problema à solução").
- Prefira SEMPRE fatos concretos, números exatos citados, nomes de leis/programas reais e verbos diretos e objetivos ("afirmou", "criticou", "prometeu", "disse", "apresentou", "rebateu", "citou").

REGRAS CRÍTICAS DE CONTEÚDO:
- NÃO invente números, dados, datas, nomes de programas ou falas que não constem na transcrição/dados.
- Se houver dúvida ou incerteza sobre algum dado citado, sinalize entre colchetes como [CONFERIR].
- AO FINAL DA RESPOSTA (separado por uma linha horizontal ---), liste exatamente os 5 números ou trechos mais importantes utilizados no texto, com o timestamp aproximado [MM:SS] em que aparecem na transcrição, para conferência rápida.

---
DADOS CONSOLIDADOS DO EVENTO:
- Título/Contexto: {contexto}
- Duração total calculada: ~{duracao_total} minutos
- Tempo de fala medido:
{metricas_candidatos}
- Ranking medido dos temas:
{metricas_temas}

---
TRECHOS PRINCIPAIS POR TEMA NA TRANSCRIÇÃO:
{amostra_transcricao}
"""


def formatar_prompt(
    contexto: str,
    metricas: Dict[str, Any],
    linhas_transcricao: List[Dict[str, Any]],
    eh_sabatina: bool = False,
) -> str:
    if eh_sabatina:
        cands_str = "\n".join(
            f"  * {c['nome']}: {c['minutos']} min ({c['palavras']} palavras)"
            for c in metricas.get("candidatos", [])
        )
        tempo_candidato = " e tempo de fala do candidato (utilize o tempo medido)"
        confronto_candidatos = ""
    else:
        cands_str = "\n".join(
            f"  * {c['nome']}: {c['minutos']} min ({c['percentual']}% do tempo entre candidatos, {c['palavras']} palavras)"
            for c in metricas.get("candidatos", [])
        )
        tempo_candidato = " e tempo de fala de cada candidato (utilize os dados exatos informados abaixo em minutos e percentual)"
        confronto_candidatos = "   - Aponte o principal ponto de divergência, confronto ou réplica entre os candidatos."

    temas_str = "\n".join(
        f"  {i+1}. {t['tema']} (~{t['minutos']} min, {t['qtd_falas']} falas)"
        for i, t in enumerate(metricas.get("ranking_temas", [])[:8])
    )

    amostras = []
    for t in metricas.get("ranking_temas", [])[:7]:
        amostras.append(f"\n### Eixo: {t['tema']} ({t['minutos']} min)")
        for tr in t.get("trechos", [])[:4]:
            amostras.append(f"[{tr['tempo']}] {tr['falante']}: {tr['fala'][:350]}")

    return PROMPT_RESUMO_EXECUTIVO.format(
        tempo_candidato=tempo_candidato,
        confronto_candidatos=confronto_candidatos,
        contexto=contexto.strip(),
        duracao_total=metricas.get("duracao_total_min", 0),
        metricas_candidatos=cands_str,
        metricas_temas=temas_str,
        amostra_transcricao="\n".join(amostras),
    )


def gerar_resumo_debate(
    linhas_csv: List[Dict[str, Any]],
    contexto: str = "Debate Eleitoral 2026",
    api_key: str | None = None,
    eh_sabatina: bool = False,
) -> str:
    chave = api_key or os.getenv("GEMINI_API_KEY", "")
    if not chave:
        raise ValueError("GEMINI_API_KEY não configurada no ambiente.")

    linhas_limpas = limpar_classificacao_procedimental(linhas_csv)
    metricas = calcular_metricas_debate(linhas_limpas)
    prompt = formatar_prompt(contexto, metricas, linhas_limpas, eh_sabatina=eh_sabatina)

    client = genai.Client(api_key=chave)
    resposta = client.models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(
            temperature=0.2,
            max_output_tokens=8192,
        ),
    )
    return (resposta.text or "").strip()


# ==============================================================================
# 4. EXECUÇÃO CLI
# ==============================================================================

def main():
    parser = argparse.ArgumentParser(description="Gera resumo executivo de debate eleitoral")
    parser.add_argument("--csv", help="Caminho do CSV de temas/eixos do debate")
    parser.add_argument("--contexto", default="Debate ao Governo de São Paulo — Band, 09/08/2026", help="Título/contexto do debate")
    parser.add_argument("--saida", default=None, help="Caminho para salvar o resumo em Markdown (.md)")
    parser.add_argument("--apenas-metricas", action="store_true", help="Apenas calcula e exibe as métricas de tempo e temas")
    parser.add_argument("--sabatina", action="store_true", help="O evento é uma sabatina (apenas um candidato)")
    args = parser.parse_args()

    if not args.csv:
        print("Uso: python -m outros.resumo_debate --csv caminho/debate_eixos.csv")
        sys.exit(1)

    caminho = Path(args.csv)
    if not caminho.exists():
        sys.exit(f"Arquivo não encontrado: {caminho}")

    with open(caminho, "r", encoding="utf-8") as f:
        linhas = list(csv.DictReader(f))

    print(f"[+] Lendo {len(linhas)} falas de {caminho.name}...")
    linhas_limpas = limpar_classificacao_procedimental(linhas)
    metricas = calcular_metricas_debate(linhas_limpas)

    print("\n" + "=" * 60)
    print("MÉTRICAS DO EVENTO")
    print("=" * 60)
    print(f"Duração total estimada: {metricas.get('duracao_total_min')} min")
    print("\nTempos de fala:")
    for c in metricas.get("candidatos", []):
        if args.sabatina:
            print(f"  - {c['nome']}: {c['minutos']} min | {c['palavras']} palavras")
        else:
            print(f"  - {c['nome']}: {c['minutos']} min ({c['percentual']}%) | {c['palavras']} palavras")

    print("\nRanking de Temas:")
    for i, t in enumerate(metricas.get("ranking_temas", [])[:10]):
        print(f"  {i+1}. {t['tema']}: {t['minutos']} min ({t['qtd_falas']} falas)")
    print("=" * 60 + "\n")

    if args.apenas_metricas:
        return

    print("[+] Chamando Gemini para redigir o Resumo Executivo...")
    try:
        resumo = gerar_resumo_debate(linhas, contexto=args.contexto, eh_sabatina=args.sabatina)
        print("\n" + "=" * 60)
        print("RESUMO EXECUTIVO GERADO")
        print("=" * 60)
        print(resumo)
        print("=" * 60 + "\n")

        if args.saida:
            out_p = Path(args.saida)
            out_p.write_text(resumo, encoding="utf-8")
            print(f"[+] Salvo em: {out_p.resolve()}")
    except Exception as e:
        print(f"[-] Erro ao gerar resumo com Gemini: {e}")


if __name__ == "__main__":
    main()
