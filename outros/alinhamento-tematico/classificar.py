"""
Temas principais por candidato, com contagem.

Por que a contagem não sai do modelo: a coluna "Temas" da aba "Alinhamento
Temático" é o resultado do tematica.py já deduplicado. Em 702 temas do ACM Neto
há 702 strings distintas, nenhuma repetida. Nessa lista, "Saúde", que apareceu
em trinta posts, e "Cultura popular", que apareceu em um, têm exatamente o mesmo
peso. Pedir um top 5 a partir dela devolve uma lista sem ordem, em que um tema
de três posts fica lado a lado com um de trinta.

Por isso a unidade aqui é o post. O script lê as abas mensais, conta em quantos posts
cada tema bruto aparece, e o Gemini entra só para agrupar as variações do mesmo
assunto ("Segurança", "Segurança (governança)", "Segurança pública" viram uma
entrada). Quem soma as contagens é o Python, depois do agrupamento. O modelo
nunca escreve um número.

O script se vira sozinho: a lista de candidatos sai da aba "Instagram", que é o
cadastro do monitoramento, e a aba "Alinhamento Temático" é criada e preenchida
se ainda não existir. Não depende de nenhuma outra rodada ter passado antes.

Grava cinco colunas na aba "Alinhamento Temático":

    Temas Principais        ranqueado, do mais frequente para o menos
    Nº de posts             em quantos posts o tema aparece
    % dos posts em que aparece  sobre o total de posts; temas se sobrepõem
    Temas brutos agrupados  o que entrou em cada tema, para conferência
    Resumo da conta         três frases sobre o que o perfil vem publicando

Uso:
    python classificar.py
    python classificar.py --candidato "ACM Neto"     # testa um só
    python classificar.py --limite 5                 # piloto
    python classificar.py --forcar                   # refaz quem já está preenchido
"""

from __future__ import annotations

import argparse
import json
import os
import random
import re
import time
import unicodedata
from collections import Counter, defaultdict

import gspread
from google import genai
from google.genai import types
from google.oauth2.service_account import Credentials

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "1piO-m19orW1i-Z-6rNeWdXnEAWqw5wneiDpdHZqOa6Y")
ABA_ALINHAMENTO = os.getenv("ABA_ALINHAMENTO_TEMATICO", "Alinhamento Temático")
ABAS_MENSAIS = [a.strip() for a in os.getenv("ABAS_MENSAIS", "julho,agosto").split(",") if a.strip()]

COLUNA_CANDIDATO = "Candidato"
COLUNA_PERCENTUAL = "% dos posts em que aparece"
COLUNA_PERCENTUAL_ANTIGA = "% dos posts"
COLUNAS_SAIDA = ["Temas Principais", "Nº de posts", COLUNA_PERCENTUAL,
                 "Temas brutos agrupados", "Resumo da conta"]
SEPARADOR = "; "

# Quantos temas entram no resultado final.
TOP_N = int(os.getenv("TOP_N_TEMAS", "10"))
# Tema que aparece em um post ou dois é ruído de rotulação, não pauta. Sobe o
# corte se a base do candidato for grande.
MINIMO_POSTS_POR_TEMA = int(os.getenv("MINIMO_POSTS_POR_TEMA", "3"))
# Abaixo disso o top N não descreve nada: são poucos posts para qualquer
# ranking. A linha sai marcada em vez de sair errada.
MINIMO_POSTS_CANDIDATO = int(os.getenv("MINIMO_POSTS_CANDIDATO", "20"))

MODELO = os.getenv("MODELO_GEMINI", "gemini-2.5-flash")
TENTATIVAS = 4
LOTE_ESCRITA = 10

PRECO_ENTRADA_POR_MILHAO = float(os.getenv("PRECO_ENTRADA_POR_MILHAO", "0.30"))
PRECO_SAIDA_POR_MILHAO = float(os.getenv("PRECO_SAIDA_POR_MILHAO", "2.50"))

# Marcas que o pipeline do instagram.py grava quando não conseguiu analisar o
# post. Não são conteúdo e não podem entrar na contagem.
MARCAS_NAO_ANALISADO = ("(mídia expirada", "(post fora do ar")

# Processo eleitoral e emoção solta. Cortar aqui, e não no prompt, tem duas
# razões: a lista é auditável (dá para discutir item a item com o cliente) e o
# modelo para de gastar decisão com o que uma comparação de string resolve.
# O corte é por igualdade exata do texto normalizado, nunca por "contém": senão
# "campanha eleitoral" derrubaria junto "financiamento de campanha".
RUIDO = {
    # processo eleitoral
    "politica", "politica brasileira", "eleicoes", "eleicoes 2022", "eleicoes 2026",
    "campanha eleitoral", "campanha politica", "campanha", "pre campanha",
    "candidatura", "candidato", "candidata", "pre candidatura", "candidatura a governador",
    "candidato a governador", "governador", "vice governador", "convencao partidaria",
    "propaganda eleitoral", "pesquisa eleitoral", "marketing politico", "comicio",
    "adesivaco", "evento politico", "lancamento de candidatura", "militancia",
    "apoio politico", "aliados politicos", "lideranca politica", "liderancas locais",
    "mobilizacao eleitoral", "mobilizacao", "engajamento eleitoral", "engajamento politico",
    "engajamento", "engajamento online", "engajamento digital", "engajamento comunitario",
    "apoio popular", "apoio", "participacao popular", "interacao com eleitores",
    "mensagem ao eleitor", "slogan", "slogan de campanha", "partido politico",
    "diplomacao", "justica eleitoral", "debate politico", "agenda do candidato",
    # emoção e elogio sem pauta
    "gratidao", "esperanca", "celebracao", "orgulho", "superacao", "mudanca",
    "mudanca politica", "transformacao", "fe", "religiosidade", "uniao", "amor",
    "cuidado", "otimismo", "confianca", "compromisso", "coragem", "humildade",
    "trabalho", "dedicacao", "proposito", "inspiracao", "valores", "futuro",
    "resiliencia", "empatia", "solidariedade", "acolhimento", "proximidade",
    "presenca politica", "conexao com o povo", "voz do povo", "povo",
    "identidade regional", "identidade local", "regionalismo", "nostalgia",
    "humor", "descontracao", "entusiasmo", "recomeco", "dignidade", "respeito",
    # avaliação genérica sem área de política pública identificada
    "critica politica", "criticas politicas", "critica governamental",
    "critica a gestao atual", "descaso governamental", "critica ao governo",
    # participação e linguagem política sem pauta concreta
    "escuta ativa", "dialogo", "engajamento popular", "participacao civica",
    "participacao cidada", "comunidade", "participacao social",
    "propostas de governo", "promessas eleitorais", "plano de governo",
    # termos institucionais genéricos; só entram quando há área ou ação nomeada
    "governo", "gestao publica", "governanca", "politicas publicas",
    "oportunidades", "futuro da bahia", "desenvolvimento", "progresso",
}


UF_PARA_NOME = {
    "AC": "acre", "AL": "alagoas", "AP": "amapa", "AM": "amazonas", "BA": "bahia",
    "CE": "ceara", "DF": "distrito federal", "ES": "espirito santo", "GO": "goias",
    "MA": "maranhao", "MT": "mato grosso", "MS": "mato grosso do sul",
    "MG": "minas gerais", "PA": "para", "PB": "paraiba", "PR": "parana",
    "PE": "pernambuco", "PI": "piaui", "RJ": "rio de janeiro",
    "RN": "rio grande do norte", "RS": "rio grande do sul", "RO": "rondonia",
    "RR": "roraima", "SC": "santa catarina", "SP": "sao paulo", "SE": "sergipe",
    "TO": "tocantins",
}


def ruido_do_candidato(candidato: str, uf: str) -> set[str]:
    """Nome do próprio candidato e do estado dele.

    Esses dois lideram a contagem de quase todo perfil ("Bahia" em 50 dos 126
    posts do ACM Neto, "Pernambuco" em 68 dos 137 da Raquel Lyra) e não dizem
    nada sobre pauta. Dá para pedir ao modelo que ignore, mas é gasto de prompt
    e de decisão num caso que comparar string resolve, e resolve igual toda vez.
    """
    termos = {normalizar(candidato)}
    estado = UF_PARA_NOME.get((uf or "").strip().upper())
    if estado:
        termos.add(estado)
        termos.add(f"{estado} estado")
        for preposicao in ("do", "da", "de"):
            termos.add(f"governo {preposicao} {estado}")
    for parte in normalizar(candidato).split():
        if len(parte) >= 4:
            termos.add(parte)
    # Variações que o rotulador escreve em volta do nome.
    for base in list(termos):
        termos.add(f"{base} candidato")
        termos.add(f"{base} politico")
    return {t for t in termos if t}


def normalizar(texto: str) -> str:
    """Minúscula, sem acento e sem pontuação, para comparar rótulo com rótulo.

    "Segurança Pública" e "seguranca publica" são o mesmo tema escrito por dois
    posts diferentes. Sem isso eles contam separado.
    """
    limpo = unicodedata.normalize("NFKD", texto.lower())
    limpo = "".join(c for c in limpo if not unicodedata.combining(c))
    return re.sub(r"\s+", " ", re.sub(r"[^a-z0-9]+", " ", limpo)).strip()


def _localizar_credentials(caminho: str = "credentials.json") -> str:
    base = os.path.dirname(os.path.abspath(__file__))
    for candidato in (caminho, os.path.join(base, caminho),
                      os.path.join(os.getcwd(), caminho), os.path.join(base, "..", caminho)):
        if os.path.isfile(candidato):
            return candidato
    return os.path.join(base, caminho)


def gs_client() -> gspread.Client:
    escopos = ["https://www.googleapis.com/auth/spreadsheets",
               "https://www.googleapis.com/auth/drive"]
    return gspread.authorize(
        Credentials.from_service_account_file(_localizar_credentials(), scopes=escopos))


def carregar_gemini_api_key() -> str:
    with open(_localizar_credentials(), encoding="utf-8") as f:
        dados = json.load(f)
    for nome in ("GEMINI_API_KEY", "gemini_api_key", "GOOGLE_API_KEY", "google_api_key"):
        valor = dados.get(nome)
        if isinstance(valor, str) and valor.strip():
            return valor.strip()
    raise RuntimeError("Não encontrei GEMINI_API_KEY em credentials.json.")


def _letra_coluna(indice: int) -> str:
    letra = ""
    indice += 1
    while indice:
        indice, resto = divmod(indice - 1, 26)
        letra = chr(65 + resto) + letra
    return letra


def _e_transitorio(erro: Exception) -> bool:
    texto = str(erro)
    return any(m in texto for m in ("429", "500", "503", "UNAVAILABLE", "RESOURCE_EXHAUSTED", "INTERNAL"))


def com_retry(funcao, contexto: str = "", tentativas: int = TENTATIVAS):
    for tentativa in range(1, tentativas + 1):
        try:
            return funcao()
        except Exception as erro:
            if tentativa == tentativas or not _e_transitorio(erro):
                raise
            espera = 2 ** tentativa + random.uniform(0, 1)
            print(f"Aviso: {contexto} falhou ({str(erro)[:120]}), de novo em {espera:.0f}s.")
            time.sleep(espera)


def temas_do_post(celula: str) -> list[str]:
    """Extrai os temas de uma célula da coluna 'Temas' de uma aba mensal.

    O Gemini grava em bullet ("*   Educação"), mas em 142 posts de agosto ele
    devolveu tudo numa linha só separado por vírgula. Sem tratar os dois casos
    esses posts contam como um tema gigante, e some do ranking o que eles tinham
    de verdade.
    """
    itens: list[str] = []
    for linha in celula.splitlines():
        limpo = linha.strip().lstrip("*-•·—– \t").strip()
        if not limpo:
            continue
        if len(celula.strip().splitlines()) <= 2 and limpo.count(",") >= 3:
            itens.extend(p.strip() for p in limpo.split(",") if p.strip())
        else:
            itens.append(limpo)
    return itens


def ler_posts(sh: gspread.Spreadsheet) -> dict[str, list[list[str]]]:
    """Temas de cada post, por candidato. É daqui que sai toda a contagem."""
    por_candidato: dict[str, list[list[str]]] = defaultdict(list)
    for nome_aba in ABAS_MENSAIS:
        try:
            aba = sh.worksheet(nome_aba)
        except gspread.exceptions.WorksheetNotFound:
            print(f"Aviso: aba '{nome_aba}' não existe, pulando.")
            continue
        valores = aba.get_all_values()
        cabecalho = valores[0]
        if "Candidato" not in cabecalho or "Temas" not in cabecalho:
            print(f"Aviso: aba '{nome_aba}' sem as colunas esperadas, pulando.")
            continue
        i_cand, i_temas = cabecalho.index("Candidato"), cabecalho.index("Temas")
        i_resumo = cabecalho.index("Resumo da legenda") if "Resumo da legenda" in cabecalho else -1

        for linha in valores[1:]:
            candidato = linha[i_cand].strip() if i_cand < len(linha) else ""
            if not candidato:
                continue
            celula = linha[i_temas] if i_temas < len(linha) else ""
            if any(celula.strip().startswith(m) for m in MARCAS_NAO_ANALISADO):
                continue
            resumo = linha[i_resumo] if 0 <= i_resumo < len(linha) else ""
            por_candidato[candidato].append([temas_do_post(celula), resumo])
    return por_candidato


def ler_uf(sh: gspread.Spreadsheet) -> dict[str, str]:
    """UF de cada candidato, da aba 'Instagram'.

    A coluna Estado só vem preenchida na primeira linha de cada bloco de UF,
    então o valor é arrastado para baixo até aparecer o próximo.
    """
    try:
        valores = sh.worksheet("Instagram").get_all_values()
    except gspread.exceptions.WorksheetNotFound:
        return {}
    cabecalho = valores[0]
    if "Estado" not in cabecalho or "Candidato" not in cabecalho:
        return {}
    i_uf, i_cand = cabecalho.index("Estado"), cabecalho.index("Candidato")

    por_candidato: dict[str, str] = {}
    uf_atual = ""
    for linha in valores[1:]:
        uf = linha[i_uf].strip() if i_uf < len(linha) else ""
        if uf:
            uf_atual = uf
        candidato = linha[i_cand].strip() if i_cand < len(linha) else ""
        if candidato:
            por_candidato[candidato] = uf_atual
    return por_candidato


def contar(posts: list[list], ruido_extra: set[str] | None = None) -> tuple[Counter, dict[str, str], int, dict[str, set[int]]]:
    """Em quantos POSTS cada tema aparece, não quantas vezes é escrito.

    A diferença importa: um post que repete "Saúde" em três bullets não vale
    três posts. Por isso o set() dentro do post.

    Devolve também a grafia original mais comum de cada tema normalizado, que é
    o que vai para o prompt e para a coluna de conferência.
    """
    contagem: Counter = Counter()
    posts_por_tema: dict[str, set[int]] = defaultdict(set)
    grafias: dict[str, Counter] = defaultdict(Counter)
    descartar = RUIDO | (ruido_extra or set())
    analisados = 0

    for indice_post, (temas, _resumo) in enumerate(posts):
        if not temas:
            continue
        analisados += 1
        vistos = set()
        for tema in temas:
            chave = normalizar(tema)
            if not chave or chave in descartar or len(chave) < 3:
                continue
            grafias[chave][tema.strip()] += 1
            vistos.add(chave)
        for chave in vistos:
            contagem[chave] += 1
            posts_por_tema[chave].add(indice_post)

    canonico = {chave: c.most_common(1)[0][0] for chave, c in grafias.items()}
    return contagem, canonico, analisados, posts_por_tema


ESQUEMA = {
    "type": "object",
    "properties": {
        "temas": {
            "type": "array",
            "items": {
                "type": "object",
                "properties": {
                    "rotulo": {"type": "string"},
                    "brutos": {"type": "array", "items": {"type": "string"}},
                },
                "required": ["rotulo", "brutos"],
            },
        },
        "resumo": {"type": "string"},
    },
    "required": ["temas", "resumo"],
}

PROMPT = """Você organiza o monitoramento de redes de pré-candidatos.

Abaixo estão os temas que apareceram nos posts de Instagram de {candidato} em julho e agosto de 2026, com o número de posts em que cada um aparece. O mesmo assunto está escrito de várias formas.

Tarefa 1. Agrupe as variações do mesmo assunto e devolva, em "temas", os grupos que são pauta de política pública ou projeto concreto: o que o candidato defende, promete, critica ou está executando.

Regras do agrupamento:
- "brutos" só pode conter texto copiado exatamente da lista abaixo, sem alterar uma letra. Não invente item.
- Cada bruto entra em um grupo só.
- Cada grupo representa UMA área de política pública. Não junte áreas diferentes no mesmo grupo: Segurança e Infraestrutura são dois grupos, Saúde e Educação são dois grupos.
- O rótulo carrega o que o candidato fala de concreto. Use também os nomes da seção "Vocabulário específico" quando eles detalharem um dos grupos. A indicação "coaparece com" mostra a qual pauta o nome está ligado.
- Se houver programa, obra ou indicador relacionado a um grupo, inclua pelo menos um no rótulo. Exemplo: "Saúde: PIX Saúde e Sistema Corujão", não apenas "Acesso à Saúde".
- Os nomes específicos servem apenas para enriquecer o rótulo: não os inclua em "brutos" e não crie grupo só para eles.
- Rótulo curto, até oito palavras.
- Deixe de fora o que é só processo de campanha, nome de lugar, de partido ou de político, e elogio sem pauta atrás.
- Não force: se um assunto não aparece na lista, ele não existe.

Tarefa 2. Escreva em "resumo" duas frases sobre o que este perfil vem publicando:
- Frase 1: as pautas concretas mais recorrentes, citando o que há de específico (nome de programa, obra, indicador).
- Frase 2: o que quase não aparece, usando somente a seção "Assuntos abaixo do corte". Se ela estiver vazia, omita esta frase.

Regras do resumo:
- Não escreva número nenhum. Nem contagem, nem porcentagem, nem data.
- Não escreva o que o candidato "busca", "pretende", "sinaliza" ou "se posiciona". Só o que ele publica.
- Só cite nome próprio que esteja escrito na lista abaixo.

Temas de {candidato}, com o número de posts:
{lista}

Vocabulário específico abaixo do corte, apenas para detalhar rótulos:
{vocabulario}

Assuntos abaixo do corte, únicos que podem ser descritos como pouco presentes:
{abaixo_corte}
"""


def _parece_especifico(texto: str) -> bool:
    """Seleciona programa, obra ou indicador raro para enriquecer rótulos.

    Não altera a contagem nem cria tema. O teste é conservador: sigla, algarismo
    ou duas palavras relevantes iniciadas por maiúscula.
    """
    palavras = re.findall(r"\b[\wÁÂÃÉÊÍÓÔÕÚÇáâãéêíóôõúç-]+\b", texto)
    maiusculas = [p for p in palavras if len(p) > 2 and p[0].isupper()]
    return bool(re.search(r"\b[A-ZÁÂÃÉÊÍÓÔÕÚÇ]{2,}\b|\d", texto) or len(maiusculas) >= 2)


def classificar(gem: genai.Client, candidato: str, contagem: Counter,
                canonico: dict[str, str],
                posts_por_tema: dict[str, set[int]]) -> tuple[list[dict], str, dict]:
    entrada = [(canonico[c], n) for c, n in contagem.most_common() if n >= MINIMO_POSTS_POR_TEMA]
    if not entrada:
        return [], "", {"entrada": 0, "saida": 0, "pensamento": 0}

    lista = "\n".join(f"- {rotulo} ({n} posts)" for rotulo, n in entrada)
    abaixo = [(canonico[c], n) for c, n in contagem.most_common()
              if n < MINIMO_POSTS_POR_TEMA]
    chaves_principais = [c for c, n in contagem.most_common()
                         if n >= MINIMO_POSTS_POR_TEMA]
    vocabulario = []
    for chave, n in contagem.most_common():
        if n >= MINIMO_POSTS_POR_TEMA or not _parece_especifico(canonico[chave]):
            continue
        relacionados = []
        posts_raro = posts_por_tema.get(chave, set())
        for principal in chaves_principais:
            intersecao = len(posts_raro & posts_por_tema.get(principal, set()))
            if intersecao:
                relacionados.append((intersecao, contagem[principal], canonico[principal]))
        relacionados.sort(reverse=True)
        if relacionados:
            vocabulario.append((canonico[chave], n, [r[2] for r in relacionados[:3]]))
    # Limites mantêm o prompt pequeno e tornam a regra de ausência auditável.
    texto_vocabulario = "\n".join(
        f"- {r} ({n} post{'s' if n != 1 else ''}; coaparece com: {', '.join(rel)})"
        for r, n, rel in vocabulario[:40]) or "(vazio)"
    texto_abaixo = "\n".join(f"- {r} ({n} post{'s' if n != 1 else ''})"
                               for r, n in abaixo[:30]) or "(vazio)"
    prompt = PROMPT.format(candidato=candidato, lista=lista,
                           vocabulario=texto_vocabulario, abaixo_corte=texto_abaixo)

    resp = com_retry(
        lambda: gem.models.generate_content(
            model=MODELO,
            contents=[prompt],
            config=types.GenerateContentConfig(
                response_mime_type="application/json",
                response_schema=ESQUEMA,
                # Sem isso o modelo gasta (e cobra) pensamento numa tarefa que é
                # agrupar string. O custo da rodada cai e o resultado não muda.
                thinking_config=types.ThinkingConfig(thinking_budget=0),
            ),
        ),
        f"classificação de {candidato}",
    )

    uso = resp.usage_metadata
    tokens = {
        "entrada": getattr(uso, "prompt_token_count", 0) or 0,
        "saida": getattr(uso, "candidates_token_count", 0) or 0,
        # O thoughts_token_count não entra no candidates_token_count, mas é
        # cobrado como saída. Somar os dois é o que faz a estimativa bater
        # com a fatura.
        "pensamento": getattr(uso, "thoughts_token_count", 0) or 0,
    }

    dados = json.loads(resp.text)
    return dados.get("temas", []), (dados.get("resumo") or "").strip(), tokens


def consolidar(grupos: list[dict], contagem: Counter, canonico: dict[str, str],
               analisados: int, posts_por_tema: dict[str, set[int]]) -> list[dict]:
    """Soma as contagens de cada grupo, no Python, e ordena.

    Aqui mora a guarda contra invenção: bruto que o modelo devolveu mas que não
    estava na entrada é descartado, e grupo que ficou sem nenhum bruto válido
    some. Um rótulo só sobrevive se tiver post real embaixo dele.
    """
    validos = set(contagem)
    usados: set[str] = set()
    resultado = []

    for grupo in grupos:
        rotulo = compactar_rotulo((grupo.get("rotulo") or "").strip())
        if not rotulo:
            continue
        membros = []
        for bruto in grupo.get("brutos", []):
            chave = normalizar(bruto)
            if chave in validos and chave not in usados:
                membros.append(chave)
                usados.add(chave)
        if not membros:
            print(f"    descartado (nenhum tema real embaixo): {rotulo}")
            continue
        # União, não soma: se um post traz "Saúde" e "Saúde Pública", o grupo
        # Saúde aparece em um post, não em dois.
        posts_do_grupo: set[int] = set()
        for chave in membros:
            posts_do_grupo.update(posts_por_tema.get(chave, set()))
        total = len(posts_do_grupo)
        resultado.append({
            "rotulo": rotulo,
            "posts": total,
            "pct": round(100 * total / analisados, 1) if analisados else 0.0,
            "brutos": [canonico[c] for c in sorted(membros, key=lambda k: -contagem[k])][:5],
        })

    resultado.sort(key=lambda t: -t["posts"])
    return resultado[:TOP_N]


def compactar_rotulo(rotulo: str, limite: int = 8) -> str:
    """Impõe no Python o limite que o prompt pede ao modelo.

    Preserva primeiro a categoria antes dos dois-pontos e depois os primeiros
    descritores completos, sem deixar vírgula ou conjunção pendurada.
    """
    # Remove conectores editoriais antes de cortar; assim nomes compostos como
    # "Primeiro Emprego" permanecem inteiros dentro do limite.
    rotulo = re.sub(r",?\s+(com foco em|incluindo|com destaque para)\s+", ": ",
                    rotulo, flags=re.IGNORECASE)
    rotulo = re.sub(r":\s*:\s*", ": ", rotulo)
    palavras = rotulo.split()
    if len(palavras) <= limite:
        return rotulo
    limite_real = limite
    if limite < len(palavras) and palavras[limite - 1].lower() in {"primeiro", "sistema"}:
        limite_real += 1
    curto = " ".join(palavras[:limite_real]).rstrip(" ,;:-")
    curto = re.sub(r"\s+(e|ou|com|de|da|do|das|dos)$", "", curto,
                   flags=re.IGNORECASE).rstrip(" ,;:-")
    return curto


def montar_resumo(candidato: str, resumo_modelo: str, temas: list[dict],
                  analisados: int, total_posts: int, contagem: Counter,
                  canonico: dict[str, str]) -> str:
    """Junta a frase de volume, escrita aqui, com as duas frases do modelo.

    O modelo não escreve número. A frase com os números é montada com o que foi
    contado, então não tem como sair um dado que ninguém apurou.
    """
    if not temas:
        return ""

    abertura = (f"Dos {total_posts} posts de julho e agosto, {analisados} têm temas analisados, "
                f"e o tema mais recorrente é {temas[0]['rotulo'].lower()}, "
                f"em {temas[0]['posts']} deles.")

    # Nome próprio citado no resumo tem que existir na lista que o modelo
    # recebeu. Se não existir, a frase sai. É a mesma checagem que o
    # analise_planos.py faz antes de gravar citação.
    fonte = normalizar(" ".join(canonico.values()))
    frases = []
    for frase in re.split(r"(?<=[.!?])\s+", resumo_modelo):
        proprios = re.findall(r"\b[A-ZÁÂÃÉÊÍÓÔÕÚÇ][\wÁÂÃÉÊÍÓÔÕÚÇáâãéêíóôõúç]{2,}", frase)
        inventados = [p for p in proprios if normalizar(p) not in fonte]
        if inventados:
            print(f"    frase cortada do resumo, cita o que não está na base: {inventados}")
            continue
        if re.search(r"\d", frase):
            print("    frase cortada do resumo, o modelo escreveu número.")
            continue
        # Frases de baixa presença não podem contradizer o próprio ranking.
        if re.search(r"quase nao|pouco apare|raramente|baixa presenca|menos present|nao apare",
                     normalizar(frase)):
            citados_top = []
            for tema in temas:
                categoria = normalizar(tema["rotulo"].split(":", 1)[0])
                if len(categoria) >= 4 and categoria in normalizar(frase):
                    citados_top.append(tema["rotulo"])
            if citados_top:
                print(f"    frase cortada do resumo, contradiz o ranking: {citados_top}")
                continue
        frases.append(frase.strip())

    return " ".join([abertura] + frases).strip()


def garantir_colunas(aba: gspread.Worksheet, cabecalho: list[str]) -> dict[str, int]:
    """Cria as colunas de saída no fim da aba, se ainda não existirem."""
    # Migração sem criar uma sexta coluna: o cabeçalho antigo ocupa a mesma
    # posição e é renomeado antes de calcular os índices.
    if COLUNA_PERCENTUAL_ANTIGA in cabecalho and COLUNA_PERCENTUAL not in cabecalho:
        indice_antigo = cabecalho.index(COLUNA_PERCENTUAL_ANTIGA)
        aba.update_cell(1, indice_antigo + 1, COLUNA_PERCENTUAL)
        cabecalho[indice_antigo] = COLUNA_PERCENTUAL
    indices = {}
    novas = []
    for nome in COLUNAS_SAIDA:
        if nome in cabecalho:
            indices[nome] = cabecalho.index(nome)
        else:
            indices[nome] = len(cabecalho) + len(novas)
            novas.append(nome)
    if novas:
        precisa = len(cabecalho) + len(novas)
        if aba.col_count < precisa:
            aba.add_cols(precisa - aba.col_count)
        aba.batch_update(
            [{"range": f"{_letra_coluna(indices[n])}1", "values": [[n]]} for n in novas],
            value_input_option="RAW",
        )
    return indices


def garantir_aba(sh: gspread.Spreadsheet, roster: list[str],
                 com_posts: set[str]) -> gspread.Worksheet:
    """Devolve a aba de destino, criando-a e preenchendo os candidatos se preciso.

    A lista de candidatos sai da aba "Instagram", que é o cadastro do
    monitoramento, filtrada por quem tem post coletado. Assim o script não
    depende de nenhuma outra rodada ter passado antes: rodar em planilha sem a
    aba de alinhamento funciona igual a rodar em planilha com ela.

    Usar a aba "Instagram" como cadastro também resolve o lixo das abas
    mensais, onde "PL CE" e "Solidariedade" aparecem na coluna Candidato como
    se fossem candidatos.
    """
    esperados = [c for c in roster if c in com_posts]

    try:
        aba = sh.worksheet(ABA_ALINHAMENTO)
    except gspread.exceptions.WorksheetNotFound:
        aba = sh.add_worksheet(title=ABA_ALINHAMENTO,
                               rows=len(esperados) + 10,
                               cols=len(COLUNAS_SAIDA) + 2)
        aba.update([[COLUNA_CANDIDATO] + COLUNAS_SAIDA], "A1", value_input_option="RAW")
        print(f"Aba '{ABA_ALINHAMENTO}' não existia e foi criada.")

    valores = aba.get_all_values()
    if not valores or not any(valores[0]):
        aba.update([[COLUNA_CANDIDATO] + COLUNAS_SAIDA], "A1", value_input_option="RAW")
        valores = aba.get_all_values()

    cabecalho = valores[0]
    if COLUNA_CANDIDATO not in cabecalho:
        raise RuntimeError(f"A aba '{ABA_ALINHAMENTO}' não tem a coluna "
                           f"'{COLUNA_CANDIDATO}' e não dá para saber onde gravar.")

    i_cand = cabecalho.index(COLUNA_CANDIDATO)
    ja_na_aba = {linha[i_cand].strip() for linha in valores[1:]
                 if i_cand < len(linha) and linha[i_cand].strip()}

    faltando = [c for c in esperados if c not in ja_na_aba]
    if faltando:
        # append_rows respeita as colunas já existentes: só a do candidato é
        # preenchida, o resto da linha fica em branco para o script gravar.
        largura = max(len(cabecalho), i_cand + 1)
        novas = [[""] * largura for _ in faltando]
        for linha, candidato in zip(novas, faltando):
            linha[i_cand] = candidato
        aba.append_rows(novas, value_input_option="RAW")
        print(f"{len(faltando)} candidato(s) acrescentado(s) à aba "
              f"'{ABA_ALINHAMENTO}': {', '.join(faltando[:5])}"
              f"{'...' if len(faltando) > 5 else ''}")

    return aba


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--candidato", default=None, help="roda um candidato só")
    parser.add_argument("--limite", type=int, default=None)
    parser.add_argument("--forcar", action="store_true",
                        help="refaz também quem já tem Temas Principais preenchido")
    args = parser.parse_args()

    gc = gs_client()
    gem = genai.Client(api_key=carregar_gemini_api_key())
    sh = gc.open_by_key(SPREADSHEET_ID)

    posts_por_candidato = ler_posts(sh)
    uf_por_candidato = ler_uf(sh)
    print(f"{sum(len(v) for v in posts_por_candidato.values())} posts lidos "
          f"de {len(posts_por_candidato)} candidato(s).")

    aba = garantir_aba(sh, list(uf_por_candidato), set(posts_por_candidato))
    valores = aba.get_all_values()
    cabecalho = valores[0]
    indices = garantir_colunas(aba, cabecalho)
    i_cand = cabecalho.index(COLUNA_CANDIDATO)
    i_feito = cabecalho.index("Temas Principais") if "Temas Principais" in cabecalho else -1

    alvos = []
    for numero, linha in enumerate(valores[1:], start=2):
        candidato = linha[i_cand].strip() if i_cand < len(linha) else ""
        if not candidato:
            continue
        if args.candidato and candidato != args.candidato:
            continue
        preenchido = 0 <= i_feito < len(linha) and linha[i_feito].strip()
        if preenchido and not args.forcar:
            continue
        alvos.append((numero, candidato))
    if args.limite:
        alvos = alvos[:args.limite]

    print(f"{len(alvos)} candidato(s) a processar.\n")

    entrada = saida = pensamento = 0
    escritas: list[dict] = []
    letra_ini = _letra_coluna(indices[COLUNAS_SAIDA[0]])
    letra_fim = _letra_coluna(indices[COLUNAS_SAIDA[-1]])

    def descarregar():
        if escritas:
            aba.batch_update(escritas, value_input_option="RAW")
            escritas.clear()

    for i, (numero, candidato) in enumerate(alvos, start=1):
        posts = posts_por_candidato.get(candidato, [])
        contagem, canonico, analisados, posts_por_tema = contar(
            posts, ruido_do_candidato(candidato, uf_por_candidato.get(candidato, "")))

        if analisados < MINIMO_POSTS_CANDIDATO:
            print(f"[{i}/{len(alvos)}] {candidato}: só {analisados} post(s) analisado(s), "
                  f"abaixo do mínimo de {MINIMO_POSTS_CANDIDATO}. Marcado, não classificado.")
            escritas.append({
                "range": f"{letra_ini}{numero}:{letra_fim}{numero}",
                "values": [["", analisados, "", "", "Base pequena demais para ranquear."]],
            })
            continue

        try:
            grupos, resumo_modelo, uso = classificar(
                gem, candidato, contagem, canonico, posts_por_tema)
        except Exception as erro:
            print(f"[{i}/{len(alvos)}] {candidato}: falhou ({str(erro)[:160]}), pulando.")
            continue

        entrada += uso["entrada"]
        saida += uso["saida"]
        pensamento += uso["pensamento"]

        temas = consolidar(grupos, contagem, canonico, analisados, posts_por_tema)
        resumo = montar_resumo(candidato, resumo_modelo, temas, analisados,
                               len(posts), contagem, canonico)

        escritas.append({
            "range": f"{letra_ini}{numero}:{letra_fim}{numero}",
            "values": [[
                SEPARADOR.join(t["rotulo"] for t in temas),
                SEPARADOR.join(str(t["posts"]) for t in temas),
                SEPARADOR.join(f"{t['pct']}%" for t in temas),
                " | ".join(f"{t['rotulo']}: {', '.join(t['brutos'])}" for t in temas),
                resumo,
            ]],
        })
        print(f"[{i}/{len(alvos)}] {candidato}: {len(temas)} tema(s) sobre "
              f"{analisados} post(s) analisado(s).")
        if temas:
            print(f"    1º: {temas[0]['rotulo']} ({temas[0]['posts']} posts, {temas[0]['pct']}%)")

        if len(escritas) >= LOTE_ESCRITA:
            descarregar()

    descarregar()

    custo = (entrada / 1_000_000 * PRECO_ENTRADA_POR_MILHAO
             + (saida + pensamento) / 1_000_000 * PRECO_SAIDA_POR_MILHAO)
    print(f"\nTokens: {entrada:,} de entrada, {saida:,} de saída, "
          f"{pensamento:,} de pensamento. Custo estimado: US$ {custo:.2f}.")


if __name__ == "__main__":
    main()
