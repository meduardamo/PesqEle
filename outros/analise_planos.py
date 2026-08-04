"""
Análise dos planos de governo.

Para cada plano, extrai o texto (PyMuPDF, com OCR só nas páginas sem texto) e pede
ao Gemini para classificar cada tema numa escala de maturidade:
  Ausente  - tema não aparece
  Genérico - citado, mas vago
  Proposta - citado com ação concreta
  Meta     - citado com alvo mensurável (número, %, prazo)
"""

from __future__ import annotations
import json
import os
import time
import re
import unicodedata
from pathlib import Path

import fitz                      # PyMuPDF
import pandas as pd

# Paleta Eixo
COR = {
    # Paleta oficial Eixo (Manual de Identidade Visual, "Cores para gráficos").
    "azul":    "#192D4E",   # marinho
    "vinho":   "#962E4D",
    "amarelo": "#E8A600",
    "azul_claro":  "#BAC8DD",
    "azul_medio":  "#6091D8",
    "cinza":   "#E1E1E1",
}

# Restrições de linguagem coladas em todo prompt que gera prosa (justificativa de
# coerência, síntese comparativa). É a "versão cotidiano" de Sampaio (2026),
# "Prompts para a IA não lavar seu texto", que a Eduarda usa nos textos dela.
# Sem isso o modelo devolve o vocabulário do próprio enunciado: as 16
# justificativas gravadas em 03/08/2026 começavam todas com "O plano", 14 delas
# seguiam o mesmo molde de elogio seguido de "Contudo" mais lacuna, e sete usavam
# "apresenta"/"possui" no lugar de "tem".
RESTRICOES_LINGUAGEM = (
    "RESTRIÇÕES DE LINGUAGEM (obrigatórias):\n"
    "- Proibido travessão (—), ponto e vírgula (;) e dois pontos (:) fora de lista formal.\n"
    "- Proibidos adjetivos vagos ou promocionais: abrangente, crucial, essencial, "
    "estratégico, fundamental, robusto, significativo, notável, sólido, claro, "
    "consistente, inovador, multifacetado.\n"
    "- Proibidas cópulas infladas. Escreva 'tem', 'é', 'traz', 'cita'. Nunca "
    "'apresenta', 'possui', 'oferece', 'serve como', 'destaca-se como'.\n"
    "- Proibidos verbos inflados: destacar, ressaltar, enfatizar, evidenciar, "
    "promover, refletir, contribuir para, aprimorar.\n"
    "- Proibido começar frase com conectivo opinativo: além disso, contudo, "
    "portanto, no entanto, assim, na verdade, notavelmente.\n"
    "- Proibido gerúndio conclusivo vago no fim da frase ('garantindo que', "
    "'resultando em', 'refletindo a importância').\n"
    "- Proibido paralelismo negativo ('não apenas X, mas também Y') e regra dos "
    "três decorativa (três adjetivos ou três itens só para parecer completo).\n"
    "- Proibida editorialização: 'vale destacar', 'é importante notar', "
    "'cabe ressaltar'.\n"
    "- Proibida metáfora morta: cenário, panorama, ótica, lente, chave, âmbito, "
    "marco, mosaico, esfera, horizonte.\n"
    "- Máximo de 25 palavras por frase. Frases declarativas, sujeito, verbo, objeto.\n"
    "- Dado concreto no lugar de adjetivo. Nome de programa, número, prazo, página.\n"
    "- Se a frase serve para qualquer outro plano sem mudar nada, reescreva."
)

# Escala de maturidade (ordem importa)
NIVEIS = ["Não menciona", "Menciona vagamente", "Propõe ação", "Define meta"]
# A escala é ORDINAL: 0, 1, 2 e 3 pontos. Rampa sequencial de um tom só, do
# claro ao escuro, no azul da marca — mais escuro = mais maduro. Mantido igual
# ao painel, que é quem exibe. Ver o comentário longo lá.
COR_NIVEL = {
    "Não menciona":       "#E3E8F0",
    "Menciona vagamente": "#9DB4D3",
    "Propõe ação":        "#3F6FB5",
    "Define meta":        COR["azul"],
}

# Nomes antigos de tema. O nome do tema é a chave que indexa a análise salva, então
# renomear sem tradução jogaria fora o que já foi processado. "Ensino Médio Integral"
# virou "Tempo Integral" em 04/08/2026: dos 13 trechos que o tema tinha capturado,
# 11 falavam de tempo integral sem dizer a etapa, e o nome prometia um recorte que a
# análise não fazia.
TEMAS_LEGADO = {"Ensino Médio Integral": "Tempo Integral"}

# Nomes antigos da escala, usados em análises salvas antes da renomeação.
# Traduzido na leitura para que dados ainda não reprocessados continuem funcionando.
NIVEIS_LEGADO = {
    "Ausente":  "Não menciona",
    "Genérico": "Menciona vagamente",
    "Proposta": "Propõe ação",
    "Meta":     "Define meta",
}

# Temas por eixo de política pública, cada um com uma descrição curta que
# orienta o Gemini.
#
# Os seis temas de Educação são os mesmos de antes, com os MESMOS nomes: a
# análise já salva na planilha é indexada por nome de tema, e renomear jogaria
# fora o que já foi processado.
#
# Custa quase nada ampliar: classificar_plano faz UMA chamada com todos os
# temas juntos, então 22 temas custam praticamente o mesmo que 6.
EIXOS = {
    "Educação": {
        "Alfabetização": "alfabetização de crianças na idade certa (PNAIC, PNA, Compromisso Nacional Criança Alfabetizada ou equivalente)",
        "Tempo Integral": "educação em tempo integral e jornada escolar ampliada, em qualquer etapa da educação básica (o estado costuma concentrar no ensino médio)",
        "Educação Profissional": "educação profissional e técnica (EPT, SENAI, SENAC, institutos federais, cursos técnicos)",
        "Primeira Infância": "creches, pré-escola, educação infantil, primeira infância (0–5 anos)",
        "Fundamental": "ensino fundamental anos iniciais (2º ao 5º ano) e anos finais (6º ao 9º ano) — exclui alfabetização do 1º ano, que é tema próprio",
        "Tecnologia na Educação": "tecnologia, conectividade, inclusão digital e uso de IA nas escolas",
    },
    "Saúde": {
        "Atenção Primária": "atenção primária, saúde da família, UBS, agentes comunitários de saúde",
        "Média e Alta Complexidade": "hospitais, leitos, UTI, cirurgias eletivas, filas e regulação de vagas",
        "Saúde Mental": "saúde mental, CAPS, dependência química, prevenção ao suicídio",
    },
    "Segurança pública": {
        "Policiamento e Efetivo": "efetivo policial, salário, equipamento, presença ostensiva, videomonitoramento",
        "Enfrentamento ao Crime Organizado": "facções, tráfico, inteligência policial, fronteiras",
        "Violência contra a Mulher": "violência doméstica, Lei Maria da Penha, delegacia da mulher, casa da mulher brasileira",
    },
    "Economia e emprego": {
        "Geração de Emprego": "geração de emprego e renda, qualificação profissional para o trabalho, intermediação de mão de obra",
        "Ambiente de Negócios": "atração de investimento, desburocratização, incentivo fiscal, apoio a micro e pequena empresa",
        "Agropecuária": "agricultura, pecuária, agronegócio, agricultura familiar, crédito rural",
    },
    "Meio ambiente e clima": {
        "Desmatamento e Conservação": "desmatamento, unidades de conservação, fiscalização ambiental, queimadas",
        "Saneamento e Recursos Hídricos": "água, esgoto, resíduos sólidos, bacias hidrográficas, seca",
        "Transição Energética": "energia renovável, solar, eólica, crédito de carbono, economia verde",
    },
    "Assistência social e pobreza": {
        "Transferência de Renda": "programas de transferência de renda, complemento ao Bolsa Família, benefício estadual",
        "Segurança Alimentar": "fome, insegurança alimentar, restaurante popular, banco de alimentos, cesta básica",
        "Habitação": "moradia, habitação popular, regularização fundiária, aluguel social",
    },
    "Infraestrutura e mobilidade": {
        "Transporte e Rodovias": "rodovias, pavimentação, ferrovias, portos, aeroportos, logística",
        "Mobilidade Urbana": "transporte público, tarifa, metrô, BRT, ciclovia",
    },
    "Gestão pública e transparência": {
        "Eficiência e Gasto Público": "reforma administrativa, corte de gasto, teto de despesa, concurso, servidor",
        "Transparência e Combate à Corrupção": "transparência, controle interno, dados abertos, combate à corrupção",
        "Governo Digital": "digitalização de serviços, atendimento ao cidadão, governo eletrônico",
    },
}

# Mapa plano tema -> eixo, para a página agrupar sem repetir a estrutura.
EIXO_DO_TEMA = {tema: eixo for eixo, temas in EIXOS.items() for tema in temas}

# Achatado: é isso que vai no prompt e que indexa a análise salva.
TEMAS = {tema: desc for temas in EIXOS.values() for tema, desc in temas.items()}

# Só os de educação, para quem quiser o recorte antigo.
TEMAS_EDUCACAO = dict(EIXOS["Educação"])

GEMINI_MODEL = "gemini-2.5-flash"


_CLIENT = None


def _gemini_client():
    # mesma config do gerador de envios: chave por env ou secrets.
    # O cliente fica guardado num global de propósito: o `Client` do google-genai
    # fecha a conexão HTTP no __del__, então um cliente temporário em
    # `_gemini_client().models.generate_content(...)` é coletado antes da chamada
    # sair e o erro vira "Cannot send a request, as the client has been closed".
    global _CLIENT
    if _CLIENT is not None:
        return _CLIENT
    from google import genai
    key = os.getenv("GEMINI_API_KEY", "")
    if not key:
        try:
            import streamlit as st
            key = st.secrets.get("GEMINI_API_KEY", "")
        except Exception:
            pass
    if not key:
        raise RuntimeError("Faltou a GEMINI_API_KEY (env var ou secrets).")
    _CLIENT = genai.Client(api_key=key)
    return _CLIENT


# extração de texto

# O TSE aceita o que o candidato subir: PDF nativo, PDF escaneado, PDF com
# fonte sem mapa ToUnicode, PDF protegido por senha de dono, e às vezes nem PDF.
# O link também falha sozinho (blip da API). Cada uma dessas situações tem
# tratamento próprio aqui; o objetivo é nunca devolver vazio sem dizer por quê.

PAGINAS_OCR_MAX = 80      # teto de páginas OCRadas por plano
OCR_DPI = 250
OCR_DPI_FALLBACK = 150    # 2ª tentativa quando o pixmap estoura memória
BAIXAR_TENTATIVAS = 4
BAIXAR_TIMEOUT = 90


class PlanoIlegivel(Exception):
    """O arquivo veio, mas não dá para virar texto (não é PDF, está corrompido,
    tem senha de abertura). A mensagem explica qual dos casos."""


class PlanoIndisponivel(PlanoIlegivel):
    """Falha TEMPORÁRIA: o DivulgaCand caiu, deu timeout ou devolveu 5xx.

    Separada de PlanoIlegivel de propósito. O DivulgaCand sai do ar por alguns
    minutos e volta; se isso virasse "plano ilegível", o painel gravaria uma
    análise vazia que só sairia da frente por reprocessamento. Quem chama deve
    PULAR o candidato e tentar de novo depois, nunca persistir resultado.
    """


def _frac_invalido(t: str) -> float:
    """Fração de caracteres fora do conjunto esperado em português. Valor alto
    indica codificação quebrada (glifos espelhados ou sem mapa ToUnicode), caso
    em que a camada de texto vem embaralhada e precisa de OCR."""
    t = (t or "").strip()
    if not t:
        return 1.0
    validos = re.findall(r"[A-Za-zÀ-ÿ0-9\s.,;:!?()\-\"'/%R$º°ªü]", t)
    return 1 - len(validos) / len(t)


_TESSERACT_OK = None


def tesseract_disponivel() -> bool:
    """Checa uma vez se dá para OCRar. Sem isso o OCR falhava em silêncio e o
    plano escaneado voltava vazio sem explicação — que é o que acontece hoje no
    Streamlit Cloud, onde o tesseract não vem instalado."""
    global _TESSERACT_OK
    if _TESSERACT_OK is None:
        try:
            import pytesseract
            pytesseract.get_tesseract_version()
            _TESSERACT_OK = True
        except Exception:
            _TESSERACT_OK = False
    return _TESSERACT_OK


def _ocr_pagina(page, dpi: int = OCR_DPI, lang: str = "por") -> str:
    if not tesseract_disponivel():
        return ""
    for tentativa_dpi in (dpi, OCR_DPI_FALLBACK):
        try:
            import pytesseract
            from PIL import Image
            pm = page.get_pixmap(dpi=tentativa_dpi, alpha=False)
            img = Image.frombytes("RGB", (pm.width, pm.height), pm.samples)
            return pytesseract.image_to_string(img, lang=lang) or ""
        except MemoryError:
            continue            # página gigante: tenta de novo em resolução menor
        except Exception:
            return ""
    return ""


def _texto_da_pagina(page) -> str:
    """Texto de uma página, tentando os modos do PyMuPDF em ordem de qualidade.
    Uma página quebrada não pode derrubar o documento inteiro."""
    for modo in ("text", "blocks"):
        try:
            bruto = page.get_text(modo)
        except Exception:
            continue
        if modo == "blocks":
            bruto = " ".join(b[4] for b in (bruto or []) if len(b) > 4 and isinstance(b[4], str))
        if (bruto or "").strip():
            return bruto
    return ""


def _abrir_pdf(data: bytes):
    """Abre o PDF tolerando senha de dono e lixo antes do cabeçalho."""
    if not data:
        raise PlanoIlegivel("download veio vazio")

    inicio = data[:1024]
    if b"%PDF" not in inicio:
        if inicio.lstrip()[:1] in (b"<", b"{"):
            raise PlanoIlegivel("o link devolveu HTML/JSON, não o arquivo do plano")
        if inicio[:2] == b"PK":
            raise PlanoIlegivel("o arquivo é DOCX/ODT, não PDF")
        raise PlanoIlegivel("o arquivo não é PDF")

    # Alguns planos vêm com bytes antes do %PDF; o MuPDF não abre assim.
    if not data.startswith(b"%PDF"):
        data = data[data.find(b"%PDF"):]

    try:
        doc = fitz.open(stream=data, filetype="pdf")
    except Exception as e:
        raise PlanoIlegivel(f"PDF corrompido: {e}") from e

    # Senha de DONO (restringe cópia/impressão) abre com senha vazia. Senha de
    # ABERTURA não abre, e aí não há o que fazer.
    if doc.is_encrypted and not doc.authenticate(""):
        doc.close()
        raise PlanoIlegivel("PDF protegido por senha de abertura")
    return doc


def _extrair_paginas(doc, usar_ocr: bool, min_chars: int) -> list[str]:
    """Texto de cada página, na ordem do documento.

    A classificação usa tudo junto, mas guardar a divisão por página é o que
    permite dizer depois em que página do PDF está o trecho citado.
    """
    partes, ocradas = [], 0
    for page in doc:
        raw = _texto_da_pagina(page)
        # OCR quando a página está sem texto OU quando a camada de texto veio
        # embaralhada (codificação de fonte quebrada).
        precisa = len(raw.strip()) < min_chars or _frac_invalido(raw) > 0.10
        if usar_ocr and precisa and ocradas < PAGINAS_OCR_MAX:
            ocr = _ocr_pagina(page)
            ocradas += 1
            if len(ocr.strip()) > len(raw.strip()):
                raw = ocr
        partes.append(raw)
    return partes


def _extrair_doc(doc, usar_ocr: bool, min_chars: int) -> str:
    return " ".join(_extrair_paginas(doc, usar_ocr, min_chars))


def extrair_texto(pdf_path: str | Path, usar_ocr: bool = True,
                  min_chars: int = 200) -> str:
    """Texto de um PDF em disco. Faz OCR só nas páginas sem camada de texto."""
    with fitz.open(pdf_path) as doc:
        return _extrair_doc(doc, usar_ocr, min_chars)


def extrair_texto_bytes(data: bytes, usar_ocr: bool = True,
                        min_chars: int = 200) -> str:
    """Texto de um PDF recebido como bytes (ex.: upload no Streamlit)."""
    doc = _abrir_pdf(data)
    try:
        return _extrair_doc(doc, usar_ocr, min_chars)
    finally:
        doc.close()


def baixar_plano(url: str) -> bytes:
    """Baixa o arquivo do plano, repetindo em falha temporária.

    404/410 é definitivo (o plano não está lá) e sai na hora. Timeout, erro de
    conexão e 5xx são o DivulgaCand fora do ar: repete e, se não voltar,
    levanta PlanoIndisponivel para quem chama saber que é para tentar depois.
    """
    import requests
    ultimo = None
    for tentativa in range(1, BAIXAR_TENTATIVAS + 1):
        try:
            r = requests.get(url, headers={"User-Agent": "Mozilla/5.0"},
                             timeout=BAIXAR_TIMEOUT, allow_redirects=True)
            # 4xx = a requisição é que está errada, repetir não muda nada. O TSE
            # responde 422 (não 404) para id de arquivo inexistente. Exceções:
            # 408 e 429, que pedem justamente para tentar de novo.
            if 400 <= r.status_code < 500 and r.status_code not in (408, 429):
                raise PlanoIlegivel(
                    f"o TSE não entrega esse arquivo (HTTP {r.status_code})")
            r.raise_for_status()
            if r.content:
                return r.content
            ultimo = "resposta vazia"
        except PlanoIlegivel:
            raise
        except Exception as e:
            ultimo = f"{type(e).__name__}: {e}"
        if tentativa < BAIXAR_TENTATIVAS:
            time.sleep(2 * tentativa)
    raise PlanoIndisponivel(
        f"DivulgaCand não respondeu após {BAIXAR_TENTATIVAS} tentativas ({ultimo}). "
        "Costuma ser queda passageira; tente de novo em alguns minutos.")


def extrair_paginas_url(url: str, usar_ocr: bool = True) -> list[str]:
    """Baixa o PDF do link e devolve o texto de cada página, na ordem."""
    doc = _abrir_pdf(baixar_plano(url))
    try:
        return _extrair_paginas(doc, usar_ocr, 200)
    finally:
        doc.close()


# ─── Em que página está o trecho ──────────────────────────────────────────────
# O painel guarda a frase que justifica cada classificação. Saber a página em
# que ela aparece é o que transforma "confie na análise" em "confira você
# mesmo": sem isso, conferir significa abrir um PDF de 90 páginas e procurar à
# mão. A conta é feita aqui, uma vez, e vai gravada na planilha — não faz
# sentido cada pessoa que abre o painel baixar o plano de novo.
_JANELA_TRECHO = 6      # palavras por janela na busca aproximada
_MIN_ESCORE = 0.25      # abaixo disso o trecho não está no PDF em forma literal


def _norm_busca(t: str) -> str:
    """Texto comparável: sem acento, sem caixa e sem pontuação. A quebra de
    linha do PDF vira espaço, senão nenhuma frase de duas linhas casa."""
    t = unicodedata.normalize("NFD", str(t or "").lower())
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return re.sub(r"[^a-z0-9]+", " ", t).strip()


def paginas_do_trecho(paginas_norm: list[str], trecho: str,
                      limite: int = 3) -> list[int]:
    """Páginas (1-based) em que o trecho aparece.

    O trecho gravado costuma ser uma citação com corte: o modelo junta partes
    distantes do plano com "[…]" ou reticências, e a extração ainda trunca em
    240 caracteres. Procurar a frase inteira nesse caso nunca acha nada, porque
    ela não existe assim em lugar nenhum do PDF. Por isso cada pedaço é
    procurado por conta própria e as páginas se somam.
    """
    pedacos = [_norm_busca(p) for p in
               re.split(r"\[\s*[.…]{1,3}\s*\]|\.{3,}|…", str(trecho or ""))]
    pedacos = [p for p in pedacos if p]
    longos = [p for p in pedacos if len(p.split()) >= 4]
    if longos:
        achadas: list[int] = []
        for pedaco in longos:
            for n in _paginas_de_um_pedaco(paginas_norm, pedaco, limite):
                if n not in achadas:
                    achadas.append(n)
        return sorted(achadas)[:limite]

    # Nenhum pedaço tem quatro palavras. Acontece quando o modelo corta o meio
    # de uma lista: "Investiremos em... saneamento" vira "investiremos em" e
    # "saneamento", curtos demais para localizar sozinhos. Juntos, na MESMA
    # página, são sinal forte. Se muitas páginas têm os dois, o trecho não
    # distingue nada e é melhor não apontar nenhuma.
    if sum(len(p.split()) for p in pedacos) < 3 or len(pedacos) < 2:
        return []
    juntas = [i + 1 for i, pag in enumerate(paginas_norm)
              if pag and all(p in pag for p in pedacos)]
    return juntas if len(juntas) <= limite else []


def _paginas_de_um_pedaco(paginas_norm: list[str], trecho: str,
                          limite: int = 3) -> list[int]:
    """Um pedaço contínuo de citação.

    Primeiro tenta a frase inteira. Como o modelo às vezes resume em vez de
    citar, o segundo passo conta quantas janelas de seis palavras do trecho
    estão na página e fica com a melhor. Lista vazia significa que a frase não
    está literalmente no PDF.
    """
    alvo = _norm_busca(trecho)
    palavras = alvo.split()
    if len(palavras) < 4:
        return []
    exatas = [i + 1 for i, t in enumerate(paginas_norm) if t and alvo in t]
    if exatas:
        return exatas[:limite]
    # Janela menor em trecho curto. Com janela fixa de seis, uma citação de
    # quatro palavras vira uma janela só, que precisa bater inteira: "preservar
    # sua riqueza ambiental" não achava a página onde está escrito "preserve
    # sua riqueza ambiental", porque o modelo trocou a conjugação do verbo.
    # Com janelas de três, "sua riqueza ambiental" casa e a página aparece.
    _jan = min(_JANELA_TRECHO, max(3, len(palavras) - 1))
    janelas = [" ".join(palavras[i:i + _jan])
               for i in range(max(1, len(palavras) - _jan + 1))]
    escores = [(sum(1 for j in janelas if j in t) / len(janelas)) if t else 0.0
               for t in paginas_norm]
    melhor = max(escores) if escores else 0.0
    if melhor < _MIN_ESCORE:
        return []
    return [i + 1 for i, s in enumerate(escores) if s >= melhor * 0.9][:limite]


def extrair_texto_url(url: str, usar_ocr: bool = True) -> str:
    """Baixa o PDF do link (coluna LINK_PLANO) e extrai o texto, na hora da análise."""
    return extrair_texto_bytes(baixar_plano(url), usar_ocr=usar_ocr)


def extrair_plano_diagnostico(url: str, usar_ocr: bool = True) -> dict:
    """Igual a extrair_texto_url, mas devolve também POR QUE deu no que deu.

    A página usa isso para dizer na tela se o plano ficou de fora porque o link
    caiu, porque não é PDF ou porque é escaneado e falta OCR no ambiente.
    """
    saida = {"texto": "", "paginas": 0, "chars": 0, "ocr_usado": False,
             "status": "ok", "detalhe": ""}
    try:
        dados = baixar_plano(url)
        doc = _abrir_pdf(dados)
    except PlanoIndisponivel as e:
        saida["status"] = "indisponivel"
        saida["detalhe"] = str(e)
        return saida
    except PlanoIlegivel as e:
        saida["status"] = "ilegivel"
        saida["detalhe"] = str(e)
        return saida

    try:
        saida["paginas"] = doc.page_count
        sem_ocr = _extrair_doc(doc, usar_ocr=False, min_chars=200)
        por_pagina = len(sem_ocr.strip()) / max(1, doc.page_count)
        if por_pagina >= 200:
            saida["texto"] = sem_ocr
        elif not usar_ocr:
            saida["status"] = "escaneado"
            saida["detalhe"] = "PDF sem camada de texto; OCR desligado nesta chamada"
            saida["texto"] = sem_ocr
        elif not tesseract_disponivel():
            saida["status"] = "escaneado_sem_ocr"
            saida["detalhe"] = ("PDF sem camada de texto e o tesseract não está "
                                "instalado neste ambiente (falta packages.txt no deploy)")
            saida["texto"] = sem_ocr
        else:
            saida["texto"] = _extrair_doc(doc, usar_ocr=True, min_chars=200)
            saida["ocr_usado"] = True
            if len(saida["texto"].strip()) / max(1, doc.page_count) < 50:
                saida["status"] = "vazio"
                saida["detalhe"] = "nem a camada de texto nem o OCR devolveram conteúdo"
    finally:
        doc.close()

    saida["chars"] = len(saida["texto"].strip())
    return saida


def _limpa(t: str, n: int = 400) -> str:
    """Normaliza espaços e corta em `n` caracteres, sem partir palavra.

    O corte antigo era em 240 e caía no meio da palavra: 143 dos 346 trechos
    gravados em 03/08/2026 terminavam assim ('Reduzir a distorção idade-s…').
    Agora o corte recua até o fim da última frase, ou até o último espaço.
    """
    t = re.sub(r"\s+", " ", t or "").strip()
    if len(t) <= n:
        return t
    pedaco = t[:n]
    fim_frase = max(pedaco.rfind(". "), pedaco.rfind("! "), pedaco.rfind("? "))
    if fim_frase >= n * 0.6:
        return pedaco[:fim_frase + 1]
    fim_palavra = pedaco.rfind(" ")
    return (pedaco[:fim_palavra] if fim_palavra > 0 else pedaco).rstrip(" ,;") + "…"


# classificação por maturidade (Gemini)

class RespostaIlegivel(RuntimeError):
    """O modelo respondeu num formato que não conseguimos mapear em temas."""


def _chave(nome: str) -> str:
    """Chave tolerante: ignora caixa, acento e espaço extra.

    O modelo às vezes devolve 'EDUCAÇÃO PROFISSIONAL' ou 'Educacao Profissional'
    no lugar do nome exato do tema.
    """
    t = unicodedata.normalize("NFD", str(nome))
    t = "".join(c for c in t if unicodedata.category(c) != "Mn")
    return " ".join(t.lower().split())


def _achatar_por_tema(data: dict, temas: dict) -> dict:
    """Mapa {chave do tema: item} a partir da resposta do modelo.

    O prompt apresenta os temas agrupados por eixo, e o modelo às vezes responde
    na mesma forma, aninhado ({'Educação': {'Alfabetização': {...}}}), em vez de
    um objeto plano por tema. Antes disso derrubar a leitura em silêncio, aqui
    aceitamos as duas formas.
    """
    esperados = {_chave(t) for t in temas}
    achatado = {}

    ALIAS_NIVEL = ("nivel", "nível", "classificacao", "classificação")

    def tem_nivel(v: dict) -> bool:
        return any(k in v for k in ALIAS_NIVEL)

    def normaliza(v: dict) -> dict:
        """Garante a chave 'nivel'. Aceitar 'nível' aqui e não normalizar faria o
        item ser reconhecido e, logo depois, lido como 'Não menciona'."""
        if "nivel" not in v:
            for k in ALIAS_NIVEL:
                if k in v:
                    v["nivel"] = v[k]
                    break
        return v

    def visita(no):
        # Lista de objetos ([{'tema': 'Alfabetização', 'nivel': ...}, ...]) é a
        # terceira forma que o modelo já devolveu. Sem tratar aqui, a resposta
        # inteira era descartada como ilegível, que foi o que aconteceu com o
        # plano do Lucien Rezende em 03/08/2026.
        if isinstance(no, list):
            for item in no:
                visita(item)
            return
        if not isinstance(no, dict):
            return
        # Item que carrega o nome do tema num campo, em vez de ser a chave.
        for campo in ("tema", "nome", "titulo", "título"):
            rotulo = no.get(campo)
            if isinstance(rotulo, str) and _chave(rotulo) in esperados and tem_nivel(no):
                achatado.setdefault(_chave(rotulo), normaliza(no))
                return
        for k, v in no.items():
            if isinstance(v, list):
                visita(v)
                continue
            if not isinstance(v, dict):
                continue
            ck = _chave(k)
            if ck in esperados and tem_nivel(v):
                achatado.setdefault(ck, normaliza(v))
            else:
                visita(v)          # provavelmente um eixo: desce mais um nível

    visita(data)
    return achatado


def _lista_por_eixo(temas: dict) -> str:
    """Lista os temas agrupados pelo eixo a que pertencem. O agrupamento ajuda o
    modelo a não confundir tema de eixos vizinhos (ex.: 'Tecnologia na Educação'
    com 'Governo Digital')."""
    por_eixo = {}
    for tema, desc in temas.items():
        por_eixo.setdefault(EIXO_DO_TEMA.get(tema, "Outros"), []).append((tema, desc))
    blocos = []
    for eixo, itens in por_eixo.items():
        linhas = "\n".join(f"  - {t}: {d}" for t, d in itens)
        blocos.append(f"{eixo}:\n{linhas}")
    return "\n\n".join(blocos)


def classificar_plano(texto: str, temas: dict = TEMAS) -> dict:
    """Uma chamada ao Gemini por plano: classifica todos os temas de uma vez.
    Retorna {tema: {nivel, score, trecho, responsavel, prazo, publico_alvo, programa_nome}}."""
    from google.genai import types

    lista = _lista_por_eixo(temas)
    prompt = (
        "Você é analista sênior de políticas públicas. Leia o plano de governo abaixo "
        "e classifique, para CADA tema listado, o nível de maturidade da proposta.\n\n"
        "ESCALA (use exatamente esses nomes):\n"
        "  Não menciona       — o tema não é mencionado no plano\n"
        "  Menciona vagamente — é citado de forma vaga, sem ação ou medida definida\n"
        "                       Ex.: 'valorizaremos a educação profissional', "
        "'a segurança será prioridade'\n"
        "  Propõe ação        — há ação ou medida concreta, mas sem alvo mensurável\n"
        "                       Ex.: 'criaremos centros de educação profissional em todos os estados'\n"
        "  Define meta        — há alvo mensurável: número, percentual, prazo ou combinação\n"
        "                       Ex.: 'ampliar para 1 milhão de vagas em EPT até 2027'\n\n"
        "ATENÇÃO: promessas de investimento genéricas ('investiremos R$ X em educação', "
        "'mais recursos para a saúde') SEM vínculo com um programa específico do tema "
        "NÃO contam como Define meta.\n\n"
        "ATENÇÃO 2: classifique cada tema pelo que o plano diz DAQUELE tema. Um plano "
        "forte em segurança não puxa para cima os temas de educação.\n\n"
        "REGRA: se o tema aparecer em múltiplos trechos, use o de maior maturidade.\n\n"
        f"Temas a classificar, agrupados por eixo:\n{lista}\n\n"
        "Responda APENAS um objeto JSON válido, sem texto extra. "
        "Estrutura: uma chave por tema (nome exato), valor = objeto com:\n"
        "  'nivel': um dos quatro valores da escala\n"
        "  'trecho': frase literal do plano que justifica a classificação (vazio se Não menciona)\n"
        "  'responsavel': quem implementa — ex: 'governo estadual', 'municípios', "
        "'parceria público-privada', 'governo federal' (vazio se Não menciona)\n"
        "  'prazo': horizonte temporal mencionado — ex: 'até 2027', 'primeiro ano de mandato' "
        "(vazio se não houver)\n"
        "  'publico_alvo': a quem se destina — ex: 'estudantes do ensino médio', "
        "'crianças de 0 a 5 anos' (vazio se Não menciona)\n"
        "  'programa_nome': nome de programa específico citado — ex: 'PNAIC', 'PRONATEC' "
        "(vazio se não houver)\n\n"
        f"PLANO DE GOVERNO:\n{texto[:80000]}"
    )
    resp = _gemini_client().models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(response_mime_type="application/json"),
    )
    raw = (getattr(resp, "text", "") or "").strip()
    data = _achatar_por_tema(json.loads(raw), temas)
    out = {}
    achou = 0
    for tema in temas:
        item = data.get(_chave(tema)) or {}
        if item:
            achou += 1
        nivel = item.get("nivel", "Não menciona")
        if nivel not in NIVEIS:
            nivel = "Não menciona"
        out[tema] = {
            "nivel":         nivel,
            "score":         NIVEIS.index(nivel),
            "trecho":        _limpa(item.get("trecho", "")),
            "responsavel":   _limpa(item.get("responsavel", ""),   n=120),
            "prazo":         _limpa(item.get("prazo", ""),          n=80),
            "publico_alvo":  _limpa(item.get("publico_alvo", ""),  n=120),
            "programa_nome": _limpa(item.get("programa_nome", ""), n=80),
        }
    if achou == 0:
        # Nenhuma chave bateu: a resposta veio em formato que não sabemos ler.
        # Devolver 26 "Não menciona" aqui seria indistinguível de um plano que
        # realmente não fala de nada, e foi assim que a análise do Marcio Bittar
        # (AC 2022) ficou gravada como vazia. Melhor falhar e tentar de novo.
        # A forma que veio entra na mensagem: sem isso, cada ocorrência exige
        # reproduzir a chamada à mão para descobrir o que o modelo devolveu.
        bruto = json.loads(raw)
        if isinstance(bruto, dict):
            forma = f"objeto com as chaves {list(bruto)[:5]}"
        elif isinstance(bruto, list):
            forma = f"lista de {len(bruto)} itens, primeiro {str(bruto[:1])[:120]}"
        else:
            forma = f"{type(bruto).__name__}"
        raise RespostaIlegivel(
            "O modelo respondeu num formato não reconhecido: nenhum dos "
            f"{len(temas)} temas foi encontrado na resposta. Veio {forma}."
        )
    return out


def avaliar_coerencia(texto: str, temas: dict = TEMAS) -> dict:
    """Avalia se as propostas formam uma estratégia coerente no plano.
    Retorna {"score": 1–5, "justificativa": str}.

    Com mais de um eixo, a pergunta deixa de ser só "a educação se sustenta" e
    passa a incluir a articulação ENTRE eixos, que é onde plano de governo
    costuma se revelar: segurança que conversa com assistência social, educação
    profissional que conversa com geração de emprego.
    """
    from google.genai import types

    lista = _lista_por_eixo(temas)
    prompt = (
        "Você é analista sênior de políticas públicas. Leia o plano de governo abaixo.\n\n"
        f"Os eixos e temas analisados são:\n{lista}\n\n"
        "Avalie se as propostas formam uma ESTRATÉGIA COERENTE e articulada, "
        "ou se são menções isoladas e desconexas.\n\n"
        "Escala:\n"
        "  1 — Apenas menções genéricas, sem proposta concreta em nenhum eixo\n"
        "  2 — Propostas isoladas, sem articulação entre temas nem entre eixos\n"
        "  3 — Alguns temas articulados dentro de um eixo, mas estratégia parcial\n"
        "  4 — Estratégia clara na maioria dos eixos, com alguma articulação entre eles\n"
        "  5 — Estratégia integrada: os eixos se conectam e reforçam entre si\n\n"
        "Um plano que cobre bem UM eixo e ignora os outros não passa de 3.\n\n"
        "Responda APENAS um objeto JSON com:\n"
        "  'score': número inteiro de 1 a 5\n"
        "  'justificativa': 2 a 4 frases factuais que sustentem o score, com no "
        "máximo 500 caracteres no total.\n\n"
        "COMO ESCREVER A JUSTIFICATIVA:\n"
        "- Comece pelo dado, nunca pela palavra 'O plano'. Abra com o nome de um "
        "programa, uma meta ou um eixo concreto do texto.\n"
        "- Não repita as palavras da escala acima ('integrada', 'articulada', "
        "'parcial', 'isoladas', 'coerente'). Descreva o que o plano faz, não em "
        "que degrau ele caiu.\n"
        "- Não use o molde 'o plano faz X, contudo falta Y'. Se um eixo não tem "
        "proposta, diga em frase própria qual eixo e o que falta nele.\n"
        "- Nomeie programas, metas, números e prazos que estão no texto. Uma "
        "justificativa sem nenhum nome próprio de programa ou número não serve.\n"
        "- Nada de juízo de valor. Só o que está escrito no plano.\n\n"
        f"{RESTRICOES_LINGUAGEM}\n\n"
        f"PLANO DE GOVERNO:\n{texto[:60000]}"
    )
    resp = _gemini_client().models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
        config=types.GenerateContentConfig(response_mime_type="application/json"),
    )
    raw = (getattr(resp, "text", "") or "").strip()
    data = json.loads(raw)
    score = int(data.get("score", 1))
    if score not in range(1, 6):
        score = 1
    # 600 e não 400: no limite antigo, 4 das 16 justificativas gravadas em
    # 03/08/2026 terminavam cortadas no meio da palavra.
    return {"score": score, "justificativa": _limpa(data.get("justificativa", ""), n=600)}


def sintetizar_comparacao(candidatos_info: list, tema: str) -> str:
    """Gera narrativa comparativa entre candidatos para um dado tema.
    candidatos_info: [{"nome": str, "partido": str, "nivel": str, "trecho": str}, ...]"""
    if not candidatos_info:
        return ""
    bloco = "\n\n".join(
        f"{i+1}. {c['nome']} ({c['partido']}) — Nível: {c['nivel']}\n"
        f"   Trecho do plano: {c['trecho'] or '(sem trecho)'}"
        for i, c in enumerate(candidatos_info)
    )
    prompt = (
        f"Compare as propostas dos candidatos abaixo sobre o tema '{tema}'.\n\n"
        f"Candidatos:\n{bloco}\n\n"
        "Escreva uma análise comparativa em 3 a 5 parágrafos curtos. Inclua:\n"
        "- O que cada candidato propõe de concreto (ou a ausência de proposta)\n"
        "- Diferenças e semelhanças entre as abordagens\n"
        "- Quem tem a proposta mais estruturada e por quê\n\n"
        "Seja direto e técnico. Use prosa corrida, sem listas ou marcadores. "
        "Não se apresente nem mencione seu papel, vá direto ao conteúdo.\n\n"
        f"{RESTRICOES_LINGUAGEM}"
    )
    resp = _gemini_client().models.generate_content(
        model=GEMINI_MODEL,
        contents=prompt,
    )
    return (getattr(resp, "text", "") or "").strip()


# pipeline principal

def analisar_da_base(df, link_col="LINK_PLANO",
                     id_cols=("NM_URNA_CANDIDATO", "SG_UF", "SG_PARTIDO", "DS_CARGO"),
                     temas: dict = TEMAS) -> pd.DataFrame:
    """Analisa os planos direto da base consolidada: para cada linha com link,
    baixa o PDF, extrai o texto e classifica no Gemini. Retorna a tabela longa."""
    linhas = []
    for _, r in df.iterrows():
        link = r.get(link_col)
        if not isinstance(link, str) or not link:
            continue
        try:
            texto = extrair_texto_url(link)
            classif = classificar_plano(texto, temas)
        except Exception as e:
            print(f"  [erro] {r.get(id_cols[0])}: {e}")
            continue
        meta = {c: r.get(c) for c in id_cols}
        for tema, res in classif.items():
            linhas.append({**meta, "tema": tema, **res})
    return pd.DataFrame(linhas)


def analisar_pasta(pasta: str | Path, metadados_csv: str | Path,
                   temas: dict = TEMAS) -> tuple[pd.DataFrame, pd.DataFrame]:
    """Processa PDFs de uma pasta. Retorna (matriz cand×tema de níveis, tabela longa)."""
    pasta = Path(pasta)
    meta = pd.read_csv(metadados_csv)
    linhas = []
    for _, row in meta.iterrows():
        pdf = pasta / row["arquivo"]
        if not pdf.exists():
            continue
        try:
            texto = extrair_texto(pdf)
            classif = classificar_plano(texto, temas)
        except Exception as e:
            print(f"  [erro] {row['arquivo']}: {e}")
            continue
        for tema, res in classif.items():
            linhas.append({**row.to_dict(), "tema": tema, **res})

    long = pd.DataFrame(linhas)
    long["rotulo"] = long["nome"] + " (" + long["partido"] + "/" + long["uf"] + ")"
    matriz = long.pivot_table(index="rotulo", columns="tema",
                              values="score", aggfunc="first")
    matriz = matriz.reindex(columns=list(temas.keys()))
    return matriz, long
