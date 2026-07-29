"""
Pesquisas eleitorais: alerta diário + extração de voto por segmento, rejeição
e aprovação (Gemini lê o PDF do relatório).

A extração de TOPLINE (voto estimulado geral) foi aposentada em 16/07/2026 -
mora em deprecated/relatorios_extracao_topline_aposentado.py, sem workflow chamando.
Cadastro de topline agora é 100% manual, via Polling Manual
(gerador-de-envios). Esta extração de segmentos/rejeição/aprovação continua
automática, sem mudança nenhuma de comportamento.

Uso:
  python -m relatorios.relatorios_extracao_segmentos alerta      # email com as pesquisas que divulgam hoje (PesqEle)
  python -m relatorios.relatorios_extracao_segmentos extrair     # extrai voto por segmento, rejeição e aprovação dos relatórios
  python -m relatorios.relatorios_extracao_segmentos rebuild_bi  # reconstrói resultados_bi nas planilhas PollingData
  python -m relatorios.relatorios_extracao_segmentos canonico    # regenera canonico.json a partir de T1/T2

Secrets: GOOGLE_CREDENTIALS_JSON, GEMINI_API_KEY, BREVO_API_KEY, EMAIL,
DESTINATARIOS, SPREADSHEET_ID (PesqEle), SPREADSHEET_ID_RELATORIOS.
"""

import json
import os
import re
import sys
from datetime import datetime

import gspread

from compartilhado.relatorios_sheets_utils import (
    ALIASES_RELATORIOS, BRT, CABECALHO_RELATORIOS, CARGOS_MONITORADOS, CARGO_ROTULO,
    REGISTRO_TSE_RE, RELATORIOS_COLUNAS, REL_COL, REL_KEY, STATUS_TOPLINE_MANUAL,
    _aba, _append_rows_compacto, _ativar_checkbox, _ativar_dropdown, _baixar_pdf,
    _blocos_ativos_cargo, _blocos_pdf, _cargo_norm, _cargos_monitorados, _chave_fila,
    _colorir_cabecalhos_relatorios, _colorir_por_valor, _creds_info, _custo_estimado,
    _data_iso, _encolher_linhas_vazias, _extrair_json_objeto, _garantir_coluna,
    _garantir_coluna_relatorios, _int0, _limpar_status_extracao, _n_paginas_pdf,
    _normalizar_booleanos_coluna, _registrar_uso, _rel_display, _rel_key, _rel_record,
    _rel_records, _rel_row, _remover_colunas_sobrando, _resetar_validacoes_relatorios,
    _resumo_uso_tokens, _row_count_atual, _sem_acento, _separar_linhas_multicargo,
    _sheets, _texto_pdf_bytes, _ultima_linha_com_dados, _ultima_linha_com_registro,
    _validar_registro_pdf, _verdadeiro,
)

CABECALHOS = {
    "relatorios": CABECALHO_RELATORIOS,
    "voto_segmento": ["registro", "cargo", "turno", "uf", "instituto", "data_divulgacao",
                      "cenario", "candidato", "tipo_segmento", "segmento", "valor"],
    "rejeicao": ["registro", "cargo", "uf", "instituto", "data_divulgacao",
                 "candidato", "tipo_segmento", "segmento", "valor"],
    "aprovacao": ["registro", "cargo", "uf", "instituto", "data_divulgacao",
                  "alvo", "tipo_avaliacao", "resposta", "tipo_segmento", "segmento", "valor"],
}


PESQELE_ID = os.getenv("SPREADSHEET_ID", "")


PESQELE_ABA = os.getenv("PESQELE_ABA", "Consolidado")


POLLING_MANUAL_URL = os.getenv("POLLING_MANUAL_URL",
                               "https://eixoestrategiapolitica.streamlit.app/Polling_Manual")


GRUPOS = ["Presidente (Nacional)", "Presidente (por UF)", "Governador / Senador"]


def _grupo(p):
    cargos = str(p.get("cargos", "")).lower()
    abr = str(p.get("abrangencia", "")).strip().upper()
    if "presidente" in cargos:
        return GRUPOS[0] if abr == "BRASIL" else GRUPOS[1]
    return GRUPOS[2]


def _tabela(pesquisas):
    linhas = "".join(
        "<tr>"
        f"<td style='padding:6px 10px;border-top:1px solid #e5e7eb'>{p.get('numero_identificacao','')}</td>"
        f"<td style='padding:6px 10px;border-top:1px solid #e5e7eb'>{p.get('empresa_contratada','')}</td>"
        f"<td style='padding:6px 10px;border-top:1px solid #e5e7eb'>{p.get('abrangencia','')}</td>"
        f"<td style='padding:6px 10px;border-top:1px solid #e5e7eb'>{p.get('cargos','')}</td>"
        "</tr>"
        for p in pesquisas
    )
    return (
        "<table style='width:100%;border-collapse:collapse;font-size:14px;margin-bottom:6px'>"
        "<tr style='text-align:left;background:#f3f4f6'>"
        "<th style='padding:6px 10px'>Registro</th><th style='padding:6px 10px'>Instituto</th>"
        "<th style='padding:6px 10px'>Abrangência</th><th style='padding:6px 10px'>Cargos</th></tr>"
        f"{linhas}</table>"
    )


def _html(pesquisas, hoje):
    por_grupo = {g: [] for g in GRUPOS}
    for p in pesquisas:
        por_grupo[_grupo(p)].append(p)
    secoes = "".join(
        f"<h3 style='margin:18px 0 6px 0'>{g} ({len(por_grupo[g])})</h3>{_tabela(por_grupo[g])}"
        for g in GRUPOS if por_grupo[g]
    )
    planilha_url = (f"https://docs.google.com/spreadsheets/d/{RELATORIOS_ID}/edit"
                    if RELATORIOS_ID else "#")
    return f"""
    <html><body style="font-family:Arial,sans-serif;color:#111">
      <h2 style="margin:0 0 6px 0">Pesquisas com divulgação prevista para hoje</h2>
      <div style="color:#374151;margin:0 0 14px 0">{hoje} · {len(pesquisas)} pesquisa(s)</div>
      <div style="background:#eef0f6;border-left:3px solid #192D4E;padding:10px 12px;margin:0 0 16px 0;font-size:13px">
        <strong style="color:#192D4E">Ação do dia</strong>, para cada pesquisa abaixo:
        <ol style="margin:8px 0 0 0;padding-left:18px">
          <li>Confira se a notícia ou o relatório confere com o que a linha traz:
            registro TSE, instituto, data de campo e cargos.</li>
          <li>Registre a pesquisa no
            <a href="{POLLING_MANUAL_URL}" style="color:#192D4E">Polling Manual</a>,
            colando a notícia ou o texto do relatório. Ele grava nas matrizes
            <b>T1</b> e <b>T2</b> e marca a linha da fila (aba <b>relatorios</b> da
            <a href="{planilha_url}" style="color:#192D4E">Voto por Segmento</a>)
            como concluída.</li>
          <li>Confira na planilha se ficou tudo certo: cenários, candidatos,
            partidos, percentuais, turno e UF.</li>
        </ol>
      </div>
      {secoes}
    </body></html>
    """


def _enviar(subject, html_body):
    import re
    api_key, sender = os.getenv("BREVO_API_KEY"), os.getenv("EMAIL")
    bruto = re.split(r"[,;\s]+", os.getenv("DESTINATARIOS", ""))
    dests = [e.strip(" <>") for e in bruto if "@" in e]
    if not (api_key and sender and dests):
        print("Config de email incompleta ou sem destinatário válido; pulando envio.")
        return
    from brevo_python import ApiClient, Configuration
    from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
    from brevo_python.models.send_smtp_email import SendSmtpEmail
    cfg = Configuration()
    cfg.api_key["api-key"] = api_key
    api = TransactionalEmailsApi(ApiClient(configuration=cfg))
    for dest in dests:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}], sender={"email": sender},
                subject=subject, html_content=html_body))
            print(f"enviado para {dest}")
        except Exception as e:
            print(f"falha para {dest}: {e}")


def _preencher_fila(pesquisas):
    """Cria na aba 'relatorios' uma linha por registro+cargo monitorado."""
    if not RELATORIOS_ID:
        print("SPREADSHEET_ID_RELATORIOS não definido; pulando preenchimento da fila.")
        return
    fila = _aba(_sheets().open_by_key(RELATORIOS_ID), "relatorios", CABECALHOS)
    header = fila.row_values(1)
    existentes = {
        _chave_fila(r.get("registro"), r.get("cargo"), r.get("uf"))
        for r in _rel_records(fila)
    }
    novas = []
    for p in pesquisas:
        reg = str(p.get("numero_identificacao", "")).strip()
        if not reg:
            continue
        for cargo in _cargos_monitorados(p.get("cargos", "")):
            chave = _chave_fila(reg, cargo, p.get("abrangencia", ""))
            if chave in existentes:
                continue
            valores = {
                "registro": reg,
                "cargo": cargo,
                "uf": p.get("abrangencia", ""),
                "instituto": p.get("empresa_contratada", ""),
                "data_divulgacao": str(p.get("data_divulgacao", ""))[:10],
            }
            novas.append(_rel_row(valores, header))
            existentes.add(chave)
    if novas:
        _append_rows_compacto(fila, novas)
        _resetar_validacoes_relatorios(fila, header, _ultima_linha_com_registro(fila))
    print(f"{len(novas)} linha(s) adicionada(s) à fila de relatórios.")


def cmd_alerta():
    if not PESQELE_ID:
        raise RuntimeError("Defina SPREADSHEET_ID (planilha do PesqEle).")
    ws = _sheets().open_by_key(PESQELE_ID).worksheet(PESQELE_ABA)
    hoje = datetime.now(BRT).strftime("%Y-%m-%d")
    pesquisas = [r for r in ws.get_all_records()
                 if str(r.get("data_divulgacao", ""))[:10] == hoje]
    print(f"{len(pesquisas)} pesquisa(s) com divulgação hoje ({hoje})")
    if pesquisas:
        # fila primeiro: mesmo que o e-mail falhe, as linhas do dia ficam criadas
        _preencher_fila(pesquisas)
        _enviar(f"Pesquisas eleitorais previstas para hoje ({len(pesquisas)})",
                _html(pesquisas, hoje))


RELATORIOS_ID = os.getenv("SPREADSHEET_ID_RELATORIOS", "")


GEMINI_MODEL = os.getenv("GEMINI_MODEL", "gemini-2.5-flash")


PROMPT = (
    "Você é um analista de dados de pesquisas eleitorais da Eixo. Você recebe o PDF do "
    "relatório completo de uma pesquisa e extrai os cruzamentos por segmento.\n\n"
    "O relatório PODE conter mais de um cargo (presidente, governador, senador) no mesmo "
    "documento, mesmo que só um esteja no título. Quando houver FOCO DE CARGO abaixo, "
    "extraia APENAS esse cargo. Sem FOCO DE CARGO, extraia todos os cargos que aparecerem, "
    "cada linha com o cargo correto.\n\n"
    "Extraia TRÊS listas, em JSON:\n\n"
    "1) voto_segmento: para CADA cenário de voto estimulado e CADA candidato, o percentual "
    "de voto quebrado por segmento demográfico. "
    'Cada item: {"cargo": "presidente|governador|senador", "turno": "t1|t2", "cenario": "...", '
    '"candidato": "Nome (PARTIDO)", "tipo_segmento": "...", "segmento": "...", "valor": número}. '
    "'cargo' é a disputa daquele cenário específico; 'turno' é t2 quando for simulação de "
    "segundo turno (confronto direto), senão t1.\n\n"
    "2) rejeicao: para CADA candidato, o percentual de rejeição quebrado por segmento. "
    'Cada item: {"cargo": "presidente|governador|senador", "candidato": "Nome (PARTIDO)", '
    '"tipo_segmento": "...", "segmento": "...", "valor": número}. '
    "'cargo' é a disputa a que a rejeição se refere.\n\n"
    "3) aprovacao: APENAS aprovação/desaprovação ou avaliação do DESEMPENHO de quem está no "
    "cargo (presidente ou governador em exercício), quebrada por segmento. "
    'Cada item: {"alvo": "...", "tipo_avaliacao": "aprova_desaprova|nota_gestao", "resposta": "...", "tipo_segmento": "...", "segmento": "...", "valor": número}.\n\n'
    "Regras:\n"
    "1) Preserve os números EXATAMENTE como no relatório. Não arredonde nem recalcule.\n"
    "1b) Percentuais com vírgula decimal devem virar número decimal com ponto: 26,1% -> 26.1; "
    "50,3% -> 50.3; 64,01% -> 64.01. NUNCA remova a vírgula transformando 26,1 em 261 "
    "ou 50,3 em 503. Todo 'valor' individual deve ficar entre 0 e 100.\n"
    "2) Não invente. Se um cruzamento não existir no relatório, omita.\n"
    "3) Use os rótulos de segmento como aparecem (ex: Masculino, Feminino, 16 a 24 anos, "
    "25 a 34 anos, Fundamental, Médio, Superior, Até 2 SM, Mais de 5 a 10 SM).\n"
    "4) 'tipo_segmento' classifica o segmento em uma destas categorias: genero, idade, "
    "escolaridade, renda, regiao, religiao, raca. Use exatamente esses rótulos minúsculos. "
    "Se não encaixar em nenhuma (ex.: ocupação, classe social, PEA/não PEA), use 'outro'. "
    "Para o total geral (sem recorte), use tipo_segmento='geral' e segmento='Total'.\n"
    "4b) REGRA DO TOTAL GERAL, diferente em cada lista: em voto_segmento, NÃO inclua o total "
    "geral/sem recorte (a intenção de voto geral já vai pro topline por outro fluxo; repetir "
    "aqui duplica o dado) - se o item de voto não tiver recorte demográfico, OMITA-o. Já em "
    "rejeicao e aprovacao é o CONTRÁRIO: o total geral (tipo_segmento='geral', segmento='Total') "
    "é o número principal e DEVE SEMPRE ser incluído quando o relatório o trouxer, além das "
    "quebras demográficas - rejeição e aprovação não têm outro fluxo, se faltar o geral aqui "
    "o dado se perde.\n"
    "5) 'valor' é número, sem o símbolo de %.\n"
    "6) Em 'cenario', use 'Cenário N' para votação estimulada de 1º turno. A única votação "
    "estimulada de um cargo/turno é 'Cenário 1' (NUNCA apenas 'Estimulada'). 'Estimulada 1', "
    "'Estimulada - Cenário 1' e 'Cenário 01' também viram 'Cenário 1'. Em confronto direto, "
    "use os dois nomes completos com partido (ex.: 'Lula (PT) x Flávio Bolsonaro (PL)'). "
    "NUNCA repita o mesmo rótulo para conjuntos de candidatos diferentes.\n"
    "7) Em 'candidato', use SEMPRE o formato 'Nome (SIGLA)'. Se o candidato constar na lista "
    "canônica fornecida abaixo, use EXATAMENTE o nome e a sigla de lá. A sigla do partido é "
    "sempre curta e em caixa alta (PT, PL, MDB, REP, UNIAO...), nunca por extenso.\n"
    "7b) Em voto_segmento, consolide as respostas inválidas (branco, nulo, não sabe, não "
    "respondeu, indeciso, nenhum) em um único candidato='Não válido' por cenário e segmento, "
    "somando os valores. Em rejeicao, mantenha as categorias de resposta como no relatório.\n"
    "7c) Quando uma tabela tiver colunas 'Porcentual' e 'Porcentagem válida', escolha UMA "
    "base só. Se você incluir 'Não válido' no cenário, use a coluna 'Porcentual' para "
    "candidatos e inválidos. Não misture 'Porcentagem válida' dos candidatos com "
    "branco/nulo/NS/NR da coluna 'Porcentual'.\n"
    "7d) Para senador, '1º voto', '2º voto' e 'média do 1º e 2º votos' continuam sendo "
    "turno='t1'. Use turno='t2' somente para confronto direto de segundo turno entre "
    "dois nomes, não para segundo voto de senador.\n"
    "8) Em aprovacao, PADRONIZE o 'alvo' assim: se for avaliação do presidente/governo "
    "federal, use SEMPRE 'Presidente <Nome>' (ex: 'Presidente Lula'), mesmo que o relatório "
    "escreva 'Governo Lula', 'Governo Federal' ou 'gestão do presidente'. Se for governador/"
    "governo estadual, use SEMPRE 'Governador <Nome>'. 'resposta' é a categoria como no "
    "relatório (ex: 'Aprova', 'Desaprova', 'Não sabe', 'Ótimo/Bom', 'Regular', 'Ruim/Péssimo').\n"
    "8b) 'tipo_avaliacao' separa as DUAS perguntas de avaliação, que são diferentes e cada uma "
    "soma ~100% sozinha, NÃO as misture: use 'aprova_desaprova' para a pergunta binária "
    "(respostas Aprova / Desaprova / Não sabe) e 'nota_gestao' para a pergunta de nota/escala "
    "(respostas Ótimo / Bom / Regular / Ruim / Péssimo, ou Ótimo-Bom / Regular / Ruim-Péssimo). "
    "Cada linha de aprovacao deve trazer o 'tipo_avaliacao' correto. Em UMA mesma pergunta, "
    "use uma base só: se Ótimo e Bom (ou Ótima e Boa) aparecem separados, NÃO inclua o subtotal "
    "Ótimo/Bom (ou Ótima/Boa); aplique a mesma regra a Ruim/Péssimo. Só use a categoria "
    "combinada quando as categorias individuais não estiverem no gráfico.\n"
    "9) NÃO inclua em aprovacao perguntas hipotéticas ou de intenção (ex: 'gostaria que se "
    "reelegesse', 'a reeleição de X', 'a eleição de Y', desejo de candidatura). aprovacao é só "
    "avaliação do trabalho de quem já governa.\n"
    "9b) Quando a fonte for matéria de portal de notícia (não PDF de instituto), ela pode "
    "trazer BARRA LATERAL ou 'leia também'/'últimas notícias' com manchetes de OUTRAS "
    "pesquisas, de OUTROS institutos, sobre o mesmo político (ex.: manchete solta tipo 'BTG/"
    "Nexus: 41% avaliam governo Lula como ruim ou péssimo; 35% consideram ótimo ou bom' ao "
    "lado do texto da pesquisa que você está processando). NÃO extraia números dessas "
    "manchetes de matéria relacionada - eles não são desta pesquisa (registro/instituto "
    "diferente) e a manchete nunca traz o cruzamento completo (fica faltando o NS/NR, a soma "
    "não bate). Só extraia números que o TEXTO PRINCIPAL da matéria atribui explicitamente à "
    "pesquisa/instituto do registro em foco.\n"
    "10) voto_segmento é a votação por segmento DEMOGRÁFICO. O campo 'segmento' deve ser uma "
    "categoria curta de gênero (Masculino/Feminino), idade (16 a 24 anos...), raça/cor "
    "(Branca, Preta, Parda, Indígena), religião (Católicos, Evangélicos...), região, "
    "escolaridade, renda, ou atividade/PEA. NUNCA use o TEXTO de uma pergunta como 'segmento' "
    "(ex: 'Nos últimos 10 dias participou de celebração religiosa? Sim' NÃO é segmento). "
    "Se o recorte não for demográfico, não inclua em voto_segmento.\n"
    "10b) Em voto_segmento, quebre por segmento APENAS a votação principal estimulada de 1º "
    "turno. NÃO quebre por segmento as simulações de 2º turno nem perguntas filtro/arrasto.\n"
    "10c) PERGUNTA FILTRO NUNCA entra em voto_segmento: a pergunta 'O candidato que você "
    "votaria é um desses nomes ou outro candidato?' (assinatura: respostas com 'Outro "
    "candidato' e alto percentual de 'Ausente'/sem resposta) NÃO é intenção de voto - é só um "
    "filtro que antecede a pergunta formal. CUIDADO: o cruzamento demográfico dela vem em "
    "tabela IGUAL à da pergunta formal e costuma aparecer ANTES no relatório; não a confunda "
    "com a estimulada real nem a rotule de 'Estimulada'. Quebre por segmento a pergunta formal "
    "('Se a eleição fosse hoje e esses fossem os candidatos...'), que vem logo depois.\n"
    "11) DADO AUSENTE: se um valor não estiver no material (cruzamento que não cabe no PDF, "
    "célula em branco, cargo sem aquela quebra), OMITA o item. NUNCA grave 0 para dado ausente "
    "- 0 é um número real (candidato com zero voto), não use 0 como 'não encontrei'.\n\n"
    "Responda SOMENTE o JSON, sem texto extra e sem markdown:\n"
    '{"voto_segmento": [...], "rejeicao": [...], "aprovacao": [...]}'
)


def _normalizar_percentual_extraido(valor):
    """Percentual individual do Gemini, tolerando vírgula decimal perdida.

    Exemplos comuns do erro: 26,1 -> 261; 50,3 -> 503; 64,01 -> 6401.
    Só corrige quando o valor veio como inteiro acima de 100; número decimal
    acima de 100 continua inválido para não esconder mistura de bases.
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


def _normalizar_percentuais_lista(itens, contexto):
    erros = []
    for idx, item in enumerate(itens, start=1):
        valor = _normalizar_percentual_extraido(item.get("valor"))
        if valor is None:
            bruto = item.get("valor", "")
            alvo = item.get("candidato") or item.get("alvo") or item.get("resposta") or "item"
            erros.append(f"{contexto} #{idx} {alvo}: valor inválido ({bruto})")
            continue
        item["valor"] = valor
    return erros


def _avisos_soma_voto(itens, cargo):
    """Aviso (não bloqueia) quando os candidatos de um mesmo cenário+segmento somam
    longe de 100% (ou ~200% no senador de 2 vagas). Pega candidato faltando, zero
    indevido, base errada. Valores já normalizados (float)."""
    from collections import defaultdict
    grupos = defaultdict(list)
    for v in itens:
        try:
            val = float(v.get("valor"))
        except (TypeError, ValueError):
            continue
        chave = (str(v.get("cenario", "")), str(v.get("tipo_segmento", "")), str(v.get("segmento", "")))
        grupos[chave].append(val)
    eh_senador = "senador" in str(cargo).lower()
    avisos = []
    for (cen, _tseg, seg), vals in grupos.items():
        if len(vals) < 2:   # 1 valor só não dá pra checar soma
            continue
        s = sum(vals)
        if 90 <= s <= 110:
            continue
        if eh_senador and 150 <= s <= 210:   # 2 vagas: soma ~200 é legítima
            continue
        rotulo = seg or cen or "Total"
        avisos.append(f"soma {s:.0f}% em '{rotulo[:24]}'")
    return avisos[:4]


def _avisos_soma_aprovacao(itens):
    """Aviso (não bloqueia) quando as respostas de uma MESMA pergunta de avaliação
    (mesmo alvo + tipo_avaliacao + segmento) somam longe de 100%. Pega pergunta
    incompleta (faltou uma resposta). Valores já normalizados (float)."""
    from collections import defaultdict
    grupos = defaultdict(list)
    for v in itens:
        try:
            val = float(v.get("valor"))
        except (TypeError, ValueError):
            continue
        chave = (str(v.get("alvo", "")), str(v.get("tipo_avaliacao", "")),
                 str(v.get("tipo_segmento", "")), str(v.get("segmento", "")))
        grupos[chave].append(val)
    avisos = []
    for (alvo, _tipo, _tseg, seg), vals in grupos.items():
        if len(vals) < 2:
            continue
        s = sum(vals)
        if 90 <= s <= 110:
            continue
        avisos.append(f"aprovação {alvo[:14]} soma {s:.0f}% em '{(seg or 'Total')[:16]}'")
    return avisos[:3]


def _turno_segmento(item):
    turno = str(item.get("turno", "t1") or "t1").strip().lower()
    cargo = _cargo_norm(item.get("cargo", ""))
    cenario = _sem_acento(item.get("cenario", "")).lower()
    if cargo == "senador" and turno == "t2":
        fala_de_voto_senado = any(t in cenario for t in (
            "1o voto", "1 voto", "primeiro voto", "2o voto", "2 voto",
            "segundo voto", "media do 1", "media do primeiro", "media do 2",
        ))
        confronto = re.search(r"\b(x|versus|contra)\b", cenario) is not None
        if fala_de_voto_senado and not confronto:
            return "t1"
    return "t2" if turno == "t2" else "t1"


def _padronizar_cenario(cenario):
    """Converte rótulos de voto estimulado para o único formato da planilha.

    O Gemini pode variar entre 'Cenário 01', 'Estimulada 1' e 'Estimulada -
    Cenário 1'. Uma estimulada sem número é, por definição, a única/primeira
    medição do cargo e turno, então também deve ser Cenário 1. Confrontos
    conhecidos também ganham nomes completos e partido.
    """
    texto = str(cenario or "").strip()
    normalizado = _sem_acento(texto).lower()
    # Com ou sem os partidos no PDF, os confrontos conhecidos usam exatamente o
    # mesmo rótulo. Isso evita que uma mesma disputa apareça misturada entre
    # "Lula x Michelle" e "Lula (PT) x Michelle Bolsonaro (PL)".
    confronto = re.sub(r"\([^)]*\)", "", normalizado)
    confronto = " ".join(confronto.split())
    confrontos = {
        "lula x flavio": "Lula (PT) x Flávio Bolsonaro (PL)",
        "lula x flavio bolsonaro": "Lula (PT) x Flávio Bolsonaro (PL)",
        "lula x michelle": "Lula (PT) x Michelle Bolsonaro (PL)",
        "lula x michelle bolsonaro": "Lula (PT) x Michelle Bolsonaro (PL)",
        "sergio moro x sandro alex": "Sergio Moro (UNIAO) x Sandro Alex (PSD)",
    }
    if confronto in confrontos:
        return confrontos[confronto]
    numero = re.search(r"\bcenario\s*0*(\d+)\b", normalizado)
    if not numero:
        numero = re.search(r"\bestimulad[ao]\s*[-:]?\s*0*(\d+)\b", normalizado)
    if numero:
        return f"Cenário {int(numero.group(1))}"
    if normalizado in {"estimulada", "estimulado", "voto estimulado", "votacao estimulada"}:
        return "Cenário 1"
    return texto


def _texto_limpo(valor):
    """Remove somente variações tipográficas, sem alterar o significado."""
    return " ".join(str(valor or "").strip().split())


def _chave_padronizacao(valor):
    texto = _sem_acento(_texto_limpo(valor)).lower()
    return " ".join(re.sub(r"[^a-z0-9]+", " ", texto).split())


def _padronizar_candidato(candidato):
    """Uniformiza grafias já conhecidas de candidatos e rótulos de rejeição."""
    texto = _texto_limpo(candidato)
    rotulos = {
        "cintia dias psol": "Cíntia Dias (PSOL)",
        "nao sabe nao opinou": "Não sabe/Não opinou",
        "nao sabe nao respondeu": "Não sabe/Não respondeu",
        "poderia votar em todos": "Poderia votar em todos",
        "branco nulo": "Branco/Nulo",
        "indeciso n nao resp": "Indeciso/N/Não resp.",
    }
    return rotulos.get(_chave_padronizacao(texto), texto)


def _padronizar_segmento(segmento):
    """Aplica os rótulos demográficos canônicos usados nas abas de saída."""
    texto = _texto_limpo(segmento)
    chave = _chave_padronizacao(texto)
    rotulos = {
        "feminino": "Feminino",
        "masculino": "Masculino",
        "catolico": "Católico",
        "evangelico": "Evangélico",
        "superior completo": "Superior completo",
        "nordeste": "Nordeste",
        "norte": "Norte",
        "sudeste": "Sudeste",
        "sul": "Sul",
        "centro oeste": "Centro-Oeste",
        "ate 02 salarios minimos": "Até 02 salários mínimos",
    }
    if chave in rotulos:
        return rotulos[chave]

    # A mesma faixa etária já apareceu como "16-24", "16 - 24" e
    # "De 16 a 24 anos". Sempre grava na última forma, explícita e legível.
    faixa = re.fullmatch(r"(?:de )?(\d{1,2})(?: a | )(\d{1,2})(?: anos?)?", chave)
    if faixa:
        return f"De {int(faixa.group(1))} a {int(faixa.group(2))} anos"

    idade_unica = re.fullmatch(r"(\d{1,2}) anos?", chave)
    if idade_unica:
        return f"{int(idade_unica.group(1))} anos"
    return texto


def _padronizar_resposta(resposta):
    """Normaliza apenas rótulos equivalentes de aprovação."""
    texto = _texto_limpo(resposta)
    rotulos = {
        "aprova": "Aprova",
        "desaprova": "Desaprova",
        "ns nr": "NS/NR",
        "nao sabe nao opinou": "Não sabe/Não opinou",
    }
    return rotulos.get(_chave_padronizacao(texto), texto)


def _resposta_aprovacao_valida(tipo_avaliacao, resposta):
    """Defesa contra bloco temático/reativo vazando pra aprovacao.

    O prompt (regras 8b/9) só permite dois vocabulários de resposta em
    aprovacao: aprova_desaprova (Aprova/Desaprova/Não sabe) e nota_gestao
    (Ótimo/Bom/Regular/Ruim/Péssimo ou consolidado). Uma pergunta temática que
    não tem onde encaixar (ex.: "já viu notícias negativas/positivas sobre o
    governo? Sim/Não") pode vazar com tipo_avaliacao forçado num desses dois
    mesmo a resposta não batendo o vocabulário - e como o aviso de soma agrupa
    só por (alvo, tipo_avaliacao, tipo_segmento, segmento), sem olhar a
    resposta, essas linhas se somam junto com a pergunta de verdade da mesma
    chave (achado em BR-07181/2026, Quaest: "Sim"/"Não"/"Mais positivas" do
    bloco de notícias vazou como aprova_desaprova em cima da aprovação real,
    dobrando a soma de ~100% pra quase 200% em vários segmentos).
    """
    r = _sem_acento(resposta).strip().lower()
    if any(t in r for t in ("nao sabe", "ns/nr", "ns nr", "nao opinou", "nao respond")):
        return True
    tipo = _sem_acento(tipo_avaliacao).strip().lower()
    if tipo == "aprova_desaprova":
        return "aprov" in r
    if tipo == "nota_gestao":
        return any(t in r for t in ("otim", "bom", "boa", "regular", "ruim", "pessim", "positiv", "negativ"))
    return True   # tipo_avaliacao fora do esperado: a validação de tipo_avaliacao cuida disso


def _filtrar_respostas_aprovacao_invalidas(itens):
    validos, descartados = [], []
    for item in itens:
        if _resposta_aprovacao_valida(item.get("tipo_avaliacao", ""), item.get("resposta", "")):
            validos.append(item)
        else:
            descartados.append(item)
    if descartados:
        exemplos = "; ".join(
            f"{d.get('alvo','')}/{d.get('tipo_avaliacao','')}: '{d.get('resposta','')}'"
            for d in descartados[:4])
        print(f"    [aprovacao] {len(descartados)} linha(s) com resposta fora do vocabulário "
              f"esperado, descartadas: {exemplos}", flush=True)
    return validos


def _remover_subtotais_avaliacao(itens):
    """Remove totais derivados quando as categorias componentes já foram extraídas.

    Alguns gráficos exibem, por exemplo, Ótima + Boa e também o número em
    destaque Ótima/Boa. O destaque não é uma nova resposta; gravá-lo junto às
    componentes duplica a pergunta e faz a soma passar de 100%. Quando o
    gráfico publica SOMENTE a categoria combinada, ela é mantida.
    """
    componentes_por_subtotal = {
        "otima boa": {"otima", "boa"},
        "otimo bom": {"otimo", "bom"},
        "ruim pessima": {"ruim", "pessima"},
        "ruim pessimo": {"ruim", "pessimo"},
    }
    grupos = {}
    for idx, item in enumerate(itens):
        chave = (
            _texto_limpo(item.get("alvo", "")),
            _texto_limpo(item.get("tipo_avaliacao", "")).lower(),
            _texto_limpo(item.get("tipo_segmento", "")).lower(),
            _texto_limpo(item.get("segmento", "")),
        )
        grupos.setdefault(chave, []).append((idx, _chave_padronizacao(item.get("resposta", ""))))

    descartar = set()
    for respostas in grupos.values():
        presentes = {resposta for _, resposta in respostas}
        for idx, resposta in respostas:
            componentes = componentes_por_subtotal.get(resposta)
            if componentes and componentes.issubset(presentes):
                descartar.add(idx)
    return [item for idx, item in enumerate(itens) if idx not in descartar]


def _candidatos_canonicos_mapa(cargo, uf):
    from compartilhado.relatorios_topline_core import _referencia
    cands, _ = _referencia(cargo, uf)
    return {_chave_padronizacao(c): c for c in cands}


def _canonizar_candidato(candidato, cargo, uf):
    """Encaixa o candidato no nome canônico (canonico.json) quando a grafia
    bater de perto, mas não for idêntica.

    Achado com AM-03497/2026 (governador AM): um bloco do PDF leu "Isael
    Munduruku (Rede)" (faltando o 'm') e outro leu certo "Ismael Munduruku
    (Rede)" - como o dedup usa o texto exato, as duas grafias sobreviveram
    como candidatos "diferentes" e a soma do segmento passou de 100% pra
    ~114%. difflib compara em cima do texto já normalizado (sem acento/caixa,
    via _chave_padronizacao) e só aceita bater com um candidato canônico
    quando a razão de similaridade é bem alta (>=0.90), pra não confundir
    dois nomes parecidos que sejam pessoas de verdade (conferido contra o
    próprio canonico.json: pares reais como "Sandro Gama"/"Sergio Gama" (PB)
    e "Helder Barbalho"/"Jader Barbalho" (PA) ficam em 0.86-0.87, abaixo do
    corte; o typo real "Isael"/"Ismael" Munduruku fica em 0.97, acima)."""
    if not candidato or not cargo or not uf:
        return candidato
    mapa = _candidatos_canonicos_mapa(cargo, uf)
    if not mapa:
        return candidato
    chave = _chave_padronizacao(candidato)
    if chave in mapa:
        return mapa[chave]
    import difflib
    proximos = difflib.get_close_matches(chave, mapa.keys(), n=1, cutoff=0.90)
    return mapa[proximos[0]] if proximos else candidato


def _corrigir_cargo_por_candidato(candidato, cargo_atual, uf):
    """Corrige o cargo de um item quando o candidato claramente pertence a
    OUTRO cargo, não ao que o modelo rotulou.

    Achado com AM-03497/2026 (relatório único cobrindo governador + senador
    do Amazonas): candidatos SÓ de governador (Maria do Carmo Seffair,
    Roberto Cidade, David Almeida - nenhum na lista canônica de senador)
    vieram rotulados cargo='senador' em alguns itens de voto_segmento, e
    como o filtro de cargo (_item_casa_cargo) confia nesse rótulo, eles
    entravam junto com os candidatos de senador de verdade, inflando a soma
    de cada segmento demográfico de ~100% pra ~149%. Só corrige quando o
    candidato NÃO bate com a lista canônica do cargo atual mas bate
    exatamente com a de outro cargo monitorado (mesma UF) - candidato fora
    de canonico.json nos dois (ou em nenhum) fica como está, pra não mexer
    em corrida sem lista canônica cadastrada."""
    if not candidato or not cargo_atual or not uf:
        return cargo_atual
    chave = _chave_padronizacao(candidato)
    if chave in _candidatos_canonicos_mapa(cargo_atual, uf):
        return cargo_atual
    for outro in CARGOS_MONITORADOS:
        if outro == cargo_atual:
            continue
        if chave in _candidatos_canonicos_mapa(outro, uf):
            return outro
    return cargo_atual


def _padronizar_dados_extraidos(dados, uf=""):
    """Normaliza antes da deduplicação e de qualquer escrita nas abas."""
    for item in dados.get("voto_segmento", []):
        item["cargo"] = _cargo_norm(item.get("cargo", ""))
        item["turno"] = _turno_segmento(item)
        item["cenario"] = _padronizar_cenario(item.get("cenario", ""))
        item["candidato"] = _canonizar_candidato(
            _padronizar_candidato(item.get("candidato", "")), item["cargo"], uf)
        item["cargo"] = _corrigir_cargo_por_candidato(item["candidato"], item["cargo"], uf)
        item["tipo_segmento"] = _sem_acento(_texto_limpo(item.get("tipo_segmento", ""))).lower()
        item["segmento"] = _padronizar_segmento(item.get("segmento", ""))
    for item in dados.get("rejeicao", []):
        item["cargo"] = _cargo_norm(item.get("cargo", ""))
        item["candidato"] = _canonizar_candidato(
            _padronizar_candidato(item.get("candidato", "")), item["cargo"], uf)
        item["cargo"] = _corrigir_cargo_por_candidato(item["candidato"], item["cargo"], uf)
        item["tipo_segmento"] = _sem_acento(_texto_limpo(item.get("tipo_segmento", ""))).lower()
        item["segmento"] = _padronizar_segmento(item.get("segmento", ""))
    for item in dados.get("aprovacao", []):
        item["tipo_avaliacao"] = _texto_limpo(item.get("tipo_avaliacao", "")).lower()
        item["resposta"] = _padronizar_resposta(item.get("resposta", ""))
        item["tipo_segmento"] = _sem_acento(_texto_limpo(item.get("tipo_segmento", ""))).lower()
        item["segmento"] = _padronizar_segmento(item.get("segmento", ""))
    return dados


USO_TOKENS = {"chamadas": 0, "entrada": 0, "saida": 0, "pensamento": 0}


def _gemini_json(pdf_bytes, extra="", texto_bloco=""):
    from google import genai
    from google.genai import types
    client = genai.Client(api_key=os.environ["GEMINI_API_KEY"])
    config = types.GenerateContentConfig(
        response_mime_type="application/json",
        temperature=0,
        max_output_tokens=65536,
        thinking_config=types.ThinkingConfig(thinking_budget=0),
    )
    prompt = PROMPT + (f"\n\n{extra}" if extra else "")
    texto_bloco = (texto_bloco or "").strip()
    # Sempre manda o PDF junto, não só quando o texto for "insuficiente" por tamanho:
    # relatório com dado só em gráfico tem texto extraído comprido (rodapé legal
    # repetido em toda página) mas sem nenhum candidato/percentual nele, e tabela
    # cruzada (região x segmento) vira uma sequência linear de números no texto, sem
    # estrutura de linha/coluna, que o modelo pode desalinhar. O visual resolve os dois.
    if texto_bloco:
        contents = [
            prompt + "\n\nTEXTO EXTRAÍDO DO PDF/PÁGINA:\n" + texto_bloco +
            "\n\nPDF ANEXO: confira o visual (tabelas e gráficos). O texto extraído pode não "
            "conter os números (dado só em gráfico) ou desalinhar tabela cruzada (linha x "
            "coluna); nesses casos, confie no PDF, não no texto.",
            types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf"),
        ]
    else:
        contents = [prompt, types.Part.from_bytes(data=pdf_bytes, mime_type="application/pdf")]
    resp = client.models.generate_content(model=GEMINI_MODEL, contents=contents, config=config)
    _registrar_uso(resp, USO_TOKENS)
    raw = (getattr(resp, "text", "") or "").strip()
    if not raw:
        fr = "?"
        cands = getattr(resp, "candidates", None)
        if cands:
            fr = getattr(cands[0], "finish_reason", "?")
        raise RuntimeError(f"resposta vazia ou truncada do Gemini (finish_reason={fr})")
    return _extrair_json_objeto(raw)


def _dedup(itens, chaves):
    # Chave em cima de _chave_padronizacao (sem acento/caixa/pontuação), não do
    # texto cru: dois blocos do mesmo PDF podem grafar o mesmo candidato/rótulo
    # com uma diferença de acentuação (ex.: "D'Ávila" x "D'ávila") e o texto
    # exato deixava as duas linhas passarem como "diferentes", dobrando a soma
    # do segmento (achado em RS-02458/2026, Manuela D'Ávila contada 2x).
    vistos, saida = set(), []
    for it in itens:
        k = tuple(_chave_padronizacao(it.get(c, "")) for c in chaves)
        if k not in vistos:
            vistos.add(k)
            saida.append(it)
    return saida


def extrair_do_pdf(pdf_bytes, extra="", uf=""):
    """Extrai bloco a bloco (para caber no limite de tokens e melhorar a
    precisão) e junta os resultados, removendo duplicatas entre blocos."""
    voto, rej, aprov = [], [], []
    for bloco in _blocos_pdf(pdf_bytes):
        texto_bloco = _texto_pdf_bytes(bloco)
        dados = _padronizar_dados_extraidos(_gemini_json(bloco, extra, texto_bloco=texto_bloco), uf=uf)
        voto += dados.get("voto_segmento", [])
        rej += dados.get("rejeicao", [])
        aprov += dados.get("aprovacao", [])
    aprov = _filtrar_respostas_aprovacao_invalidas(aprov)
    aprov = _remover_subtotais_avaliacao(aprov)
    voto = _dedup(voto, ["cargo", "turno", "cenario", "candidato", "tipo_segmento", "segmento"])
    rej = _dedup(rej, ["cargo", "candidato", "tipo_segmento", "segmento"])
    aprov = _dedup(aprov, ["alvo", "tipo_avaliacao", "resposta", "tipo_segmento", "segmento"])
    return {"voto_segmento": voto, "rejeicao": rej, "aprovacao": aprov}


def cmd_extrair():
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    from compartilhado.relatorios_topline_core import _referencia, ficha_instituto, instituto_canonico, sigla_uf

    sh = _sheets().open_by_key(RELATORIOS_ID)
    fila = _aba(sh, "relatorios", CABECALHOS, manutencao=False)
    ws_voto = _aba(sh, "voto_segmento", CABECALHOS)
    ws_rej = _aba(sh, "rejeicao", CABECALHOS)
    ws_aprov = _aba(sh, "aprovacao", CABECALHOS)

    header = fila.row_values(1)
    col_err = _garantir_coluna_relatorios(fila, header, "segmentos_erro")
    col_ten = _garantir_coluna_relatorios(fila, header, "segmentos_tentativas")

    ci_ext = _garantir_coluna_relatorios(fila, header, "segmentos_extraido")
    ci_data = _garantir_coluna_relatorios(fila, header, "segmentos_data_extracao")
    _colorir_por_valor(fila, _rel_display("segmentos_extraido"), header, _ultima_linha_com_registro(fila), {
        "sim": (0.82, 0.93, 0.82),
        "não": (0.96, 0.80, 0.80),
    })

    linhas = _rel_records(fila)
    ok_regs, err_regs = [], []
    agora = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")

    print(f"Extrair segmentos: {len(linhas)} linha(s) na fila.", flush=True)

    def _bloco_canonico(uf, cargo_fila):
        """Lista canônica do cargo da linha; a fila agora é registro+cargo."""
        partes = []
        cargos = _cargos_monitorados(cargo_fila) or [cargo_fila]
        partes.append(f"FOCO DE CARGO: extraia APENAS {', '.join(cargos)}.")
        for cargo in cargos:
            cands, _ = _referencia(cargo, uf)
            if cands:
                partes.append(f"CANDIDATOS CANÔNICOS ({cargo} {sigla_uf(uf)}):\n" + "\n".join(cands))
        return "\n\n".join(partes)

    def _item_casa_cargo(item, cargo_fila):
        cargo_item = _cargo_norm(item.get("cargo", ""))
        return not cargo_item or cargo_item == _cargo_norm(cargo_fila)

    # voto_segmento é só quebra DEMOGRÁFICA. Cenário geral/total, turno, pergunta filtro
    # etc. já vão pro topline; se entrarem aqui também, duplicam o dado e inflam a aba.
    # Filtro no código, não só no prompt, porque o Gemini às vezes desobedece a regra.
    TIPO_SEGMENTO_FORA = {"geral", "total", "cenario", "cenário", "turno", "pergunta", "voto"}
    SEGMENTO_FORA = {"total", "geral"}

    def _e_segmento_demografico(item):
        tipo = _sem_acento(item.get("tipo_segmento", "")).strip().lower()
        seg = _sem_acento(item.get("segmento", "")).strip().lower()
        if tipo in TIPO_SEGMENTO_FORA or seg in SEGMENTO_FORA:
            return False
        return True

    # Chaves já gravadas em cada aba, carregadas UMA vez pra dedup em memória (sem
    # reler a aba a cada gravação). Protege contra duplicata quando um lote foi
    # gravado mas a linha não chegou a ser marcada (ex.: timeout no meio).
    CH_VOTO = ["registro", "cargo", "turno", "cenario", "candidato", "tipo_segmento", "segmento"]
    CH_REJ = ["registro", "cargo", "candidato", "tipo_segmento", "segmento"]
    CH_APROV = ["registro", "alvo", "tipo_avaliacao", "resposta", "tipo_segmento", "segmento"]

    def _carregar_chaves(ws, aba_nome, chaves):
        return {tuple(str(e.get(c, "")).strip() for c in chaves) for e in ws.get_all_records()}

    voto_keys = _carregar_chaves(ws_voto, "voto_segmento", CH_VOTO)
    rej_keys = _carregar_chaves(ws_rej, "rejeicao", CH_REJ)
    aprov_keys = _carregar_chaves(ws_aprov, "aprovacao", CH_APROV)

    # Grava em lotes ao longo da rodada: se o passo estourar o tempo (timeout do
    # Actions), o que já foi extraído fica salvo e a marcação segue junto, então a
    # próxima rodada continua de onde parou em vez de perder tudo e reprocessar o
    # mesmo backlog eternamente.
    LOTE = 5
    voto_buf, rej_buf, aprov_buf, updates = [], [], [], []
    _contador = {"n": 0}

    def _flush(ctx=""):
        if voto_buf:
            _append_rows_compacto(ws_voto, voto_buf)
        if rej_buf:
            _append_rows_compacto(ws_rej, rej_buf)
        if aprov_buf:
            _append_rows_compacto(ws_aprov, aprov_buf)
        if updates:   # marca as linhas extraídas DEPOIS de gravar os dados delas
            fila.update_cells(updates, value_input_option="RAW")
        if voto_buf or rej_buf or aprov_buf or updates:
            print(f"  [gravado{(' ' + ctx) if ctx else ''}] "
                  f"{len(voto_buf)} voto, {len(rej_buf)} rejeição, {len(aprov_buf)} aprovação", flush=True)
        voto_buf.clear(); rej_buf.clear(); aprov_buf.clear(); updates.clear()
        _contador["n"] = 0

    for i, r in enumerate(linhas, start=2):   # linha 1 = cabeçalho
        link = str(r.get("link", "")).strip()
        # segmentos_extraido="não" é uma conclusão válida (relatório só tem
        # RESULTADO GERAL, sem quebra por segmento) — não é falha pra tentar de
        # novo. _verdadeiro() só reconhece "sim"/"true"/etc, então "não" não
        # batia aqui e a linha reprocessava (baixa PDF + chama Gemini) todo run,
        # pra sempre, sem nem cair no limite de 3 tentativas (que também não é
        # incrementado nesse caminho). Qualquer valor não-vazio == já processado.
        if not link or not _verdadeiro(r.get("conferido")) or str(r.get("segmentos_extraido", "")).strip():
            continue
        tentativas = _int0(r.get("segmentos_tentativas"))
        if tentativas >= 3:   # desiste após 3 falhas; limpe a coluna pra tentar de novo
            continue
        try:
            print(f"linha {i} ({r.get('registro')} / {r.get('cargo')}): baixando PDF para segmentos...", flush=True)
            pdf = _baixar_pdf(link)
            erro_registro = _validar_registro_pdf(pdf, r.get("registro", ""))
            if erro_registro:
                raise RuntimeError(erro_registro)
            print(f"linha {i} ({r.get('registro')} / {r.get('cargo')}): PDF baixado ({len(pdf)} bytes), enviando ao Gemini...", flush=True)
            extra = ficha_instituto(r.get("instituto", "")) + _bloco_canonico(r.get("uf"), r.get("cargo"))
            dados = extrair_do_pdf(pdf, extra=extra, uf=r.get("uf", ""))
        except Exception as e:
            msg = str(e)
            updates.extend([gspread.Cell(i, col_err, msg[:300]),
                            gspread.Cell(i, col_ten, tentativas + 1)])
            err_regs.append(f"{r.get('registro')} [{msg[:80]}]")
            print(f"linha {i} ({r.get('registro')}): erro {msg}")
            _contador["n"] += 1
            if _contador["n"] >= LOTE:
                _flush("parcial")
            continue
        registro = r.get("registro", "")
        cargo_fila = r.get("cargo", "")
        uf = sigla_uf(r.get("uf", ""))
        inst = instituto_canonico(r.get("instituto", ""))
        data_div = _data_iso(r.get("data_divulgacao", ""))
        voto_filtrado = [v for v in dados.get("voto_segmento", [])
                         if _item_casa_cargo(v, cargo_fila) and _e_segmento_demografico(v)]
        rej_filtrada = [v for v in dados.get("rejeicao", []) if _item_casa_cargo(v, cargo_fila)]
        aprov_filtrada = dados.get("aprovacao", []) if _cargo_norm(cargo_fila) in ("presidente", "governador") else []
        erros_valor = []
        erros_valor += _normalizar_percentuais_lista(voto_filtrado, "voto_segmento")
        erros_valor += _normalizar_percentuais_lista(rej_filtrada, "rejeicao")
        erros_valor += _normalizar_percentuais_lista(aprov_filtrada, "aprovacao")
        if erros_valor:
            msg = "; ".join(erros_valor[:4])
            updates.extend([gspread.Cell(i, col_err, msg[:300]),
                            gspread.Cell(i, col_ten, tentativas + 1)])
            err_regs.append(f"{registro} {cargo_fila} [{msg[:80]}]")
            print(f"linha {i} ({registro} / {cargo_fila}): valores inválidos: {msg}", flush=True)
            _contador["n"] += 1
            if _contador["n"] >= LOTE:
                _flush("parcial")
            continue
        if not (voto_filtrado or rej_filtrada or aprov_filtrada):
            # Sem dado de segmento pode ser NORMAL: relatório que só traz "RESULTADO GERAL"
            # (sem quebra demográfica) não tem o que extrair aqui, o número geral vai no
            # topline. Só é erro de verdade se o PDF TEM quebra demográfica PRO CARGO DA
            # LINHA e mesmo assim não veio nada. Distingue procurando termo demográfico
            # no texto do PDF, mas só na seção do cargo pedido: um relatório multi-cargo
            # pode ter quebra pra Governador e não pra Senador, e olhar o PDF inteiro
            # faria a checagem "ver" a quebra do Governador e gerar erro falso pro
            # Senador (aconteceu com RO-07927: IHPEC tem quebra só pra Governador).
            from compartilhado.relatorios_topline_core import extrair_texto_pdf_bytes
            if _n_paginas_pdf(pdf) <= 10:
                try:
                    txt_pdf = extrair_texto_pdf_bytes(pdf).lower()
                except Exception:
                    txt_pdf = ""
            else:
                txt_pdf = " ".join(t for _, t in _blocos_ativos_cargo(pdf, _cargo_norm(cargo_fila))).lower()
            # Exige 2+ termos demográticos DIFERENTES: um único hit isolado costuma ser
            # falso positivo (ex.: matéria de portal de notícia com link/manchete não
            # relacionada mencionando "feminino" de passagem, sem tabela de quebra
            # nenhuma no relatório em si).
            termos_batidos = sum(1 for w in (
                "masculino", "feminino", "evangélic", "evangelic", "católic", "catolic",
                "renda familiar", "faixa etária", "faixa etaria", "escolaridade",
                "por região", "por regiao") if w in txt_pdf)
            tem_quebra = termos_batidos >= 2
            if not tem_quebra:
                updates.extend([gspread.Cell(i, col_err, "sem quebra por segmento (só resultado geral; topline cobre)"),
                                gspread.Cell(i, ci_ext, "não"), gspread.Cell(i, ci_data, agora)])
                print(f"linha {i} ({registro} / {cargo_fila}): sem quebra por segmento, marcado como não", flush=True)
            else:
                msg = f"nenhum dado encontrado para o cargo da linha ({cargo_fila})"
                updates.extend([gspread.Cell(i, col_err, msg), gspread.Cell(i, col_ten, tentativas + 1)])
                err_regs.append(f"{registro} {cargo_fila} [{msg}]")
                print(f"linha {i} ({registro} / {cargo_fila}): {msg}", flush=True)
            _contador["n"] += 1
            if _contador["n"] >= LOTE:
                _flush("parcial")
            continue
        for v in voto_filtrado:
            # cargo/turno da disputa daquele cenário (o Gemini identifica); se faltar,
            # cai no texto da fila, pra linha nunca ficar sem referência
            cargo_item = _cargo_norm(v.get("cargo") or cargo_fila)
            turno = _turno_segmento(v)
            cenario = _padronizar_cenario(v.get("cenario", ""))
            chave = (str(registro).strip(), cargo_item,
                     turno, cenario,
                     str(v.get("candidato", "")).strip(), str(v.get("tipo_segmento", "")).strip(),
                     str(v.get("segmento", "")).strip())
            if chave in voto_keys:
                continue
            voto_keys.add(chave)
            voto_buf.append([registro, cargo_item, turno,
                             uf, inst, data_div,
                             cenario, v.get("candidato", ""),
                             v.get("tipo_segmento", ""), v.get("segmento", ""),
                             v.get("valor", "")])
        for v in rej_filtrada:
            cargo_item = _cargo_norm(v.get("cargo") or cargo_fila)
            chave = (str(registro).strip(), cargo_item,
                     str(v.get("candidato", "")).strip(), str(v.get("tipo_segmento", "")).strip(),
                     str(v.get("segmento", "")).strip())
            if chave in rej_keys:
                continue
            rej_keys.add(chave)
            rej_buf.append([registro, cargo_item, uf, inst, data_div,
                            v.get("candidato", ""), v.get("tipo_segmento", ""),
                            v.get("segmento", ""), v.get("valor", "")])
        for v in aprov_filtrada:
            tipo_aval = str(v.get("tipo_avaliacao", "")).strip()
            chave = (str(registro).strip(), str(v.get("alvo", "")).strip(), tipo_aval,
                     str(v.get("resposta", "")).strip(), str(v.get("tipo_segmento", "")).strip(),
                     str(v.get("segmento", "")).strip())
            if chave in aprov_keys:
                continue
            aprov_keys.add(chave)
            aprov_buf.append([registro, _cargo_norm(cargo_fila), uf, inst, data_div,
                              v.get("alvo", ""), tipo_aval, v.get("resposta", ""),
                              v.get("tipo_segmento", ""), v.get("segmento", ""),
                              v.get("valor", "")])
        # aviso de soma (não bloqueia): fica na coluna de erro como alerta pra conferência
        avisos_soma = _avisos_soma_voto(voto_filtrado, cargo_fila) + _avisos_soma_aprovacao(aprov_filtrada)
        nota_soma = ("conferir: " + "; ".join(avisos_soma[:4])) if avisos_soma else ""
        # marca a linha como extraída no MESMO lote em que os dados dela vão (progresso durável)
        updates.extend([gspread.Cell(i, col_err, nota_soma[:300]),
                        gspread.Cell(i, ci_ext, "sim"),
                        gspread.Cell(i, ci_data, agora)])
        ok_regs.append(registro)
        aviso_txt = f"  [!] {nota_soma}" if nota_soma else ""
        print(f"linha {i} ({registro} / {cargo_fila}): "
              f"{len(voto_filtrado)} voto, "
              f"{len(rej_filtrada)} rejeição, "
              f"{len(aprov_filtrada)} aprovação{aviso_txt}", flush=True)
        _contador["n"] += 1
        if _contador["n"] >= LOTE:
            _flush("parcial")

    _flush("fim")

    print("\n───────── resumo ─────────")
    print(f"extraídos: {len(ok_regs)}  {ok_regs}")
    print(f"com erro:  {len(err_regs)}  {err_regs}")
    _resumo_uso_tokens("segmentos", USO_TOKENS)


T1_ID = os.getenv("SPREADSHEET_ID_POLLINGDATA", "")


T2_ID = os.getenv("SPREADSHEET_ID_POLLINGDATA_T2", "")


def cmd_rebuild_bi():
    if not (T1_ID or T2_ID):
        raise RuntimeError("Defina SPREADSHEET_ID_POLLINGDATA e/ou SPREADSHEET_ID_POLLINGDATA_T2.")
    from compartilhado.pollingdata_scraper import gs_client_from_env, reconstruir_resultados_bi

    gc = gs_client_from_env()
    for turno, sheet_id in (("t1", T1_ID), ("t2", T2_ID)):
        if not sheet_id:
            continue
        print(f"{turno}: reconstruindo resultados_bi...")
        reconstruir_resultados_bi(gc, sheet_id)


def cmd_canonico():
    """Regenera o canonico.json a partir das planilhas T1/T2 do PollingData.
    Rodar localmente quando surgirem candidatos/institutos novos; commitar o arquivo."""
    if not (T1_ID or T2_ID):
        raise RuntimeError("Defina SPREADSHEET_ID_POLLINGDATA e/ou SPREADSHEET_ID_POLLINGDATA_T2.")
    from compartilhado.pollingdata_scraper import gs_client_from_env

    gc = gs_client_from_env()
    institutos, pres = set(), set()
    gov, sen = {}, {}
    for sid in (T1_ID, T2_ID):
        if not sid:
            continue
        for r in gc.open_by_key(sid).worksheet("resultados").get_all_records():
            inst = str(r.get("instituto", "")).strip()
            if inst:
                institutos.add(inst)
            if str(r.get("tipo", "")).strip().lower() == "nao_valido":
                continue
            cp = str(r.get("candidato_partido", "")).strip()
            cargo = str(r.get("cargo", "")).strip().lower()
            uf = str(r.get("uf", "")).strip().upper()
            if not cp:
                continue
            if cargo == "presidente":
                pres.add(cp)
            elif cargo == "governador":
                gov.setdefault(uf, set()).add(cp)
            elif cargo == "senador":
                sen.setdefault(uf, set()).add(cp)

    data = {
        "institutos": sorted(institutos),
        "presidente": sorted(pres),
        "governador": {uf: sorted(v) for uf, v in sorted(gov.items())},
        "senador": {uf: sorted(v) for uf, v in sorted(sen.items())},
    }
    caminho = "canonico.json"  # sempre relativo à raiz do repo (cwd do workflow)
    with open(caminho, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=1)
    print(f"canonico.json atualizado: {len(data['institutos'])} institutos, "
          f"{len(data['presidente'])} presidenciáveis, "
          f"{sum(len(v) for v in data['governador'].values())} gov, "
          f"{sum(len(v) for v in data['senador'].values())} sen. Commite o arquivo.")




# Presidente não tem página '/br/t1' seca no flex: o 1º turno é publicado por
# cenário, e o vigente é o de Lula x Flávio sem os demais Bolsonaros.
PRESIDENTE_T1_SLUG = "t1_lula-flavio-sem-bolsonaros"


def _link_pollingdata_url(registro: str, cargo: str) -> str:
    """Página do PollingData por cargo/uf (turno 1), derivada do registro TSE
    (o prefixo do registro é a UF: 'PI-04468/2026' -> pi)."""
    registro = str(registro)
    uf = registro.split("-")[0].strip().lower() if "-" in registro else ""
    cargo = str(cargo).strip().lower()
    if not (uf and cargo):
        return ""
    turno = PRESIDENTE_T1_SLUG if cargo == "presidente" else "t1"
    return f"https://flex.pollingdata.com.br/pdvoto/2026/{cargo}/{uf}/{turno}"


def cmd_sync_cadastrado():
    """Cruza o `relatorios` com as matrizes T1/T2 do PollingData:
    - marca 'Voto cadastrado?'=sim (+ data) para (registro, cargo) já presente
      na aba `pesquisas` de qualquer matriz, venha do Polling Manual ou do Colar;
    - preenche 'Link PollingData' (página do PollingData por cargo/uf) em todas
      as linhas, pra guiar o cadastro sem varrer todas as UFs.
    Não altera linhas 'sim' nem 'N/A'."""
    from datetime import datetime
    from gspread.utils import rowcol_to_a1
    if not RELATORIOS_ID:
        raise RuntimeError("Defina SPREADSHEET_ID_RELATORIOS.")
    if not (T1_ID or T2_ID):
        raise RuntimeError("Defina SPREADSHEET_ID_POLLINGDATA e/ou SPREADSHEET_ID_POLLINGDATA_T2.")
    gc = _sheets()

    presentes = set()
    for sid in (T1_ID, T2_ID):
        if not sid:
            continue
        for r in gc.open_by_key(sid).worksheet("pesquisas").get_all_records():
            pid = str(r.get("poll_id", ""))
            reg = pid.split("|")[3] if pid.count("|") >= 3 else ""
            if reg:
                presentes.add((reg.strip().upper(), str(r.get("cargo", "")).strip().lower()))
    print(f"[sync] {len(presentes)} (registro, cargo) presentes nas matrizes")

    ws = gc.open_by_key(RELATORIOS_ID).worksheet("relatorios")
    vals = ws.get_all_values()
    if len(vals) < 2:
        print("[sync] relatorios sem linhas de dados")
        return
    header = vals[0]

    def _c(chave):
        rotulo = _rel_display(chave)
        return header.index(rotulo) if rotulo in header else None

    c_reg, c_cargo = _c("registro"), _c("cargo")
    c_link, c_voto, c_data = _c("link_pollingdata"), _c("topline_extraido"), _c("topline_data_extracao")
    agora = datetime.now(BRT).strftime("%Y-%m-%d %H:%M")

    reqs, n_voto, n_link = [], 0, 0
    for i, row in enumerate(vals[1:], start=2):
        cel = lambda c: (row[c] if c is not None and c < len(row) else "")
        reg = cel(c_reg).strip()
        cargo = cel(c_cargo).strip().lower()
        if c_link is not None:
            novo = _link_pollingdata_url(reg, cargo)
            if novo and cel(c_link) != novo:
                reqs.append({"range": rowcol_to_a1(i, c_link + 1), "values": [[novo]]})
                n_link += 1
        if c_voto is not None:
            v = cel(c_voto).strip().lower()
            if v not in ("sim", "n/a", "na") and (reg.upper(), cargo) in presentes:
                reqs.append({"range": rowcol_to_a1(i, c_voto + 1), "values": [["sim"]]})
                if c_data is not None:
                    reqs.append({"range": rowcol_to_a1(i, c_data + 1), "values": [[agora]]})
                n_voto += 1
    if reqs:
        ws.batch_update(reqs, value_input_option="USER_ENTERED")
    print(f"[sync] Link atualizado: {n_link} | 'Voto cadastrado?' marcado 'sim': {n_voto}")


if __name__ == "__main__":
    cmd = sys.argv[1] if len(sys.argv) > 1 else ""
    if cmd == "alerta":
        cmd_alerta()
    elif cmd == "extrair":
        cmd_extrair()
    elif cmd == "rebuild_bi":
        cmd_rebuild_bi()
    elif cmd == "canonico":
        cmd_canonico()
    elif cmd == "sync_cadastrado":
        cmd_sync_cadastrado()
    else:
        print("uso: python -m relatorios.relatorios_extracao_segmentos "
              "[alerta|extrair|rebuild_bi|canonico|sync_cadastrado]")
