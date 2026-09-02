"""Grava as notas de cabeçalho das duas abas de Senado do Radar do Congresso 2026.

Roda depois do `indice_senado --apply`:  python -m outros.indice_senado_notas

As notas são endereçadas por NOME de coluna, não por letra. A versão anterior
escrevia em A1, B1, D1 e assim por diante; quando entrou uma coluna nova em D, a
planilha ficou com a nota de cada coluna pendurada na coluna anterior, da
"Alta, média ou baixa" até a última. Aqui a letra sai do cabeçalho lido na hora,
e coluna que não existe naquela aba é ignorada.

Os números que aparecem no texto (quantas linhas, quantos competitivos, quantos
têm declaração nominal) também são lidos da aba, para não envelhecerem sozinhos.
"""
import os

import gspread
from google.oauth2.service_account import Credentials

from outros import regua_senado as regua

ESCOPO = ["https://www.googleapis.com/auth/spreadsheets"]
ID = os.getenv("SPREADSHEET_ID_RECANDIDATURAS", "").strip()
if not ID:
    raise RuntimeError("Variável SPREADSHEET_ID_RECANDIDATURAS não configurada.")

ABA_CANDIDATURAS = "Competitividade Senado (todas as candidaturas)"
ABA_ATUAL = "Competitividade Senado (em exercício)"


def contexto(valores):
    """Números da aba publicada, para o texto das notas não virar literal velho."""
    cab = valores[0]
    linhas = [l for l in valores[1:] if any(c.strip() for c in l)]
    col = {nome: i for i, nome in enumerate(cab)}

    def coluna(nome):
        i = col.get(nome)
        return [l[i].strip() if i is not None and i < len(l) else "" for l in linhas]

    competitivos = sum(1 for v in coluna("É competitivo?") if v == "Sim")
    declarados = sum(1 for v in coluna("Apoio presidencial declarado")
                     if v not in ("", "—", "Não declarado", "Não se aplica"))
    sem_pesquisa = sum(1 for v in coluna("Alta, média ou baixa") if v == "Sem pesquisa recente")
    por_uf = {}
    ufs = coluna("UF")
    for uf, comp in zip(ufs, coluna("É competitivo?")):
        if comp == "Sim":
            por_uf[uf] = por_uf.get(uf, 0) + 1
    return {"linhas": len(linhas), "competitivos": competitivos, "declarados": declarados,
            "sem_pesquisa": sem_pesquisa,
            "min_uf": min(por_uf.values()) if por_uf else 0,
            "max_uf": max(por_uf.values()) if por_uf else 0}


def notas(c):
    """Texto de cada coluna, pelo nome da coluna. Uma ideia por nota, sem rodeio."""
    taxas = ", ".join(f"{n} vale {regua.TAXA[n]}" for n in (7, 6, 5, 4))
    return {
        "Candidato":
            "Todo mundo que registrou candidatura ao Senado no TSE, menos quem renunciou. "
            f"São {c['linhas']} nomes, com ou sem pesquisa. Não é lista de nomes "
            "competitivos.",
        "Partido":
            "Partido do registro no TSE. Se a matriz de pesquisa trouxer outro, vale o do "
            "registro.",
        "Índice de competitividade eleitoral":
            "Entre os candidatos que estavam nesta mesma banda nas pesquisas de 2010 e "
            "2018, essa foi a proporção que terminou entre os 2 mais votados. É o resultado "
            "do grupo, não uma previsão sobre a pessoa. Banda "
            f"{taxas}; bandas 3, 2 e 1 valem {regua.TAXA[1]}. A conta está em "
            "research/calibrar_senado.py.",
        "Alta, média ou baixa":
            "Leitura rápida da coluna ao lado, para comparar com as outras abas. Alta são "
            "as bandas 6 e 7, dentro das duas vagas. Média, as bandas 4 e 5. Baixa, as "
            "bandas 1 a 3.",
        "Nota da régua":
            "De 1 a 7. Quanto maior, mais perto da vaga. O nome de cada banda está na "
            "coluna ao lado.",
        "Cenário eleitoral (banda)":
            "Onde o candidato está na disputa pelas duas vagas do estado. A linha de corte "
            "é o 2º colocado; para quem já está em 1º ou 2º, a referência passa a ser o 3º. "
            "A banda é calculada com os percentuais do estado normalizados para somar 100, "
            "que é a escala do histórico de 2010 e 2018.",
        "É competitivo?":
            "Sim para quem está numa das duas vagas ou empatado tecnicamente com a segunda, "
            "quer dizer banda 5, 6 ou 7. São "
            f"{c['competitivos']} nomes, de {c['min_uf']} a {c['max_uf']} por UF.",
        "Posição na disputa":
            "Posição na média móvel do estado, entre quem aparece em pesquisa.",
        "Média das pesquisas":
            "Média móvel de 30 dias da matriz de polling, na data mais recente do estado. É "
            "o percentual como o instituto publicou, sem redistribuir branco, nulo e "
            "indeciso, então a soma do estado não fecha em 100.",
        "Distância para a linha de corte":
            "Em pontos da média móvel. Positivo quer dizer dentro das vagas.",
        "Como o índice foi calculado":
            "A conta da linha, aberta: banda, média, posição, distância, quantas pesquisas "
            "sustentam o número e a unidade da régua.",
        "Chapa presidencial":
            "Vínculo com uma das duas chapas, pelo registro no TSE, não por leitura "
            "política. Lula, quando o PT está na coligação estadual. Flávio, quando o PL "
            "está. Lula (só nacional), quando o partido está na coligação nacional de Lula "
            "mas o PT ficou fora daquele estado. Outro presidenciável, quando o partido "
            "lançou candidato próprio. Sem vínculo, quando não está em nenhuma das duas.",
        "Base do vínculo de chapa":
            "Qual fato do registro sustenta a coluna anterior.",
        "Coligação no estado":
            "Nome da coligação e os partidos que a compõem, como está no DivulgaCand.",
        "Cabeça de chapa no estado":
            "Candidato a governador da mesma coligação. Traço quando a coligação não tem "
            "candidatura ao governo.",
        "Apoio presidencial declarado":
            "Declaração nominal de apoio, que é outra coisa que a coligação. Vem dos 47 "
            "nomes que Flávio leu na transmissão de 05/08/2026, do mapa do time de Lula "
            "publicado pelo PT em 24/08/2026 e de declarações achadas na imprensa, com a "
            "fonte ao lado. Quem declarou apoio sem estar na lista do presidenciável "
            "aparece como fora da lista, porque é apoio de mão única. Oposição a um lado "
            "não conta como adesão ao outro. Cobre "
            f"{c['declarados']} dos {c['linhas']} nomes.",
        "Fonte do apoio declarado":
            "Link da matéria. Traço quando não há declaração.",
        "Mandato legislativo atual":
            "Se a pessoa tem cadeira hoje, casado com as outras abas de competitividade. "
            "Nove senadores só casam por lista manual, porque o nome de urna não parece com "
            "o nome parlamentar.",
        "Situação do registro no TSE":
            "Como está o registro no DivulgaCand. Aguardando julgamento é o normal para boa "
            "parte deles neste momento do calendário.",
        "Pesquisas na conta":
            "Quantas pesquisas dos últimos 90 dias mediram este nome. Com uma só, a banda é "
            "provisória e o texto da linha avisa.",
        "Última pesquisa do estado":
            "Data de campo da pesquisa mais recente do estado que entrou na média móvel. "
            f"{c['sem_pesquisa']} linhas não têm banda porque nenhuma pesquisa dos últimos "
            "90 dias mede o nome.",
    }


def gravar(sh, aba, texto, pular=()):
    ws = sh.worksheet(aba)
    cabecalho = ws.row_values(1)
    escrever, ignoradas = {}, []
    for i, nome in enumerate(cabecalho):
        if nome in pular:
            continue
        if nome in texto:
            escrever[gspread.utils.rowcol_to_a1(1, i + 1)] = texto[nome]
    for nome in texto:
        if nome not in cabecalho:
            ignoradas.append(nome)
    ws.update_notes(escrever)
    print(f"{aba}: {len(escrever)} notas gravadas"
          + (f", fora do cabeçalho: {ignoradas}" if ignoradas else ""))


def main():
    cred = Credentials.from_service_account_file(
        os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"), scopes=ESCOPO)
    sh = gspread.authorize(cred).open_by_key(ID)

    valores = sh.worksheet(ABA_CANDIDATURAS).get_all_values()
    texto = notas(contexto(valores))
    gravar(sh, ABA_CANDIDATURAS, texto)

    # A aba Atual tem as mesmas colunas de índice, mais as cinco de mandato, que
    # já têm nota do outro pipeline e não são tocadas aqui.
    atual = dict(texto)
    atual["Índice de competitividade eleitoral"] = (
        texto["Índice de competitividade eleitoral"]
        + " Não se aplica é quem não disputa o Senado em 2026.")
    atual.pop("Candidato", None)   # a aba Atual chama a coluna de Parlamentar
    gravar(sh, ABA_ATUAL, atual,
           pular=("Parlamentar", "Partido", "UF", "O que disputa em 2026",
                  "Se perder, o que acontece"))


if __name__ == "__main__":
    main()
