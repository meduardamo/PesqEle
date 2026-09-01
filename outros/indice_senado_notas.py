"""Grava as notas de cabeçalho das duas abas de Senado da Recandidaturas 2026.

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

ABA_TESTE = "Competitividade Senado 2026 (teste)"
ABA_ATUAL = "Competitividade Senado Atual"


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
    """Texto de cada coluna, pelo nome da coluna."""
    taxas = ", ".join(f"banda {n} {regua.TAXA[n]}" for n in (7, 6, 5, 4))
    return {
        "Candidato":
            "Universo: todo titular com registro de candidatura ao Senado no TSE "
            f"(DivulgaCand), menos as renúncias. São {c['linhas']} nomes. Não é um recorte "
            "de nomes competitivos: quem tem registro está aqui, com ou sem pesquisa.",
        "Partido":
            "Partido do registro no TSE. Quando a matriz de pesquisa traz outro partido, "
            "vale o do registro.",
        "Índice de competitividade eleitoral":
            "Taxa de eleição observada na banda em que o candidato está hoje. Sai de 203 "
            "rodadas de pesquisa de 2010 e 2018, as duas eleições anteriores com duas vagas "
            "por estado: entre os candidatos que estavam nesta mesma banda, essa foi a "
            f"proporção que terminou entre os 2 mais votados. {taxas}; bandas 3, 2 e 1 "
            f"{regua.TAXA[1]}. É o resultado do grupo, não uma previsão sobre a pessoa, e a "
            "conta está em research/calibrar_senado.py.",
        "Alta, média ou baixa":
            "Classe da taxa, para comparar com as outras abas do Radar. Alta são as bandas "
            "6 e 7, dentro das duas vagas. Média são as bandas 4 e 5, à distância de uma "
            "margem. Baixa são as bandas 1 a 3.",
        "Nota da régua":
            "7 lidera isoladamente; 6 entre os 2 primeiros; 5 empatado tecnicamente com o "
            "2º; 4 até 1 margem de erro atrás; 3 entre 1 e 2 margens; 2 entre 2 e 3 margens; "
            "1 mais de 3 margens atrás. A linha de corte é o 2º colocado, porque 2026 tem "
            "duas vagas por estado; para quem já está em 1º ou 2º, a referência passa a ser "
            "o 3º. A banda é calculada na escala comparável, com os percentuais do estado "
            "normalizados para somar 100 entre as candidaturas medidas, que é a escala em "
            "que o histórico de 2010 e 2018 foi medido.",
        "É competitivo?":
            "Competitivo é quem está numa das duas vagas do estado (banda 6 ou 7) ou "
            "empatado tecnicamente com quem está na segunda (banda 5). O corte é o mesmo da "
            f"régua, não um critério à parte. Dá de {c['min_uf']} a {c['max_uf']} nomes por "
            f"UF, {c['competitivos']} no total.",
        "Posição na disputa":
            "Posição na média móvel do estado, entre as candidaturas que aparecem em "
            "pesquisa. Ordenar não depende da escala: é a mesma posição nas duas.",
        "Média das pesquisas":
            "Média móvel híbrida de 30 dias da matriz de polling, na data mais recente do "
            "estado. É o percentual como o instituto publicou, sem redistribuir branco, "
            "nulo e indeciso, e por isso a soma do estado não fecha em 100. Não é o número "
            "de uma pesquisa avulsa.",
        "Distância para a linha de corte":
            "Distância em pontos da média móvel para a linha de corte: o 3º colocado, para "
            "quem está em 1º ou 2º; o 2º colocado, para os demais. A mesma distância na "
            "escala comparável, que é a que define a banda, está na coluna ao lado.",
        "Como o índice foi calculado":
            "Abre a conta: banda, média, posição, distância para a linha de corte, a mesma "
            "distância na escala comparável, quantas pesquisas sustentam o número, data da "
            "última pesquisa do estado, a unidade da régua e o que a taxa quer dizer.",
        "Chapa presidencial":
            "Vínculo com uma das duas chapas presidenciais, pelo registro no TSE. Lula, "
            "quando o PT está na coligação estadual do candidato. Flávio, quando o PL está. "
            "Lula (só nacional), quando o partido integra a coligação nacional de Lula "
            "(PSB, PDT, PT, PC do B, PV, PSOL, REDE) mas o PT não entrou na coligação "
            "daquele estado. Outro presidenciável, quando o partido lançou candidato "
            "próprio à Presidência. Sem vínculo, quando o partido não está em nenhuma das "
            "duas coligações presidenciais. É fato de registro, não leitura política.",
        "Base do vínculo de chapa":
            "Qual fato do registro sustenta a classificação da coluna anterior.",
        "Coligação no estado":
            "Nome da coligação estadual e os partidos que a compõem, como registrado no "
            "DivulgaCand.",
        "Cabeça de chapa no estado":
            "Candidato a governador que disputa pela mesma coligação estadual. Traço quando "
            "a coligação do Senado não tem candidatura ao governo.",
        "Apoio presidencial declarado":
            "Camada separada da chapa: declaração nominal de apoio. Três fontes, nesta "
            "ordem. Flávio: os 47 nomes que ele leu na própria transmissão de 05/08/2026, "
            "publicados por Poder360 em 07/08 e Gazeta do Povo em 13/08, que batem nome a "
            "nome. Lula: o mapa do time de Lula publicado pelo PT em 24/08/2026, com 50 "
            "nomes, incluindo aliados de outros partidos. Declarações individuais achadas "
            "em varredura de imprensa, com a fonte na coluna ao lado. Quem declarou apoio "
            "sem estar na lista do presidenciável aparece como declarou, fora da lista dos "
            "47, porque é apoio de mão única e não vale o mesmo. Oposição a um lado não é "
            "contada como adesão ao outro, porque Caiado e Zema também disputam. Cobre "
            f"{c['declarados']} dos {c['linhas']} nomes e não contradiz a coluna de chapa "
            "em nenhum caso.",
        "Fonte do apoio declarado":
            "Link da matéria que sustenta o apoio declarado. Traço quando não há declaração.",
        "Mandato legislativo atual":
            "Casado com as abas Competitividade Senado, Câmara e Assembleias desta "
            "planilha. Nove senadores só casam por lista manual, porque o nome de urna do "
            "registro não parece com o nome parlamentar.",
        "Situação do registro no TSE":
            "Situação do registro no DivulgaCand na data da atualização. Aguardando "
            "julgamento é o estado normal de boa parte dos registros neste momento do "
            "calendário.",
        "Pesquisas na conta":
            "Pesquisas distintas dos últimos 90 dias que mediram este nome. Com uma só, a "
            "banda é provisória e o texto da linha avisa.",
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

    valores = sh.worksheet(ABA_TESTE).get_all_values()
    texto = notas(contexto(valores))
    gravar(sh, ABA_TESTE, texto)

    # A aba Atual tem as mesmas colunas de índice, mais as cinco de mandato, que
    # já têm nota do outro pipeline e não são tocadas aqui.
    atual = dict(texto)
    atual["Índice de competitividade eleitoral"] = (
        texto["Índice de competitividade eleitoral"]
        + " Não se aplica é quem não disputa o Senado em 2026.")
    gravar(sh, ABA_ATUAL, atual,
           pular=("Parlamentar", "Partido", "UF", "O que disputa em 2026",
                  "Se perder, o que acontece"))


if __name__ == "__main__":
    main()
