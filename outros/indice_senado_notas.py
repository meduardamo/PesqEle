import os
import gspread
from google.oauth2.service_account import Credentials
c=Credentials.from_service_account_file(os.getenv("GOOGLE_CREDENTIALS_PATH", "credentials.json"),
    scopes=["https://www.googleapis.com/auth/spreadsheets"])
ID=os.getenv("SPREADSHEET_ID_RECANDIDATURAS","").strip()
if not ID: raise RuntimeError("Variável SPREADSHEET_ID_RECANDIDATURAS não configurada.")
ws=gspread.authorize(c).open_by_key(ID).worksheet(
    "Competitividade Senado 2026")
N={
"A1":"Universo: todo titular com registro de candidatura ao Senado no TSE (DivulgaCand), "
     "menos as quatro renúncias. São 313 nomes. Não é um recorte de nomes competitivos: "
     "quem tem registro está aqui, com ou sem pesquisa.",
"B1":"Partido do registro no TSE. Quando a matriz de pesquisa traz outro partido, vale o do registro.",
"D1":"Índice de competitividade eleitoral: em que altura da disputa o candidato está hoje, "
     "em sete bandas. A linha de corte é o 2º colocado, porque 2026 tem duas vagas por estado. "
     "Para quem já está em 1º ou 2º, a referência passa a ser o 3º. O empate técnico usa a margem "
     "da diferença entre dois percentuais, que é a margem de erro do estado multiplicada por "
     "raiz de 2. O índice é flutuante: muda quando a média móvel muda.",
"F1":"Competitivo é quem está numa das duas vagas do estado (banda 6 ou 7) ou "
     "empatado tecnicamente com quem está na segunda (banda 5). O corte é o mesmo da "
     "régua, não um critério à parte. Dá de 2 a 5 nomes por UF, 82 no total.",
"E1":"7 lidera isoladamente; 6 entre os 2 primeiros; 5 empatado tecnicamente com o 2º; "
     "4 até 1 margem de erro atrás; 3 entre 1 e 2 margens; 2 entre 2 e 3 margens; "
     "1 mais de 3 margens atrás. Serve para ordenar e comparar entre estados. Não é "
     "probabilidade de eleição.",
"H1":"Média móvel híbrida de 30 dias da matriz de polling, na data mais recente do estado. "
     "Não é o número de uma pesquisa avulsa.",
"I1":"Distância em pontos para a linha de corte: o 3º colocado, para quem está em 1º ou 2º; "
     "o 2º colocado, para os demais.",
"J1":"Abre a conta: banda, média, posição, distância para a linha de corte, quantas pesquisas "
     "sustentam o número, data da última pesquisa do estado e a margem de erro usada.",
"K1":"Vínculo com uma das duas chapas presidenciais, pelo registro no TSE. Lula, quando o PT "
     "está na coligação estadual do candidato. Flávio, quando o PL está. Lula (só nacional), "
     "quando o partido integra a coligação nacional de Lula (PSB, PDT, PT, PC do B, PV, PSOL, "
     "REDE) mas o PT não entrou na coligação daquele estado. Outro presidenciável, quando o "
     "partido lançou candidato próprio à Presidência. Sem vínculo, quando o partido não está em "
     "nenhuma das duas coligações presidenciais. É fato de registro, não leitura política.",
"L1":"Qual fato do registro sustenta a classificação da coluna anterior.",
"M1":"Nome da coligação estadual e os partidos que a compõem, como registrado no DivulgaCand.",
"N1":"Candidato a governador que disputa pela mesma coligação estadual. Vazio quando a coligação "
     "do Senado não tem candidatura ao governo.",
"O1":"Camada separada da chapa: declaração nominal de apoio. Três fontes, nesta ordem. "
     "Flávio: os 47 nomes que ele leu na própria transmissão de 05/08/2026, publicados por "
     "Poder360 em 07/08 e Gazeta do Povo em 13/08, que batem nome a nome. Lula: o mapa do time "
     "de Lula publicado pelo PT em 24/08/2026, com 50 nomes, incluindo aliados de outros "
     "partidos. Declarações individuais achadas em varredura de imprensa, com a fonte na coluna "
     "ao lado. Quem declarou apoio sem estar na lista do presidenciável aparece como declarou, "
     "fora da lista dos 47, porque é apoio de mão única e não vale o mesmo. Oposição a um lado "
     "não é contada como adesão ao outro, porque Caiado e Zema também disputam. Cobre 104 dos "
     "313 nomes e não contradiz a coluna de chapa em nenhum caso.",
"P1":"Link da matéria que sustenta o apoio declarado.",
"Q1":"Casado com as abas Radar Senado, Radar Câmara e Radar Assembleias desta planilha.",
"R1":"Situação do registro no DivulgaCand na data da atualização. Aguardando julgamento é o "
     "estado normal de boa parte dos registros neste momento do calendário.",
"S1":"Pesquisas distintas dos últimos 90 dias que mediram este nome. Com uma só, a banda é provisória.",
"T1":"Data de campo da pesquisa mais recente do estado que entrou na média móvel.",
}
ws.update_notes(N)
print("notas gravadas:", len(N))
