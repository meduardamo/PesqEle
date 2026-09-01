"""Listas nominais de apoio presidencial ao Senado, de fonte primária.

Flávio: os 47 nomes que ele leu na própria transmissão de 05/08/2026, publicados
por Poder360 (07/08) e Gazeta do Povo (13/08), que batem nome a nome.
Lula: o "mapa do time de Lula" publicado pelo PT em 24/08/2026, que inclui os
candidatos de outros partidos apoiados pelo partido.
"""
FONTE_FLAVIO = "https://www.poder360.com.br/poder-eleicoes-2026/saiba-quais-sao-os-47-candidatos-ao-senado-apoiados-por-flavio/"
FONTE_LULA = "https://pt.org.br/eleicoes-2026-conheca-o-mapa-do-time-de-lula-em-todo-o-pais/"

FLAVIO = [
 ("AC","Márcio Bittar"),("AC","Mara Rocha"),("AL","Marina Cândia"),("AL","Arthur Lira"),
 ("AP","Lucas Barreto"),("AP","Rayssa Furlan"),("AM","Capitão Alberto Neto"),("BA","João Roma"),
 ("BA","Angelo Coronel"),("CE","Capitão Wagner"),("CE","Alcides Fernandes"),("DF","Bia Kicis"),
 ("DF","Michelle Bolsonaro"),("ES","Maguinha Malta"),("ES","Evair de Melo"),("GO","Gustavo Gayer"),
 ("GO","Oséias Varão"),("MA","Cidônio Gonçalves"),("MG","Domingos Sávio"),("MS","Capitão Contar"),
 ("MS","Reinaldo Azambuja"),("MT","José Medeiros"),("PA","Éder Mauro"),("PA","Zequinha Marinho"),
 ("PB","Marcelo Queiroga"),("PE","Mendonça Filho"),("PI","Tiago Junqueira"),("PR","Filipe Barros"),
 ("PR","Deltan Dallagnol"),("RS","Ubiratan Sanderson"),("RS","Marcel van Hattem"),("SC","Caroline de Toni"),
 ("SC","Carlos Bolsonaro"),("RJ","Carlos Portinho"),("RJ","Carlos Jordy"),("RN","Styvenson Valentim"),
 ("RN","Coronel Hélio"),("RO","Fernando Máximo"),("RO","Bruno Scheid"),("RR","Hélio Lopes"),
 ("RR","Nicoletti"),("SP","Guilherme Derrite"),("SP","André do Prado"),("SE","Coronel Rocha"),
 ("SE","Rodrigo Valadares"),("TO","Eduardo Gomes"),("TO","Gaguim"),
]

LULA = [
 ("AC","Jorge Viana"),("AL","Renan Calheiros"),("AL","Dr. Wanderley"),("AP","Randolfe Rodrigues"),
 ("AP","Alliny Serrão"),("BA","Rui Costa"),("BA","Jaques Wagner"),("RJ","Benedita da Silva"),
 ("RJ","Pedro Paulo"),("CE","Cid Gomes"),("CE","Luizianne Lins"),("PE","Humberto Costa"),
 ("PE","Marília Arraes"),("DF","Erika Kokay"),("DF","Leila Barros"),("AM","Eduardo Braga"),
 ("ES","Fabiano Contarato"),("ES","Renato Casagrande"),("PA","Helder Barbalho"),("PA","Chicão"),
 ("GO","Cintia Dias"),("GO","Isaura Lemos"),("MA","Eliziane Gama"),("MA","Enilton Rodrigues"),
 ("MG","Marília Campos"),("MG","Áurea Carolina"),("MS","Vander Loubet"),("MS","Soraya Thronicke"),
 ("MT","Carlos Fávaro"),("MT","Pedro Taques"),("PB","João Azevêdo"),("PB","Veneziano"),
 ("PI","Marcelo Castro"),("PI","Júlio Cesar"),("PR","Gleisi Hoffmann"),("PR","Dr Rosinha"),
 ("RN","Samanda Alves"),("RN","Rafael Motta"),("RO","Luciana Oliveira"),("RO","Aires Mota"),
 ("RR","Hiperion de Oliveira"),("RS","Paulo Pimenta"),("RS","Manuela d'Ávila"),("SC","Décio Lima"),
 ("SC","Afrânio Boppré"),("SE","Rogério Carvalho"),("SE","André Moura"),("SP","Simone Tebet"),
 ("SP","Marina Silva"),("TO","Paulo Mourão"),
]

# Declarações individuais achadas na varredura de 01/09/2026, uma a uma. Todas são
# de mão única: o candidato declarou, mas não está na lista que Flávio leu.
# Ficam separadas por isso, e não somadas aos 47.
UNILATERAL = {
 ("SC","Esperidião Amin"): ("Flávio (declarou, fora da lista dos 47)",
   "https://www.karinamanarin.com.br/artigo/na-chapa-de-joao-flavio-bolsonaro-segundo-turno-em-sc-o-que-disse-esperidiao-amin-em-criciuma/"),
 ("PI","Ciro Nogueira"): ("Flávio (declarou, fora da lista dos 47)",
   "https://www.cnnbrasil.com.br/eleicoes/flavio-bolsonaro-deixa-ciro-nogueira-de-fora-da-lista-de-apoios-ao-senado/"),
 ("RO","Sílvia Cristina"): ("Flávio (declarou, fora da lista dos 47)",
   "https://www.rondoniadinamica.com/noticias/2026/08/silvia-cristina-diz-estar-superpreparada-para-analisar-impeachment-de-ministros-do-stf-fala-a-respeito-de-hildon-e-furia-e-trata-sobre-noticias-falsas,250700.shtml"),
 ("MT","Mauro Mendes"): ("Flávio (sinalizou, fora da lista dos 47)",
   "https://www.olhardireto.com.br/noticias/apesar-de-amizade-com-caiado-mauro-mendes-sinaliza-apoio-a-flavio-bolsonaro-e-aponta-tendencia-da-direita-em-mt"),
 ("GO","Gracinha Caiado"): ("Caiado",
   "https://www.jornalopcao.com.br/ultimas-noticias/romario-policarpo-endossa-apoio-a-gracinha-caiado-para-um-das-vagas-no-senado-861501/"),
 ("MG","Carlos Viana"): ("Caiado",
   "https://www.otempo.com.br/eleicoes/2026/senadores/2026/6/4/presidente-do-psd-garante-viana-na-chapa-ao-senado-e-ve-aro-com-segunda-vaga"),
}
