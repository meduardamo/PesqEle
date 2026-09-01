# Histórico para calibração do Senado: 2010 e 2018

## Decisão

Usar 2010 e 2018 como pleitos comparáveis a 2026. Nos três casos, a eleição
renova dois terços do Senado: o eleitor pode votar em dois nomes e são eleitos
os dois mais votados em cada UF. A eleição de 2022, com uma vaga por UF, não
entra na calibração principal.

Fonte institucional sobre a regra e a comparabilidade com 2018:
https://www12.senado.leg.br/noticias/materias/2026/08/17/quem-disputa-o-senado-em-2026-veja-perfil-partidos-e-distribuicao-pelo-pais

## Coleta concluída

- Wikipédia, 2018: 128 rodadas em 26 UFs, 1.329 observações de candidato.
  São Paulo não tem tabela de pesquisas na página estadual.
- Wikipédia, 2010: 23 rodadas em 4 UFs, 157 observações de candidato.
- Congresso em Foco, 2010: 81 rodadas em 27 UFs, 642 observações de
  candidato. A matéria foi publicada em 2 de outubro de 2010 e funciona como
  índice secundário contemporâneo.

Fonte de 2010:
https://www.congressoemfoco.com.br/noticia/82698/a-disputa-para-o-senado-estado-por-estado

Os coletores removem colunas auxiliares como `Vantagem`, `Sem candidato`,
brancos, nulos e indecisos da lista de candidatos. A linha original, a URL e o
formato inferido ficam preservados para auditoria.

## Regra de normalização

1. Preservar o percentual publicado e o formato da pergunta.
2. Não dobrar uma pergunta de voto único.
3. Quando a pesquisa publicar duas menções consolidadas, usar o valor
   publicado; quando publicar primeiro e segundo votos separados, só somar se
   as bases forem compatíveis.
4. Para comparar com o resultado, usar a participação entre menções nominais:
   `menções do candidato / soma das menções de candidatos`.
5. No resultado, usar `votos do candidato / soma dos votos nominais válidos`.
6. Manter uma medida de completude do segundo voto, sem redistribuir branco,
   nulo, indecisão ou voto não utilizado.
7. Rodadas cujo formato não puder ser confirmado ficam fora da calibração
   principal e entram apenas em análise de sensibilidade.

## Resultado eleitoral

O desfecho deve vir do TSE, não da Wikipédia. Conjuntos oficiais:

- 2018: https://dadosabertos.tse.jus.br/dataset/resultados-2018
- 2010: https://dadosabertos.tse.jus.br/dataset/resultados-2010

Em 2010, AP, PA, PB e TO exigem rótulos separados para voto bruto, proclamação
em 2010 e ocupação posterior da cadeira, por causa das mudanças judiciais da
Lei da Ficha Limpa. A calibração eleitoral principal deve usar voto bruto e
registrar as demais situações como metadados.

## Próximo passo

Casar candidatos das pesquisas com os resultados oficiais do TSE, selecionar a
última rodada comparável de cada instituto/UF e estimar o erro em relação à
linha de corte do segundo colocado.

## Resultado do Cruzamento e Cálculo de Erro

- A limpeza das tabelas da Wikipédia permitiu extrair 582 candidatos a senadores (ignorando suplentes e candidatos a outros cargos).
- Taxa de match (fuzzy matching) entre pesquisas e resultados: **88.4%**.
- A normalização utilizada converteu tanto pesquisas agregadas de 200% quanto pesquisas de 100% no modelo **share de votos válidos nominais**, limitando cada candidato a um teto prático de 50%.
- O M.A.E. (Erro Absoluto Médio) histórico foi calculado selecionando apenas a última pesquisa de cada instituto/UF:
  - **2010:** 5,52 pp
  - **2018:** 5,12 pp
- Os arquivos finais estão armazenados em `dados_senado_historico/pesquisas_com_resultados.csv` e `dados_senado_historico/erros_ultimas_pesquisas.csv`.

## Pendências (Próxima etapa)
- **Calibração 2026:** Injetar esses valores de MAE histórico no pipeline do "Radar Senado Atual" (ex: scripts da pasta `outros/`) para descontar o viés na eleição de 2/3 deste ano.
