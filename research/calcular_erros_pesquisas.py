import pandas as pd
import re
from datetime import datetime

print("Carregando base casada...")
df = pd.read_csv('research/dados_senado_historico/pesquisas_com_resultados.csv')

# 1. Filtrar dados válidos
df = df[df['candidato_resultado'].notna()].copy()

# Converter percentual para float
def parse_pct(val):
    if pd.isna(val): return None
    v = str(val).replace('%', '').replace(',', '.').strip()
    try:
        return float(v)
    except:
        return None

df['percentual_num'] = df['percentual'].apply(parse_pct)
df = df[df['percentual_num'].notna()]

# 2. Extrair data final do campo para achar a última pesquisa
def extract_end_date(periodo, ano):
    if pd.isna(periodo): return pd.Timestamp(f"{ano}-01-01")
    # Tenta pegar a última data (ex: "28 a 30/08" -> "30/08")
    datas = re.findall(r'(\d{1,2}/\d{1,2})', str(periodo))
    if not datas:
        # Tenta pegar só mês
        meses = re.findall(r'(jan|fev|mar|abr|mai|jun|jul|ago|set|out|nov|dez)', str(periodo).lower())
        if meses:
            mes_map = {'jan':1,'fev':2,'mar':3,'abr':4,'mai':5,'jun':6,'jul':7,'ago':8,'set':9,'out':10,'nov':11,'dez':12}
            return pd.Timestamp(f"{ano}-{mes_map[meses[-1]]}-01")
        return pd.Timestamp(f"{ano}-01-01")
    
    dia, mes = datas[-1].split('/')
    try:
        # Algumas datas podem estar com ano "30/08/2018"
        if len(mes) == 4:
            return pd.Timestamp(f"{mes}-{dia.zfill(2)}-01") # formato sujo
        return pd.Timestamp(f"{ano}-{mes.zfill(2)}-{dia.zfill(2)}")
    except:
        return pd.Timestamp(f"{ano}-01-01")

df['data_pesquisa'] = df.apply(lambda row: extract_end_date(row['periodo_campo'], row['ano']), axis=1)

# 3. Pegar apenas a última pesquisa de cada instituto por estado
# Identificador único da rodada (caso não tenha id_rodada)
df['rodada_id'] = df['ano'].astype(str) + '_' + df['uf'] + '_' + df['instituto'] + '_' + df['periodo_campo'].astype(str)

ultimas_pesquisas = []
for (ano, uf, instituto), group in df.groupby(['ano', 'uf', 'instituto']):
    # Ordena pela data
    sorted_group = group.sort_values('data_pesquisa', ascending=False)
    # A última rodada é a primeira após ordenar decrescente
    ultima_rodada = sorted_group['rodada_id'].iloc[0]
    ultimas_pesquisas.append(group[group['rodada_id'] == ultima_rodada])

df_ultimas = pd.concat(ultimas_pesquisas)

# 4. Normalizar os percentuais das pesquisas (share válido)
# Regra: menções do candidato / soma das menções de candidatos
soma_mencoes = df_ultimas.groupby('rodada_id')['percentual_num'].transform('sum')
df_ultimas['share_pesquisa'] = df_ultimas['percentual_num'] / soma_mencoes

# 5. Calcular o erro em relação ao resultado
df_ultimas['erro_estimativa_pp'] = (df_ultimas['share_pesquisa'] - df_ultimas['share_voto_nominal_calculado']) * 100

# 6. Calcular a distância para a "linha de corte" (2º colocado)
# Para cada rodada e candidato, calculamos a diferença dele para o corte
# O corte real = share_voto do 2º colocado oficial
# Corte estimado = share_pesquisa do 2º colocado na pesquisa
df_corte_real = df_ultimas[df_ultimas['ranking_voto_bruto'] == 2].groupby(['ano', 'uf'])['share_voto_nominal_calculado'].first().reset_index()
df_corte_real.rename(columns={'share_voto_nominal_calculado': 'share_corte_real'}, inplace=True)

df_ultimas['ranking_pesquisa'] = df_ultimas.groupby('rodada_id')['share_pesquisa'].rank(method='first', ascending=False)
df_corte_pesquisa = df_ultimas[df_ultimas['ranking_pesquisa'] == 2].groupby('rodada_id')['share_pesquisa'].first().reset_index()
df_corte_pesquisa.rename(columns={'share_pesquisa': 'share_corte_pesquisa'}, inplace=True)

df_ultimas = df_ultimas.merge(df_corte_real, on=['ano', 'uf'], how='left')
df_ultimas = df_ultimas.merge(df_corte_pesquisa, on=['rodada_id'], how='left')

df_ultimas['distancia_corte_real_pp'] = (df_ultimas['share_voto_nominal_calculado'] - df_ultimas['share_corte_real']) * 100
df_ultimas['distancia_corte_pesq_pp'] = (df_ultimas['share_pesquisa'] - df_ultimas['share_corte_pesquisa']) * 100
df_ultimas['erro_distancia_corte_pp'] = df_ultimas['distancia_corte_pesq_pp'] - df_ultimas['distancia_corte_real_pp']

# Ordenar e exportar
df_ultimas = df_ultimas.sort_values(['ano', 'uf', 'instituto', 'ranking_pesquisa'])
cols_export = ['ano', 'uf', 'instituto', 'periodo_campo', 'candidato_resultado', 
               'ranking_voto_bruto', 'ranking_pesquisa', 'share_voto_nominal_calculado', 
               'share_pesquisa', 'erro_estimativa_pp', 'erro_distancia_corte_pp']

df_export = df_ultimas[cols_export].copy()
df_export.to_csv('research/dados_senado_historico/erros_ultimas_pesquisas.csv', index=False)

print(f"Análise concluída. Foram avaliadas as últimas pesquisas de {df_ultimas['instituto'].nunique()} institutos nas {df_ultimas['uf'].nunique()} UFs.")
print(f"Total de {df_ultimas['rodada_id'].nunique()} rodadas únicas e {len(df_ultimas)} observações de candidatos.")
print(f"Arquivo exportado para: research/dados_senado_historico/erros_ultimas_pesquisas.csv")
print("\nMédia Absoluta de Erro (MAE) por Ano:")
print(df_ultimas.groupby('ano')['erro_estimativa_pp'].apply(lambda x: x.abs().mean()))
