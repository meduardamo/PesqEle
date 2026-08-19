import sys
sys.path.append('/Users/eduardamarques/painel-eleitoral-interno')
from data_loaders import load_tab, values_to_df
tab = load_tab("Intenção de Voto - Presidente")
df = values_to_df(tab)
lula = df[df['candidato_partido'] == 'Lula (PT)']
lula = lula.sort_values('data_campo_dt').tail(10)
print(lula[['data_campo_dt', 'instituto', 'percentual_base', 'candidato_partido']])
