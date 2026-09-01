import pandas as pd
import unicodedata
import re
import difflib

def normalize_text(text):
    if pd.isna(text): return ""
    text = str(text).lower()
    text = ''.join(c for c in unicodedata.normalize('NFD', text) if unicodedata.category(c) != 'Mn')
    # replace common punctuation
    text = text.replace('(', ' ').replace(')', ' ').replace('-', ' ')
    # split and keep words
    return ' '.join(text.split())

print("Carregando bases...")
df_wiki = pd.read_csv('research/dados_senado_historico/pesquisas_senado_wikipedia.csv')
df_wiki['fonte_pesquisa'] = 'wikipedia'
df_cef = pd.read_csv('research/dados_senado_historico/pesquisas_senado_2010_congresso_em_foco.csv')
df_cef['fonte_pesquisa'] = 'congresso_em_foco'

df_pesquisas = pd.concat([df_wiki, df_cef], ignore_index=True)
df_pesquisas['cand_norm'] = df_pesquisas['candidato_rotulo'].apply(normalize_text)

df_resultados = pd.read_csv('research/dados_senado_historico/resultados_senado_2010_2018_wikipedia.csv')
df_resultados['cand_res_norm'] = df_resultados['candidato_resultado'].apply(normalize_text)

print("Realizando match de candidatos (Pesquisa -> Resultado)...")
de_para = []

for (ano, uf), group in df_pesquisas.groupby(['ano', 'uf']):
    res_group = df_resultados[(df_resultados['ano'] == ano) & (df_resultados['uf'] == uf)]
    res_candidates = res_group['cand_res_norm'].tolist()
    res_original = res_group['candidato_resultado'].tolist()
    
    for cand in group['candidato_rotulo'].unique():
        if pd.isna(cand) or 'branco' in str(cand).lower() or 'nulo' in str(cand).lower() or 'indeciso' in str(cand).lower() or 'não sabe' in str(cand).lower() or 'nenhum' in str(cand).lower():
            continue
        cand_norm = normalize_text(cand)
        # remove party from poll name specifically (usually last word in parenthesis)
        cand_norm_no_party = re.sub(r'\b(pt|pmdb|psdb|dem|psol|pdt|psb|pcdob|pp|pr|ptb|pv|pps|prb|pmn|psc|ptc|prp|phs|pode|psl|solidariedade|pros|rede|novo|prtb|pmb|ptdoB|psdc|pstu|pco|pcb|up|dc|avante|patriota|pmdb|mdb|pl|republicanos|cidadania)\b', '', cand_norm).strip()
        cand_norm_no_party = ' '.join(cand_norm_no_party.split())
        
        match = None
        
        # 1. Any result candidate has cand_norm_no_party in it?
        for rc, ro in zip(res_candidates, res_original):
            if not rc: continue
            rc_clean = re.sub(r'\b(pt|pmdb|psdb|dem|psol|pdt|psb|pcdob|pp|pr|ptb|pv|pps|prb|pmn|psc|ptc|prp|phs|pode|psl|solidariedade|pros|rede|novo|prtb|pmb|ptdoB|psdc|pstu|pco|pcb|up|dc|avante|patriota|pmdb|mdb|pl|republicanos|cidadania)\b', '', rc).strip()
            rc_clean = ' '.join(rc_clean.split())
            
            # Substring match on cleaned
            if len(cand_norm_no_party) > 3 and (cand_norm_no_party in rc_clean or rc_clean in cand_norm_no_party):
                match = ro
                break
                
        # 2. Fuzzy match
        if not match and res_candidates:
            matches = difflib.get_close_matches(cand_norm_no_party, [re.sub(r'\b(pt|pmdb|psdb|dem|psol|pdt|psb|pcdob|pp|pr|ptb|pv|pps|prb|pmn|psc|ptc|prp|phs|pode|psl|solidariedade|pros|rede|novo|prtb|pmb|ptdoB|psdc|pstu|pco|pcb|up|dc|avante|patriota|pmdb|mdb|pl|republicanos|cidadania)\b', '', r).strip() for r in res_candidates], n=1, cutoff=0.5)
            if matches:
                # find index of matched cleaned candidate
                cleaned_rcs = [re.sub(r'\b(pt|pmdb|psdb|dem|psol|pdt|psb|pcdob|pp|pr|ptb|pv|pps|prb|pmn|psc|ptc|prp|phs|pode|psl|solidariedade|pros|rede|novo|prtb|pmb|ptdoB|psdc|pstu|pco|pcb|up|dc|avante|patriota|pmdb|mdb|pl|republicanos|cidadania)\b', '', r).strip() for r in res_candidates]
                idx = cleaned_rcs.index(matches[0])
                match = res_original[idx]

        de_para.append({
            'ano': ano,
            'uf': uf,
            'candidato_pesquisa': cand,
            'candidato_resultado': match
        })

df_de_para = pd.DataFrame(de_para)
df_de_para.to_csv('research/dados_senado_historico/de_para_candidatos_pesquisas.csv', index=False)

df_pesquisas_casadas = df_pesquisas.merge(
    df_de_para, 
    left_on=['ano', 'uf', 'candidato_rotulo'], 
    right_on=['ano', 'uf', 'candidato_pesquisa'], 
    how='left'
)
df_final = df_pesquisas_casadas.merge(
    df_resultados[['ano', 'uf', 'candidato_resultado', 'share_voto_nominal_calculado', 'eleito_top2_voto_bruto', 'ranking_voto_bruto']],
    on=['ano', 'uf', 'candidato_resultado'],
    how='left'
)
df_final.to_csv('research/dados_senado_historico/pesquisas_com_resultados.csv', index=False)
matched_pct = df_de_para['candidato_resultado'].notna().mean()
print(f"De-para gerado com {len(df_de_para)} nomes únicos de candidatos nas pesquisas.")
print(f"Taxa de match inicial: {matched_pct:.1%} dos nomes foram casados com sucesso.")
