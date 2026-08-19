import gspread
import pandas as pd
import re
from google.oauth2.service_account import Credentials
import time

def main():
    print("Iniciando conferência da planilha...")
    
    # Setup gspread
    scopes = [
        'https://www.googleapis.com/auth/spreadsheets',
        'https://www.googleapis.com/auth/drive'
    ]
    credentials = Credentials.from_service_account_file(
        '/Users/eduardamarques/eixo-eleicoes/credentials.json',
        scopes=scopes
    )
    gc = gspread.authorize(credentials)
    
    # Open sheet
    sheet_id = '1Vo-2oa11JpPaYC051Z0UYNR1yJZdhYW4RJeylHfX-bA'
    sh = gc.open_by_key(sheet_id)
    ws = sh.worksheet('analise_planos')
    
    # Get all records
    print("Baixando dados...")
    data = ws.get_all_values()
    headers = data[0]
    
    # Find columns
    idx_nivel = headers.index('nivel')
    idx_trecho = headers.index('trecho')
    
    updates = []
    
    # Patterns for external targets
    external_patterns = [
        r'\b(pne|plano nacional de educa[çc][ãa]o)\b',
        r'\b(ods|objetivos? de desenvolvimento sustent[áa]vel)\b',
        r'\bagenda 2030\b',
        r'\bonu\b'
    ]
    
    # Generic action patterns (short text)
    generic_patterns = [
        r'^implanta[çc][ãa]o de (programas|projetos)',
        r'^cria[çc][ãa]o de (programas|projetos|campanhas)',
        r'^desenvolvimento de (programas|projetos)',
        r'^abertura de editais',
        r'^amplia[çc][ãa]o de (programas|projetos)'
    ]
    
    for row_idx, row in enumerate(data[1:], start=2): # 1-based, header is 1, data starts at 2
        nivel = row[idx_nivel].strip()
        trecho = row[idx_trecho].strip()
        trecho_lower = trecho.lower()
        
        # Rule 1: Define meta but cites external laws
        if nivel == 'Define meta':
            if any(re.search(p, trecho_lower) for p in external_patterns):
                # Suggest downgrade to Propõe ação or Menciona vagamente
                print(f"Row {row_idx}: [Define meta -> Propõe ação] External target found.\nText: {trecho}\n")
                updates.append({
                    'range': f"{gspread.utils.rowcol_to_a1(row_idx, idx_nivel + 1)}",
                    'values': [['Propõe ação']]
                })
        
        # Rule 2: Propõe ação but very generic and short
        elif nivel == 'Propõe ação':
            words = trecho.split()
            if len(words) <= 15: # Arbitrary threshold for "short"
                if any(re.search(p, trecho_lower) for p in generic_patterns):
                    print(f"Row {row_idx}: [Propõe ação -> Menciona vagamente] Generic/Short text.\nText: {trecho}\n")
                    updates.append({
                        'range': f"{gspread.utils.rowcol_to_a1(row_idx, idx_nivel + 1)}",
                        'values': [['Menciona vagamente']]
                    })
    
    print(f"\nEncontrados {len(updates)} problemas.")
    
    if updates:
        print("Aplicando correções...")
        ws.batch_update(updates)
        print("Correções aplicadas com sucesso.")

if __name__ == '__main__':
    main()
