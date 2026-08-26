from outros.transcricao_debates import clientes_google, PLANILHA, com_retentativa

def main():
    gc, _ = clientes_google()
    ws = com_retentativa("abertura da planilha de eventos", lambda: gc.open_by_key(PLANILHA).worksheet("eventos"))
    todas = com_retentativa("leitura da planilha", ws.get_all_values)
    cabecalho = list(todas[0])
    
    print("Cabeçalho:", cabecalho)
    for i, linha in enumerate(todas[1:], start=2):
        if not linha: continue
        # Print row index and first few fields
        print(f"Linha {i}: {linha[:5]} | Colunas preenchidas: {len(linha)}")
        
if __name__ == "__main__":
    main()
