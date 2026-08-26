from outros.transcricao_debates import clientes_google, PLANILHA, com_retentativa, COL

def main():
    gc, _ = clientes_google()
    ws = com_retentativa("abertura da planilha de eventos", lambda: gc.open_by_key(PLANILHA).worksheet("eventos"))
    todas = com_retentativa("leitura da planilha", ws.get_all_values)
    cabecalho = list(todas[0])
    
    print("Mapeamento das colunas:")
    for k, v in COL.items():
        print(f"  {k}: index {v}")
        
    for i, linha in enumerate(todas[1:], start=2):
        if not linha: continue
        row_id = linha[0] if len(linha) > 0 else ""
        if "ac-gov" in row_id or "mailza" in str(linha).lower():
            print(f"\nLinha {i}:")
            for k, idx in COL.items():
                val = linha[idx] if idx < len(linha) else "(fora do range)"
                print(f"  {k}: {val}")

if __name__ == "__main__":
    main()
