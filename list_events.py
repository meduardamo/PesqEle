from outros.transcricao_debates import clientes_google, PLANILHA, com_retentativa

def main():
    gc, _ = clientes_google()
    ws = com_retentativa("abertura da planilha de eventos", lambda: gc.open_by_key(PLANILHA).worksheet("eventos"))
    todas = com_retentativa("leitura da planilha", ws.get_all_values)
    cabecalho = list(todas[0])
    
    # We want to print columns like id, data, link_transcricao, link_resumo
    cols = ["id", "data", "link_transcricao", "link_resumo"]
    indices = [cabecalho.index(c) for c in cols if c in cabecalho]
    print("Cabeçalho reduzido:", [cabecalho[i] for i in indices])
    
    for i, linha in enumerate(todas[1:], start=2):
        if not linha: continue
        row_id = linha[cabecalho.index("id")] if "id" in cabecalho else ""
        if "ac-gov" in row_id or "mailza" in str(linha).lower():
            row_data = [linha[idx] if idx < len(linha) else "" for idx in indices]
            print(f"Linha {i}: {row_data}")

if __name__ == "__main__":
    main()
