import re
import sys
from pathlib import Path
from outros.transcricao_debates import clientes_google, com_retentativa, PLANILHA, COL, ABA
from outros.resumo_debates import meta_da_linha

def main():
    gc, drive = clientes_google()
    ws = com_retentativa("abertura da planilha de debates", lambda: gc.open_by_key(PLANILHA).worksheet("eventos"))
    todas = com_retentativa("leitura", ws.get_all_values)
    cabecalho = [c.strip().lower() for c in todas[0]]
    col_resumo = cabecalho.index("link_resumo")
    
    for i, linha in enumerate(todas[1:], start=2):
        if not linha: continue
        link = linha[col_resumo] if col_resumo < len(linha) else ""
        if not link.strip(): continue
        meta = meta_da_linha(linha, COL, todas)
        
        arq_docx = Path(f"fix_docx/{meta['id']}_resumo.docx")
        if not arq_docx.exists(): continue
        
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", link)
        file_id = m.group(1) if m else link.strip()
        print(f"Atualizando {meta['id']} ({file_id})")
        
        com_retentativa("update_drive", lambda: drive.files().update(
            fileId=file_id,
            media_body=str(arq_docx),
            supportsAllDrives=True
        ).execute())
        print(f"Sucesso para {meta['id']}")

if __name__ == "__main__":
    main()
