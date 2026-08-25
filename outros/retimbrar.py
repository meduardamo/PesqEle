import re
import sys
from pathlib import Path
from outros.transcricao_debates import clientes_google, com_retentativa, enviar_drive, COL, PLANILHA, ABA
from outros.resumo_debates import criar_docx_timbrado, meta_da_linha, coluna_do_resumo

def extract_md_from_html(html_str):
    # Muito simplificado para extrair o texto de Google Docs
    # Pega só o body
    m_body = re.search(r'<body[^>]*>(.*?)</body>', html_str, re.DOTALL)
    if m_body:
        html_str = m_body.group(1)
    
    # Converte tags comuns de negrito
    html_str = re.sub(r'<span[^>]*font-weight:\s*(?:700|bold)[^>]*>(.*?)</span>', r'**\1**', html_str, flags=re.IGNORECASE)
    html_str = re.sub(r'<b>(.*?)</b>', r'**\1**', html_str, flags=re.IGNORECASE)
    html_str = re.sub(r'<strong>(.*?)</strong>', r'**\1**', html_str, flags=re.IGNORECASE)
    
    # Limpa classes
    html_str = re.sub(r'class="[^"]*"', '', html_str)
    
    # Converte paragrafos
    paras = re.findall(r'<p[^>]*>(.*?)</p>', html_str, flags=re.DOTALL)
    
    md_lines = []
    for p in paras:
        p = re.sub(r'<span[^>]*>(.*?)</span>', r'\1', p) # remove outras spans
        p = re.sub(r'<[^>]+>', '', p) # remove outras tags
        p = p.strip()
        if p:
            # Recupera formatações basicas (títulos, divisórias)
            if "Resumo da " in p or "Resumo do " in p:
                p = f"# {p}"
            md_lines.append(p)
            
    # Restaura o markdown de divisorias
    texto_final = "\n\n".join(md_lines)
    return texto_final

def main():
    if not PLANILHA:
        sys.exit("SPREADSHEET_ID_DEBATES não definido.")
    gc, drive = clientes_google()
    
    ws = com_retentativa("abertura da planilha de debates", lambda: gc.open_by_key(PLANILHA).worksheet("eventos"))
    todas = com_retentativa("leitura", ws.get_all_values)
    
    # Acha a coluna link_resumo
    cabecalho = [c.strip().lower() for c in todas[0]]
    if "link_resumo" not in cabecalho:
        sys.exit("Nenhuma coluna link_resumo encontrada.")
    col_resumo = cabecalho.index("link_resumo")
    
    template_docx = Path("outros/templates/timbrado_eleicoes.docx")
    if not template_docx.exists():
        sys.exit("Template DOCX não encontrado.")
        
    saida = Path("transcricoes")
    saida.mkdir(parents=True, exist_ok=True)
    
    for i, linha in enumerate(todas[1:], start=2):
        if not linha:
            continue
        link = linha[col_resumo] if col_resumo < len(linha) else ""
        if not link.strip():
            continue
            
        meta = meta_da_linha(linha, COL, todas)
        
        # Pega o ID do arquivo no drive
        m = re.search(r"/d/([a-zA-Z0-9_-]+)", link)
        file_id = m.group(1) if m else link.strip()
        print(f"[{meta['id']}] Retimbrando documento: {file_id}")
        
        try:
            # Baixa em HTML para não perder negrito
            request = drive.files().export_media(fileId=file_id, mimeType="text/html")
            html_content = request.execute().decode('utf-8')
        except Exception as e:
            print(f"[{meta['id']}] Erro ao baixar texto: {e}")
            continue
            
        md_text = extract_md_from_html(html_content)
        
        arq_docx = saida / f"{meta['id']}_resumo_retimbrado.docx"
        
        titulo_doc = f"Resumo — {meta['id']}"
        if criar_docx_timbrado(md_text, titulo_doc, "Eleições 2026 • Monitoramento de Debates", template_docx, arq_docx):
            print(f"[{meta['id']}] DOCX gerado. Fazendo upload...")
            # Sobe como revisão (update) para manter o link
            com_retentativa("update_drive", lambda: drive.files().update(
                fileId=file_id,
                media_body=str(arq_docx),
                supportsAllDrives=True
            ).execute())
            print(f"[{meta['id']}] Sucesso!")
        else:
            print(f"[{meta['id']}] Erro ao gerar DOCX timbrado.")

if __name__ == "__main__":
    main()
