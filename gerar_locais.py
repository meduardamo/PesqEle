from pathlib import Path
import re
from outros.resumo_debates import criar_docx_timbrado

template = Path("outros/templates/timbrado_eleicoes.docx")

for f in Path("fix_docx").glob("*.md"):
    markdown = f.read_text(encoding="utf-8")
    
    lines = markdown.split("\n")
    titulo = ""
    sub = ""
    if "band-sp" in f.name:
        titulo = "DEBATE AO GOVERNO – SÃO PAULO"
        sub = "Band, 09 de agosto de 2026"
    elif "band-mg" in f.name:
        titulo = "DEBATE AO GOVERNO – MINAS GERAIS"
        sub = "Band, 08 de agosto de 2026"
    else:
        titulo = "SABATINA AO GOVERNO – CEARÁ"
        sub = "PontoPoder, 20 de agosto de 2026"
        
    out_docx = str(f.with_suffix(".docx"))
    criar_docx_timbrado(markdown, titulo, sub, template, out_docx)
    print(f"Gerado {out_docx}")
