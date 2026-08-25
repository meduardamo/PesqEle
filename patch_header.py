import re

with open("outros/resumo_debates.py", "r") as f:
    text = f.read()

# First, remove the old footer injection logic completely
text = re.sub(r'# Mover o footer image para word/footer1.xml.*?novo_doc = f"""<\?xml version="1.0"', 'novo_doc = f"""<?xml version="1.0"', text, flags=re.DOTALL)

# Now, add the new logic to inject into header1.xml
header_logic = """
    # Mover o footer image para word/header1.xml para que repita em todas as paginas e o Google Docs aceite
    if "word/header1.xml" in file_dict and drawings_xml:
        header_xml = file_dict["word/header1.xml"].decode("utf-8")
        
        # Troca rId7 (do document.xml) para rId2 (do header1.xml)
        drawing_footer = drawings_xml.replace('r:embed="rId7"', 'r:embed="rId2"')
        
        if "</w:hdr>" in header_xml:
            header_xml = header_xml.replace("</w:hdr>", drawing_footer + "</w:hdr>")
        file_dict["word/header1.xml"] = header_xml.encode("utf-8")
        
        # Adicionar rId2 no header1.xml.rels
        if "word/_rels/header1.xml.rels" in file_dict:
            rels_xml = file_dict["word/_rels/header1.xml.rels"].decode("utf-8")
            if "</Relationships>" in rels_xml:
                rels_xml = rels_xml.replace("</Relationships>", '<Relationship Id="rId2" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="media/image2.png"/></Relationships>')
            file_dict["word/_rels/header1.xml.rels"] = rels_xml.encode("utf-8")

    novo_doc = f""\"<?xml version="1.0"
"""

text = text.replace('    novo_doc = f"""<?xml version="1.0"', header_logic)

with open("outros/resumo_debates.py", "w") as f:
    f.write(text)
