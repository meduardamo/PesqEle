with open("outros/resumo_debates.py", "r") as f:
    text = f.read()

# Remove drawings_xml from document body
text = text.replace("{drawings_xml}\n{sect_xml}", "{sect_xml}")

# Inject into footer1.xml processing
footer_logic = """
    # Mover o footer image para word/footer1.xml
    if "word/footer1.xml" in file_dict and drawings_xml:
        footer_xml = file_dict["word/footer1.xml"].decode("utf-8")
        # Troca rId7 (do document.xml) para rId1 (do footer1.xml)
        drawing_footer = drawings_xml.replace('r:embed="rId7"', 'r:embed="rId1"')
        # Tira a parte de wp:anchor se houver conflitos com footer? Nao, mantem
        if "</w:ftr>" in footer_xml:
            footer_xml = footer_xml.replace("</w:ftr>", drawing_footer + "</w:ftr>")
        file_dict["word/footer1.xml"] = footer_xml.encode("utf-8")
        
        # Cria as dependencias do footer1.xml
        rels_xml = '<?xml version="1.0" encoding="UTF-8" standalone="yes"?><Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships"><Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="media/image2.png"/></Relationships>'
        file_dict["word/_rels/footer1.xml.rels"] = rels_xml.encode("utf-8")

    novo_doc = f""\"<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
"""

text = text.replace('    novo_doc = f"""<?xml version="1.0" encoding="UTF-8" standalone="yes"?>\n', footer_logic)

with open("outros/resumo_debates.py", "w") as f:
    f.write(text)
