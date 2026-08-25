import re

with open("outros/resumo_debates.py", "r") as f:
    text = f.read()

# We need to find the footer logic and add the regex to convert wp:anchor to wp:inline
footer_logic_old = """
        # Troca rId7 (do document.xml) para rId1 (do footer1.xml)
        drawing_footer = drawings_xml.replace('r:embed="rId7"', 'r:embed="rId1"')
        # Tira a parte de wp:anchor se houver conflitos com footer? Nao, mantem
        if "</w:ftr>" in footer_xml:
            footer_xml = footer_xml.replace("</w:ftr>", drawing_footer + "</w:ftr>")
"""

footer_logic_new = """
        import re
        # Troca rId7 (do document.xml) para rId1 (do footer1.xml)
        drawing_footer = drawings_xml.replace('r:embed="rId7"', 'r:embed="rId1"')
        
        # O Google Docs NÃO suporta wp:anchor (imagens flutuantes fixas) no footer direito,
        # então vamos converter o xml da imagem para wp:inline (imagem em linha)
        drawing_footer = re.sub(r'<wp:anchor[^>]*>.*?<wp:extent', '<wp:inline distT="0" distB="0" distL="0" distR="0"><wp:extent', drawing_footer)
        drawing_footer = drawing_footer.replace('</wp:anchor>', '</wp:inline>')
        drawing_footer = re.sub(r'<wp:positionH.*?</wp:positionH>', '', drawing_footer)
        drawing_footer = re.sub(r'<wp:positionV.*?</wp:positionV>', '', drawing_footer)
        drawing_footer = re.sub(r'<wp:wrapNone/>', '', drawing_footer)
        drawing_footer = re.sub(r'<wp:simplePos[^>]*/>', '', drawing_footer)

        if "</w:ftr>" in footer_xml:
            footer_xml = footer_xml.replace("</w:ftr>", drawing_footer + "</w:ftr>")
"""

if footer_logic_old in text:
    text = text.replace(footer_logic_old, footer_logic_new)
    with open("outros/resumo_debates.py", "w") as f:
        f.write(text)
    print("Patched successfully")
else:
    print("Could not find footer logic")
