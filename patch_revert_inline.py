import re

with open("outros/resumo_debates.py", "r") as f:
    text = f.read()

# We want to remove the conversion to wp:inline
logic_to_remove = """
        # O Google Docs NÃO suporta wp:anchor (imagens flutuantes fixas) no footer direito,
        # então vamos converter o xml da imagem para wp:inline (imagem em linha)
        drawing_footer = re.sub(r'<wp:anchor[^>]*>.*?<wp:extent', '<wp:inline distT="0" distB="0" distL="0" distR="0"><wp:extent', drawing_footer)
        drawing_footer = drawing_footer.replace('</wp:anchor>', '</wp:inline>')
        drawing_footer = re.sub(r'<wp:positionH.*?</wp:positionH>', '', drawing_footer)
        drawing_footer = re.sub(r'<wp:positionV.*?</wp:positionV>', '', drawing_footer)
        drawing_footer = re.sub(r'<wp:wrapNone/>', '', drawing_footer)
        drawing_footer = re.sub(r'<wp:simplePos[^>]*/>', '', drawing_footer)
"""

if logic_to_remove in text:
    text = text.replace(logic_to_remove, "")
    with open("outros/resumo_debates.py", "w") as f:
        f.write(text)
    print("Reverted inline conversion")
else:
    print("Could not find logic")
