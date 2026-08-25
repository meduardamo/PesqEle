import zipfile
import re
from pathlib import Path

def test_fix_footer():
    template_path = "outros/templates/timbrado_eleicoes.docx"
    out_path = "test_footer_out.docx"
    
    with zipfile.ZipFile(template_path, "r") as z_in:
        with zipfile.ZipFile(out_path, "w", zipfile.ZIP_DEFLATED) as z_out:
            drawings_xml = ""
            for item in z_in.infolist():
                conteudo = z_in.read(item.filename)
                
                if item.filename == "word/document.xml":
                    doc_xml_str = conteudo.decode("utf-8")
                    
                    # Find all drawings
                    for m in re.finditer(r"<w:p\b.*?</w:p>", doc_xml_str):
                        p_xml = m.group(0)
                        if "<w:drawing>" in p_xml and "image2.png" in p_xml:
                            drawings_xml += p_xml
                    
                    # Create a sample document body
                    new_body = """<w:body><w:p><w:r><w:t>Hello world, page 1</w:t></w:r></w:p><w:p><w:r><w:br w:type="page"/></w:r></w:p><w:p><w:r><w:t>Page 2 text</w:t></w:r></w:p><w:sectPr><w:headerReference w:type="default" r:id="rId8"/><w:footerReference w:type="default" r:id="rId9"/><w:pgSz w:w="11906" w:h="16838"/><w:pgMar w:top="1134" w:right="1134" w:bottom="1134" w:left="1134" w:header="708" w:footer="708" w:gutter="0"/></w:sectPr></w:body>"""
                    
                    new_doc = re.sub(r"<w:body>.*</w:body>", new_body, doc_xml_str, flags=re.DOTALL)
                    z_out.writestr(item.filename, new_doc)
                    
                elif item.filename == "word/footer1.xml":
                    footer_xml = conteudo.decode("utf-8")
                    
                    if drawings_xml:
                        # Change rId7 to rId1 for the footer relationships
                        drawing_footer = drawings_xml.replace('r:embed="rId7"', 'r:embed="rId1"')
                        
                        # Insert before the last </w:ftr>
                        if "</w:ftr>" in footer_xml:
                            footer_xml = footer_xml.replace("</w:ftr>", drawing_footer + "</w:ftr>")
                    
                    z_out.writestr(item.filename, footer_xml)
                    
                else:
                    z_out.writestr(item.filename, conteudo)
                    
            if drawings_xml:
                # Need to write word/_rels/footer1.xml.rels
                rels_xml = """<?xml version="1.0" encoding="UTF-8" standalone="yes"?>
<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">
  <Relationship Id="rId1" Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/image" Target="media/image2.png"/>
</Relationships>"""
                z_out.writestr("word/_rels/footer1.xml.rels", rels_xml)

test_fix_footer()
print("Generated test_footer_out.docx")
