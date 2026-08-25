import re

with open("footer_test.xml", "r") as f:
    xml = f.read()

# Replace wp:anchor with wp:inline
xml = re.sub(r'<wp:anchor[^>]*>.*?<wp:extent', '<wp:inline distT="0" distB="0" distL="0" distR="0"><wp:extent', xml)
xml = xml.replace('</wp:anchor>', '</wp:inline>')

# Also remove wp:positionH and wp:positionV and wp:wrapNone and wp:simplePos
xml = re.sub(r'<wp:positionH.*?</wp:positionH>', '', xml)
xml = re.sub(r'<wp:positionV.*?</wp:positionV>', '', xml)
xml = re.sub(r'<wp:wrapNone/>', '', xml)
xml = re.sub(r'<wp:simplePos[^>]*/>', '', xml)

with open("footer_inline.xml", "w") as f:
    f.write(xml)
