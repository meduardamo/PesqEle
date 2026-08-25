import re
from pathlib import Path

for md_file in Path("resumos_markdown/resumos-18").glob("*.md"):
    text = md_file.read_text(encoding="utf-8")
    # Replace "**Topic —**" with "**Topic:**"
    text = re.sub(r'\*\*(.*?)\s*[—-]\s*\*\*', r'**\1:**', text)
    md_file.write_text(text, encoding="utf-8")
    print(f"Fixed dashes in {md_file.name}")
