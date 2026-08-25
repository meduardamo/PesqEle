with open("outros/resumo_debate.py", "r") as f:
    text = f.read()

# Replace the specific instruction for the em dash
old_instruction = "- Comece o parágrafo com o nome do tema em negrito (exemplo: **Segurança pública —** texto...)."
new_instruction = "- Comece o parágrafo com o nome do tema em negrito e dois pontos (exemplo: **Segurança pública:** texto...)."

if old_instruction in text:
    text = text.replace(old_instruction, new_instruction)
    with open("outros/resumo_debate.py", "w") as f:
        f.write(text)
    print("Prompt patched successfully.")
else:
    print("Could not find the instruction in the prompt.")
