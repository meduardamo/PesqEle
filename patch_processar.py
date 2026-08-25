with open("outros/processar_planos.py", "r", encoding="utf-8") as f:
    content = f.read()

# Fix 1: linha_coe
old_linha_coe = """    linha_coe = dict(comum, versao=VERSAO_COERENCIA,
                     resumo=coe["resumo"],
                     pontes=coe.get("pontes_texto", ""))"""
new_linha_coe = """    linha_coe = dict(comum, versao=VERSAO_COERENCIA,
                     resumo=coe["resumo"],
                     pontes=coe.get("pontes_texto", ""),
                     resumos_eixos=coe.get("resumos_eixos", ""))"""
content = content.replace(old_linha_coe, new_linha_coe)

# Fix 2: refazer_coerencia
old_buffer = """            "resumo": coe["resumo"],
            "pontes": coe.get("pontes_texto", ""),"""
new_buffer = """            "resumo": coe["resumo"],
            "pontes": coe.get("pontes_texto", ""),
            "resumos_eixos": coe.get("resumos_eixos", ""),"""
content = content.replace(old_buffer, new_buffer)

# Fix 3: COLS_COE
old_cols = 'COLS_COE = ["ano", "sq_candidato", "candidato", "partido", "uf", "cargo", "link",\n            "resumo", "pontes", "chars", "chars_analisados", "versao", "analisado_em"]'
new_cols = 'COLS_COE = ["ano", "sq_candidato", "candidato", "partido", "uf", "cargo", "link",\n            "resumo", "pontes", "resumos_eixos", "chars", "chars_analisados", "versao", "analisado_em"]'
content = content.replace(old_cols, new_cols)

with open("outros/processar_planos.py", "w", encoding="utf-8") as f:
    f.write(content)

