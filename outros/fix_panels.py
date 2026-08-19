import glob
import re

def main():
    files = glob.glob('/Users/eduardamarques/painel-eleitoral-*/pages/*Planos_de_Governo.py')
    
    replacements = [
        (r'COR_NIVEL\[_n\]', r'COR_NIVEL.get(_n, EIXO["borda"])'),
        (r'COR_NIVEL\[_niv\]', r'COR_NIVEL.get(_niv, EIXO["borda"])'),
        (r'COR_NIVEL\[r\["Nível"\]\]', r'COR_NIVEL.get(r["Nível"], EIXO["borda"])'),
    ]
    
    for fpath in files:
        with open(fpath, 'r', encoding='utf-8') as f:
            content = f.read()
            
        new_content = content
        for old, new in replacements:
            new_content = re.sub(old, new, new_content)
            
        if new_content != content:
            with open(fpath, 'w', encoding='utf-8') as f:
                f.write(new_content)
            print(f"Fixed {fpath}")
        else:
            print(f"No changes in {fpath}")

if __name__ == '__main__':
    main()
