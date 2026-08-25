#!/bin/sh
# Refaz o resumo dos 33 planos que saíram com linguagem proibida no resumo de
# eixo (conferência de 21/08/2026). Não baixa PDF e não reclassifica tema.
# Como módulo, e não como arquivo: `python3 outros/processar_planos.py` põe
# outros/ no sys.path e o pacote `compartilhado` some.
cd "$(dirname "$0")"
python3 -m outros.processar_planos --so-coerencia --cargo TODOS \
  --credenciais credentials.json \
  --planilha 1Vo-2oa11JpPaYC051Z0UYNR1yJZdhYW4RJeylHfX-bA \
  --sq "$(cat "$1")"
