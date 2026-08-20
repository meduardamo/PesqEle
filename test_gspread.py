from outros.transcricao_debates import clientes_google, PLANILHA, ABA
gc, _ = clientes_google()
ws = gc.open_by_key(PLANILHA).worksheet("debates")
print("debates headers:", ws.get_all_values()[0])

from outros.transcricao_debates import FONTE_PLANILHA, FONTE_ABA
ws_fonte = gc.open_by_key(FONTE_PLANILHA).worksheet(FONTE_ABA)
print("fonte headers:", ws_fonte.get_all_values()[0])
