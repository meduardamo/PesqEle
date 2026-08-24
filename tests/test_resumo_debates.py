"""O que o resumo mede em Python, preso como teste.

O texto que o modelo redige não é testável aqui, e é justamente por isso que
tudo que vira número no resumo (duração, tempo por candidato, minutos por
assunto) é contado antes dele. Os casos abaixo são o CSV do debate da Band de
SP de 09/08/2026 em miniatura, com os mesmos formatos de coluna.

A guarda do parágrafo tem teste próprio: ela é o que separa 'o modelo escreveu
bonito' de 'o número está na fala', e é a que não pode afrouxar sem alguém ver.

Uso:
    pytest tests/test_resumo_debates.py
"""

from outros.resumo_debates import (abertura, conferir_paragrafo, duracoes,
                                   elenco, fechamento, medir, meta_da_linha,
                                   num, para_conferir)

FALAS = [
    {"segundos": "0", "tempo": "00:00:00", "falante": "RODOLFO SCHNEIDER",
     "fala": "Boa noite, começa agora o debate", "eixo": ""},
    {"segundos": "60", "tempo": "00:01:00", "falante": "FERNANDO HADDAD",
     "fala": "A alfabetização em São Paulo está em 61%, abaixo da média nacional de 66%",
     "eixo": "Educação"},
    {"segundos": "240", "tempo": "00:04:00", "falante": "TARCISIO DE FREITAS",
     "fala": "O ensino técnico saiu de 12% para 42% dos alunos da rede",
     "eixo": "Educação"},
    {"segundos": "420", "tempo": "00:07:00", "falante": "FERNANDO HADDAD",
     "fala": "O feminicídio cresceu 10% no semestre no estado",
     "eixo": "Segurança pública; Direitos humanos e igualdade"},
    {"segundos": "600", "tempo": "00:10:00", "falante": "TARCISIO DE FREITAS",
     "fala": "Temos a menor taxa de homicídios da história de São Paulo",
     "eixo": "Segurança pública"},
]


def test_duracao_da_fala_vai_ate_a_seguinte():
    assert duracoes(FALAS)[:4] == [60, 180, 180, 180]


def test_ultima_fala_estimada_pelo_ritmo_e_nao_zerada():
    assert duracoes(FALAS)[-1] > 0


def test_tempo_por_candidato_soma_os_turnos_dele():
    m = medir(FALAS)
    assert m["por_falante"]["FERNANDO HADDAD"] == 360
    assert m["por_falante"]["TARCISIO DE FREITAS"] == 180 + duracoes(FALAS)[-1]


def test_fala_com_dois_eixos_conta_inteira_nos_dois():
    # É o que faz a soma dos assuntos passar da duração do debate, e o texto
    # do resumo diz isso onde os minutos aparecem.
    m = medir(FALAS)
    assert m["por_eixo"]["Segurança pública"] >= 180
    assert m["por_eixo"]["Direitos humanos e igualdade"] == 180


def test_planilha_manda_no_elenco_e_o_mediador_nao_vira_candidato():
    cands, med = elenco(medir(FALAS),
                        participantes=["Fernando Haddad", "Tarcísio de Freitas"],
                        mediador="Rodolfo Schneider")
    assert set(cands) == {"FERNANDO HADDAD", "TARCISIO DE FREITAS"}
    assert med == "RODOLFO SCHNEIDER"


def test_sem_planilha_o_corte_de_tempo_separa_candidato_de_mediador():
    cands, med = elenco(medir(FALAS))
    assert set(cands) == {"FERNANDO HADDAD", "TARCISIO DE FREITAS"}
    assert med == "RODOLFO SCHNEIDER"


def test_abertura_usa_a_grafia_da_planilha_e_nao_a_da_transcricao():
    # O modelo transcreveu 'TARCISIO', sem acento; num texto para cliente o
    # nome sai como a planilha escreve.
    medida = medir(FALAS)
    cands, med = elenco(medida, ["Fernando Haddad", "Tarcísio de Freitas"],
                        "Rodolfo Schneider")
    texto = abertura({"data": "09/08/2026", "emissora": "Band", "cargo": "governador",
                      "uf": "SP", "turno": "1", "ordinal": 1}, medida, cands, med)
    assert "Tarcísio de Freitas" in texto
    assert "TARCISIO" not in texto
    assert texto.startswith("No domingo, 9 de agosto de 2026")


ACHADOS = [
    {"falante": "FERNANDO HADDAD", "categoria": "Contestação", "tempo": "00:01:00",
     "resumo": "Diz que a alfabetização está em 61%, contra 66% no país",
     "trecho": "A alfabetização em São Paulo está em 61%, abaixo da média nacional de 66%"},
    {"falante": "TARCISIO DE FREITAS", "categoria": "Balanço", "tempo": "00:04:00",
     "resumo": "Afirma que o ensino técnico foi de 12% para 42%",
     "trecho": "O ensino técnico saiu de 12% para 42% dos alunos da rede"},
]


def test_paragrafo_com_numero_da_fonte_passa():
    texto = ("Haddad citou alfabetização de 61% contra 66% no país, e Tarcísio "
             "respondeu com o ensino técnico de 12% para 42%.")
    assert conferir_paragrafo(texto, ACHADOS, ["FERNANDO HADDAD", "TARCISIO DE FREITAS"]) == ""


def test_paragrafo_com_numero_inventado_e_reprovado():
    # O erro típico do modelo: arredondar ou somar por conta própria.
    texto = "Haddad citou alfabetização de 60% e Tarcísio falou em 45% no técnico."
    assert "número fora" in conferir_paragrafo(
        texto, ACHADOS, ["FERNANDO HADDAD", "TARCISIO DE FREITAS"])


def test_paragrafo_sem_nenhum_participante_e_reprovado():
    texto = "O governo federal afirmou que a alfabetização está em 61%."
    assert conferir_paragrafo(
        texto, ACHADOS, ["FERNANDO HADDAD", "TARCISIO DE FREITAS"])


def test_lista_de_conferencia_traz_horario_e_citacao():
    linhas = para_conferir({"Educação": ACHADOS})
    assert linhas and linhas[0].startswith("- [00:")
    assert "61%" in " ".join(linhas)


COL = {"id": 0, "data": 1, "cargo": 2, "uf": 3, "turno": 4, "emissora": 5,
       "mediador": 6, "participantes": 7, "status": 8, "link_csv": 9}
TODAS = [
    ["id", "data", "cargo", "uf", "turno", "emissora", "mediador",
     "participantes", "status", "link_csv"],
    ["sp1", "2026-08-09", "governador", "SP", "1", "Band", "Rodolfo Schneider",
     "Fernando Haddad e Tarcísio de Freitas", "pronto", "link1"],
    ["sp2", "2026-09-20", "governador", "SP", "1", "Globo", "Bonner",
     "Fernando Haddad e Tarcísio de Freitas", "pronto", "link2"],
    ["mg1", "2026-08-16", "governador", "MG", "1", "Band", "Mediador",
     "A e B", "pronto", "link3"],
]


def test_ordinal_conta_os_debates_anteriores_do_mesmo_cargo_uf_e_turno():
    # O segundo debate de SP é o 2º; o de MG do meio não entra na conta.
    assert meta_da_linha(TODAS[1], COL, TODAS)["ordinal"] == 1
    assert meta_da_linha(TODAS[2], COL, TODAS)["ordinal"] == 2
    assert meta_da_linha(TODAS[3], COL, TODAS)["ordinal"] == 1


def test_participantes_saem_separados_do_campo_da_planilha():
    assert meta_da_linha(TODAS[1], COL, TODAS)["participantes"] == [
        "Fernando Haddad", "Tarcísio de Freitas"]


def test_fechamento_conta_so_candidato_e_concorda_no_singular():
    # O mediador tem fala em quase todo eixo e apareceria na conta de quem
    # atacou quem, que é justamente o que a frase responde.
    achados = ACHADOS + [{"falante": "RODOLFO SCHNEIDER", "categoria": "Balanço",
                          "tempo": "00:00:00", "resumo": "encaminha o bloco",
                          "trecho": "vamos ao segundo bloco do debate"}]
    texto = fechamento({"Educação": achados},
                       ["FERNANDO HADDAD", "TARCISIO DE FREITAS"])
    assert "Rodolfo" not in texto
    assert "1 balanço de gestão" in texto and "1 balanços" not in texto
    assert "1 contestação" in texto


def test_minuto_sai_com_virgula_e_sem_zero_a_toa():
    assert num(53.42) == "53,4"
    assert num(52.0) == "52"


class _AbaFalsa:
    """Aba de planilha só com o que coluna_por_nome usa."""

    def __init__(self, col_count=20):
        self.col_count = col_count
        self.escritas = []

    def update_cell(self, linha, coluna, valor):
        self.escritas.append((linha, coluna, valor))


def test_coluna_nova_entra_no_fim_e_a_seguinte_nao_repete_o_indice(monkeypatch):
    # As duas colunas de saída (link e texto) são pedidas a partir do mesmo
    # cabeçalho lido uma vez. Sem atualizar a lista, a segunda cairia em cima
    # da primeira e o texto do resumo sobrescreveria o link do Drive.
    from outros import resumo_debates as rd
    import outros.transcricao_debates as td
    monkeypatch.setattr(td, "com_retentativa", lambda _d, fn: fn())
    ws = _AbaFalsa()
    cabecalho = ["id", "data", "cargo"]
    assert rd.coluna_por_nome(ws, cabecalho, "link_resumo") == 4
    assert rd.coluna_por_nome(ws, cabecalho, "resumo_md") == 5
    assert cabecalho[-2:] == ["link_resumo", "resumo_md"]


def test_coluna_que_ja_existe_nao_e_criada_de_novo(monkeypatch):
    from outros import resumo_debates as rd
    import outros.transcricao_debates as td
    monkeypatch.setattr(td, "com_retentativa", lambda _d, fn: fn())
    ws = _AbaFalsa()
    cabecalho = ["id", "link_resumo", "resumo_md"]
    assert rd.coluna_por_nome(ws, cabecalho, "resumo_md") == 3
    assert ws.escritas == []


def test_titulo_concorda_com_o_artigo_do_estado():
    # A capa do documento vai para o cliente: "ao governo de Ceará" e a data em
    # aaaa-mm-dd eram as duas coisas que apareciam lá.
    from outros.resumo_debates import cargo_por_extenso, titulo
    assert cargo_por_extenso("Governador", "CE") == "ao governo do Ceará"
    assert cargo_por_extenso("Governador", "BA") == "ao governo da Bahia"
    assert cargo_por_extenso("Governador", "SP") == "ao governo de São Paulo"
    assert cargo_por_extenso("Senador", "RJ") == "ao Senado pelo Rio de Janeiro"
    assert cargo_por_extenso("Senador", "SP") == "ao Senado por São Paulo"
    assert titulo({"ordinal": 1, "emissora": "Band", "data": "2026-08-09",
                   "cargo": "Governador", "uf": "SP"}) == (
        "Resumo do 1º debate ao governo de São Paulo, Band, 09/08/2026")
    assert titulo({"emissora": "PontoPoder", "data": "20/08/2026",
                   "cargo": "Governador", "uf": "CE"}, True) == (
        "Resumo da sabatina ao governo do Ceará, PontoPoder, 20/08/2026")
