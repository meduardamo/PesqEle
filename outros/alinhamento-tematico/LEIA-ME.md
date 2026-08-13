# Alinhamento temático: top 10 por candidato

Monta, para cada perfil monitorado, os temas principais com o número de posts
atrás de cada um, mais um resumo curto do que a conta vem publicando.

## Arquivos

| Arquivo | Para que serve |
|---|---|
| `classificar.py` | roda a coisa toda, é o que vai para produção |
| `classificar.ipynb` | o mesmo fluxo em etapas, para inspecionar antes de gastar chamada |
| `LEIA-ME.md` | este arquivo |

Falta o `credentials.json`: é a conta de serviço do Google com a chave do
Gemini dentro, no campo `GEMINI_API_KEY`. Ele não vai junto por não ser
commitável. Peça e coloque na mesma pasta.

## Instalar

```bash
pip install gspread google-genai google-auth
```

## Antes de rodar: confira se a base está completa

A contagem sai dos posts, então post sem análise é post que não entra no
ranking. Se a base estiver com buraco, quem teve mais post travado aparece
subcontado, e isso não dá erro em lugar nenhum: só sai um número errado.

Cole isto num terminal ou numa célula para conferir:

```python
import gspread
from google.oauth2.service_account import Credentials

esc = ["https://www.googleapis.com/auth/spreadsheets",
       "https://www.googleapis.com/auth/drive"]
creds = Credentials.from_service_account_file("credentials.json", scopes=esc)
sh = gspread.authorize(creds).open_by_key(
    "1piO-m19orW1i-Z-6rNeWdXnEAWqw5wneiDpdHZqOa6Y")

for nome in ("julho", "agosto"):
    v = sh.worksheet(nome).get_all_values()
    i = v[0].index("Temas")
    sem = sum(1 for r in v[1:] if len(r) <= i or not r[i].strip())
    print(f"{nome}: {len(v)-1} posts, {sem} sem temas "
          f"({100*sem/(len(v)-1):.1f}%)")
```

Abaixo de 2% sem temas, pode rodar. Acima disso, o enriquecimento ficou para
trás: é o workflow **16 - Instagram Análise** que preenche essas linhas, e o
**18 - Instagram Backfill** que recupera as que ficaram sem `URL da mídia`.

## Rodar

Comece pelo piloto, sempre. São 78 candidatos e não vale descobrir um erro de
rótulo no candidato 60.

```bash
python classificar.py --candidato "ACM Neto"   # um só, para olhar o resultado
python classificar.py --limite 5               # cinco, para ver se o padrão se sustenta
python classificar.py                          # rodada completa
python classificar.py --forcar                 # refaz quem já está preenchido
```

Sem `--forcar` ele pula quem já tem `Temas Principais`, então dá para
interromper e continuar depois. Grava de 10 em 10 candidatos: se cair no meio,
o que já foi feito está na planilha.

Rodada completa: 10 a 15 minutos, custo abaixo de US$ 0,50.

## Como o resultado é montado

A unidade de contagem é o **post**, não a string de tema.

O script lê as abas mensais (`julho`, `agosto`), onde cada post tem sua própria
lista de temas, e conta em quantos posts cada tema aparece. Só depois disso o
Gemini entra, e só para agrupar variações de escrita do mesmo assunto:
`Segurança`, `Segurança (governança)` e `Segurança pública` viram uma entrada.

Quem soma as contagens é o Python, depois do agrupamento. **O modelo nunca
escreve um número.**

Isso importa porque a coluna `Temas` da aba de alinhamento é o resultado do
`tematica.py` já deduplicado: em 702 temas do ACM Neto há 702 strings distintas,
nenhuma repetida. Nessa lista, um tema de trinta posts e um de um post pesam
igual, e qualquer ranking tirado dali é chute.

## As cinco colunas gravadas

| Coluna | O que traz |
|---|---|
| `Temas Principais` | ranqueado, do mais frequente para o menos |
| `Nº de posts` | em quantos posts o tema aparece |
| `% dos posts em que aparece` | sobre o total analisado; os temas podem se sobrepor |
| `Temas brutos agrupados` | o que entrou em cada tema, para conferir |
| `Resumo da conta` | três frases sobre o que o perfil vem publicando |

A coluna de temas brutos é a que permite auditar. Se um rótulo parecer estranho,
ela mostra exatamente quais temas o alimentaram.

## Regra de rótulo

É o que separa útil de genérico. Quando os temas brutos trazem nome de programa,
obra ou indicador, o rótulo leva junto:

```
Saúde: fila de regulação e PIX Saúde     -> diz o que o candidato fala
Saúde                                     -> não diz nada
```

Programa ou indicador que aparece em apenas um ou dois posts pode enriquecer o
rótulo, mas não vira tema nem aumenta sua contagem. O rótulo final tem no máximo
oito palavras; o Python aplica esse limite mesmo se o modelo não obedecer.

## As guardas

Cinco checagens rodam antes de qualquer coisa ser gravada.

1. **Tema inventado é descartado.** Todo tema bruto que o modelo devolve é
   conferido contra a lista que ele recebeu. O que não estava lá sai. Grupo que
   fica sem nenhum bruto válido some inteiro: um rótulo só sobrevive se tiver
   post real embaixo dele.

2. **Número não vem do modelo.** A frase de abertura do resumo é montada pelo
   Python com o que foi contado. O modelo é proibido de escrever número, e
   qualquer frase dele com dígito é cortada.

3. **Nome próprio é verificado.** Se o resumo citar um programa ou indicador que
   não está na base do candidato, a frase sai.

4. **Ausência não contradiz o ranking.** O modelo só pode descrever como pouco
   presente um assunto abaixo do corte. Se a frase citar um tema do top N, ela é
   descartada.

5. **Variações no mesmo post contam uma vez.** Se um post traz `Saúde` e
   `Saúde Pública`, o grupo Saúde aparece em um post, não em dois.

O log mostra o que foi cortado, com o motivo. Vale ler.

## O que vem filtrado antes do modelo

Dois filtros rodam no Python, na função `contar`:

- **Lista `RUIDO`**, no topo do arquivo: processo de campanha (comício,
  adesivaço, convenção) e emoção solta (gratidão, esperança, orgulho). Fica no
  código, e não no prompt, por dois motivos: dá para discutir item a item com o
  cliente, e o resultado é o mesmo em toda rodada.

- **Nome do candidato e do estado.** `Bahia` aparecia em 50 dos 126 posts do
  ACM Neto e `Pernambuco` em 68 dos 137 da Raquel Lyra. Lideravam a contagem sem
  dizer nada sobre pauta.

A comparação é sempre por igualdade exata do texto normalizado, nunca por
"contém". Se fosse por "contém", `campanha eleitoral` derrubaria junto
`financiamento de campanha`.

Se algo que interessa estiver caindo no filtro, mexa na lista `RUIDO`. O
notebook tem uma célula que mostra o que foi cortado.

## Não precisa rodar nada antes

O script se vira sozinho. A lista de candidatos sai da aba `Instagram`, que é o
cadastro do monitoramento, filtrada por quem tem post coletado. Se a aba
`Alinhamento Temático` não existir, ele cria. Se existir mas faltar candidato,
ele acrescenta a linha.

Usar o cadastro como fonte também resolve um lixo das abas mensais: `PL CE` e
`Solidariedade` aparecem lá na coluna Candidato como se fossem candidatos, e
ficam de fora automaticamente.

## Armadilhas conhecidas

**Se você rodar o `tematica.py` depois, perde o resultado.** Ele reescreve a aba
`Alinhamento Temático` inteira, com `Candidato` e `Temas` apenas, e leva junto
as cinco colunas. O `classificar.py` não depende mais dele, então o mais simples
é não rodar os dois na mesma aba. Se for rodar, `tematica.py` primeiro.

**Nem todo post tem os temas em bullet.** Em 142 posts de agosto os temas
vieram numa linha só, separados por vírgula. A função `temas_do_post` trata os
dois casos. Sem isso, esses posts contam como um tema gigante.

**Post não analisado não entra na conta.** Linha marcada como
`(mídia expirada, não analisado)` ou `(post fora do ar, não analisado)` é
ignorada. O denominador do percentual é o número de posts com tema, não o total
de posts. Como um post pode tratar de mais de um tema, os percentuais das linhas
não devem ser somados e podem ultrapassar 100% no conjunto.

**Candidato com base pequena não é ranqueado.** Abaixo de 20 posts analisados a
linha sai marcada como base pequena, sem top 10. Um ranking sobre 13 posts não
descreve nada, e a planilha não deixa claro que um candidato tem 16 posts e
outro tem 126.

## Se for mexer

Os parâmetros que mais mudam resultado estão no topo do arquivo, todos com
variável de ambiente equivalente:

```python
TOP_N                     10   # quantos temas entram no resultado
MINIMO_POSTS_POR_TEMA      3   # abaixo disso é ruído de rotulação
MINIMO_POSTS_CANDIDATO    20   # abaixo disso o candidato não é ranqueado
```

Suba `MINIMO_POSTS_POR_TEMA` para candidato com base grande. Com 137 posts, um
tema de 3 ainda é barulho.
