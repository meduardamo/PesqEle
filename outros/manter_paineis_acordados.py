"""
Abre os painéis Streamlit num navegador headless para eles não dormirem.

Por que existe: no Streamlit Community Cloud o app hiberna depois de um tempo
sem tráfego e o visitante cai na tela "This app has gone to sleep due to
inactivity", com botão para acordar e ~30s de boot. Um cliente que abre o link
e vê isso já viu o painel quebrado.

Por que navegador e não curl: o que conta como tráfego para o Community Cloud é
sessão de usuário, que só existe quando o WebSocket de /_stcore/stream abre.
Um GET no HTML devolve a casca da página sem abrir sessão nenhuma.

De quebra serve de teste de vida: se o app subir com erro (dependência,
credencial, planilha), o container renderiza a tela de exceção do Streamlit e
aqui vira falha do workflow, com e-mail do Actions.

URLs em PAINEIS, separadas por vírgula ou quebra de linha.
"""

import os
import sys

from playwright.sync_api import sync_playwright

# Tempo segurando a sessão aberta depois que a página renderizou. O Cloud marca
# atividade pela sessão viva, não pelo hit; fechar na hora arrisca não contar.
SEGURAR_MS = 20_000

# Boot de app hibernado no Community Cloud leva de 20s a 1min: instala o
# requirements de novo antes de servir a primeira tela.
ESPERA_APP_MS = 120_000

# Marcadores da tela de hibernação. O texto do botão já mudou mais de uma vez,
# então procuramos qualquer um deles antes de desistir.
BOTOES_ACORDAR = [
    "Yes, get this app back up!",
    "get this app back up",
    "Wake up",
]

# Container raiz do app renderizado. Presente em qualquer página Streamlit,
# inclusive na de exceção.
SELETOR_APP = '[data-testid="stAppViewContainer"]'

# O Community Cloud não serve o app direto: <nome>.streamlit.app é uma casca
# com o app dentro de um iframe em /~/+/. Procurar o container na página de
# fora não acha nada, o body dela vem vazio.
CAMINHO_IFRAME = "/~/+/"

# Se aparecer no corpo da página, o app subiu quebrado.
ERROS_NA_TELA = [
    "Error installing requirements",
    "ModuleNotFoundError",
    "StreamlitAPIException",
    "This app has encountered an error",
    "Oh no.",
]


def urls():
    bruto = os.environ.get("PAINEIS", "")
    return [u.strip() for u in bruto.replace(",", "\n").splitlines() if u.strip()]


def acordar(pagina):
    """Clica no botão de acordar, se a tela de hibernação estiver na frente."""
    for texto in BOTOES_ACORDAR:
        botao = pagina.get_by_text(texto, exact=False)
        if botao.count() and botao.first.is_visible():
            print("    hibernado, acordando")
            botao.first.click()
            return True
    return False


def visitar(navegador, url):
    pagina = navegador.new_page()
    try:
        pagina.goto(url, wait_until="domcontentloaded", timeout=ESPERA_APP_MS)
        pagina.wait_for_timeout(3_000)   # a tela de hibernação demora a pintar

        if acordar(pagina):
            pagina.wait_for_timeout(5_000)

        app = pagina.frame_locator(f'iframe[src*="{CAMINHO_IFRAME}"]')
        app.locator(SELETOR_APP).wait_for(timeout=ESPERA_APP_MS)
        pagina.wait_for_timeout(SEGURAR_MS)

        corpo = app.locator("body").inner_text()
        for erro in ERROS_NA_TELA:
            if erro in corpo:
                print(f"    FALHA: app subiu com erro na tela ({erro})")
                return False

        print(f"    ok, {len(corpo)} caracteres na tela")
        return True

    except Exception as e:
        print(f"    FALHA: {type(e).__name__}: {e}")
        return False
    finally:
        pagina.close()


def main():
    alvos = urls()
    if not alvos:
        print("PAINEIS vazio, nada a fazer.")
        return 1

    falhas = []
    with sync_playwright() as p:
        navegador = p.chromium.launch()
        for url in alvos:
            print(url)
            if not visitar(navegador, url):
                falhas.append(url)
        navegador.close()

    if falhas:
        print(f"\n{len(falhas)} de {len(alvos)} painéis não responderam:")
        for url in falhas:
            print(f"  {url}")
        return 1

    print(f"\n{len(alvos)} painéis de pé.")
    return 0


if __name__ == "__main__":
    sys.exit(main())
