"""Envio de email transacional pela Brevo.

Estava dentro de relatorios/relatorios_extracao_segmentos.py como _enviar().
Saiu pra cá quando o alerta de notícias (outros/noticias_eleicoes_scraper.py)
passou a mandar email também: dois módulos com a mesma função copiada dão dois
comportamentos diferentes na primeira vez que alguém mexe em um só.

Secrets: BREVO_API_KEY (chave da conta), EMAIL (remetente verificado na Brevo),
DESTINATARIOS (lista separada por vírgula, ponto e vírgula ou espaço).
"""

import os
import re

# Marinho da identidade Eixo. O RGB do manual está errado; o hex é este.
EIXO_MARINHO = "#192D4E"


def destinatarios(*envs, padrao="DESTINATARIOS"):
    """Lê a primeira variável de ambiente preenchida e devolve os emails válidos.

    Aceita vários nomes pra dar lista específica por rotina (ex.:
    DESTINATARIOS_NOTICIAS) sem perder o DESTINATARIOS geral como reserva.
    """
    for env in (*envs, padrao):
        if not env:
            continue
        bruto = re.split(r"[,;\s]+", os.getenv(env, ""))
        emails = [e.strip(" <>") for e in bruto if "@" in e]
        if emails:
            return emails
    return []


def enviar_email(subject, html_body, dests=None):
    """Manda o email pra cada destinatário. Devolve True se pelo menos um saiu.

    Um por vez, e não em cópia: a lista mistura gente de dentro e de fora, e o
    to= com todo mundo junto expõe os endereços entre si.

    Não levanta exceção: quem chama decide o que fazer com o False. No alerta de
    notícias, por exemplo, a rodada continua e a linha fica sem carimbo de envio,
    pra ser reenviada na rodada seguinte.
    """
    api_key, sender = os.getenv("BREVO_API_KEY"), os.getenv("EMAIL")
    dests = dests if dests is not None else destinatarios()
    if not (api_key and sender and dests and html_body):
        print("Config de email incompleta ou sem destinatário válido; pulando envio.")
        return False
    from brevo_python import ApiClient, Configuration
    from brevo_python.api.transactional_emails_api import TransactionalEmailsApi
    from brevo_python.models.send_smtp_email import SendSmtpEmail
    cfg = Configuration()
    cfg.api_key["api-key"] = api_key
    api = TransactionalEmailsApi(ApiClient(configuration=cfg))
    enviados = 0
    for dest in dests:
        try:
            api.send_transac_email(SendSmtpEmail(
                to=[{"email": dest}], sender={"email": sender},
                subject=subject, html_content=html_body))
            print(f"enviado para {dest}")
            enviados += 1
        except Exception as e:
            print(f"falha para {dest}: {e}")
    return enviados > 0
