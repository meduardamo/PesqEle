import os
import re
import json
import time
from datetime import datetime
from typing import List, Dict, Set

import gspread
from google.oauth2.service_account import Credentials as SACredentials
from gspread.exceptions import APIError

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
)

# Secrets:
# - SPREADSHEET_ID: ID da planilha Google
# - GOOGLE_CREDENTIALS_JSON: JSON inteiro da service account
# Pré-requisito: compartilhar a planilha com o e-mail da service account (editor)
#
# Comportamento:
# - Filtra "Eleições Gerais 2026"
# - Varre UFs (ignora Selecione e Brasil)
# - Dedup por numero_identificacao já existente na aba da UF
# - Insere (insert) linhas novas na row=2, sem sobrescrever histórico

URL = "https://pesqele-divulgacao.tse.jus.br/app/pesquisa/listar.xhtml"
ELEICAO_TEXT = "Eleições Gerais 2026"

SPREADSHEET_ID = os.getenv("SPREADSHEET_ID", "").strip()
GOOGLE_CREDENTIALS_JSON = os.getenv("GOOGLE_CREDENTIALS_JSON", "").strip()

SCOPES = [
    "https://www.googleapis.com/auth/spreadsheets",
    "https://www.googleapis.com/auth/drive",
]

HEADLESS = os.getenv("HEADLESS", "true").lower() in {"1", "true", "yes", "y"}
SEL_TIMEOUT = int(os.getenv("SEL_TIMEOUT", "30"))
INSERT_AT_ROW = 2

ID_ELEICAO_LABEL = "formPesquisa:eleicoes_label"
ID_ELEICAO_PANEL = "formPesquisa:eleicoes_panel"

ID_UF_LABEL = "formPesquisa:filtroUF_label"
ID_UF_PANEL = "formPesquisa:filtroUF_panel"

ID_BTN_PESQUISAR = "formPesquisa:idBtnPesquisar"
ID_TBODY = "formPesquisa:tabelaPesquisas_data"
ID_PAGINATOR = "formPesquisa:tabelaPesquisas_paginator_bottom"


def make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    if headless:
        opts.add_argument("--headless=new")
    return webdriver.Chrome(options=opts)


def wait_dom_ready(driver: webdriver.Chrome, timeout: int = 30) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
    )


def safe_click(driver: webdriver.Chrome, wait: WebDriverWait, by: By, value: str, timeout: int = 30):
    el = WebDriverWait(driver, timeout).until(EC.element_to_be_clickable((by, value)))
    try:
        el.click()
        return el
    except ElementClickInterceptedException:
        driver.execute_script("arguments[0].click();", el)
        return el


def force_close_any_menu(driver: webdriver.Chrome):
    try:
        driver.switch_to.active_element.send_keys(Keys.ESCAPE)
    except Exception:
        pass
    try:
        driver.find_element(By.TAG_NAME, "body").click()
    except Exception:
        pass


def open_menu(driver: webdriver.Chrome, wait: WebDriverWait, label_id: str, panel_id: str) -> None:
    safe_click(driver, wait, By.ID, label_id)
    wait.until(EC.presence_of_element_located((By.ID, panel_id)))
    wait.until(EC.visibility_of_element_located((By.ID, panel_id)))


def select_one_menu_by_text(driver: webdriver.Chrome, wait: WebDriverWait, label_id: str, panel_id: str, text: str) -> None:
    open_menu(driver, wait, label_id, panel_id)
    panel = driver.find_element(By.ID, panel_id)
    item = panel.find_element(By.XPATH, f".//li[normalize-space()='{text}']")
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", item)
    try:
        item.click()
    except Exception:
        driver.execute_script("arguments[0].click();", item)
    force_close_any_menu(driver)


def list_one_menu_items(driver: webdriver.Chrome, wait: WebDriverWait, label_id: str, panel_id: str) -> List[str]:
    open_menu(driver, wait, label_id, panel_id)
    panel = driver.find_element(By.ID, panel_id)
    lis = panel.find_elements(By.CSS_SELECTOR, "li.ui-selectonemenu-item")
    items = []
    for li in lis:
        t = (li.text or "").strip()
        if not t:
            continue
        if t.lower() == "selecione":
            continue
        items.append(t)
    force_close_any_menu(driver)
    return items


def click_and_wait_table_refresh(driver: webdriver.Chrome, wait: WebDriverWait, btn_id: str, tbody_id: str) -> None:
    try:
        old_tbody = driver.find_element(By.ID, tbody_id)
    except Exception:
        old_tbody = None

    btn = safe_click(driver, wait, By.ID, btn_id)
    driver.execute_script("arguments[0].scrollIntoView({block:'center'});", btn)
    try:
        btn.click()
    except Exception:
        driver.execute_script("arguments[0].click();", btn)

    if old_tbody is not None:
        try:
            wait.until(EC.staleness_of(old_tbody))
        except TimeoutException:
            pass

    wait.until(EC.presence_of_element_located((By.ID, tbody_id)))


def parse_current_table(driver: webdriver.Chrome, tbody_id: str) -> List[Dict[str, str]]:
    tbody = driver.find_element(By.ID, tbody_id)
    rows = tbody.find_elements(By.XPATH, ".//tr")
    out: List[Dict[str, str]] = []
    for r in rows:
        cols = [c.text.strip() for c in r.find_elements(By.XPATH, "./td")]
        if len(cols) < 5:
            continue
        out.append({
            "numero_identificacao": cols[0],
            "eleicao": cols[1],
            "empresa_contratada": cols[2],
            "data_registro": cols[3],
            "abrangencia": cols[4],
        })
    return out


def dedup_by_numero(rows: List[Dict[str, str]]) -> List[Dict[str, str]]:
    seen = set()
    out = []
    for r in rows:
        k = (r.get("numero_identificacao") or "").strip()
        if not k or k in seen:
            continue
        seen.add(k)
        out.append(r)
    return out


def get_page_numbers(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str) -> List[int]:
    pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
    links = pag.find_elements(By.CSS_SELECTOR, "a.ui-paginator-page")
    nums = []
    for a in links:
        txt = (a.text or "").strip()
        if txt.isdigit():
            nums.append(int(txt))
    return sorted(set(nums))


def go_to_page(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str, tbody_id: str, page_num: int, max_tries: int = 6) -> None:
    last_err = None
    for _ in range(max_tries):
        try:
            pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
            a = pag.find_element(By.CSS_SELECTOR, f"a.ui-paginator-page[aria-label='Page {page_num}']")
            driver.execute_script("arguments[0].scrollIntoView({block:'center'});", a)

            tbody_before = driver.find_element(By.ID, tbody_id)
            driver.execute_script("arguments[0].click();", a)

            try:
                wait.until(EC.staleness_of(tbody_before))
            except TimeoutException:
                pass

            wait.until(EC.presence_of_element_located((By.ID, tbody_id)))
            return

        except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException) as e:
            last_err = e
            time.sleep(0.5)

    raise last_err


def scrape_all_pages_current_query(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str, tbody_id: str) -> List[Dict[str, str]]:
    pages = get_page_numbers(driver, wait, paginator_id)
    if not pages:
        return dedup_by_numero(parse_current_table(driver, tbody_id))

    all_rows: List[Dict[str, str]] = []
    for p in pages:
        go_to_page(driver, wait, paginator_id, tbody_id, p)
        all_rows.extend(parse_current_table(driver, tbody_id))

    return dedup_by_numero(all_rows)


def connect_gsheets() -> gspread.Spreadsheet:
    if not SPREADSHEET_ID:
        raise RuntimeError("SPREADSHEET_ID vazio")

    if not GOOGLE_CREDENTIALS_JSON:
        raise RuntimeError("GOOGLE_CREDENTIALS_JSON vazio")

    info = json.loads(GOOGLE_CREDENTIALS_JSON)
    creds = SACredentials.from_service_account_info(info, scopes=SCOPES)
    gc = gspread.authorize(creds)
    return gc.open_by_key(SPREADSHEET_ID)


def sheet_safe(name: str) -> str:
    s = re.sub(r"[\[\]\:\*\?\/\\]", "-", (name or "").strip())
    return s[:100]


def ensure_worksheet(ss: gspread.Spreadsheet, title: str, header: List[str]) -> gspread.Worksheet:
    title = sheet_safe(title)
    try:
        ws = ss.worksheet(title)
    except gspread.WorksheetNotFound:
        ws = ss.add_worksheet(title=title, rows=2000, cols=max(10, len(header)))

    values = ws.get_all_values()
    if not values:
        ws.insert_row(header, index=1)

    return ws


def read_existing_ids(ws: gspread.Worksheet, id_col_name: str = "numero_identificacao") -> Set[str]:
    values = ws.get_all_values()
    if not values or len(values) < 2:
        return set()

    header = values[0]
    try:
        idx = header.index(id_col_name)
    except ValueError:
        return set()

    existing = set()
    for row in values[1:]:
        if idx < len(row):
            v = (row[idx] or "").strip()
            if v:
                existing.add(v)
    return existing


def insert_rows_batched(ws: gspread.Worksheet, rows: List[List[str]], insert_at: int = 2, batch_size: int = 200) -> None:
    i = 0
    while i < len(rows):
        chunk = rows[i:i + batch_size]
        try:
            ws.insert_rows(chunk, row=insert_at, value_input_option="RAW")
        except APIError:
            time.sleep(2)
            ws.insert_rows(chunk, row=insert_at, value_input_option="RAW")
        i += batch_size


def run_daily_scrape_to_sheets(eleicao_text: str = ELEICAO_TEXT, headless: bool = HEADLESS) -> None:
    ss = connect_gsheets()
    driver = make_driver(headless=headless)
    wait = WebDriverWait(driver, SEL_TIMEOUT)

    now_str = datetime.now().strftime("%d/%m/%Y %H:%M:%S")
    header = [
        "numero_identificacao",
        "eleicao",
        "empresa_contratada",
        "data_registro",
        "abrangencia",
        "uf_filtro",
        "capturado_em",
    ]

    try:
        driver.get(URL)
        wait_dom_ready(driver, timeout=SEL_TIMEOUT)

        select_one_menu_by_text(driver, wait, ID_ELEICAO_LABEL, ID_ELEICAO_PANEL, eleicao_text)

        ufs = list_one_menu_items(driver, wait, ID_UF_LABEL, ID_UF_PANEL)
        ufs = [u for u in ufs if u.upper() not in {"BRASIL"}]

        for uf in ufs:
            uf_clean = uf.strip()
            if not uf_clean:
                continue

            ws = ensure_worksheet(ss, title=uf_clean, header=header)
            existing_ids = read_existing_ids(ws, id_col_name="numero_identificacao")

            select_one_menu_by_text(driver, wait, ID_UF_LABEL, ID_UF_PANEL, uf_clean)
            click_and_wait_table_refresh(driver, wait, ID_BTN_PESQUISAR, ID_TBODY)

            rows = scrape_all_pages_current_query(driver, wait, ID_PAGINATOR, ID_TBODY)

            new_rows = []
            for r in rows:
                rid = (r.get("numero_identificacao") or "").strip()
                if not rid or rid in existing_ids:
                    continue

                new_rows.append([
                    rid,
                    r.get("eleicao", ""),
                    r.get("empresa_contratada", ""),
                    r.get("data_registro", ""),
                    r.get("abrangencia", ""),
                    uf_clean,
                    now_str,
                ])

            if new_rows:
                insert_rows_batched(ws, new_rows, insert_at=INSERT_AT_ROW, batch_size=200)

            time.sleep(0.4)

    finally:
        driver.quit()


if __name__ == "__main__":
    run_daily_scrape_to_sheets(eleicao_text=ELEICAO_TEXT, headless=HEADLESS)
