import os
import re
import time
from datetime import datetime, timedelta
from typing import List, Dict, Optional

import pandas as pd
from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support.ui import WebDriverWait
from selenium.webdriver.support import expected_conditions as EC
from selenium.common.exceptions import (
    TimeoutException,
    ElementClickInterceptedException,
    StaleElementReferenceException,
    NoSuchElementException,
    WebDriverException,
)

import gspread
from google.oauth2.service_account import Credentials


URL_LISTAR = "https://pesqele-divulgacao.tse.jus.br/app/pesquisa/listar.xhtml"

ID_ELEICAO_LABEL = "formPesquisa:eleicoes_label"
ID_ELEICAO_PANEL = "formPesquisa:eleicoes_panel"

ID_UF_LABEL = "formPesquisa:filtroUF_label"
ID_UF_PANEL = "formPesquisa:filtroUF_panel"

ID_BTN_PESQUISAR = "formPesquisa:idBtnPesquisar"

ID_TBODY = "formPesquisa:tabelaPesquisas_data"
ID_PAGINATOR = "formPesquisa:tabelaPesquisas_paginator_bottom"

CREDS_PATH = "credentials.json"

HEADER_ROW = 3
DATA_START_ROW = 4

COLS_BASE = [
    "numero_identificacao",
    "eleicao",
    "empresa_contratada",
    "data_registro",
    "abrangencia",
    "data_divulgacao",
    "uf_filtro",
    "capturado_em",
]

SKIP_SHEETS = {"Dashboard"}


def env_bool(name: str, default: bool = False) -> bool:
    v = os.getenv(name)
    if v is None:
        return default
    return str(v).strip().lower() in {"1", "true", "yes", "y", "sim"}


def now_with_offset() -> datetime:
    add_hours = int(os.getenv("ADD_HOURS", "0"))
    return datetime.now() + timedelta(hours=add_hours)


def make_driver(headless: bool = True) -> webdriver.Chrome:
    opts = webdriver.ChromeOptions()

    # no GH Actions isso é obrigatório na prática
    opts.add_argument("--no-sandbox")
    opts.add_argument("--disable-dev-shm-usage")
    opts.add_argument("--disable-gpu")
    opts.add_argument("--window-size=1920,1080")
    opts.add_argument("--lang=pt-BR")

    if headless:
        opts.add_argument("--headless=new")

    # evita profile persistente no Actions (pasta pode dar permissão/lock)
    # se você quiser usar profile localmente, rode com USE_PROFILE=1 e CHROME_PROFILE_DIR=...
    if env_bool("USE_PROFILE", False):
        profile_dir = os.getenv("CHROME_PROFILE_DIR", "./chrome-profile-pesqele")
        opts.add_argument(f"--user-data-dir={os.path.abspath(profile_dir)}")

    return webdriver.Chrome(options=opts)


def wait_dom_ready(driver: webdriver.Chrome, timeout: int = 60) -> None:
    WebDriverWait(driver, timeout).until(
        lambda d: d.execute_script("return document.readyState") in ("interactive", "complete")
    )


def js_click(driver: webdriver.Chrome, el) -> None:
    driver.execute_script("arguments[0].click();", el)


def scroll_center(driver: webdriver.Chrome, el) -> None:
    driver.execute_script("arguments[0].scrollIntoView({block:'center', inline:'center'});", el)


def force_close_any_menu(driver: webdriver.Chrome) -> None:
    try:
        driver.switch_to.active_element.send_keys(Keys.ESCAPE)
    except Exception:
        pass
    try:
        driver.find_element(By.TAG_NAME, "body").send_keys(Keys.ESCAPE)
    except Exception:
        pass


def dismiss_overlays_best_effort(driver: webdriver.Chrome) -> None:
    # best-effort: cookie banners / dialogs comuns
    xpaths = [
        "//button[contains(translate(.,'ACEITARCONCORDOFECHAROK','aceitarconcordofecharok'),'aceitar')]",
        "//button[contains(translate(.,'ACEITARCONCORDOFECHAROK','aceitarconcordofecharok'),'concordo')]",
        "//button[contains(translate(.,'ACEITARCONCORDOFECHAROK','aceitarconcordofecharok'),'fechar')]",
        "//button[contains(translate(.,'ACEITARCONCORDOFECHAROK','aceitarconcordofecharok'),'ok')]",
        "//a[contains(translate(.,'FECHAR','fechar'),'fechar')]",
    ]
    for xp in xpaths:
        try:
            els = driver.find_elements(By.XPATH, xp)
            if els:
                try:
                    scroll_center(driver, els[0])
                    js_click(driver, els[0])
                    time.sleep(0.2)
                except Exception:
                    pass
        except Exception:
            pass


def open_menu(driver: webdriver.Chrome, wait: WebDriverWait, label_id: str, panel_id: str) -> None:
    # PrimeFaces: às vezes clicar no label não abre o panel no headless
    # aqui fazemos retries + JS click + tentativa de clicar no "trigger" interno.
    last_err = None
    for _ in range(8):
        try:
            dismiss_overlays_best_effort(driver)
            force_close_any_menu(driver)

            label = wait.until(EC.presence_of_element_located((By.ID, label_id)))
            scroll_center(driver, label)

            try:
                label.click()
            except Exception:
                js_click(driver, label)

            # tentativa extra: clicar no botão gatilho dentro do componente, se existir
            try:
                wrapper = driver.find_element(By.ID, label_id).find_element(By.XPATH, "./ancestor::*[contains(@class,'ui-selectonemenu')]")
                trig = wrapper.find_elements(By.CSS_SELECTOR, ".ui-selectonemenu-trigger")
                if trig:
                    scroll_center(driver, trig[0])
                    js_click(driver, trig[0])
            except Exception:
                pass

            # o panel pode existir e só não estar visível ainda; checa presença + visibilidade
            wait.until(EC.presence_of_element_located((By.ID, panel_id)))
            panel = driver.find_element(By.ID, panel_id)
            if panel.is_displayed():
                return

            # às vezes abre e fecha rápido: dá um respiro e tenta de novo
            time.sleep(0.25)

        except Exception as e:
            last_err = e
            time.sleep(0.35)

    raise TimeoutException(f"Falha ao abrir menu {label_id} -> {panel_id}. Último erro: {last_err}")


def select_one_menu_by_text(driver: webdriver.Chrome, wait: WebDriverWait, label_id: str, panel_id: str, text: str) -> None:
    open_menu(driver, wait, label_id, panel_id)
    panel = driver.find_element(By.ID, panel_id)

    # normaliza espaços e tenta match exato; fallback para contains
    target = None
    try:
        target = panel.find_element(By.XPATH, f".//li[contains(@class,'ui-selectonemenu-item')][normalize-space()='{text}']")
    except NoSuchElementException:
        try:
            target = panel.find_element(By.XPATH, f".//li[contains(@class,'ui-selectonemenu-item')][contains(normalize-space(),'{text}')]")
        except NoSuchElementException:
            target = None

    if target is None:
        force_close_any_menu(driver)
        raise NoSuchElementException(f"Item '{text}' não encontrado no menu {label_id}")

    scroll_center(driver, target)
    try:
        target.click()
    except Exception:
        js_click(driver, target)

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
        if t.lower() in {"selecione"}:
            continue
        items.append(t)

    force_close_any_menu(driver)
    return items


def click_and_wait_table_refresh(driver: webdriver.Chrome, wait: WebDriverWait, btn_id: str, tbody_id: str) -> None:
    try:
        old_tbody = driver.find_element(By.ID, tbody_id)
    except Exception:
        old_tbody = None

    btn = wait.until(EC.presence_of_element_located((By.ID, btn_id)))
    scroll_center(driver, btn)

    try:
        btn.click()
    except ElementClickInterceptedException:
        js_click(driver, btn)
    except Exception:
        js_click(driver, btn)

    if old_tbody is not None:
        try:
            wait.until(EC.staleness_of(old_tbody))
        except TimeoutException:
            pass

    wait.until(EC.presence_of_element_located((By.ID, tbody_id)))


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


def get_active_page(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str) -> Optional[int]:
    try:
        pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
        active = pag.find_elements(By.CSS_SELECTOR, "span.ui-paginator-page.ui-state-active")
        if not active:
            active = pag.find_elements(By.CSS_SELECTOR, "a.ui-paginator-page.ui-state-active")
        if not active:
            return None
        txt = (active[0].text or "").strip()
        return int(txt) if txt.isdigit() else None
    except Exception:
        return None


def go_to_page(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str, tbody_id: str, page_num: int, max_tries: int = 8) -> None:
    last_err = None
    for _ in range(max_tries):
        try:
            pag = wait.until(EC.presence_of_element_located((By.ID, paginator_id)))
            a = pag.find_element(By.CSS_SELECTOR, f"a.ui-paginator-page[aria-label='Page {page_num}']")
            scroll_center(driver, a)

            tbody_before = driver.find_element(By.ID, tbody_id)
            js_click(driver, a)

            try:
                wait.until(EC.staleness_of(tbody_before))
            except TimeoutException:
                pass

            wait.until(EC.presence_of_element_located((By.ID, tbody_id)))
            return
        except (StaleElementReferenceException, ElementClickInterceptedException, TimeoutException, WebDriverException) as e:
            last_err = e
            time.sleep(0.4)

    raise last_err


def wait_list_page_ready(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    wait.until(EC.presence_of_element_located((By.ID, ID_TBODY)))
    wait.until(EC.presence_of_element_located((By.ID, ID_PAGINATOR)))


def wait_detail_page_ready(driver: webdriver.Chrome, wait: WebDriverWait) -> None:
    wait.until(EC.presence_of_element_located((By.ID, "print")))


def extract_field_by_label(driver: webdriver.Chrome, label_text: str) -> Optional[str]:
    xpaths = [
        f"//label[normalize-space()='{label_text}']/parent::td/following-sibling::td[1]",
        f"//label[contains(normalize-space(),'{label_text.rstrip(':')}')]/parent::td/following-sibling::td[1]",
        f"//td[normalize-space()='{label_text}']/following-sibling::td[1]",
    ]
    for xp in xpaths:
        try:
            el = driver.find_element(By.XPATH, xp)
            txt = (el.text or "").strip()
            if txt:
                return txt
        except NoSuchElementException:
            continue
    return None


def click_row_lupa_and_get_data_divulgacao(driver: webdriver.Chrome, wait: WebDriverWait, row_el, current_page: Optional[int]) -> Optional[str]:
    try:
        lupa = row_el.find_element(By.CSS_SELECTOR, "a[id$=':detalhar']")
    except NoSuchElementException:
        try:
            lupa = row_el.find_element(By.CSS_SELECTOR, "a[title='Visualizar dados da pesquisa']")
        except NoSuchElementException:
            return None

    scroll_center(driver, lupa)
    try:
        js_click(driver, lupa)
    except Exception:
        try:
            lupa.click()
        except Exception:
            return None

    wait_detail_page_ready(driver, wait)
    data_div = extract_field_by_label(driver, "Data de divulgação:")

    driver.back()
    wait_dom_ready(driver)
    wait_list_page_ready(driver, wait)

    if current_page and current_page > 1:
        try:
            active = get_active_page(driver, wait, ID_PAGINATOR)
            if active != current_page:
                go_to_page(driver, wait, ID_PAGINATOR, ID_TBODY, current_page)
        except Exception:
            pass

    return data_div


def parse_current_table_with_details(driver: webdriver.Chrome, wait: WebDriverWait, tbody_id: str) -> List[Dict[str, str]]:
    tbody = driver.find_element(By.ID, tbody_id)
    rows = tbody.find_elements(By.XPATH, ".//tr")

    out: List[Dict[str, str]] = []
    current_page = get_active_page(driver, wait, ID_PAGINATOR)

    for r in rows:
        cols = [c.text.strip() for c in r.find_elements(By.XPATH, "./td")]
        if len(cols) < 5:
            continue

        try:
            data_divulgacao = click_row_lupa_and_get_data_divulgacao(driver, wait, r, current_page)
        except Exception:
            data_divulgacao = None

        out.append({
            "numero_identificacao": cols[0],
            "eleicao": cols[1],
            "empresa_contratada": cols[2],
            "data_registro": cols[3],
            "abrangencia": cols[4],
            "data_divulgacao": data_divulgacao,
        })

        # re-cache após voltar do detalhe
        try:
            tbody = driver.find_element(By.ID, tbody_id)
            rows = tbody.find_elements(By.XPATH, ".//tr")
        except Exception:
            pass

    return out


def scrape_all_pages_current_query(driver: webdriver.Chrome, wait: WebDriverWait, paginator_id: str, tbody_id: str) -> List[Dict[str, str]]:
    pages = get_page_numbers(driver, wait, paginator_id)
    if not pages:
        return dedup_by_numero(parse_current_table_with_details(driver, wait, tbody_id))

    all_rows: List[Dict[str, str]] = []
    for p in pages:
        go_to_page(driver, wait, paginator_id, tbody_id, p)
        all_rows.extend(parse_current_table_with_details(driver, wait, tbody_id))

    return dedup_by_numero(all_rows)


def sheet_safe(name: str) -> str:
    s = re.sub(r"[\[\]\:\*\?\/\\]", "-", name.strip())
    return s[:31] if len(s) > 31 else s


def gspread_client(creds_path: str) -> gspread.Client:
    scopes = [
        "https://www.googleapis.com/auth/spreadsheets",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = Credentials.from_service_account_file(creds_path, scopes=scopes)
    return gspread.authorize(creds)


def ensure_worksheet(ss: gspread.Spreadsheet, title: str, rows: int = 2000, cols: int = 30) -> gspread.Worksheet:
    title = sheet_safe(title)
    try:
        return ss.worksheet(title)
    except Exception:
        return ss.add_worksheet(title=title, rows=rows, cols=cols)


def ensure_header(ws: gspread.Worksheet, header: List[str]) -> None:
    current = ws.row_values(HEADER_ROW)
    if current != header:
        ws.update(f"A{HEADER_ROW}", [header])


def get_existing_keys(ws: gspread.Worksheet, key_col_name: str = "numero_identificacao") -> set:
    header = ws.row_values(HEADER_ROW)
    if not header:
        return set()

    try:
        idx = header.index(key_col_name) + 1
    except ValueError:
        return set()

    col_vals = ws.col_values(idx)
    keys = set()
    for v in col_vals[DATA_START_ROW - 1:]:
        v = (v or "").strip()
        if v:
            keys.add(v)
    return keys


def parse_br_date_to_iso(s: str) -> str:
    s = (str(s) if s is not None else "").strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})$", s)
    if not m:
        return ""
    d, mth, y = m.groups()
    return f"{y}-{mth}-{d}"


def parse_br_datetime_to_iso(s: str) -> str:
    s = (str(s) if s is not None else "").strip()
    m = re.match(r"^(\d{2})/(\d{2})/(\d{4})\s+(\d{2}):(\d{2}):(\d{2})$", s)
    if not m:
        return ""
    d, mth, y, hh, mm, ss = m.groups()
    return f"{y}-{mth}-{d} {hh}:{mm}:{ss}"


def iso_date_sort_key(x: str) -> str:
    x = (str(x) if x is not None else "").strip()
    return x if re.match(r"^\d{4}-\d{2}-\d{2}$", x) else ""


def insert_new_rows_top(ws: gspread.Worksheet, df: pd.DataFrame, key_col_name: str = "numero_identificacao") -> int:
    if df is None or df.empty:
        return 0

    df = df.copy()
    for c in COLS_BASE:
        if c not in df.columns:
            df[c] = ""
    df = df[COLS_BASE].fillna("")

    df["data_registro"] = df["data_registro"].apply(parse_br_date_to_iso)
    df["data_divulgacao"] = df["data_divulgacao"].apply(parse_br_date_to_iso)
    df["capturado_em"] = df["capturado_em"].apply(parse_br_datetime_to_iso)

    ensure_header(ws, COLS_BASE)

    existing = get_existing_keys(ws, key_col_name=key_col_name)
    df_new = df[~df[key_col_name].astype(str).str.strip().isin(existing)].copy()

    if df_new.empty:
        return 0

    df_new["__sort"] = df_new["data_divulgacao"].apply(iso_date_sort_key)
    df_new = df_new.sort_values(by=["__sort", "numero_identificacao"], ascending=[False, False], kind="mergesort")
    df_new = df_new.drop(columns=["__sort"])

    values = df_new.astype(str).values.tolist()
    ws.insert_rows(values, row=DATA_START_ROW, value_input_option="USER_ENTERED")
    return len(df_new)


def run_one_scope(driver: webdriver.Chrome, wait: WebDriverWait, eleicao_text: str, uf_text: str) -> pd.DataFrame:
    select_one_menu_by_text(driver, wait, ID_ELEICAO_LABEL, ID_ELEICAO_PANEL, eleicao_text)
    select_one_menu_by_text(driver, wait, ID_UF_LABEL, ID_UF_PANEL, uf_text)

    click_and_wait_table_refresh(driver, wait, ID_BTN_PESQUISAR, ID_TBODY)
    wait_list_page_ready(driver, wait)

    rows = scrape_all_pages_current_query(driver, wait, ID_PAGINATOR, ID_TBODY)
    df = pd.DataFrame(rows)

    df["uf_filtro"] = uf_text
    df["capturado_em"] = now_with_offset().strftime("%d/%m/%Y %H:%M:%S")

    for c in COLS_BASE:
        if c not in df.columns:
            df[c] = ""

    return df[COLS_BASE]


def open_spreadsheet(gc: gspread.Client) -> gspread.Spreadsheet:
    sid = (os.getenv("SPREADSHEET_ID", "") or "").strip()
    if not sid:
        raise RuntimeError("Defina SPREADSHEET_ID (secret/variável).")
    return gc.open_by_key(sid)


def run_to_google_sheets_insert_dedup(eleicao_text: str, headless: bool = True) -> None:
    gc = gspread_client(CREDS_PATH)
    ss = open_spreadsheet(gc)

    driver = make_driver(headless=headless)
    wait = WebDriverWait(driver, 60)

    try:
        driver.get(URL_LISTAR)
        wait_dom_ready(driver, timeout=60)
        dismiss_overlays_best_effort(driver)

        # BRASIL
        df_brasil = run_one_scope(driver, wait, eleicao_text=eleicao_text, uf_text="BRASIL")
        ws_brasil = ensure_worksheet(ss, "BRASIL", rows=3000, cols=max(30, len(COLS_BASE) + 5))
        insert_new_rows_top(ws_brasil, df_brasil)

        # UFs
        ufs = list_one_menu_items(driver, wait, ID_UF_LABEL, ID_UF_PANEL)
        ufs = [u for u in ufs if u.upper() != "BRASIL"]

        for uf in ufs:
            if uf in SKIP_SHEETS:
                continue
            try:
                df_uf = run_one_scope(driver, wait, eleicao_text=eleicao_text, uf_text=uf)
                ws = ensure_worksheet(ss, uf, rows=3000, cols=max(30, len(COLS_BASE) + 5))
                insert_new_rows_top(ws, df_uf)
            except Exception:
                # não derruba o job por UF problemática
                continue

    finally:
        try:
            driver.quit()
        except Exception:
            pass


if __name__ == "__main__":
    eleicao = os.getenv("ELEICAO_TEXT", "Eleições Gerais 2026").strip()
    headless = env_bool("HEADLESS", True)

    run_to_google_sheets_insert_dedup(eleicao_text=eleicao, headless=headless)
    print("Atualização concluída (INSERT na linha 4; USER_ENTERED; dedup por numero_identificacao).")
