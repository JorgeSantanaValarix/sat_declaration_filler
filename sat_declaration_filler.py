#!/usr/bin/env python3
"""
SAT Declaration Filler — Fill SAT provisional declaration from Contaayuda Excel workpaper.
Reads Impuestos tab (D4:E29 ISR, D33:E58 IVA), logs in with e.firma from DB, fills form, checks totals, sends.
See PLAN_FORM_FILL_AUTOMATION.md.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import re
import sys
import time
from datetime import datetime
from pathlib import Path

import openpyxl

try:
    import pyodbc
except ImportError:
    pyodbc = None

from playwright.sync_api import sync_playwright

# --- Constants from plan ---
IMPUESTOS_SHEET = "Impuestos"
ISR_RANGE = (4, 29)   # rows 4-29, cols D,E
IVA_RANGE = (33, 58)  # rows 33-58, cols D,E
TOLERANCE_PESOS = 1
SAT_PORTAL_URL = "https://ptscdecprov.clouda.sat.gob.mx/"
SP_GET_EFIRMA = "[GET_AUTOMATICTAXDECLARATION_CUSTOMERDATA]"
LOG = logging.getLogger("sat_declaration_filler")


def _parse_currency(val) -> float:
    """Parse Excel currency (e.g. '$ 1,132,090' or '$ -') to float."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    s = str(val).strip().replace("$", "").replace(",", "").strip()
    if not s or s == "-":
        return 0.0
    try:
        return float(s)
    except ValueError:
        return 0.0


def _cell_value(cell) -> str | float | None:
    v = cell.value
    if v is None:
        return None
    if isinstance(v, (int, float)):
        return v
    return str(v).strip()


def read_impuestos(workbook_path: str) -> dict:
    """
    Read Impuestos tab: labels in column D, values in column E.
    D4:E29 = ISR, D33:E58 = IVA.
    Returns dict with keys: label_map (label->value), year, month, periodicidad, tipo_declaracion.
    Period from filename YYYYMM_... when possible.
    """
    wb = openpyxl.load_workbook(workbook_path, read_only=False, data_only=True)
    if IMPUESTOS_SHEET not in wb.sheetnames:
        raise ValueError(f"Sheet '{IMPUESTOS_SHEET}' not found in workbook. Sheets: {wb.sheetnames}")
    ws = wb[IMPUESTOS_SHEET]
    label_map = {}

    for start_row, end_row in (ISR_RANGE, IVA_RANGE):
        for row in range(start_row, end_row + 1):
            label_cell = ws.cell(row=row, column=4)   # D
            value_cell = ws.cell(row=row, column=5)   # E
            label = _cell_value(label_cell)
            if not label or not str(label).strip():
                continue
            label = str(label).strip()
            raw = value_cell.value
            if isinstance(raw, (int, float)):
                label_map[label] = float(raw)
            else:
                label_map[label] = _parse_currency(raw)

    wb.close()

    # Period from filename: YYYYMM_CustomerRfc_Hoja de Trabajo.xlsx
    year, month = None, None
    basename = os.path.basename(workbook_path)
    m = re.match(r"^(\d{4})(\d{2})_", basename)
    if m:
        year = int(m.group(1))
        month = int(m.group(2))

    # Periodicidad: default 1 mensual; could be read from sheet if present
    periodicidad = label_map.get("Periodicidad") or 1
    if isinstance(periodicidad, str):
        periodicidad = 1

    return {
        "label_map": label_map,
        "year": year,
        "month": month,
        "periodicidad": periodicidad,
        "tipo_declaracion": "Normal",
    }


def get_efirma_from_db(company_id: int, branch_id: int, config: dict) -> dict:
    """
    Call Contaayuda SP [GET_AUTOMATICTAXDECLARATION_CUSTOMERDATA].
    Returns dict with cer_path, key_path, password, rfc (TAXID).
    """
    if pyodbc is None:
        raise RuntimeError("pyodbc is required for DB access. Install with: pip install pyodbc")
    conn_str = config.get("db_connection_string")
    if not conn_str:
        raise ValueError("config must set db_connection_string")
    base = config.get("fiel_certificate_base_path", "").rstrip("/\\")
    if not base:
        raise ValueError("config must set fiel_certificate_base_path")

    conn = pyodbc.connect(conn_str)
    try:
        cursor = conn.cursor()
        cursor.execute(
            f"EXEC {SP_GET_EFIRMA} @CompanyId = ?, @BranchId = ?",
            (company_id, branch_id),
        )
        row = cursor.fetchone()
        if not row:
            raise ValueError(f"No e.firma data for CompanyId={company_id}, BranchId={branch_id}")
        columns = [col[0] for col in cursor.description]
        r = dict(zip(columns, row))
        cer_name = r.get("FIELXMLCERTIFICATE") or r.get("FielXmlCertificate")
        key_name = r.get("FIELXMLKEY") or r.get("FielXmlKey")
        password = r.get("FIELTIMBARDOPASSWORD") or r.get("FielTimbardoPassword")
        rfc = r.get("TAXID") or r.get("TaxId") or ""
        if not cer_name or not key_name:
            raise ValueError("SP did not return FIELXMLCERTIFICATE / FIELXMLKEY")
        folder = os.path.join(base, str(company_id), str(branch_id))
        cer_path = os.path.join(folder, cer_name)
        key_path = os.path.join(folder, key_name)
        if not os.path.isfile(cer_path):
            raise FileNotFoundError(f"Certificate file not found: {cer_path}")
        if not os.path.isfile(key_path):
            raise FileNotFoundError(f"Key file not found: {key_path}")
        return {
            "cer_path": os.path.abspath(cer_path),
            "key_path": os.path.abspath(key_path),
            "password": password or "",
            "rfc": rfc,
        }
    finally:
        conn.close()


def load_config(path: str | None) -> dict:
    """Load config JSON. If path is None, try config.json in script dir."""
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "config.json")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Config not found: {path}. Copy config.example.json to config.json and edit.")
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def load_mapping(path: str | None) -> dict:
    """Load form_field_mapping.json."""
    if path is None:
        path = os.path.join(os.path.dirname(__file__), "form_field_mapping.json")
    if not os.path.isfile(path):
        raise FileNotFoundError(f"Mapping not found: {path}")
    with open(path, "r", encoding="utf-8") as f:
        data = json.load(f)
    return {k: v for k, v in data.items() if not k.startswith("_comment") and isinstance(v, list)}


def setup_logging(log_file: str | None) -> None:
    """Print to console and append to log file."""
    if log_file is None:
        log_file = "sat_declaration_filler.log"
    log_path = os.path.abspath(log_file)
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(message)s",
        handlers=[
            logging.StreamHandler(sys.stdout),
            logging.FileHandler(log_path, encoding="utf-8", mode="a"),
        ],
    )
    LOG.info("Log file: %s", log_path)


def _try_fill(page, mapping: dict, key: str, value: str | float, *, is_file: bool = False) -> bool:
    """Try selectors for key; fill first that exists. For file inputs use set_input_files."""
    selectors = mapping.get(key)
    if not selectors:
        return False
    for sel in selectors:
        try:
            # SAT e.firma: two input[type='file'] with no name/accept; first=cer, second=key.
            if is_file and sel == "input[type='file']" and key in ("_login_cer_file_input", "_login_key_file_input"):
                loc = page.locator(sel)
                if loc.count() < 2 and key == "_login_key_file_input":
                    continue
                target = loc.first if key == "_login_cer_file_input" else loc.nth(1)
                target.wait_for(state="attached", timeout=1000)
                target.set_input_files(value)
                return True
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            first = loc.first
            first.wait_for(state="visible", timeout=1000)
            if is_file:
                first.set_input_files(value)  # value = path
            else:
                first.fill(str(value))
            return True
        except Exception:
            continue
    return False


# Phrases that indicate SAT portal error (page down, 500, maintenance). Checked case-insensitive.
_SAT_ERROR_PHRASES = (
    "500",
    "internal server error",
    "error del servidor",
    "servidor no disponible",
    "no disponible",
    "service unavailable",
    "unavailable",
    "error http",
    "mantenimiento",
    "maintenance",
    "try again later",
    "intente más tarde",
)


def _check_sat_page_error(page, *, response_status: int | None = None) -> str | None:
    """
    Check if the current page shows an SAT error (500, down, maintenance).
    Returns an error message to raise if an issue is detected, None otherwise.
    """
    if response_status is not None and response_status >= 400:
        return f"SAT returned HTTP {response_status}"
    try:
        body = page.locator("body").inner_text(timeout=3000) or ""
        text = body.lower()
        for phrase in _SAT_ERROR_PHRASES:
            if phrase in text:
                return f"SAT page shows error: found '{phrase}' on page"
    except Exception as e:
        LOG.debug("Could not read page body for error check: %s", e)
    return None


def _try_click(page, mapping: dict, key: str) -> bool:
    selectors = mapping.get(key)
    if not selectors:
        return False
    for sel in selectors:
        try:
            loc = page.locator(sel)
            if loc.count() == 0:
                continue
            first = loc.first
            first.wait_for(state="visible", timeout=1000)
            first.click()
            return True
        except Exception:
            continue
    return False


def login_sat(page, efirma: dict, mapping: dict, base_url: str = SAT_PORTAL_URL) -> None:
    """Open SAT portal, click e.firma, fill .cer, .key, password, Enviar."""
    t0 = time.perf_counter()

    def _ts() -> str:
        now = datetime.now()
        return now.strftime("%Y-%m-%d %H:%M:%S") + f",{now.microsecond // 1000:03d}"

    def _elapsed() -> float:
        return round(time.perf_counter() - t0, 2)

    # Use 'load' so we don't timeout: SAT page often never reaches networkidle (long-lived connections).
    response = page.goto(base_url, wait_until="load", timeout=60000)
    page.wait_for_load_state("domcontentloaded")
    err = _check_sat_page_error(page, response_status=response.status if response else None)
    if err:
        LOG.error("SAT issue on load: %s", err)
        print(err, file=sys.stderr)
        raise RuntimeError(err)
    print(f"{_ts()} [{_elapsed()}s] SAT page loaded, looking for e.firma button")

    if not _try_click(page, mapping, "_login_e_firma_button"):
        raise RuntimeError("Could not find e.firma button on SAT login page")
    print(f"{_ts()} [{_elapsed()}s] e.firma pressed")
    # Wait for e.firma form to be ready (short timeouts; max ~1s extra delay).
    try:
        page.wait_for_url(re.compile(r".*id=fiel.*", re.I), timeout=500)
    except Exception:
        pass
    try:
        page.locator("input[type='file'], input[type='password']").first.wait_for(state="visible", timeout=1000)
    except Exception:
        pass
    # Set cer, key, password with no extra delay (target ~1s total).
    cer_input_ok = _try_fill(page, mapping, "_login_cer_file_input", efirma["cer_path"], is_file=True)
    if cer_input_ok:
        print(f"{_ts()} [{_elapsed()}s] filled .cer: {os.path.basename(efirma['cer_path'])}")
    key_input_ok = _try_fill(page, mapping, "_login_key_file_input", efirma["key_path"], is_file=True)
    if key_input_ok:
        print(f"{_ts()} [{_elapsed()}s] filled .key: {os.path.basename(efirma['key_path'])}")
    if not cer_input_ok or not key_input_ok:
        LOG.warning("One or both file inputs not found by selector; using generic input[type='file'] order (first=cer, second=key)")
        inputs = page.locator("input[type='file']").all()
        if len(inputs) >= 2:
            inputs[0].set_input_files(efirma["cer_path"])
            inputs[1].set_input_files(efirma["key_path"])
            print(f"{_ts()} [{_elapsed()}s] filled .cer (fallback): {os.path.basename(efirma['cer_path'])}")
            print(f"{_ts()} [{_elapsed()}s] filled .key (fallback): {os.path.basename(efirma['key_path'])}")
        elif len(inputs) == 1:
            inputs[0].set_input_files(efirma["cer_path"])
            print(f"{_ts()} [{_elapsed()}s] filled .cer (fallback): {os.path.basename(efirma['cer_path'])}")
            LOG.warning("Only one file input found; key may need manual selection")

    _try_fill(page, mapping, "_login_password_input", efirma["password"])
    print(f"{_ts()} [{_elapsed()}s] filled password: ***")
    # RFC is not filled: SAT typically derives it from the .cer; filling it can cause long delays.
    page.wait_for_timeout(1000)
    print(f"{_ts()} [{_elapsed()}s] pressing Enviar")
    # Click Enviar: try mapping first, then fallbacks (SAT markup varies; Enviar can be button or input).
    if not _try_click(page, mapping, "_login_enviar_button"):
        enviar_clicked = False
        for try_fn in [
            lambda: page.get_by_role("button", name=re.compile(r"Enviar", re.I)).first.click(timeout=3000),
            lambda: page.locator("button").filter(has_text=re.compile(r"Enviar", re.I)).first.click(timeout=3000),
            lambda: page.locator("input[type='submit'][value='Enviar']").first.click(timeout=3000),
            lambda: page.locator("input[type='submit'][value*='Enviar']").first.click(timeout=3000),
        ]:
            try:
                try_fn()
                enviar_clicked = True
                LOG.info("Clicked Enviar via fallback")
                print(f"{_ts()} [{_elapsed()}s] Enviar pressed (fallback)")
                break
            except Exception:
                continue
        if not enviar_clicked:
            # Last resort: second submit button (first is often "Contraseña")
            try:
                page.locator("input[type='submit'], button[type='submit']").nth(1).click(timeout=3000)
                enviar_clicked = True
                LOG.info("Clicked Enviar via nth(1) submit")
                print(f"{_ts()} [{_elapsed()}s] Enviar pressed (nth(1))")
            except Exception:
                pass
        if not enviar_clicked:
            raise RuntimeError("Could not find Enviar button on e.firma form")
    else:
        print(f"{_ts()} [{_elapsed()}s] Enviar pressed")
    page.wait_for_load_state("domcontentloaded")
    # Wait for redirect after login (e.g. to portal home)
    page.wait_for_timeout(3000)
    err = _check_sat_page_error(page)
    if err:
        LOG.error("SAT issue after login: %s", err)
        print(err, file=sys.stderr)
        raise RuntimeError(err)


def navigate_to_declaration(page, mapping: dict) -> None:
    """Click Nuevo Portal → Presentar Declaración → Iniciar una nueva declaración."""
    _try_click(page, mapping, "_nav_nuevo_portal")
    page.wait_for_timeout(2000)
    _try_click(page, mapping, "_nav_presentar_declaracion")
    page.wait_for_timeout(2000)
    _try_click(page, mapping, "_nav_iniciar_nueva")
    page.wait_for_timeout(3000)


def fill_initial_form(page, data: dict, mapping: dict) -> None:
    """Fill Ejercicio, Periodicidad, Periodo, Tipo de declaración (in order; dynamic UI)."""
    year = data.get("year")
    month = data.get("month")
    if year is not None:
        _try_fill(page, mapping, "initial_ejercicio", str(year))
        page.wait_for_timeout(500)
    _try_fill(page, mapping, "initial_periodicidad", str(data.get("periodicidad", 1)))
    page.wait_for_timeout(500)
    if month is not None:
        _try_fill(page, mapping, "initial_periodo", f"{month:02d}")
    page.wait_for_timeout(500)
    _try_fill(page, mapping, "initial_tipo_declaracion", data.get("tipo_declaracion", "Normal"))
    page.wait_for_timeout(1000)


def fill_obligation_section(page, mapping: dict, label_map: dict, labels: list[str]) -> None:
    """Fill form fields for given Excel labels (try each label's selectors with value from label_map)."""
    for label in labels:
        value = label_map.get(label)
        if value is None:
            continue
        if isinstance(value, float) and value == 0.0:
            continue
        _try_fill(page, mapping, label, value)
        page.wait_for_timeout(200)


def check_totals(page, data: dict, mapping: dict, tolerance: int) -> tuple[bool, str]:
    """
    Compare SAT summary (ISR a pagar, IVA a pagar, Total a pagar) with Excel.
    Tolerance ±tolerance pesos each. Returns (ok, message).
    """
    label_map = data["label_map"]
    excel_isr = _parse_currency(label_map.get("ISR a cargo"))
    excel_iva = _parse_currency(label_map.get("IVA a cargo"))
    excel_total = excel_isr + excel_iva

    # Read displayed totals from SAT (selectors are placeholders; implementation reads text)
    def _read_summary(selector_key: str) -> float:
        selectors = mapping.get(selector_key)
        if not selectors:
            return 0.0
        for sel in selectors:
            try:
                loc = page.locator(sel)
                if loc.count() == 0:
                    continue
                text = loc.first.text_content(timeout=2000) or ""
                # Extract number from text like "$ 95" or "1,480"
                n = re.sub(r"[^\d.]", "", text.replace(",", ""))
                if n:
                    return float(n)
            except Exception:
                continue
        return 0.0

    sat_isr = _read_summary("_summary_isr_a_pagar")
    sat_iva = _read_summary("_summary_iva_a_pagar")
    sat_total = _read_summary("_summary_total_a_pagar")
    if sat_total == 0.0 and (sat_isr != 0.0 or sat_iva != 0.0):
        sat_total = sat_isr + sat_iva

    ok_isr = abs(sat_isr - excel_isr) <= tolerance
    ok_iva = abs(sat_iva - excel_iva) <= tolerance
    ok_total = abs(sat_total - excel_total) <= tolerance
    if ok_isr and ok_iva and ok_total:
        return True, f"Totals OK: ISR {sat_isr}, IVA {sat_iva}, Total {sat_total}"
    msg = (
        f"Totals mismatch (tolerance ±{tolerance}): "
        f"Excel ISR={excel_isr} IVA={excel_iva} Total={excel_total}; "
        f"SAT ISR={sat_isr} IVA={sat_iva} Total={sat_total}"
    )
    return False, msg


def send_declaration(page, mapping: dict) -> bool:
    """Click Enviar declaración."""
    return _try_click(page, mapping, "_btn_enviar_declaracion")


def get_efirma_from_config(config: dict) -> dict:
    """
    Get e.firma from config (test mode: local .cer/.key paths and password, no DB).
    Accepts either root keys (test_cer_path, test_key_path, test_password) or nested config["test"] (cer_path, key_path, password).
    """
    t = config.get("test") or {}
    cer = config.get("test_cer_path") or t.get("cer_path") or ""
    key = config.get("test_key_path") or t.get("key_path") or ""
    pwd = config.get("test_password") or t.get("password") or ""
    rfc = config.get("test_rfc") or t.get("rfc") or ""
    if not cer or not key:
        raise ValueError(
            "Test mode requires test_cer_path and test_key_path in config.json (or test.cer_path, test.key_path). "
            "Add test_password (or test.password) for e.firma."
        )
    cer_path = os.path.abspath(cer)
    key_path = os.path.abspath(key)
    if not os.path.isfile(cer_path):
        raise FileNotFoundError(f"test cer path not found: {cer_path}")
    if not os.path.isfile(key_path):
        raise FileNotFoundError(f"test key path not found: {key_path}")
    return {
        "cer_path": cer_path,
        "key_path": key_path,
        "password": pwd,
        "rfc": rfc,
    }


def run(
    workbook_path: str | None = None,
    company_id: int | None = None,
    branch_id: int | None = None,
    config_path: str | None = None,
    mapping_path: str | None = None,
    test_login: bool = False,
) -> bool:
    """
    Full flow: read Excel → get e.firma → login SAT → navigate → fill initial → fill ISR → fill IVA → check totals → send.
    If test_login=True: only open SAT and perform e.firma login (no DB; use test_* in config). Returns True if login succeeded.
    Returns True if declaration was sent (or test step OK), False otherwise. Logs and prints outcome.
    """
    config = load_config(config_path)
    mapping = load_mapping(mapping_path)
    setup_logging(config.get("log_file"))

    base_url = config.get("sat_portal_url", SAT_PORTAL_URL)

    if test_login:
        LOG.info("Test mode: login only (no DB, using test_cer_path / test_key_path / test_password from config)")
        efirma = get_efirma_from_config(config)
        with sync_playwright() as p:
            browser = p.chromium.launch(headless=False)
            context = browser.new_context(accept_downloads=True)
            page = context.new_page()
            try:
                login_sat(page, efirma, mapping, base_url)
                LOG.info("Test login: e.firma login completed. Browser will stay open 10s for inspection.")
                page.wait_for_timeout(10000)
                return True
            except Exception as e:
                LOG.exception("Test login failed")
                print(str(e), file=sys.stderr)
                return False
            finally:
                context.close()
                browser.close()

    # Normal flow
    if not workbook_path or company_id is None or branch_id is None:
        raise ValueError("Normal run requires --workbook, --company-id, --branch-id")
    LOG.info("Reading workbook: %s", workbook_path)
    data = read_impuestos(workbook_path)
    label_map = data["label_map"]
    LOG.info("Period: %s-%s, labels read: %d", data.get("year"), data.get("month"), len(label_map))

    LOG.info("Fetching e.firma from DB for company=%s branch=%s", company_id, branch_id)
    efirma = get_efirma_from_db(company_id, branch_id, config)
    tolerance = config.get("totals_tolerance_pesos", TOLERANCE_PESOS)

    with sync_playwright() as p:
        browser = p.chromium.launch(headless=False)  # headless=False so user can see; set True for automation
        context = browser.new_context(accept_downloads=True)
        page = context.new_page()
        try:
            login_sat(page, efirma, mapping, base_url)
            LOG.info("Logged in to SAT")
            navigate_to_declaration(page, mapping)
            fill_initial_form(page, data, mapping)
            # Submit initial form if there is a submit button (add selector to mapping if needed)
            page.wait_for_timeout(2000)

            # Fill ISR section (order: open obligation then fill fields)
            isr_labels = [
                "Ingresos nominales facturados",
                "Total de ingresos acumulados",
                "Base gravable del pago provisional",
                "Impuesto del periodo",
                "Total ISR retenido del periodo",
                "ISR a cargo",
            ]
            fill_obligation_section(page, mapping, label_map, isr_labels)
            page.wait_for_timeout(1500)

            # Fill IVA section
            iva_labels = [
                "Actividades gravadas a la tasa del 16%",
                "Actividades gravadas a la tasa del 8%",
                "Actividades gravadas a la tasa del 0% otros",
                "Actividades exentas",
                "Actividades no objeto de impuesto",
                "IVA a cargo a la tasa del 16% y 8%",
                "Total IVA Trasladado",
                "IVA retenido a favor",
                "IVA acreditable del periodo",
                "Cantidad a cargo",
                "IVA a cargo",
                "IVA a favor",
            ]
            fill_obligation_section(page, mapping, label_map, iva_labels)
            page.wait_for_timeout(2000)

            ok, msg = check_totals(page, data, mapping, tolerance)
            LOG.info(msg)
            if not ok:
                LOG.error("Totals check failed. Not sending declaration.")
                print(msg, file=sys.stderr)
                return False

            if not send_declaration(page, mapping):
                LOG.warning("Could not find/click Enviar declaración button")
                return False
            page.wait_for_timeout(3000)
            LOG.info("Declaration send clicked; complete any remaining steps in the browser.")
            return True
        except Exception as e:
            LOG.exception("Error during run")
            print(str(e), file=sys.stderr)
            return False
        finally:
            context.close()
            browser.close()

    return False


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Fill SAT provisional declaration from Contaayuda Excel workpaper (Impuestos tab)."
    )
    parser.add_argument("--workbook", "-w", help="Full path to the .xlsx workpaper (required unless --test-login)")
    parser.add_argument("--company-id", "-c", type=int, help="Company ID for e.firma lookup (required unless --test-login)")
    parser.add_argument("--branch-id", "-b", type=int, help="Branch ID for e.firma lookup (required unless --test-login)")
    parser.add_argument("--config", help="Path to config.json (default: script dir/config.json)")
    parser.add_argument("--mapping", help="Path to form_field_mapping.json (default: script dir)")
    parser.add_argument("--test-login", action="store_true", help="Test only: open SAT and log in with local .cer/.key and password from config (no DB)")
    args = parser.parse_args()

    if args.test_login:
        success = run(
            workbook_path=None,
            company_id=None,
            branch_id=None,
            config_path=args.config,
            mapping_path=args.mapping,
            test_login=True,
        )
        sys.exit(0 if success else 1)

    if not args.workbook or args.company_id is None or args.branch_id is None:
        print("Error: --workbook, --company-id, and --branch-id are required unless --test-login.", file=sys.stderr)
        sys.exit(2)
    if not os.path.isfile(args.workbook):
        print(f"Error: Workbook not found: {args.workbook}", file=sys.stderr)
        sys.exit(2)
    success = run(
        workbook_path=args.workbook,
        company_id=args.company_id,
        branch_id=args.branch_id,
        config_path=args.config,
        mapping_path=args.mapping,
        test_login=False,
    )
    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
