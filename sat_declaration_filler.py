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
import signal
import sys
import threading
import time
from datetime import datetime
from pathlib import Path

# Used by SIGINT handler to logout from SAT when user presses Ctrl+C.
_run_context: dict | None = None

import openpyxl

try:
    import pyodbc
except ImportError:
    pyodbc = None

from playwright.sync_api import Frame, Page, sync_playwright

# --- Constants from plan ---
IMPUESTOS_SHEET = "Impuestos"
ISR_RANGE = (4, 29)   # rows 4-29, cols D,E
IVA_RANGE = (33, 58)  # rows 33-58, cols D,E
TOLERANCE_PESOS = 1
# Declaración Provisional (pstcdypisr): after login, click "Presentar declaración" to open Configuración de la declaración.
SAT_PORTAL_URL = "https://pstcdypisr.clouda.sat.gob.mx/"
RETRY_WAIT_SECONDS = 60  # wait before single retry after error (e.g. SAT HTTP 500; give server time to recover)
# Phase 2 → 3: wait for "Cargando información" to disappear (max seconds), then for pre-fill pop-up CERRAR to appear
PHASE3_LOADING_MAX_WAIT_SEC = 90
PHASE3_POPUP_WAIT_FOR_CERRAR_SEC = 45   # wait for CERRAR button to appear (pop-up can take several seconds)
PHASE3_POPUP_CERRAR_CLICK_MS = 5000    # timeout when clicking CERRAR once it's visible
PHASE3_POPUP_CERRAR_SELECTOR_MS = 4000  # per mapping selector (fail fast)
SP_GET_EFIRMA = "[GET_AUTOMATICTAXDECLARATION_CUSTOMERDATA]"
LOG = logging.getLogger("sat_declaration_filler")


def _parse_currency(val) -> float:
    """Parse Excel currency (e.g. '$ 1,132,090' or '$ -') to float. Handles formula cached values and datetime (returns 0.0)."""
    if val is None:
        return 0.0
    if isinstance(val, (int, float)):
        return float(val)
    # Excel may store dates as datetime; don't treat as currency
    if hasattr(val, "year") and hasattr(val, "month"):
        return 0.0
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


def _parse_cell_ref(formula: str) -> tuple[int, int] | None:
    """Parse simple formula like =F8 or =+F8 to (row, col). Column A=1, B=2, ..., F=6. Returns None if not a simple cell ref."""
    if not formula or not isinstance(formula, str):
        return None
    s = formula.strip().lstrip("=+").strip().upper()
    m = re.match(r"^([A-Z]+)(\d+)$", s)
    if not m:
        return None
    col_letters, row_str = m.group(1), m.group(2)
    col = 0
    for c in col_letters:
        col = col * 26 + (ord(c) - ord("A") + 1)
    return (int(row_str), col)


def _row_col_to_a1(row: int, col: int) -> str:
    """Convert 1-based (row, col) to Excel A1 notation (e.g. (8, 6) -> 'F8')."""
    s = ""
    c = col
    while c:
        c, r = divmod(c - 1, 26)
        s = chr(ord("A") + r) + s
    return (s or "A") + str(row)


_XLCALC_IMPORT_FAILED: bool | None = None  # None=not tried, True=failed, False=ok
_XLCALC_TIMEOUT_SEC = 3   # skip xlcalculator if loading takes longer (fail fast; openpyxl usually sufficient)


def _xlcalculator_evaluator(workbook_path: str):
    """
    Build xlcalculator model once (can be slow/hang). Runs with timeout; returns (Evaluator, None) or (None, error_msg).
    """
    global _XLCALC_IMPORT_FAILED
    try:
        from xlcalculator import ModelCompiler, Evaluator
    except ImportError:
        if _XLCALC_IMPORT_FAILED is None:
            _XLCALC_IMPORT_FAILED = True
            LOG.info("xlcalculator not installed; formula fallback disabled. Install with: pip install xlcalculator")
        return None, "not installed"
    if _XLCALC_IMPORT_FAILED is None:
        _XLCALC_IMPORT_FAILED = False

    result: list = []  # [evaluator] or [] on error; error_msg in err[0]
    err: list = []

    def _build():
        try:
            path = os.path.abspath(workbook_path)
            compiler = ModelCompiler()
            model = compiler.read_and_parse_archive(path, build_code=True)
            result.append(Evaluator(model))
        except Exception as e:
            err.append(str(e))

    LOG.info("xlcalculator: loading workbook (timeout %ss)...", _XLCALC_TIMEOUT_SEC)
    t = threading.Thread(target=_build, daemon=True)
    t.start()
    t.join(timeout=_XLCALC_TIMEOUT_SEC)
    if t.is_alive():
        LOG.warning("xlcalculator: loading timed out after %ss; skipping formula fallback", _XLCALC_TIMEOUT_SEC)
        return None, "timeout"
    if err:
        return None, err[0]
    if result:
        return result[0], None
    return None, "unknown"


def _evaluate_cell(evaluator, sheet_name: str, row: int, col: int) -> float | None:
    """Evaluate one cell with an existing xlcalculator Evaluator. Returns float or None."""
    try:
        cell_ref = f"{sheet_name}!{_row_col_to_a1(row, col)}"
        result = evaluator.evaluate(cell_ref)
        if result is None:
            LOG.info("xlcalculator %s returned None", cell_ref)
            return None
        return _parse_currency(result)
    except Exception as e:
        LOG.info("xlcalculator evaluate %s!%s: %s", sheet_name, _row_col_to_a1(row, col), e)
        return None


def read_impuestos(workbook_path: str) -> dict:
    """
    Read Impuestos tab. Two layouts supported:
    - Layout 1: label in column D, value in column E (D4:E29 ISR, D33:E58 IVA).
    - Layout 2: label in column E, value in column F (same row ranges).
    Values in E or F can be numeric or formulas. We use data_only=True so formula cells
    return the cached result (what you see in Excel). If the file was not saved in Excel
    and the cache is missing, we resolve simple refs (e.g. =F8, =+E8) by following the reference.
    Returns dict with keys: label_map (label->value), year, month, periodicidad, tipo_declaracion.
    """
    wb = openpyxl.load_workbook(workbook_path, read_only=False, data_only=True)
    if IMPUESTOS_SHEET not in wb.sheetnames:
        raise ValueError(f"Sheet '{IMPUESTOS_SHEET}' not found in workbook. Sheets: {wb.sheetnames}")
    ws = wb[IMPUESTOS_SHEET]
    label_map = {}
    formula_cells: list[tuple[str, int, int]] = []  # (label, row, col) where value was None

    def add_label_value(label: str, raw, row: int | None = None, col: int | None = None) -> None:
        if not label or not str(label).strip():
            return
        label = str(label).strip()
        if raw is not None and not (isinstance(raw, str) and not raw.strip()):
            if isinstance(raw, (int, float)):
                label_map[label] = float(raw)
            else:
                label_map[label] = _parse_currency(raw)
        elif row is not None and col is not None:
            formula_cells.append((label, row, col))

    for start_row, end_row in (ISR_RANGE, IVA_RANGE):
        for row in range(start_row, end_row + 1):
            # Layout 1: D = label, E = value
            label_d = _cell_value(ws.cell(row=row, column=4))
            value_e = ws.cell(row=row, column=5).value
            add_label_value(label_d, value_e, row=row, col=5)
            # Layout 2: E = label, F = value (only when E looks like text)
            label_e_cell = ws.cell(row=row, column=5)
            value_f_cell = ws.cell(row=row, column=6)
            label_e = _cell_value(label_e_cell)
            value_f = value_f_cell.value
            if label_e is not None and str(label_e).strip():
                label_e_str = str(label_e).strip()
                if not re.match(r"^[\d\s\$,.\-]+$", label_e_str):
                    add_label_value(label_e, value_f, row=row, col=6)

    # Resolve formula cells to get value-as-displayed (e.g. =+F8 -> value of F8; resolve ref recursively if it's a formula)
    # With data_only=True, formula cells often have a cached value (what you see in Excel); if missing, we resolve simple refs.
    def _cell_display_value(ws_data, ws_formula, r: int, c: int, visited: set | None = None) -> float:
        visited = visited or set()
        key = (r, c)
        if key in visited:
            return 0.0
        visited.add(key)
        raw = ws_data.cell(row=r, column=c).value
        # Use cached value if present (what user sees when file was saved in Excel) — works for both literal and formula cells
        if raw is not None and not (isinstance(raw, str) and str(raw).strip().startswith("=")):
            return _parse_currency(raw)
        # No cached value: resolve formula or use literal from formula sheet
        formula = ws_formula.cell(row=r, column=c).value
        if isinstance(formula, (int, float)):
            return float(formula)
        ref = _parse_cell_ref(formula) if isinstance(formula, str) else None
        if ref:
            r2, c2 = ref
            return _cell_display_value(ws_data, ws_formula, r2, c2, visited)
        return 0.0

    if formula_cells:
        try:
            wb_formula = openpyxl.load_workbook(workbook_path, read_only=False, data_only=False)
            ws_formula = wb_formula[IMPUESTOS_SHEET]
            for label, row, col in formula_cells:
                formula = ws_formula.cell(row=row, column=col).value
                ref = _parse_cell_ref(formula) if isinstance(formula, str) else None
                if ref:
                    r2, c2 = ref
                    val = _cell_display_value(ws, ws_formula, r2, c2)
                    label_map[label] = val
                    LOG.info("Excel formula resolved (value as displayed): %r at (%s,%s) -> ref %s = %.2f", formula, row, col, ref, val)
                    if val == 0.0 and label == "Base gravable del pago provisional":
                        fallback = label_map.get("Ingresos cobrados y amparados por factura del mes")
                        if fallback is not None and _parse_currency(fallback) != 0.0:
                            label_map[label] = _parse_currency(fallback)
                            LOG.info("Excel: using fallback for Base gravable: %.2f", label_map[label])
            wb_formula.close()
        except Exception as e:
            LOG.warning("Could not resolve formula cells: %s", e)

    wb.close()

    # Optional: when openpyxl gave 0.0 or no value, try xlcalculator (build model once, then evaluate each cell)
    evaluator, xlcalc_err = _xlcalculator_evaluator(workbook_path)
    if xlcalc_err and evaluator is None:
        if xlcalc_err != "not installed":
            LOG.warning("xlcalculator: %s", xlcalc_err)
    elif evaluator is not None:
        for label, row, col in formula_cells:
            current = label_map.get(label)
            if current is not None and abs(float(current)) > 1e-9:
                continue  # keep existing non-zero value from cache/simple-ref
            xval = _evaluate_cell(evaluator, IMPUESTOS_SHEET, row, col)
            # If formula cell (e.g. F9 =+F8) didn't evaluate, try the referenced cell (F8) directly
            if xval is None:
                try:
                    wb_f = openpyxl.load_workbook(workbook_path, read_only=False, data_only=False)
                    ws_f = wb_f[IMPUESTOS_SHEET]
                    formula = ws_f.cell(row=row, column=col).value
                    wb_f.close()
                    ref = _parse_cell_ref(formula) if isinstance(formula, str) else None
                    if ref:
                        r2, c2 = ref
                        xval = _evaluate_cell(evaluator, IMPUESTOS_SHEET, r2, c2)
                except Exception as e:
                    LOG.debug("xlcalculator ref fallback for (%s,%s): %s", row, col, e)
            if xval is not None:
                label_map[label] = xval
                LOG.info("Excel value (xlcalculator): %r at (%s,%s) = %.2f", label, row, col, xval)
            else:
                LOG.info("xlcalculator: no value for %r at (%s,%s) (tried cell and ref fallback)", label, row, col)

    LOG.info(
        "Excel opened: %s | Sheet=%s, layouts D/E and E/F, rows ISR %s–%s, IVA %s–%s | %s labels read. "
        "'Base gravable del pago provisional' in sheet: %s",
        workbook_path, IMPUESTOS_SHEET, ISR_RANGE[0], ISR_RANGE[1], IVA_RANGE[0], IVA_RANGE[1],
        len(label_map), "Base gravable del pago provisional" in label_map,
    )
    if label_map and "Base gravable del pago provisional" not in label_map:
        sample = [k for k in list(label_map.keys())[:15]]
        LOG.info("Impuestos column D labels (sample): %s", sample)

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

    tipo_declaracion = "Normal"
    periodo_str = f"{month:02d}" if month is not None else "(none)"
    print(
        f"{_debug_ts()} [Excel] initial form data to fill: Ejercicio={year}, Periodicidad={periodicidad}, "
        f"Período={periodo_str}, Tipo={tipo_declaracion} (year/month from filename YYYYMM_; periodicidad from sheet)"
    )
    return {
        "label_map": label_map,
        "year": year,
        "month": month,
        "periodicidad": periodicidad,
        "tipo_declaracion": tipo_declaracion,
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


_FILL_SELECTOR_TIMEOUT_MS = 1000
_INITIAL_FORM_SELECTOR_TIMEOUT_MS = 1000

# SAT Periodicidad dropdown: option labels shown in UI → we use value= for select_option.
# Options: 1-Mensual, 3-Trimestral, 4-Cuatrimestral, 5-Semestral (A), 6-Semestral (B) Liquidación,
#          7-Ajuste, 8-Del Ejercicio, 9-Sin Periodo.
_SAT_PERIODICIDAD_VALUE = {
    1: "M",   # 1-Mensual
    3: "T",   # 3-Trimestral
    4: "Q",   # 4-Cuatrimestral
    5: "S",   # 5-Semestral (A)
    6: "L",   # 6-Semestral (B) Liquidación
    7: "J",   # 7-Ajuste
    8: "Y",   # 8-Del Ejercicio
    9: "N",   # 9-Sin Periodo
}

# SAT Ejercicio dropdown: options are years 2022–2026 (we pass str(year), e.g. "2026").

# SAT Período dropdown: options are months Enero–Diciembre (labels in Spanish). We pass label for select_option.
_SAT_PERIODO_LABEL = {
    1: "Enero", 2: "Febrero", 3: "Marzo", 4: "Abril", 5: "Mayo", 6: "Junio",
    7: "Julio", 8: "Agosto", 9: "Septiembre", 10: "Octubre", 11: "Noviembre", 12: "Diciembre",
}


def _try_fill(scope: Page | Frame, page_for_wait: Page, mapping: dict, key: str, value: str | float, *, is_file: bool = False) -> bool:
    """Try selectors for key; fill first that exists. scope = page or frame where elements live; page_for_wait for timeouts."""
    selectors = mapping.get(key)
    if not selectors:
        return False
    # Login: short timeouts for 5s target. Initial form: longer. Others: short.
    if key.startswith("_login_"):
        timeout_ms = 400
    elif key.startswith("initial_"):
        timeout_ms = _INITIAL_FORM_SELECTOR_TIMEOUT_MS
    else:
        timeout_ms = _FILL_SELECTOR_TIMEOUT_MS
    value_str = str(value)
    for sel in selectors:
        try:
            # SAT e.firma: two input[type='file'] with no name/accept; first=cer, second=key.
            if is_file and sel == "input[type='file']" and key in ("_login_cer_file_input", "_login_key_file_input"):
                loc = scope.locator(sel)
                if loc.count() < 2 and key == "_login_key_file_input":
                    continue
                target = loc.first if key == "_login_cer_file_input" else loc.nth(1)
                target.wait_for(state="attached", timeout=min(300, timeout_ms))
                target.set_input_files(value)
                return True
            # Label-based: find control by its visible label (works for dropdowns and inputs).
            if sel.startswith("label="):
                label_text = sel[6:].strip()
                loc = scope.get_by_label(label_text, exact=False)
                if loc.count() == 0:
                    continue
                first = loc.first
                first.wait_for(state="visible", timeout=timeout_ms or 1000)
                if is_file:
                    first.set_input_files(value)
                else:
                    tag = first.evaluate("el => el.tagName.toLowerCase()")
                    if tag == "select":
                        try:
                            first.select_option(value=value_str)
                        except Exception:
                            first.select_option(label=value_str)
                    else:
                        first.click()
                        page_for_wait.wait_for_timeout(100 if (timeout_ms or 0) <= 500 else 300)
                        first.fill(value_str)
                return True
            loc = scope.locator(sel)
            if loc.count() == 0:
                continue
            first = loc.first
            first.wait_for(state="visible", timeout=timeout_ms or 1000)
            if is_file:
                first.set_input_files(value)
            else:
                tag = first.evaluate("el => el.tagName.toLowerCase()")
                if tag == "select":
                    try:
                        first.select_option(value=value_str)
                    except Exception:
                        first.select_option(label=value_str)
                else:
                    first.click()
                    page_for_wait.wait_for_timeout(100 if (timeout_ms or 0) <= 500 else 300)
                    first.fill(value_str)
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
    try:
        page.wait_for_url(re.compile(r".*id=fiel.*", re.I), timeout=200)
    except Exception:
        pass
    try:
        page.locator("input[type='file'], input[type='password']").first.wait_for(state="visible", timeout=300)
    except Exception:
        pass
    # Set cer, key, password with minimal delay.
    cer_input_ok = _try_fill(page, page, mapping, "_login_cer_file_input", efirma["cer_path"], is_file=True)
    if cer_input_ok:
        print(f"{_ts()} [{_elapsed()}s] filled .cer: {os.path.basename(efirma['cer_path'])}")
    key_input_ok = _try_fill(page, page, mapping, "_login_key_file_input", efirma["key_path"], is_file=True)
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

    pwd_ok = _try_fill(page, page, mapping, "_login_password_input", efirma["password"])
    if pwd_ok:
        print(f"{_ts()} [{_elapsed()}s] filled password: ***")
    else:
        LOG.warning("Password field not filled — check selectors")
        print(f"{_ts()} [{_elapsed()}s] password NOT filled (check selectors)")
    page.wait_for_timeout(50)
    print(f"{_ts()} [{_elapsed()}s] pressing Enviar")
    # Click Enviar: try mapping first, then fallbacks (SAT markup varies; Enviar can be button or input).
    if not _try_click(page, mapping, "_login_enviar_button"):
        enviar_clicked = False
        for try_fn in [
            lambda: page.get_by_role("button", name=re.compile(r"Enviar", re.I)).first.click(timeout=1200),
            lambda: page.locator("button").filter(has_text=re.compile(r"Enviar", re.I)).first.click(timeout=1200),
            lambda: page.locator("input[type='submit'][value='Enviar']").first.click(timeout=1200),
            lambda: page.locator("input[type='submit'][value*='Enviar']").first.click(timeout=1200),
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
                page.locator("input[type='submit'], button[type='submit']").nth(1).click(timeout=1200)
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
    print(f"{_ts()} [{_elapsed()}s] waiting for SAT post-login page...")
    post_login_timeout = 8000
    poll_ms = 150
    t_end = time.perf_counter() + (post_login_timeout / 1000.0)
    while time.perf_counter() < t_end:
        page.wait_for_timeout(poll_ms)
        try:
            url = page.url or ""
            if "clouda.sat.gob.mx" in url.lower():
                break
            body = (page.locator("body").inner_text(timeout=400) or "").lower()
            if "500" in body or "internal server error" in body or "error: http 500" in body:
                err = "SAT login returned HTTP 500 Internal Server Error (server-side). Try again later or check SAT status."
                LOG.error("SAT 500 after login: %s", err)
                print(err, file=sys.stderr)
                raise RuntimeError(err)
        except RuntimeError:
            raise
        except Exception:
            pass
    try:
        page.wait_for_url(re.compile(r"clouda\.sat\.gob\.mx", re.I), timeout=1000)
    except Exception:
        pass
    try:
        for sel in (mapping.get("_nav_presentar_declaracion") or []) + (mapping.get("_nav_nuevo_portal") or []) + ["text=Presentar declaración", "text=Cerrar Sesión", "text=Bienvenido"]:
            try:
                page.locator(sel).first.wait_for(state="visible", timeout=1000)
                break
            except Exception:
                continue
    except Exception:
        pass
    page.wait_for_timeout(80)
    print(f"{_ts()} [{_elapsed()}s] post-login page ready.")
    err = _check_sat_page_error(page)
    if err:
        LOG.error("SAT issue after login: %s", err)
        print(err, file=sys.stderr)
        raise RuntimeError(err)


def navigate_to_declaration(page, mapping: dict) -> None:
    """Click Nuevo Portal → Presentar Declaración → Iniciar una nueva declaración (legacy portal)."""
    _try_click(page, mapping, "_nav_nuevo_portal")
    page.wait_for_timeout(1000)
    _try_click(page, mapping, "_nav_presentar_declaracion")
    page.wait_for_timeout(1000)
    _try_click(page, mapping, "_nav_iniciar_nueva")
    page.wait_for_timeout(1000)


DRAFT_PAGE_WAIT_MS = 2000   # max 1–2 sec to detect draft text
DRAFT_POLL_MS = 100          # poll page body every 100 ms
DRAFT_INITIAL_WAIT_MS = 100  # minimal wait before first check


def dismiss_draft_if_present(page: Page, mapping: dict) -> bool:
    """If 'Formulario no concluido' is shown (saved draft), click trash icon and confirm 'Sí' to delete; then we can continue to initial form. Returns True if a draft was dismissed."""
    LOG.info("Checking for draft declaration (Formulario no concluido) after Presentar declaración, before filling initial form...")
    page.wait_for_timeout(DRAFT_INITIAL_WAIT_MS)
    draft_markers = ("formulario no concluido", "formularios no enviados")
    t_end = (time.perf_counter() * 1000) + DRAFT_PAGE_WAIT_MS
    draft_found = False
    while (time.perf_counter() * 1000) < t_end:
        try:
            body = (page.locator("body").inner_text(timeout=2000) or "").lower()
            if any(m in body for m in draft_markers):
                draft_found = True
                break
        except Exception:
            pass
        page.wait_for_timeout(DRAFT_POLL_MS)
    if not draft_found:
        LOG.info("No draft declaration detected; proceeding to configuration form.")
        return False
    LOG.info("Formulario no concluido detected; dismissing saved draft (trash → Sí)")
    try:
        # Click trash icon (first one next to a draft row)
        trash_clicked = False
        if mapping.get("_draft_trash"):
            for sel in mapping["_draft_trash"]:
                try:
                    loc = page.locator(sel).first
                    loc.wait_for(state="visible", timeout=1000)
                    loc.click()
                    trash_clicked = True
                    break
                except Exception:
                    continue
        if not trash_clicked:
            for fallback_selector in [
                "[aria-label*='eliminar']", "[aria-label*='Eliminar']",
                "[title*='eliminar']", "[title*='Eliminar']",
                "button:has(svg)", "a:has(svg)",
                "[class*='trash']", "[class*='eliminar']", "[class*='borrar']",
            ]:
                try:
                    loc = page.locator(fallback_selector).first
                    loc.wait_for(state="visible", timeout=800)
                    loc.click()
                    trash_clicked = True
                    break
                except Exception:
                    continue
        if not trash_clicked:
            try:
                card = page.get_by_text("Formularios no enviados", exact=False).locator("..").locator("..").locator("..").first
                trash = card.locator("button, a").filter(has_not=page.get_by_text(re.compile(r"INICIAR.*NUEVA DECLARACIÓN", re.I))).first
                trash.wait_for(state="visible", timeout=1000)
                trash.click()
                trash_clicked = True
            except Exception:
                try:
                    row = page.get_by_text("Formularios no enviados", exact=False).locator("..").locator("..")
                    trash = row.locator("button, a").filter(has=page.locator("svg, [class*='trash'], [class*='eliminar']")).first
                    trash.wait_for(state="visible", timeout=1000)
                    trash.click()
                    trash_clicked = True
                except Exception:
                    pass
        if not trash_clicked:
            LOG.warning("Could not find/click draft trash icon")
            return False
        page.wait_for_timeout(200)
        try:
            page.get_by_text("¿Deseas eliminar esta declaración?", exact=False).wait_for(state="visible", timeout=1000)
        except Exception:
            LOG.warning("Delete confirmation popup did not appear")
            return True
        si_clicked = False
        if mapping.get("_popup_eliminar_si"):
            for sel in mapping["_popup_eliminar_si"]:
                try:
                    page.locator(sel).first.click(timeout=1000)
                    si_clicked = True
                    break
                except Exception:
                    continue
        if not si_clicked:
            try:
                page.get_by_role("button", name=re.compile(r"^sí$", re.I)).first.click(timeout=1000)
                si_clicked = True
            except Exception:
                pass
        if not si_clicked:
            try:
                page.get_by_text("sí", exact=True).first.click(timeout=1000)
                si_clicked = True
            except Exception:
                pass
        if si_clicked:
            LOG.info("Draft deleted (Sí confirmed); continuing to initial form")
        page.wait_for_timeout(300)
        return True
    except Exception as e:
        LOG.warning("Dismiss draft failed: %s", e)
        return False


def open_configuration_form(page: Page, mapping: dict) -> bool:
    """Open 'Configuración de la declaración' by clicking 'Presentar declaración'. If draft page loads (no select), avoid long wait so draft check runs quickly."""
    ok = _try_click(page, mapping, "_nav_presentar_declaracion")
    if ok:
        page.wait_for_timeout(1000)
        try:
            page.locator("select").first.wait_for(state="visible", timeout=1200)
        except Exception:
            pass
    return ok


def transition_initial_to_phase3(page: Page, mapping: dict) -> bool:
    """After initial form: click SIGUIENTE, wait for loading to finish, then click CERRAR on the pre-fill info pop-up. Returns True if the full sequence succeeded."""
    if not _try_click(page, mapping, "_btn_siguiente"):
        LOG.warning("Could not click SIGUIENTE after initial form")
        return False
    page.wait_for_timeout(500)
    # Wait for "Cargando información" to disappear (variable time)
    try:
        loading = page.get_by_text("Cargando información", exact=False)
        for _ in range(PHASE3_LOADING_MAX_WAIT_SEC * 2):  # poll every 500ms
            if loading.count() == 0:
                break
            try:
                if not loading.first.is_visible():
                    break
            except Exception:
                break
            page.wait_for_timeout(500)
    except Exception:
        pass
    page.wait_for_timeout(300)
    # Wait for pre-fill pop-up to appear (CERRAR button can take several seconds after loading ends)
    cerrar_btn = page.get_by_role("button", name=re.compile(r"CERRAR", re.I)).first
    try:
        cerrar_btn.wait_for(state="visible", timeout=PHASE3_POPUP_WAIT_FOR_CERRAR_SEC * 1000)
        LOG.info("Pre-fill pop-up visible, clicking CERRAR")
    except Exception as e:
        LOG.warning("Pre-fill pop-up CERRAR button did not appear within %ss: %s", PHASE3_POPUP_WAIT_FOR_CERRAR_SEC, e)
    cerrar_ok = False
    try:
        cerrar_btn.click(timeout=PHASE3_POPUP_CERRAR_CLICK_MS)
        cerrar_ok = True
        LOG.info("Clicked CERRAR on pre-fill pop-up")
    except Exception:
        pass
    if not cerrar_ok and mapping.get("_popup_cerrar"):
        for sel in mapping["_popup_cerrar"]:
            try:
                btn = page.locator(sel).first
                btn.wait_for(state="visible", timeout=PHASE3_POPUP_CERRAR_SELECTOR_MS)
                btn.click()
                cerrar_ok = True
                LOG.info("Clicked CERRAR on pre-fill pop-up (mapping)")
                break
            except Exception:
                continue
    if not cerrar_ok:
        try:
            page.get_by_role("button", name=re.compile(r"CERRAR", re.I)).first.click(timeout=PHASE3_POPUP_CERRAR_CLICK_MS)
            cerrar_ok = True
            LOG.info("Clicked CERRAR on pre-fill pop-up (fallback)")
        except Exception as e:
            LOG.warning("Could not click CERRAR on pop-up: %s", e)
    if cerrar_ok:
        page.wait_for_timeout(1500)
    return cerrar_ok


def open_obligation_isr(page, mapping: dict) -> bool:
    """Select 'ISR simplificado de confianza. Personas físicas' (checkmark + label) to open the ISR section; then the Ingresos form loads. Returns True if clicked."""
    ok = _try_click(page, mapping, "_select_obligation_isr")
    if ok:
        page.wait_for_timeout(1500)
    return ok


def _set_dropdown_by_label(page: Page, label_substring: str, value: str, timeout_ms: int = 3000) -> bool:
    """Set a <select> that is associated with a label containing label_substring. value is option label (e.g. Sí/No)."""
    try:
        loc = page.get_by_label(re.compile(re.escape(label_substring), re.I))
        if loc.count() == 0:
            return False
        first = loc.first
        first.wait_for(state="visible", timeout=timeout_ms)
        tag = first.evaluate("el => el.tagName.toLowerCase()")
        if tag == "select":
            first.select_option(label=value)
            return True
    except Exception:
        pass
    return False


def _get_isr_ingresos_scope(page: Page) -> Page:
    """Return the page; ISR Ingresos form is a tab panel (id=tab457maincontainer1) on the main page, not an iframe."""
    return page


def _read_sat_total_ingresos_cobrados(page: Page, scope: Page, mapping: dict | None = None) -> float:
    """Read the value from SAT form section 'Total de ingresos efectivamente cobrados' (textbox/field in Ingresos tab). Ignores hidden modal. Returns 0.0 if not found."""
    sat_label = "Total de ingresos efectivamente cobrados"
    # If mapping has a selector for this field, try it first
    if mapping:
        for sel in mapping.get("_isr_ingresos_total_cobrados") or []:
            try:
                el = scope.locator(sel).first
                el.wait_for(state="visible", timeout=1200)
                raw = el.get_attribute("value") or el.input_value() or el.inner_text()
                parsed = _parse_currency(raw)
                if parsed != 0.0 or (raw and re.search(r"[\d,]", str(raw))):
                    LOG.info("[Phase3 DEBUG] SAT value: section=%r (mapping %r), raw=%r, parsed=%.2f", sat_label, sel, raw, parsed)
                    return parsed
            except Exception:
                continue
    # Prefer scope to main tab content so we don't match the hidden modal title
    tab_scope = scope.locator("#tab457maincontainer1").first if scope.locator("#tab457maincontainer1").count() > 0 else scope
    try:
        label_el = None
        for el in tab_scope.get_by_text(re.compile(r"Total de ingresos efectivamente cobrados", re.I)).all():
            try:
                if el.is_visible():
                    label_el = el
                    break
            except Exception:
                continue
        if label_el is None:
            for el in scope.get_by_text(re.compile(r"Total de ingresos efectivamente cobrados", re.I)).all():
                try:
                    if el.is_visible():
                        label_el = el
                        break
                except Exception:
                    continue
        if label_el is None:
            raise RuntimeError("No visible label 'Total de ingresos efectivamente cobrados' found")
        # Label's "for" attribute points to input id (SAT often uses this)
        try:
            for_id = label_el.get_attribute("for")
            if for_id and for_id.strip():
                inp = tab_scope.locator(f"#{re.escape(for_id.strip())}").first
                if inp.count() > 0:
                    inp.wait_for(state="visible", timeout=1200)
                    raw = inp.get_attribute("value") or inp.input_value() or inp.inner_text()
                    parsed = _parse_currency(raw)
                    if parsed != 0.0 or (raw and re.search(r"[\d,]", str(raw))):
                        LOG.info("[Phase3 DEBUG] SAT value: section=%r (label for=), raw=%r, parsed=%.2f", sat_label, raw, parsed)
                        return parsed
        except Exception:
            pass
        # Input(s) after the label in DOM order (SAT form: label then input with value 66,264; sometimes 2nd or 3rd input)
        for input_index in range(1, 5):
            try:
                following_input = label_el.locator(f"xpath=following::input[{input_index}]")
                if following_input.count() > 0:
                    following_input.first.wait_for(state="visible", timeout=900)
                    raw = following_input.first.get_attribute("value") or following_input.first.input_value() or following_input.first.inner_text()
                    parsed = _parse_currency(raw)
                    if parsed > 0 or (raw and re.search(r"[\d,]", str(raw))):
                        LOG.info("[Phase3 DEBUG] SAT value: section=%r (following::input[%s]), raw=%r, parsed=%.2f", sat_label, input_index, raw, parsed)
                        return parsed
            except Exception:
                continue
        # First try whole row text (works when value is in span/div next to label)
        for _ in range(2):
            try:
                row = label_el.locator("xpath=ancestor::tr[1]").first
                row.wait_for(state="visible", timeout=1200)
                row_text = row.inner_text()
                # Match number with optional commas: 66,264 or 66264
                m = re.search(r"[\d][\d,]*\.?\d*", row_text.replace(" ", ""))
                if m:
                    raw = m.group(0)
                    parsed = _parse_currency(raw)
                    if parsed > 0 or re.search(r"[\d]", raw):
                        LOG.info("[Phase3 DEBUG] SAT value: section=%r (from row text), raw=%r, parsed=%.2f", sat_label, raw, parsed)
                        return parsed
            except Exception:
                break
        # Try input in same row
        for xpath in [
            "xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*//input",
            "xpath=(ancestor::tr[1])//input",
            "xpath=following-sibling::*//input",
            "xpath=..//input",
        ]:
            try:
                inp = label_el.locator(xpath).first
                inp.wait_for(state="visible", timeout=900)
                raw = inp.get_attribute("value") or inp.inner_text()
                parsed = _parse_currency(raw)
                if parsed != 0.0 or (raw and re.search(r"[\d,]", str(raw))):
                    LOG.info("[Phase3 DEBUG] SAT value: section=%r, raw=%r, parsed=%.2f", sat_label, raw, parsed)
                    return parsed
            except Exception:
                continue
        # Sibling/div with number
        for xpath in [
            "xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*",
            "xpath=following-sibling::*",
        ]:
            try:
                val_el = label_el.locator(xpath).first
                val_el.wait_for(state="visible", timeout=900)
                raw = val_el.inner_text()
                parsed = _parse_currency(raw)
                if parsed != 0.0 or re.search(r"[\d,]", str(raw)):
                    LOG.info("[Phase3 DEBUG] SAT value: section=%r (sibling), raw=%r, parsed=%.2f", sat_label, raw, parsed)
                    return parsed
            except Exception:
                continue
        # Try get_by_label in full scope (control may not be inside tab container)
        for try_scope in (scope, tab_scope):
            try:
                control = try_scope.get_by_label(re.compile(r"Total de ingresos efectivamente cobrados", re.I)).first
                control.wait_for(state="visible", timeout=900)
                raw = control.get_attribute("value")
                if not raw or (isinstance(raw, str) and not raw.strip()):
                    try:
                        raw = control.input_value()
                    except Exception:
                        raw = control.inner_text() or ""
                raw = raw or control.inner_text()
                parsed = _parse_currency(raw)
                LOG.info("[Phase3 DEBUG] SAT value: section=%r (get_by_label), raw=%r, parsed=%.2f", sat_label, raw, parsed)
                return parsed
            except Exception:
                continue
        # SAT tab 457: try any visible input in tab that has a numeric value (prefilled total is often the only one)
        try:
            for inp in tab_scope.locator("input[type='text'], input[type='number'], input:not([type])").all():
                try:
                    if not inp.is_visible():
                        continue
                    raw = inp.get_attribute("value") or inp.input_value() or ""
                    parsed = _parse_currency(raw)
                    if parsed > 0 and re.search(r"[\d,]", str(raw)):
                        LOG.info("[Phase3 DEBUG] SAT value: section=%r (tab input), raw=%r, parsed=%.2f", sat_label, raw, parsed)
                        return parsed
                except Exception:
                    continue
        except Exception:
            pass
        # Last resort: any visible input/span in same table or container that contains a number
        try:
            container = label_el.locator("xpath=ancestor::table[1] | ancestor::*[contains(@class,'tab')][1] | ancestor::*[@id][1]").first
            for sel in ["input[type='text']", "input[type='number']", "span", "[role='textbox']"]:
                for node in container.locator(sel).all():
                    try:
                        if not node.is_visible():
                            continue
                        raw = node.get_attribute("value") or node.inner_text()
                        parsed = _parse_currency(raw)
                        if parsed > 0 and re.search(r"[\d,]", str(raw)):
                            LOG.info("[Phase3 DEBUG] SAT value: section=%r (container), raw=%r, parsed=%.2f", sat_label, raw, parsed)
                            return parsed
                    except Exception:
                        continue
        except Exception:
            pass
        LOG.warning("[Phase3 DEBUG] SAT value: %r not found in form (all strategies failed); using 0.0 for comparison", sat_label)
        return 0.0
    except Exception as e:
        LOG.info("[Phase3 DEBUG] SAT value: section=%r — could not read (returning 0.0): %s", sat_label, e)
        return 0.0


def _click_capturar_next_to_label(page_or_scope: Page | Frame, label_substring: str, *, occurrence: int = 0) -> bool:
    """Find the label containing label_substring (use occurrence=1 for second match, e.g. *Ingresos a disminuir section), then click the CAPTURAR in the same row only (not the first CAPTURAR in a large container). Returns True if clicked."""
    try:
        loc = page_or_scope.get_by_text(re.compile(re.escape(label_substring), re.I))
        if loc.count() <= occurrence:
            return False
        label_el = loc.nth(occurrence)
        label_el.wait_for(state="visible", timeout=800)
    except Exception:
        return False
    for i in range(1, 10):
        try:
            container = label_el.locator(f"xpath=(ancestor::*)[{i}]")
            if container.count() == 0:
                break
            caps = container.first.locator("a, button").filter(has_text=re.compile(r"CAPTURAR", re.I))
            n = caps.count()
            if n == 0:
                continue
            # Prefer container that has exactly one CAPTURAR (same row as label); else we might hit a big div and get the wrong (first) CAPTURAR
            if n == 1:
                capturar = caps.first
            else:
                # Multiple CAPTURARs: pick the one in the same row as label_el (row contains label and only one CAPTURAR)
                for j in range(n):
                    cap = caps.nth(j)
                    row = cap.locator("xpath=ancestor::tr[1]").first
                    if row.count() == 0:
                        continue
                    row_text = row.inner_text(timeout=200) or ""
                    # Same row as our label if it contains the label text; exclude question row "¿Tienes ingresos a disminuir?"
                    if label_substring.lower() in row_text.lower() and "¿Tienes" not in row_text:
                        capturar = cap
                        break
                else:
                    continue
            capturar.wait_for(state="visible", timeout=500)
            try:
                capturar.scroll_into_view_if_needed(timeout=250)
            except Exception:
                pass
            capturar.click()
            return True
        except Exception:
            continue
    return False


def _click_capturar_ingresos_a_disminuir(page: Page, scope: Page | Frame) -> bool:
    """Click the CAPTURAR for '*Ingresos a disminuir' only (the second CAPTURAR on the form; the first is Descuentos). Order: Sí → section appears → this CAPTURAR → popup AGREGAR → Concepto → Importe (int) → GUARDAR → CERRAR."""
    tab_scope = scope.locator("#tab457maincontainer1").first if scope.locator("#tab457maincontainer1").count() > 0 else scope
    # Strategy 1: CAPTURAR that immediately follows the dropdown "¿Tienes ingresos a disminuir?" (id 457select48) in DOM — that is the *Ingresos a disminuir row's CAPTURAR, not Descuentos
    try:
        for sel in ["#457select48", "select[id='457select48']", "[id='457select48']"]:
            try:
                dd = tab_scope.locator(sel).first
                dd.wait_for(state="visible", timeout=800)
                cap = dd.locator("xpath=following::a[contains(., 'CAPTURAR')]").first
                cap.wait_for(state="visible", timeout=1200)
                try:
                    cap.scroll_into_view_if_needed(timeout=400)
                except Exception:
                    pass
                cap.click()
                return True
            except Exception:
                continue
    except Exception:
        pass
    # Strategy 2: of all CAPTURARs in tab, click the one whose row contains "*Ingresos a disminuir" / "Ingresos a disminuir" but NOT "¿Tienes ingresos a disminuir" (so we get the section row, not the question)
    try:
        for cap in tab_scope.locator("a, button").filter(has_text=re.compile(r"CAPTURAR", re.I)).all():
            try:
                row = cap.locator("xpath=ancestor::tr[1] | ancestor::div[contains(@class,'row')][1]").first
                if row.count() == 0:
                    continue
                row_text = row.inner_text(timeout=500) or ""
                if "Ingresos a disminuir" in row_text and "¿Tienes ingresos a disminuir" not in row_text:
                    cap.scroll_into_view_if_needed(timeout=500)
                    cap.click()
                    return True
            except Exception:
                continue
    except Exception:
        pass
    # Strategy 3: second label "Ingresos a disminuir" (section input) → its row → CAPTURAR in that row
    try:
        inp = tab_scope.get_by_label(re.compile(r"Ingresos a disminuir", re.I))
        if inp.count() >= 2:
            row = inp.nth(1).locator("xpath=ancestor::tr[1] | ancestor::div[contains(@class,'row')][1]").first
            cap = row.locator("a, button").filter(has_text=re.compile(r"CAPTURAR", re.I)).first
            cap.wait_for(state="visible", timeout=800)
            try:
                cap.scroll_into_view_if_needed(timeout=500)
            except Exception:
                pass
            cap.click()
            return True
    except Exception:
        pass
    # Strategy 4: label-based with occurrence=1 (uses same-row logic to avoid clicking Descuentos CAPTURAR)
    if _click_capturar_next_to_label(tab_scope, "Ingresos a disminuir", occurrence=1):
        return True
    return False


def _click_capturar_total_percibidos(page: Page, scope: Page | Frame) -> bool:
    """Click the CAPTURAR to the right of 'Total de ingresos percibidos por la actividad' only (the third CAPTURAR; do not click Ingresos a disminuir CAPTURAR)."""
    tab_scope = scope.locator("#tab457maincontainer1").first if scope.locator("#tab457maincontainer1").count() > 0 else scope
    try:
        label_el = tab_scope.get_by_text("Total de ingresos percibidos por la actividad", exact=False).first
        label_el.wait_for(state="visible", timeout=800)
        for i in range(1, 10):
            try:
                container = label_el.locator(f"xpath=(ancestor::*)[{i}]")
                if container.count() == 0:
                    break
                caps = container.first.locator("a, button").filter(has_text=re.compile(r"CAPTURAR", re.I))
                n = caps.count()
                if n == 0:
                    continue
                if n == 1:
                    capturar = caps.first
                else:
                    for j in range(n):
                        cap = caps.nth(j)
                        cap_row = cap.locator("xpath=ancestor::tr[1]").first
                        if cap_row.count() == 0:
                            continue
                        if "Total de ingresos percibidos" in (cap_row.inner_text(timeout=200) or ""):
                            capturar = cap
                            break
                    else:
                        continue
                capturar.wait_for(state="visible", timeout=500)
                try:
                    capturar.scroll_into_view_if_needed(timeout=250)
                except Exception:
                    pass
                capturar.click()
                return True
            except Exception:
                continue
    except Exception:
        pass
    return False


def _set_dropdown_by_label_scope(scope: Page | Frame, page_for_click: Page, label_substring: str, value: str, timeout_ms: int = 5000) -> bool:
    """Set a <select> in scope by label containing label_substring. value is option label (e.g. Sí/No)."""
    try:
        loc = scope.get_by_label(re.compile(re.escape(label_substring), re.I))
        if loc.count() == 0:
            return False
        first = loc.first
        first.wait_for(state="visible", timeout=timeout_ms)
        tag = first.evaluate("el => el.tagName.toLowerCase()")
        if tag == "select":
            first.select_option(label=value)
            return True
    except Exception:
        pass
    return False


def fill_isr_ingresos_form(page: Page, mapping: dict, data: dict) -> None:
    """Fill ISR simplificado Ingresos form. Phase 3 (ISR simplificado de confianza. Personas físicas):
    - *¿Los ingresos fueron obtenidos a través de copropiedad? → always "No"
    - Total de ingresos efectivamente cobrados → skip (prefilled, no action)
    - Descuentos, devoluciones y bonificaciones → CAPTURAR → popup: set *Descuentos...integrantes por copropiedad to 0 → CERRAR
    - ¿Tienes ingresos a disminuir? → if (SAT Total de ingresos efectivamente cobrados - Excel Total de ingresos acumulados) > 1 then Sí + CAPTURAR popup (AGREGAR → Concepto "Ingresos facturados pendientes de cancelacion con aceptacion del receptor" → Importe = difference → GUARDAR → CERRAR); else No
    - ¿Tienes ingresos adicionales? → if (Excel Total de ingresos acumulados - SAT Total de ingresos efectivamente cobrados) > 1 then Sí + CAPTURAR popup (AGREGAR → Concepto "Ingresos no considerados en el prellenado" → Importe = difference → GUARDAR → CERRAR); else No
    - Then Total percibidos CAPTURAR popup."""
    LOG.info("Filling ISR Ingresos form...")
    label_map = data.get("label_map") or {}
    # Always "No" per requirements for *¿Los ingresos fueron obtenidos a través de copropiedad?
    copropiedad = "No"
    # Descuentos popup: value for *Descuentos, devoluciones y bonificaciones de integrantes por copropiedad (0 if not in Excel)
    descuentos_copropiedad = label_map.get("Descuentos devoluciones y bonificaciones de integrantes por copropiedad")
    if descuentos_copropiedad is None:
        descuentos_copropiedad = 0
    # Excel value for "Total de ingresos" (Impuestos tab col D→E); compared with SAT "Total de ingresos efectivamente cobrados"
    # Primary: row with "Base gravable del pago provisional" (col D = label, col E = value); fallbacks for other templates
    excel_label_1 = "Base gravable del pago provisional"
    excel_label_2 = "Ingresos cobrados y amparados por factura del mes"
    excel_label_3 = "Total de ingresos acumulados"
    excel_label_4 = "Ingresos nominales facturados"
    # Use first label that has a value (including 0.0); 'or' would treat 0.0 as missing
    _v1, _v2, _v3, _v4 = label_map.get(excel_label_1), label_map.get(excel_label_2), label_map.get(excel_label_3), label_map.get(excel_label_4)
    used_key = (
        excel_label_1 if _v1 is not None
        else (excel_label_2 if _v2 is not None
              else (excel_label_3 if _v3 is not None
                    else (excel_label_4 if _v4 is not None else None)))
    )
    excel_total_cobrados_raw = _v1 if _v1 is not None else (_v2 if _v2 is not None else (_v3 if _v3 is not None else _v4))
    excel_total_cobrados = _parse_currency(excel_total_cobrados_raw)
    workbook_path = data.get("workbook_path") or "(workbook path not set)"
    LOG.info(
        "[Phase3 DEBUG] Excel: workbook=%s | Sheet=Impuestos (layout: D/E or E/F). "
        "Row used: label=%r, value=%s → parsed=%.2f",
        workbook_path, used_key, excel_total_cobrados_raw, excel_total_cobrados,
    )
    LOG.info(
        "[Phase3 DEBUG] Excel values found: %r=%s, %r=%s, %r=%s, %r=%s",
        excel_label_1, _v1, excel_label_2, _v2, excel_label_3, _v3, excel_label_4, _v4,
    )
    importe_total = _v1 if _v1 is not None else (_v2 if _v2 is not None else (_v3 if _v3 is not None else _v4))
    if importe_total is None:
        importe_total = 0
    try:
        n = float(importe_total)
        importe_str = f"{n:,.2f}".replace(",", "")
    except (TypeError, ValueError):
        importe_str = str(importe_total)
    concepto_label = data.get("isr_ingresos_concepto") or "Actividad empresarial"

    page.wait_for_timeout(100)
    LOG.info("")
    LOG.info("===== ISR Ingresos: sección 1 - ¿Los ingresos fueron obtenidos a través de copropiedad? =====")
    # Wait for Ingresos form by label text (minimal wait for 5–10s total Phase 3)
    scope = _get_isr_ingresos_scope(page)
    try:
        scope.get_by_text("copropiedad", exact=False).first.wait_for(state="visible", timeout=600)
    except Exception:
        try:
            scope.get_by_text("ingresos fueron obtenidos", exact=False).first.wait_for(state="visible", timeout=500)
        except Exception:
            LOG.warning("ISR Ingresos form (label 'copropiedad') not visible")
    # 1. ¿Los ingresos fueron obtenidos a través de copropiedad? — use unique label (avoid matching "integrantes por copropiedad" in Descuentos)
    _si = copropiedad.strip().lower() in ("sí", "si", "yes")
    si_no_label = "Sí" if _si else "No"
    copropiedad_ok = _fill_select_next_to_label(scope, page, "ingresos fueron obtenidos a través de copropiedad", si_no_label, mapping=None, initial_dropdown_key=None)
    if not copropiedad_ok:
        copropiedad_ok = _set_dropdown_by_label_scope(scope, page, "ingresos fueron obtenidos a través de copropiedad", si_no_label, timeout_ms=1500)
    if copropiedad_ok:
        LOG.info("ISR Ingresos: copropiedad = %s (dropdown)", si_no_label)
    else:
        LOG.warning("ISR Ingresos: could not set copropiedad dropdown")
    page.wait_for_timeout(50)
    # 2. Total de ingresos efectivamente cobrados — no need to fill or do anything (prefilled, skip)
    LOG.info("")
    LOG.info("===== ISR Ingresos: sección 3 - Descuentos, devoluciones y bonificaciones =====")
    # 3. Descuentos, devoluciones y bonificaciones: press CAPTURAR → popup: add "0" on *Descuentos, devoluciones y bonificaciones de integrantes por copropiedad → CERRAR
    try:
        capturar_clicked = _try_click(page, mapping, "_isr_ingresos_capturar_descuentos")
        if not capturar_clicked:
            capturar_clicked = _click_capturar_next_to_label(scope, "Descuentos, devoluciones y bonificaciones")
        if not capturar_clicked:
            capturar_clicked = _click_capturar_next_to_label(scope, "Descuentos")
        if capturar_clicked:
            page.wait_for_timeout(50)
            descuentos_value = str(descuentos_copropiedad)
            filled = False
            try:
                page.get_by_text("Devoluciones, descuentos y bonificaciones facturadas", exact=False).first.wait_for(state="visible", timeout=300)
                LOG.info("ISR Ingresos: Descuentos popup (Devoluciones, descuentos y bonificaciones facturadas) visible")
            except Exception:
                pass
            page.wait_for_timeout(20)
            dialog = None
            for try_dialog in [
                lambda: page.get_by_role("dialog").first,
                lambda: page.locator(".modal, [role='dialog']").first,
                lambda: page.get_by_text("Devoluciones, descuentos y bonificaciones facturadas", exact=False).first.locator("xpath=ancestor::*[contains(@class,'modal') or contains(@class,'dialog') or @role='dialog'][1]"),
            ]:
                try:
                    d = try_dialog()
                    d.wait_for(state="visible", timeout=150)
                    dialog = d
                    break
                except Exception:
                    continue
            if dialog is None:
                dialog = page
            # Fast path: input in same row as label "integrantes por copropiedad"
            try:
                label_el = dialog.get_by_text(re.compile(r"integrantes por copropiedad", re.I)).first
                label_el.wait_for(state="visible", timeout=300)
                for xpath in [
                    "xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*//input",
                    "xpath=(ancestor::tr[1])//input",
                    "xpath=following-sibling::*//input",
                    "xpath=..//input",
                ]:
                    try:
                        inp = label_el.locator(xpath).first
                        inp.wait_for(state="visible", timeout=150)
                        inp.click()
                        inp.clear()
                        inp.fill(descuentos_value)
                        filled = True
                        break
                    except Exception:
                        continue
            except Exception:
                pass
            if not filled:
                try:
                    inp = dialog.get_by_label(re.compile(r"Descuentos.*integrantes por copropiedad", re.I)).first
                    inp.wait_for(state="visible", timeout=300)
                    inp.click()
                    inp.clear()
                    inp.fill(descuentos_value)
                    filled = True
                except Exception:
                    pass
            if not filled:
                try:
                    label_el = dialog.get_by_text(re.compile(r"Descuentos.*de integrantes por copropiedad", re.I)).first
                    label_el.wait_for(state="visible", timeout=200)
                    inp = label_el.locator("xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*//input").first
                    inp.wait_for(state="visible", timeout=150)
                    inp.click()
                    inp.clear()
                    inp.fill(descuentos_value)
                    filled = True
                except Exception:
                    pass
            if not filled:
                try:
                    modal_inputs = dialog.locator("input[type='text'], input[type='number'], input:not([type])")
                    n = min(modal_inputs.count(), 8)
                    for idx in range(n):
                        inp = modal_inputs.nth(idx)
                        try:
                            if inp.get_attribute("disabled"):
                                continue
                            inp.wait_for(state="visible", timeout=100)
                            row = inp.locator("xpath=ancestor::tr[1] | ancestor::div[contains(@class,'row')][1]")
                            if row.count() > 0 and row.first.get_by_text("integrantes por copropiedad").count() > 0:
                                inp.click()
                                inp.clear()
                                inp.fill(descuentos_value)
                                filled = True
                                break
                        except Exception:
                            continue
                except Exception:
                    pass
            if not filled:
                LOG.warning("ISR Ingresos: could not find Descuentos popup textbox (*Descuentos...integrantes por copropiedad)")
            page.wait_for_timeout(30)
            page.get_by_role("button", name=re.compile(r"CERRAR", re.I)).first.click(timeout=800)
            LOG.info("ISR Ingresos: Descuentos popup filled and closed")
        else:
            LOG.warning("ISR Ingresos: could not click Descuentos CAPTURAR link")
        page.wait_for_timeout(80)
    except Exception as e:
        LOG.warning("ISR Ingresos: Descuentos CAPTURAR/popup failed: %s", e)
    LOG.info("")
    LOG.info("===== ISR Ingresos: sección 4 - ¿Tienes ingresos a disminuir? / *Ingresos a disminuir =====")
    # 4. ¿Tienes ingresos a disminuir? — compare SAT "Total de ingresos efectivamente cobrados" vs Excel (Total de ingresos acumulados); if SAT - Excel > 1 → Sí + CAPTURAR popup
    sat_total_cobrados = _read_sat_total_ingresos_cobrados(page, scope, mapping)
    sat_total_cobrados = float(sat_total_cobrados) if sat_total_cobrados is not None else 0.0
    excel_total_cobrados = float(excel_total_cobrados) if excel_total_cobrados is not None else 0.0
    LOG.info("[Phase3 DEBUG] Comparison for *¿Tienes ingresos a disminuir? / *¿Tienes ingresos adicionales?: SAT=%.2f, Excel=%.2f", sat_total_cobrados, excel_total_cobrados)
    diferencia = sat_total_cobrados - excel_total_cobrados
    need_ingresos_a_disminuir = diferencia > 1
    si_no_disminuir_lbl = "Sí" if need_ingresos_a_disminuir else "No"
    page.wait_for_timeout(200)
    if _fill_select_next_to_label(scope, page, "ingresos a disminuir", si_no_disminuir_lbl, mapping=None, initial_dropdown_key=None):
        LOG.info("ISR Ingresos: ingresos a disminuir = %s (SAT=%.2f Excel=%.2f diff=%.2f)", si_no_disminuir_lbl, sat_total_cobrados, excel_total_cobrados, diferencia)
    else:
        LOG.warning("ISR Ingresos: could not set ingresos a disminuir dropdown")
    page.wait_for_timeout(150)
    if need_ingresos_a_disminuir:
        # *Ingresos a disminuir (section below dropdown) appears: wait for it, then CAPTURAR → popup AGREGAR → Concepto → Importe (int, no decimals) → GUARDAR → CERRAR
        page.wait_for_timeout(400)
        tab_scope = scope.locator("#tab457maincontainer1").first if scope.locator("#tab457maincontainer1").count() > 0 else scope
        try:
            tab_scope.get_by_text(re.compile(r"Ingresos a disminuir", re.I)).nth(1).wait_for(state="visible", timeout=2000)
        except Exception:
            pass
        page.wait_for_timeout(150)
        try:
            capturar_clicked = _click_capturar_ingresos_a_disminuir(page, scope)
            if capturar_clicked:
                LOG.info("ISR Ingresos: Ingresos a disminuir CAPTURAR pressed")
                page.wait_for_timeout(150)
                try:
                    page.get_by_role("button", name=re.compile(r"AGREGAR", re.I)).first.click(timeout=1500)
                    page.wait_for_timeout(80)
                except Exception as e_ag:
                    LOG.warning("ISR Ingresos: could not click AGREGAR in Ingresos a disminuir popup: %s", e_ag)
                # Importe = difference with no decimals (e.g. 8178.81 → 8178)
                importe_val = int(round(diferencia))
                importe_str = str(importe_val)
                # Scope to the "Ingresos a disminuir" popup: modal has title + Concepto + Importe + GUARDAR; use .last for div so we get the modal, not main form
                dialog = None
                for use_last, dialog_loc in [
                    (False, page.get_by_role("dialog")),
                    (False, page.locator("[role='dialog']")),
                    (True, page.locator("div").filter(has_text=re.compile(r"Ingresos a disminuir", re.I)).filter(has_text="Concepto").filter(has_text="Importe").filter(has_text="GUARDAR")),
                    (True, page.locator("div").filter(has_text=re.compile(r"Ingresos a disminuir", re.I)).filter(has=page.locator("select"))),
                ]:
                    try:
                        if dialog_loc.count() > 0:
                            d = dialog_loc.last if use_last else dialog_loc.first
                            d.wait_for(state="visible", timeout=1200)
                            dialog = d
                            break
                    except Exception:
                        continue
                if dialog is None:
                    dialog = page
                # Click AGREGAR inside the popup so Concepto/Importe row is ready
                if dialog != page:
                    try:
                        dialog.get_by_role("button", name=re.compile(r"AGREGAR", re.I)).first.click(timeout=1000)
                        page.wait_for_timeout(80)
                    except Exception:
                        pass
                # Option text variants (SAT may use accents)
                concepto_option_labels = [
                    "Ingresos facturados pendientes de cancelacion con aceptacion del receptor",
                    "Ingresos facturados pendientes de cancelación con aceptación del receptor",
                ]
                # Strategy 1: *Concepto is label, dropdown on the right; then select option
                concepto_ok = False
                for opt_label in concepto_option_labels:
                    if concepto_ok:
                        break
                    try:
                        loc_concepto = dialog.get_by_text(re.compile(r"\*?\s*Concepto", re.I))
                        if loc_concepto.count() == 0:
                            loc_concepto = dialog.get_by_text("Concepto", exact=False)
                        label_concepto = loc_concepto.first
                        label_concepto.wait_for(state="visible", timeout=800)
                        for xpath in [
                            "xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*//select",
                            "xpath=(ancestor::tr[1])//select",
                            "xpath=following-sibling::*//select",
                            "xpath=..//select",
                        ]:
                            try:
                                concepto_dd = label_concepto.locator(xpath).first
                                concepto_dd.wait_for(state="visible", timeout=500)
                                concepto_dd.select_option(label=opt_label)
                                concepto_ok = True
                                page.wait_for_timeout(100)
                                break
                            except Exception:
                                continue
                    except Exception:
                        continue
                if not concepto_ok:
                    try:
                        sel = dialog.locator("select").first
                        sel.wait_for(state="visible", timeout=800)
                        for opt_label in concepto_option_labels:
                            try:
                                sel.select_option(label=opt_label)
                                concepto_ok = True
                                page.wait_for_timeout(100)
                                break
                            except Exception:
                                continue
                    except Exception as e_c:
                        LOG.warning("ISR Ingresos: Concepto in popup: %s", e_c)
                if concepto_ok:
                    LOG.info("ISR Ingresos: Ingresos a disminuir popup: selected *Concepto (Ingresos facturados pendientes de cancelación con aceptación del receptor)")
                # Importe: same row as Concepto dropdown (form row), then GUARDAR → CERRAR
                importe_ok = False
                # Strategy A: input in the same row as the Concepto select we just used (most reliable)
                try:
                    sel_first = dialog.locator("select").first
                    sel_first.wait_for(state="visible", timeout=500)
                    for row_xpath in ["xpath=ancestor::tr[1]", "xpath=ancestor::*[.//input][1]"]:
                        try:
                            row = sel_first.locator(row_xpath)
                            if row.count() == 0:
                                continue
                            inp = row.locator("input[type='text'], input[type='number'], input:not([type])").first
                            inp.wait_for(state="visible", timeout=400)
                            if inp.get_attribute("disabled") or inp.get_attribute("readonly"):
                                continue
                            inp.click()
                            inp.fill(importe_str)
                            importe_ok = True
                            page.wait_for_timeout(80)
                            break
                        except Exception:
                            continue
                except Exception:
                    pass
                if not importe_ok:
                    try:
                        label_importe = dialog.get_by_text("Importe", exact=False).first
                        label_importe.wait_for(state="visible", timeout=800)
                        for xpath in [
                            "xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*//input[not(@disabled)]",
                            "xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*//input",
                            "xpath=(ancestor::tr[1])//input[not(@disabled)]",
                            "xpath=(ancestor::tr[1])//input",
                            "xpath=following-sibling::*//input",
                        ]:
                            try:
                                importe_inp = label_importe.locator(xpath).first
                                importe_inp.wait_for(state="visible", timeout=400)
                                if importe_inp.get_attribute("disabled") or importe_inp.get_attribute("readonly"):
                                    continue
                                importe_inp.click()
                                importe_inp.fill(importe_str)
                                importe_ok = True
                                page.wait_for_timeout(80)
                                break
                            except Exception:
                                continue
                    except Exception as e_i:
                        LOG.warning("ISR Ingresos: Importe in popup: %s", e_i)
                if not importe_ok:
                    try:
                        for inp in dialog.locator("input[type='text'], input[type='number'], input:not([type])").all():
                            try:
                                if inp.get_attribute("disabled") or inp.get_attribute("readonly"):
                                    continue
                                inp.wait_for(state="visible", timeout=300)
                                inp.fill(importe_str)
                                importe_ok = True
                                break
                            except Exception:
                                continue
                    except Exception:
                        pass
                if importe_ok:
                    LOG.info("ISR Ingresos: Ingresos a disminuir popup: filled Importe=%s", importe_str)
                if concepto_ok and importe_ok:
                    try:
                        btn_scope = dialog if dialog != page else page
                        print(f"{_debug_ts()} [Phase3 DEBUG] Ingresos a disminuir popup flags: concepto_ok={concepto_ok}, importe_ok={importe_ok}, importe_str={importe_str}")
                        print(f"{_debug_ts()} [Phase3 DEBUG] Ingresos a disminuir: clicking GUARDAR")
                        btn_scope.get_by_role("button", name=re.compile(r"GUARDAR", re.I)).first.click(timeout=1500)
                        page.wait_for_timeout(200)
                        # Small confirmation popup ("Captura la información requerida") with ACEPTAR button
                        try:
                            confirm_btn = page.get_by_role("button", name=re.compile(r"ACEPTAR", re.I)).first
                            confirm_btn.wait_for(state="visible", timeout=1500)
                            print(f"{_debug_ts()} [Phase3 DEBUG] Ingresos a disminuir: clicking ACEPTAR")
                            confirm_btn.click()
                            page.wait_for_timeout(300)
                        except Exception:
                            pass
                        print(f"{_debug_ts()} [Phase3 DEBUG] Ingresos a disminuir: clicking CERRAR")
                        btn_scope.get_by_role("button", name=re.compile(r"CERRAR", re.I)).first.click(timeout=1500)
                        LOG.info("ISR Ingresos: Ingresos a disminuir popup filled (importe=%s), closed", importe_str)
                        page.wait_for_timeout(80)
                    except Exception as e_btn:
                        LOG.warning("ISR Ingresos: GUARDAR/CERRAR in Ingresos a disminuir popup: %s", e_btn)
                else:
                    LOG.warning("ISR Ingresos: Ingresos a disminuir popup not filled (concepto=%s, importe=%s); not clicking GUARDAR", concepto_ok, importe_ok)
            else:
                LOG.warning("ISR Ingresos: could not click Ingresos a disminuir CAPTURAR")
        except Exception as e:
            LOG.warning("ISR Ingresos: Ingresos a disminuir CAPTURAR/popup failed: %s", e)
    page.wait_for_timeout(80)
    LOG.info("")
    LOG.info("===== ISR Ingresos: sección 5 - ¿Tienes ingresos adicionales? =====")
    # 5. ¿Tienes ingresos adicionales? — if Excel > SAT (difference > 1) → Sí + CAPTURAR popup with "Ingresos no considerados en el prellenado" and Importe = difference
    diferencia_adicionales = (excel_total_cobrados or 0.0) - (sat_total_cobrados or 0.0)
    need_ingresos_adicionales = diferencia_adicionales > 1
    si_no_adicionales_lbl = "Sí" if need_ingresos_adicionales else "No"
    if _fill_select_next_to_label(scope, page, "¿Tienes ingresos adicionales", si_no_adicionales_lbl, mapping=None, initial_dropdown_key=None):
        LOG.info("ISR Ingresos: ingresos adicionales = %s (SAT=%.2f Excel=%.2f diff=%.2f)", si_no_adicionales_lbl, sat_total_cobrados, excel_total_cobrados, diferencia_adicionales)
    else:
        LOG.warning("ISR Ingresos: could not set ingresos adicionales dropdown")
    page.wait_for_timeout(80)
    if need_ingresos_adicionales:
        # *Ingresos adicionales appears: CAPTURAR → popup AGREGAR → Concepto "Ingresos no considerados en el prellenado" → Importe = difference → GUARDAR → CERRAR
        try:
            capturar_clicked = _click_capturar_next_to_label(page, "Ingresos adicionales")
            if capturar_clicked:
                page.wait_for_timeout(200)
                try:
                    page.get_by_role("button", name=re.compile(r"AGREGAR", re.I)).first.click(timeout=1500)
                    page.wait_for_timeout(150)
                except Exception:
                    pass
                diferencia_adic_str = f"{diferencia_adicionales:,.2f}".replace(",", "")
                try:
                    concepto_dd = page.get_by_label(re.compile(r"Concepto", re.I)).first
                    concepto_dd.wait_for(state="visible", timeout=4000)
                    concepto_dd.select_option(label=re.compile(r"Ingresos no considerados en el prellenado", re.I))
                    page.wait_for_timeout(200)
                except Exception:
                    try:
                        page.locator("select").first.select_option(label=re.compile(r"no considerados en el prellenado", re.I))
                        page.wait_for_timeout(200)
                    except Exception:
                        pass
                try:
                    importe_inp = page.get_by_label(re.compile(r"Importe", re.I)).first
                    importe_inp.wait_for(state="visible", timeout=1200)
                    importe_inp.fill(diferencia_adic_str)
                    page.wait_for_timeout(200)
                except Exception:
                    pass
                if mapping.get("_popup_guardar"):
                    for sel in mapping["_popup_guardar"]:
                        try:
                            page.locator(sel).first.click(timeout=3000)
                            break
                        except Exception:
                            continue
                else:
                    page.get_by_role("button", name=re.compile(r"GUARDAR", re.I)).first.click(timeout=3000)
                page.wait_for_timeout(500)
                page.get_by_role("button", name=re.compile(r"CERRAR", re.I)).first.click(timeout=3000)
                LOG.info("ISR Ingresos: Ingresos adicionales popup filled (diff=%.2f), closed", diferencia_adicionales)
                page.wait_for_timeout(500)
            else:
                LOG.warning("ISR Ingresos: could not click Ingresos adicionales CAPTURAR")
        except Exception as e:
            LOG.warning("ISR Ingresos: Ingresos adicionales CAPTURAR/popup failed: %s", e)
        page.wait_for_timeout(80)
    LOG.info("")
    LOG.info("===== ISR Ingresos: sección 6 - Total de ingresos percibidos por la actividad =====")
    # 6. Total de ingresos percibidos por la actividad: press CAPTURAR → popup "Total de ingresos efectivamente cobrados" → for each Excel row (Actividad empresarial, Actividad profesional, Uso o goce temporal) with value != "-": AGREGAR → Concepto → Importe → GUARDAR → ACEPTAR → then CERRAR
    try:
        capturar_clicked = _try_click(page, mapping, "_isr_ingresos_capturar_total")
        if not capturar_clicked:
            capturar_clicked = _click_capturar_total_percibidos(page, scope)
        if not capturar_clicked:
            capturar_clicked = _click_capturar_next_to_label(scope, "Total de ingresos percibidos por la actividad")
        if not capturar_clicked:
            capturar_clicked = _click_capturar_next_to_label(scope, "Total de ingresos percibidos")
        if not capturar_clicked:
            raise RuntimeError("Could not click Total percibidos CAPTURAR link")
        LOG.info("ISR Ingresos: Total percibidos CAPTURAR pressed")
        page.wait_for_timeout(150)
        # Resolve "Total de ingresos efectivamente cobrados" popup dialog
        dialog = None
        for use_last, dialog_loc in [
            (False, page.get_by_role("dialog")),
            (False, page.locator("[role='dialog']")),
            (True, page.locator("div").filter(has_text=re.compile(r"Total de ingresos efectivamente cobrados", re.I)).filter(has_text=re.compile(r"Concepto|AGREGAR|Monto", re.I))),
            (True, page.locator("div").filter(has_text=re.compile(r"Total de ingresos efectivamente cobrados", re.I)).filter(has=page.locator("select"))),
        ]:
            try:
                if dialog_loc.count() > 0:
                    d = dialog_loc.last if use_last else dialog_loc.first
                    d.wait_for(state="visible", timeout=2000)
                    dialog = d
                    break
            except Exception:
                continue
        if dialog is None:
            dialog = page
        # Three Excel labels (col D or E) → SAT Concepto dropdown option; only add when value is not "-" or missing
        total_percibidos_entries = [
            ("Actividad empresarial", "Actividad empresarial"),
            ("Actividad profesional (honorarios)", "Servicios profesionales (Honorarios)"),
            ("Uso o goce temporal de bienes (arrendamiento)", "Uso o goce temporal de bienes"),
        ]
        for excel_label, sat_concepto in total_percibidos_entries:
            raw = label_map.get(excel_label)
            if raw is None:
                continue
            # Skip when Excel shows "-" or the parsed numeric value is zero
            if isinstance(raw, str) and str(raw).strip() == "-":
                continue
            parsed = _parse_currency(raw)
            if abs(parsed) < 0.005:
                continue
            importe_str = str(int(round(parsed)))
            LOG.info("ISR Ingresos: Total percibidos adding entry: excel_label=%r, sat_concepto=%r, importe=%s", excel_label, sat_concepto, importe_str)
            try:
                if dialog != page:
                    dialog.get_by_role("button", name=re.compile(r"AGREGAR", re.I)).first.click(timeout=1500)
                    page.wait_for_timeout(150)
            except Exception as e_ag:
                LOG.warning("ISR Ingresos: Total percibidos AGREGAR for %r: %s", excel_label, e_ag)
                continue
            concepto_ok = False
            try:
                loc_concepto = dialog.get_by_text(re.compile(r"\*?\s*Concepto", re.I))
                if loc_concepto.count() == 0:
                    loc_concepto = dialog.get_by_text("Concepto", exact=False)
                if loc_concepto.count() > 0:
                    label_concepto = loc_concepto.first
                    label_concepto.wait_for(state="visible", timeout=800)
                    for xpath in [
                        "xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*//select",
                        "xpath=(ancestor::tr[1])//select",
                        "xpath=following-sibling::*//select",
                        "xpath=..//select",
                    ]:
                        try:
                            concepto_dd = label_concepto.locator(xpath).first
                            concepto_dd.wait_for(state="visible", timeout=500)
                            concepto_dd.select_option(label=sat_concepto)
                            concepto_ok = True
                            page.wait_for_timeout(100)
                            break
                        except Exception:
                            continue
            except Exception:
                pass
            if not concepto_ok:
                try:
                    sel = dialog.locator("select").first
                    sel.wait_for(state="visible", timeout=800)
                    sel.select_option(label=sat_concepto)
                    concepto_ok = True
                    page.wait_for_timeout(100)
                except Exception as e_c:
                    LOG.warning("ISR Ingresos: Total percibidos Concepto %r: %s", sat_concepto, e_c)
            if concepto_ok:
                LOG.info(
                    "ISR Ingresos: Total percibidos popup: selected Concepto=%r for excel_label=%r",
                    sat_concepto,
                    excel_label,
                )
            importe_ok = False
            try:
                sel_first = dialog.locator("select").first
                sel_first.wait_for(state="visible", timeout=500)
                for row_xpath in ["xpath=ancestor::tr[1]", "xpath=ancestor::*[.//input][1]"]:
                    try:
                        row = sel_first.locator(row_xpath)
                        if row.count() == 0:
                            continue
                        inp = row.locator("input[type='text'], input[type='number'], input:not([type])").first
                        inp.wait_for(state="visible", timeout=400)
                        if inp.get_attribute("disabled") or inp.get_attribute("readonly"):
                            continue
                        inp.click()
                        inp.fill(importe_str)
                        importe_ok = True
                        page.wait_for_timeout(80)
                        break
                    except Exception:
                        continue
            except Exception:
                pass
            if not importe_ok:
                try:
                    label_importe = dialog.get_by_text("Importe", exact=False).first
                    label_importe.wait_for(state="visible", timeout=500)
                    for xpath in [
                        "xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*//input[not(@disabled)]",
                        "xpath=(ancestor::tr[1])//input[not(@disabled)]",
                        "xpath=following-sibling::*//input",
                    ]:
                        try:
                            importe_inp = label_importe.locator(xpath).first
                            importe_inp.wait_for(state="visible", timeout=400)
                            if importe_inp.get_attribute("readonly"):
                                continue
                            importe_inp.click()
                            importe_inp.fill(importe_str)
                            importe_ok = True
                            page.wait_for_timeout(80)
                            break
                        except Exception:
                            continue
                except Exception:
                    pass
            if importe_ok:
                LOG.info(
                    "ISR Ingresos: Total percibidos popup: filled Importe=%s for excel_label=%r",
                    importe_str,
                    excel_label,
                )
            if concepto_ok and importe_ok:
                try:
                    btn_scope = dialog if dialog != page else page
                    btn_scope.get_by_role("button", name=re.compile(r"GUARDAR", re.I)).first.click(timeout=1500)
                    page.wait_for_timeout(200)
                    try:
                        confirm_btn = page.get_by_role("button", name=re.compile(r"ACEPTAR", re.I)).first
                        confirm_btn.wait_for(state="visible", timeout=1500)
                        confirm_btn.click()
                        page.wait_for_timeout(300)
                    except Exception:
                        pass
                except Exception as e_btn:
                    LOG.warning("ISR Ingresos: Total percibidos GUARDAR/ACEPTAR for %r: %s", excel_label, e_btn)
            else:
                LOG.warning("ISR Ingresos: Total percibidos entry not filled (concepto=%s, importe=%s) for %r", concepto_ok, importe_ok, excel_label)
        # Close the popup
        try:
            btn_scope = dialog if dialog != page else page
            btn_scope.get_by_role("button", name=re.compile(r"CERRAR", re.I)).first.click(timeout=1500)
            LOG.info("ISR Ingresos: Total percibidos popup filled and closed")
        except Exception as e_close:
            LOG.warning("ISR Ingresos: Total percibidos CERRAR: %s", e_close)
        page.wait_for_timeout(150)
    except Exception as e:
        LOG.warning("ISR Ingresos: Total percibidos CAPTURAR/popup failed: %s", e)
    LOG.info("ISR Ingresos form fill completed")


def logout_sat(page: Page, mapping: dict) -> None:
    """Click 'Cerrar' (next to Inicio) in the SAT nav bar to log out. Safe to call if not logged in or element missing."""
    try:
        if _try_click(page, mapping, "_nav_cerrar"):
            LOG.info("Logged out from SAT (Cerrar clicked)")
            page.wait_for_timeout(700)
        else:
            page.get_by_role("link", name=re.compile(r"Cerrar", re.I)).first.click(timeout=2000)
            page.wait_for_timeout(700)
            LOG.info("Logged out from SAT (Cerrar clicked via fallback)")
    except Exception as e:
        LOG.debug("Logout (Cerrar) skipped or failed: %s", e)


def _debug_ts() -> str:
    """Timestamp for debug prints (same format as login_sat)."""
    now = datetime.now()
    return now.strftime("%Y-%m-%d %H:%M:%S") + f",{now.microsecond // 1000:03d}"


def _try_fill_select_by_index(scope: Page | Frame, index: int, value_str: str) -> bool:
    """Try to set a <select> by its index in scope (0-based). Uses value then label. Returns True if successful."""
    try:
        loc = scope.locator("select")
        sel = loc.nth(index)
        sel.wait_for(state="visible", timeout=4000)
        try:
            sel.select_option(value=value_str)
            return True
        except Exception:
            sel.select_option(label=value_str)
            return True
    except Exception:
        return False


def _fill_select_by_mapping(
    scope: Page | Frame, page_for_wait: Page, selector_list: list, value_str: str
) -> bool:
    """Try each selector in the list (e.g. from form_field_mapping) and set the select by value then label. Returns True if any succeeds."""
    if not selector_list:
        return False
    for sel_str in selector_list:
        try:
            dropdown = scope.locator(sel_str).first
            dropdown.wait_for(state="visible", timeout=1200)
            try:
                dropdown.select_option(value=value_str, timeout=2000)
                return True
            except Exception:
                dropdown.select_option(label=value_str, timeout=2000)
                return True
        except Exception:
            continue
    return False


def _fill_select_next_to_label(
    scope: Page | Frame,
    page_for_wait: Page,
    label_text: str,
    value_str: str,
    mapping: dict | None = None,
    initial_dropdown_key: str | None = None,
) -> bool:
    """Locate dropdown (by label or mapping), press dropdown, scroll if needed, select option matching Excel value (fallback to select_option for native <select>)."""
    scope_type = "iframe" if isinstance(scope, Frame) else "page"
    print(f"{_debug_ts()} [initial form DEBUG] Label={label_text!r} value={value_str!r} scope={scope_type}")

    _SCROLL_TIMEOUT_MS = 200
    _OPTION_CLICK_TIMEOUT_MS = 350

    def do_press_dropdown_then_click_option(dropdown) -> bool:
        """Try fast path (select_option) first; else open dropdown and click option."""
        try:
            dropdown.wait_for(state="visible", timeout=1000)
            try:
                dropdown.scroll_into_view_if_needed(timeout=_SCROLL_TIMEOUT_MS)
            except Exception:
                pass
            try:
                dropdown.select_option(value=value_str, timeout=800)
                return True
            except Exception:
                pass
            try:
                dropdown.select_option(label=value_str, timeout=800)
                return True
            except Exception:
                pass
            page_for_wait.wait_for_timeout(80)
            dropdown.click(timeout=600)
            page_for_wait.wait_for_timeout(120)
            opt_by_value = dropdown.locator(f"option[value={repr(value_str)}]")
            if opt_by_value.count() > 0:
                option = opt_by_value.first
            else:
                option = dropdown.locator("option").filter(has_text=re.compile(re.escape(value_str), re.I)).first
            option.wait_for(state="attached", timeout=400)
            try:
                option.click(timeout=_OPTION_CLICK_TIMEOUT_MS)
                return True
            except Exception:
                try:
                    dropdown.select_option(value=value_str, timeout=800)
                    return True
                except Exception:
                    dropdown.select_option(label=value_str, timeout=800)
                    return True
        except Exception as e2:
            print(f"{_debug_ts()} [initial form DEBUG]   Strategy 1 (press dropdown + select) failed: {e2}")
            return False

    def resolve_dropdown_from_label(label_el):
        """Return a visible <select> near the label (prefer dropdown to the right of label; skip hidden selects)."""
        def first_visible_select(loc):
            if loc.count() == 0:
                return None
            for i in range(loc.count()):
                try:
                    el = loc.nth(i)
                    el.wait_for(state="visible", timeout=400)
                    return el
                except Exception:
                    continue
            return None
        # 1) Prefer select in the next cell to the right (dropdown to the right of label)
        sel = label_el.locator("xpath=((ancestor::td | ancestor::th)[1])/following-sibling::*[1]//select")
        out = first_visible_select(sel)
        if out is not None:
            return out
        # 2) Select in same row (tr); pick first visible (phase-2 style)
        sel = label_el.locator("xpath=ancestor::tr[1]//select")
        out = first_visible_select(sel)
        if out is not None:
            return out
        # 3) Same cell
        sel = label_el.locator("xpath=((ancestor::td | ancestor::th)[1])//select")
        out = first_visible_select(sel)
        if out is not None:
            return out
        # 4) Ancestor with single select
        sel = label_el.locator("xpath=(ancestor::*[.//select and count(descendant::select)=1])[1]//select")
        out = first_visible_select(sel)
        if out is not None:
            return out
        sel = label_el.locator("xpath=..").locator("select")
        out = first_visible_select(sel)
        if out is not None:
            return out
        if sel.count() > 0:
            return sel.first
        return None

    # Locate dropdown: try mapping first when provided (fast id/selectors); else resolve by label (slow xpath).
    dropdown = None
    if initial_dropdown_key and mapping and mapping.get(initial_dropdown_key):
        sel_list = mapping[initial_dropdown_key] if isinstance(mapping[initial_dropdown_key], list) else [mapping[initial_dropdown_key]]
        for sel_str in sel_list:
            try:
                loc = scope.locator(sel_str).first
                loc.wait_for(state="visible", timeout=1000)
                dropdown = loc
                break
            except Exception:
                continue
    if dropdown is None:
        try:
            xpath_contains_arg = "'" + label_text.replace("'", "''") + "'"
            xpath_label = (
                "//*[(self::label or self::td or self::th or self::span or self::div)"
                " and not(ancestor::select) and contains(., " + xpath_contains_arg + ")]"
            )
            label_el = scope.locator("xpath=" + xpath_label).last
            label_el.wait_for(state="attached", timeout=1200)
            dropdown = resolve_dropdown_from_label(label_el)
        except Exception:
            pass
    try:
        if dropdown is not None:
            try:
                el_id = dropdown.get_attribute("id") or "(no id)"
                print(f"{_debug_ts()} [initial form DEBUG]   Strategy 1 (press dropdown, scroll, select option): dropdown id={el_id!r}")
            except Exception:
                pass
            if do_press_dropdown_then_click_option(dropdown):
                print(f"{_debug_ts()} [initial form DEBUG]   Strategy 1: filled OK")
                return True
    except Exception as e:
        print(f"{_debug_ts()} [initial form DEBUG]   Strategy 1 exception: {e}")

    print(f"{_debug_ts()} [initial form DEBUG]   Result: NOT filled for label={label_text!r}")
    return False


def _get_declaration_form_scope(page: Page) -> Page | Frame:
    """Return the page or the frame that contains the declaration form selects. SAT often loads the form in an iframe."""
    probe = "select[id*='TipoDeclaracion'], select[id*='EjercicioFiscal']"
    try:
        loc = page.locator(probe)
        if loc.count() > 0:
            return page
    except Exception:
        pass
    for frame in page.frames:
        if frame == page.main_frame:
            continue
        try:
            loc = frame.locator(probe)
            if loc.count() > 0:
                LOG.info("Declaration form found in iframe")
                return frame
        except Exception:
            continue
    return page


def fill_initial_form(page: Page, data: dict, mapping: dict) -> None:
    """Fill Configuración de la declaración in order: Ejercicio (2022–2026) → Periodicidad → Periodo (Enero–Diciembre, YTD) → Tipo de declaración (Normal / Normal por Corrección Fiscal). Each dropdown may appear after the previous is selected."""
    scope = _get_declaration_form_scope(page)
    page_for_wait = scope.page if isinstance(scope, Frame) else scope
    year = data.get("year")
    month = data.get("month")
    periodicidad = data.get("periodicidad", 1)
    tipo = data.get("tipo_declaracion", "Normal")
    try:
        p = int(periodicidad) if periodicidad is not None else 1
    except (TypeError, ValueError):
        p = 1
    periodicidad_value = _SAT_PERIODICIDAD_VALUE.get(p, "M")
    periodo_str = f"{month:02d}" if month is not None else "N/A"
    print(f"{_debug_ts()} [initial form DEBUG] Scope: {'iframe' if isinstance(scope, Frame) else 'main page'}. Will fill in order: Ejercicio={year}, Periodicidad={periodicidad} (value {periodicidad_value!r}), Periodo={periodo_str}, Tipo={tipo!r}")
    # Wait for at least one select (Ejercicio) to be visible.
    try:
        loc = scope.locator("select")
        loc.first.wait_for(state="visible", timeout=12000)
        n_selects = loc.count()
        print(f"{_debug_ts()} [initial form DEBUG] Selects in scope: {n_selects}")
    except Exception as e:
        print(f"{_debug_ts()} [initial form DEBUG] Wait for select failed: {e}")
    page_for_wait.wait_for_timeout(60)
    # Order per updated SAT form: Ejercicio → Periodicidad → Periodo (appears after Periodicidad) → Tipo de declaración (appears after Periodo).
    if year is not None:
        print(f"{_debug_ts()} [initial form DEBUG] --- Filling Ejercicio ---")
        ok = _fill_select_next_to_label(scope, page_for_wait, "Ejercicio", str(year), mapping=mapping, initial_dropdown_key="initial_ejercicio")
        if not ok and mapping.get("initial_ejercicio"):
            sel_list = mapping["initial_ejercicio"] if isinstance(mapping["initial_ejercicio"], list) else [mapping["initial_ejercicio"]]
            ok = _fill_select_by_mapping(scope, page_for_wait, sel_list, str(year))
            if ok:
                print(f"{_debug_ts()} [initial form DEBUG] Ejercicio filled via mapping selectors")
        print(f"{_debug_ts()} [initial form] Ejercicio: {year}" + (" (filled)" if ok else " (NOT filled — check selectors)"))
        page_for_wait.wait_for_timeout(80)
    print(f"{_debug_ts()} [initial form DEBUG] --- Filling Periodicidad ---")
    ok = _fill_select_next_to_label(scope, page_for_wait, "Periodicidad", periodicidad_value, mapping=mapping, initial_dropdown_key="initial_periodicidad")
    print(f"{_debug_ts()} [initial form] Periodicidad: {periodicidad}" + (" (filled)" if ok else " (NOT filled — check selectors)"))
    page_for_wait.wait_for_timeout(250)
    # Periodo dropdown appears after Periodicidad (Enero–Diciembre; SAT may show only YTD months). pstcdypisr uses label "Periodo" (no accent).
    if month is not None:
        print(f"{_debug_ts()} [initial form DEBUG] --- Filling Periodo ---")
        periodo_value = _SAT_PERIODO_LABEL.get(month, "Enero")
        ok = _fill_select_next_to_label(scope, page_for_wait, "Periodo", periodo_value, mapping=mapping, initial_dropdown_key="initial_periodo")
        if not ok:
            ok = _fill_select_next_to_label(scope, page_for_wait, "Período", periodo_value, mapping=mapping, initial_dropdown_key="initial_periodo")
        if not ok and mapping.get("initial_periodo"):
            sel_list = mapping["initial_periodo"] if isinstance(mapping["initial_periodo"], list) else [mapping["initial_periodo"]]
            ok = _fill_select_by_mapping(scope, page_for_wait, sel_list, periodo_value)
            if ok:
                print(f"{_debug_ts()} [initial form DEBUG] Periodo filled via mapping selectors")
        print(f"{_debug_ts()} [initial form] Periodo: {month:02d} ({periodo_value})" + (" (filled)" if ok else " (NOT filled — check selectors)"))
        page_for_wait.wait_for_timeout(200)
    # Tipo de declaración appears after Periodo (Normal / Normal por Corrección Fiscal). pstcdypisr uses label "Tipo de declaración" (lowercase d).
    print(f"{_debug_ts()} [initial form DEBUG] --- Filling Tipo de Declaración ---")
    ok = _fill_select_next_to_label(scope, page_for_wait, "Tipo de declaración", str(tipo), mapping=mapping, initial_dropdown_key="initial_tipo_declaracion")
    if not ok:
        ok = _fill_select_next_to_label(scope, page_for_wait, "Tipo de Declaración", str(tipo), mapping=mapping, initial_dropdown_key="initial_tipo_declaracion")
    if not ok and mapping.get("initial_tipo_declaracion"):
        sel_list = mapping["initial_tipo_declaracion"] if isinstance(mapping["initial_tipo_declaracion"], list) else [mapping["initial_tipo_declaracion"]]
        ok = _fill_select_by_mapping(scope, page_for_wait, sel_list, str(tipo))
    print(f"{_debug_ts()} [initial form] Tipo de Declaración: {tipo}" + (" (filled)" if ok else " (NOT filled — check selectors)"))
    page_for_wait.wait_for_timeout(40)


def fill_obligation_section(page, mapping: dict, label_map: dict, labels: list[str]) -> None:
    """Fill form fields for given Excel labels (try each label's selectors with value from label_map).
    For ISR simplificado de confianza, the Ingresos form should match the field list and order in
    FILL THE FORM ON SAT.pdf (e.g. pages 25-42). Add or reorder labels/mappings if the PDF differs."""
    for label in labels:
        value = label_map.get(label)
        if value is None:
            continue
        if isinstance(value, float) and value == 0.0:
            continue
        _try_fill(page, page, mapping, label, value)
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
    test_initial_form: bool = False,
    test_full: bool = False,
    test_phase3: bool = False,
) -> bool:
    """
    Full flow: read Excel → get e.firma → login SAT → navigate → fill initial → fill ISR → fill IVA → check totals → send.
    If test_login=True: only open SAT and perform e.firma login (no DB; use test_* in config). Returns True if login succeeded.
    If test_initial_form=True: login then fill Declaración Provisional initial form (no Excel; use test_year, test_month, test_periodicidad in config).
    If test_full=True: login + fill initial form + phase 3 (SIGUIENTE, CERRAR, ISR simplificado, fill ISR section). e.firma and data from config/Excel. No IVA/send.
    If test_phase3=True: login + fill initial form + select ISR simplificado de confianza + fill ISR section (Ingresos, etc.). Stops after ISR fill; no IVA/send. Requires --workbook.
    Returns True if declaration was sent (or test step OK), False otherwise. Logs and prints outcome.
    """
    config = load_config(config_path)
    mapping = load_mapping(mapping_path)
    setup_logging(config.get("log_file"))

    def _on_sigint(_signum, _frame):
        # Do not call any Playwright APIs here — they can block and freeze the terminal.
        print("Ctrl+C: exiting.", file=sys.stderr)
        os._exit(130)

    signal.signal(signal.SIGINT, _on_sigint)
    base_url = config.get("sat_portal_url", SAT_PORTAL_URL)

    global _run_context
    if test_login:
        LOG.info("Test mode: login only (no DB, using test_cer_path / test_key_path / test_password from config)")
        efirma = get_efirma_from_config(config)
        for attempt in range(2):
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=False)
                    context = browser.new_context(accept_downloads=True)
                    page = context.new_page()
                    _run_context = {"page": page, "mapping": mapping}
                    try:
                        login_sat(page, efirma, mapping, base_url)
                        LOG.info("Test login: e.firma login completed. Browser will stay open 10s for inspection.")
                        page.wait_for_timeout(10000)
                        return True
                    finally:
                        logout_sat(page, mapping)
                        _run_context = None
                        context.close()
                        browser.close()
            except Exception as e:
                LOG.exception("Test login failed")
                print(str(e), file=sys.stderr)
                if attempt == 0:
                    LOG.info("Closing and retrying once in %s seconds...", RETRY_WAIT_SECONDS)
                    time.sleep(RETRY_WAIT_SECONDS)
                else:
                    return False
        return False

    if test_initial_form:
        LOG.info("Test mode: login + fill initial Declaración Provisional form (no Excel/DB; using test_* from config)")
        efirma = get_efirma_from_config(config)
        t = config.get("test") or {}
        year = config.get("test_year") or t.get("year")
        month = config.get("test_month") or t.get("month")
        periodicidad = config.get("test_periodicidad") or t.get("periodicidad") or 1
        if year is None:
            year = datetime.now().year
        if month is None:
            month = 1
        data = {"year": int(year), "month": int(month), "periodicidad": int(periodicidad), "tipo_declaracion": "Normal", "label_map": {}}
        LOG.info("Initial form test data: year=%s month=%s periodicidad=%s", data["year"], data["month"], data["periodicidad"])
        for attempt in range(2):
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=False)
                    context = browser.new_context(accept_downloads=True)
                    page = context.new_page()
                    _run_context = {"page": page, "mapping": mapping}
                    try:
                        login_sat(page, efirma, mapping, base_url)
                        print()
                        LOG.info("\n")
                        LOG.info("Opening Configuración de la declaración (Presentar declaración)")
                        if not open_configuration_form(page, mapping):
                            LOG.warning("Could not click Presentar declaración; continuing to fill initial form.")
                        dismiss_draft_if_present(page, mapping)
                        fill_initial_form(page, data, mapping)
                        LOG.info("Test initial form: initial form step complete. Browser will stay open 10s for inspection.")
                        page.wait_for_timeout(10000)
                        return True
                    finally:
                        logout_sat(page, mapping)
                        _run_context = None
                        context.close()
                        browser.close()
            except Exception as e:
                LOG.exception("Test initial form failed")
                print(str(e), file=sys.stderr)
                if attempt == 0:
                    LOG.info("Closing and retrying once in %s seconds...", RETRY_WAIT_SECONDS)
                    time.sleep(RETRY_WAIT_SECONDS)
                else:
                    return False
        return False

    # Full run in test mode: Excel for initial form + phase 3 (SIGUIENTE, CERRAR, ISR selection, ISR Ingresos fill). Config for e.firma (no DB). No IVA/send.
    if test_full:
        if not workbook_path:
            raise ValueError("Test full run requires --workbook")
        LOG.info("Test full run: e.firma from config (no DB); initial form from Excel; then phase 3 (SIGUIENTE, CERRAR, ISR simplificado, fill ISR); no IVA/send")
        data = read_impuestos(workbook_path)
        data["workbook_path"] = workbook_path
        label_map = data["label_map"]
        LOG.info("Period: %s-%s, periodicidad: %s", data.get("year"), data.get("month"), data.get("periodicidad"))
        efirma = get_efirma_from_config(config)
        isr_labels = [
            "Ingresos nominales facturados",
            "Total de ingresos acumulados",
            "Base gravable del pago provisional",
            "Impuesto del periodo",
            "Total ISR retenido del periodo",
            "ISR a cargo",
        ]
        for attempt in range(2):
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=False)
                    context = browser.new_context(accept_downloads=True)
                    page = context.new_page()
                    _run_context = {"page": page, "mapping": mapping}
                    try:
                        login_sat(page, efirma, mapping, base_url)
                        LOG.info("Logged in to SAT")
                        print()
                        LOG.info("\n")
                        if not open_configuration_form(page, mapping):
                            LOG.warning("Could not click Presentar declaración; continuing to fill initial form.")
                        dismiss_draft_if_present(page, mapping)
                        fill_initial_form(page, data, mapping)
                        page.wait_for_timeout(400)
                        print()
                        LOG.info("\n")
                        LOG.info("Phase 2→3: SIGUIENTE, wait for load, CERRAR pop-up")
                        if not transition_initial_to_phase3(page, mapping):
                            LOG.warning("Transition to phase 3 had issues; continuing.")
                        LOG.info("Phase 3: Selecting ISR simplificado de confianza and filling ISR section")
                        if not open_obligation_isr(page, mapping):
                            LOG.warning("Could not click ISR simplificado de confianza")
                        page.wait_for_timeout(500)
                        fill_isr_ingresos_form(page, mapping, data)
                        fill_obligation_section(page, mapping, label_map, isr_labels)
                        LOG.info("Test full run: initial form + phase 3 complete. Browser will stay open 10s for inspection.")
                        page.wait_for_timeout(10000)
                        return True
                    finally:
                        logout_sat(page, mapping)
                        _run_context = None
                        context.close()
                        browser.close()
            except Exception as e:
                LOG.exception("Test full run failed")
                print(str(e), file=sys.stderr)
                if attempt == 0:
                    LOG.info("Closing and retrying once in %s seconds...", RETRY_WAIT_SECONDS)
                    time.sleep(RETRY_WAIT_SECONDS)
                else:
                    return False
        return False

    # Phase 3 test: login, initial form, select ISR simplificado de confianza, fill ISR section (pages 24–51); stop for inspection.
    if test_phase3:
        if not workbook_path:
            raise ValueError("Test phase 3 requires --workbook")
        LOG.info("Test phase 3: Phase 1 (login) + Phase 2 (initial form) + Phase 3 (select ISR simplificado, fill ISR section); e.firma from config, data from Excel; no IVA/send")
        data = read_impuestos(workbook_path)
        data["workbook_path"] = workbook_path
        label_map = data["label_map"]
        LOG.info("Period: %s-%s, periodicidad: %s", data.get("year"), data.get("month"), data.get("periodicidad"))
        efirma = get_efirma_from_config(config)
        isr_labels = [
            "Ingresos nominales facturados",
            "Total de ingresos acumulados",
            "Base gravable del pago provisional",
            "Impuesto del periodo",
            "Total ISR retenido del periodo",
            "ISR a cargo",
        ]
        for attempt in range(2):
            try:
                with sync_playwright() as p:
                    browser = p.chromium.launch(headless=False)
                    context = browser.new_context(accept_downloads=True)
                    page = context.new_page()
                    _run_context = {"page": page, "mapping": mapping}
                    try:
                        LOG.info("Phase 1: Logging in to SAT (e.firma)")
                        login_sat(page, efirma, mapping, base_url)
                        print()
                        LOG.info("\n")
                        LOG.info("Phase 2: Opening Configuración (Presentar declaración), then filling Ejercicio → Periodicidad → Periodo → Tipo")
                        if not open_configuration_form(page, mapping):
                            LOG.warning("Could not click Presentar declaración")
                        dismiss_draft_if_present(page, mapping)
                        fill_initial_form(page, data, mapping)
                        page.wait_for_timeout(400)
                        print()
                        LOG.info("\n")
                        LOG.info("Phase 2→3: Clicking SIGUIENTE, waiting for load, closing pre-fill pop-up (CERRAR)")
                        if not transition_initial_to_phase3(page, mapping):
                            LOG.warning("Transition to phase 3 (SIGUIENTE/CERRAR) had issues; continuing.")
                        LOG.info("Phase 3: Selecting ISR simplificado de confianza and filling ISR section (Ingresos form per PDF pp 25-42)")
                        if not open_obligation_isr(page, mapping):
                            LOG.warning("Could not click ISR simplificado de confianza")
                        page.wait_for_timeout(500)
                        fill_isr_ingresos_form(page, mapping, data)
                        fill_obligation_section(page, mapping, label_map, isr_labels)
                        LOG.info("Test phase 3 complete (Phase 1 login + Phase 2 initial form + Phase 3 ISR section). Browser will stay open 10s for inspection.")
                        page.wait_for_timeout(10000)
                        return True
                    finally:
                        logout_sat(page, mapping)
                        _run_context = None
                        context.close()
                        browser.close()
            except Exception as e:
                LOG.exception("Test phase 3 failed")
                print(str(e), file=sys.stderr)
                if attempt == 0:
                    LOG.info("Closing and retrying once in %s seconds...", RETRY_WAIT_SECONDS)
                    time.sleep(RETRY_WAIT_SECONDS)
                else:
                    return False
        return False

    # Normal flow (DB for e.firma)
    if not workbook_path or company_id is None or branch_id is None:
        raise ValueError("Normal run requires --workbook, --company-id, --branch-id")
    LOG.info("Reading workbook: %s", workbook_path)
    data = read_impuestos(workbook_path)
    data["workbook_path"] = workbook_path
    label_map = data["label_map"]
    LOG.info("Period: %s-%s, labels read: %d", data.get("year"), data.get("month"), len(label_map))

    LOG.info("Fetching e.firma from DB for company=%s branch=%s", company_id, branch_id)
    efirma = get_efirma_from_db(company_id, branch_id, config)
    tolerance = config.get("totals_tolerance_pesos", TOLERANCE_PESOS)

    for attempt in range(2):
        try:
            with sync_playwright() as p:
                browser = p.chromium.launch(headless=False)  # headless=False so user can see; set True for automation
                context = browser.new_context(accept_downloads=True)
                page = context.new_page()
                _run_context = {"page": page, "mapping": mapping}
                try:
                    login_sat(page, efirma, mapping, base_url)
                    LOG.info("Logged in to SAT")
                    print()
                    LOG.info("\n")
                    if not open_configuration_form(page, mapping):
                        LOG.warning("Could not click Presentar declaración")
                    dismiss_draft_if_present(page, mapping)
                    fill_initial_form(page, data, mapping)
                    page.wait_for_timeout(400)
                    print()
                    LOG.info("\n")
                    # Phase 2→3: SIGUIENTE, wait for load, click CERRAR on pre-fill pop-up
                    if not transition_initial_to_phase3(page, mapping):
                        LOG.warning("Transition to phase 3 (SIGUIENTE/CERRAR) had issues; continuing.")
                    # Phase 3: Select ISR simplificado de confianza, then fill ISR Ingresos form (per PDF pp 25-42)
                    if not open_obligation_isr(page, mapping):
                        LOG.warning("Could not click ISR simplificado de confianza; continuing to fill ISR fields anyway.")
                    page.wait_for_timeout(500)
                    fill_isr_ingresos_form(page, mapping, data)
                    isr_labels = [
                        "Ingresos nominales facturados",
                        "Total de ingresos acumulados",
                        "Base gravable del pago provisional",
                        "Impuesto del periodo",
                        "Total ISR retenido del periodo",
                        "ISR a cargo",
                    ]
                    fill_obligation_section(page, mapping, label_map, isr_labels)
                    page.wait_for_timeout(700)

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
                    page.wait_for_timeout(1000)

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
                finally:
                    logout_sat(page, mapping)
                    _run_context = None
                    context.close()
                    browser.close()
        except Exception as e:
            LOG.exception("Error during run")
            print(str(e), file=sys.stderr)
            if attempt == 0:
                LOG.info("Closing and retrying once in %s seconds...", RETRY_WAIT_SECONDS)
                time.sleep(RETRY_WAIT_SECONDS)
            else:
                return False
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
    parser.add_argument("--test-initial-form", action="store_true", help="Test phase 2: login then fill Declaración Provisional initial form (no Excel; use test_year, test_month, test_periodicidad in config)")
    parser.add_argument("--test-full", action="store_true", help="Test full: login + initial form + phase 3 (SIGUIENTE, CERRAR, ISR simplificado, fill ISR); e.firma from config, data from Excel (--workbook); no IVA/send")
    parser.add_argument("--test-phase3", action="store_true", help="Test phase 3: login, initial form, select ISR simplificado de confianza, fill ISR section (Ingresos etc.); requires --workbook; stops before IVA/send")
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

    if args.test_initial_form:
        success = run(
            workbook_path=None,
            company_id=None,
            branch_id=None,
            config_path=args.config,
            mapping_path=args.mapping,
            test_initial_form=True,
        )
        sys.exit(0 if success else 1)

    if args.test_full:
        workbook = args.workbook
        if not workbook:
            config_path = args.config or os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
            config = load_config(config_path)
            t = config.get("test") or {}
            workbook = config.get("test_workbook_path") or t.get("workbook_path")
        if not workbook:
            print("Error: --test-full requires --workbook or test_workbook_path in config.json.", file=sys.stderr)
            sys.exit(2)
        if not os.path.isfile(workbook):
            print(f"Error: Workbook not found: {workbook}", file=sys.stderr)
            sys.exit(2)
        success = run(
            workbook_path=workbook,
            company_id=None,
            branch_id=None,
            config_path=args.config,
            mapping_path=args.mapping,
            test_full=True,
        )
        sys.exit(0 if success else 1)

    if args.test_phase3:
        workbook = args.workbook
        if not workbook:
            config_path = args.config or os.path.join(os.path.dirname(os.path.abspath(__file__)), "config.json")
            config = load_config(config_path)
            t = config.get("test") or {}
            workbook = config.get("test_workbook_path") or t.get("workbook_path")
        if not workbook:
            print("Error: --test-phase3 requires --workbook or test_workbook_path in config.json.", file=sys.stderr)
            sys.exit(2)
        if not os.path.isfile(workbook):
            print(f"Error: Workbook not found: {workbook}", file=sys.stderr)
            sys.exit(2)
        success = run(
            workbook_path=workbook,
            company_id=None,
            branch_id=None,
            config_path=args.config,
            mapping_path=args.mapping,
            test_phase3=True,
        )
        sys.exit(0 if success else 1)

    if not args.workbook or args.company_id is None or args.branch_id is None:
        print("Error: --workbook, --company-id, and --branch-id are required unless using a test mode.", file=sys.stderr)
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
