# SAT Declaration Filler

Fills the SAT provisional declaration (ISR simplificado de confianza + IVA simplificado de confianza) from a Contaayuda-generated Excel workpaper. Reads the **Impuestos** tab, logs in with e.firma (credentials from Contaayuda DB), fills the form, checks totals (±1 peso), and sends the declaration.

See **PLAN_FORM_FILL_AUTOMATION.md** for the full plan and flow.

## Requirements

- **Python 3.10+** (3.10 or 3.11 recommended)
- Windows (SAT portal and Contaayuda DB are typically on Windows)

## Setup

1. **Install dependencies**

   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

2. **Config**

   Copy `config.example.json` to `config.json` and set:

   - **db_connection_string** — ODBC connection string to the Contaayuda database (SQL Server).
   - **fiel_certificate_base_path** — Base path where .cer/.key files are stored (e.g. `C:\FielCertificate`). The script appends `CompanyId/BranchId/` and the filename from the DB.
   - Optionally: **sat_portal_url**, **totals_tolerance_pesos** (default 1), **log_file**.

3. **Field mapping**

   `form_field_mapping.json` maps Excel labels (and login/nav keys) to Playwright selectors. **You must update the selectors** after inspecting the live SAT pages (see below). The file includes placeholder selectors to get started.

## Usage

One workbook path and company/branch IDs (for e.firma lookup):

```bash
python sat_declaration_filler.py --workbook "C:\path\to\202501_RFC_Hoja de Trabajo.xlsx" --company-id 1 --branch-id 2
```

Options:

- `--workbook`, `-w` — Full path to the .xlsx workpaper (required).
- `--company-id`, `-c` — Company ID for e.firma (required).
- `--branch-id`, `-b` — Branch ID for e.firma (required).
- `--config` — Path to `config.json` (default: same folder as script).
- `--mapping` — Path to `form_field_mapping.json` (default: same folder as script).

The script:

1. Reads the **Impuestos** sheet (columns D and E: D4:E29 for ISR, D33:E58 for IVA).
2. Fetches e.firma (.cer path, .key path, password) from the DB via `[GET_AUTOMATICTAXDECLARATION_CUSTOMERDATA]`.
3. Opens the SAT portal, clicks **e.firma**, fills certificate, key, and password, then **Enviar**.
4. Navigates to **Nuevo Portal de pagos provisionales** → **Presentar Declaración** → **Iniciar una nueva declaración**.
5. Fills the initial form (ejercicio, periodicidad, periodo, tipo) and then ISR and IVA sections from the Excel data.
6. Compares **ISR a pagar**, **IVA a pagar**, and **Total a pagar** with the workbook (±1 peso). If any mismatch, it does **not** send and reports the error.
7. If totals match, clicks **Enviar declaración**.

Output: errors are printed to stderr; all outcomes are appended to the log file (default `sat_declaration_filler.log`).

## Finding selectors for `form_field_mapping.json`

When the SAT form or login page changes, update the selectors in `form_field_mapping.json`:

1. Open the SAT page in Chrome.
2. Open DevTools (F12) → Elements.
3. Right-click the field or button → **Copy** → **Copy selector** (or use a unique id/name/data-attribute).
4. In Playwright, prefer:
   - `input[name='...']`, `select[name='...']`
   - `button:has-text('Enviar')`, `a:has-text('e.firma')`
   - `#id` if the element has a stable id.
5. Put the best selector first in the list; the script tries them in order until one matches.

Keys in the JSON:

- **Login:** `_login_e_firma_button`, `_login_cer_file_input`, `_login_key_file_input`, `_login_password_input`, `_login_enviar_button`.
- **Navigation:** `_nav_nuevo_portal`, `_nav_presentar_declaracion`, `_nav_iniciar_nueva`.
- **Initial form:** `initial_ejercicio`, `initial_periodicidad`, `initial_periodo`, `initial_tipo_declaracion`.
- **Excel labels** (same as column D in Impuestos): e.g. `Base gravable del pago provisional`, `ISR a cargo`, `IVA a cargo`, etc.
- **Totals (read-only):** `_summary_isr_a_pagar`, `_summary_iva_a_pagar`, `_summary_total_a_pagar`.
- **Send:** `_btn_enviar_declaracion`.

## Excel layout

- **Sheet name:** `Impuestos`.
- **Labels** in column **D**, **values** in column **E**.
- **ISR:** rows 4–29 (section “ISR General de Ley”).
- **IVA:** rows 33–58 (section “Impuesto al Valor Agregado”).
- Period can be taken from the filename prefix `YYYYMM_` (e.g. `202501_` → 2025, January).

## Integration with Contaayuda

Contaayuda (or the caller) resolves the workbook path (e.g. from workpaper record + `ApplicationPhysicalPath`), then invokes this script with `--workbook`, `--company-id`, and `--branch-id`. The script does not connect to the DB for the workbook path; it only uses the DB to fetch e.firma for the given company/branch.
