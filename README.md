# SAT Declaration Filler

Fills the SAT provisional declaration (ISR simplificado de confianza + IVA simplificado de confianza) from a Contaayuda-generated Excel workpaper. Reads the **Impuestos** tab, logs in with e.firma (credentials from Contaayuda DB), fills the form, checks totals (±1 peso), and sends the declaration.

See **PLAN_FORM_FILL_AUTOMATION.md** for the full plan and flow.

## Requirements

- **Python 3.10+** (3.10 or 3.11 recommended)
- Windows (SAT portal and Contaayuda DB are typically on Windows)

## How to run

### First-time setup

1. **Open a terminal** in the project folder (e.g. `D:\GitHub\sat_declaration_filler`).

2. **Create and activate a virtual environment** (optional but recommended):

   ```bash
   python -m venv .venv
   .venv\Scripts\activate
   ```

3. **Install dependencies** — either run the batch file or the commands manually:

   **Option A (Windows):** Double-click `setup.bat` in the project folder. It checks for Python and runs `pip install -r requirements.txt` and `playwright install chromium`.

   **Option B (manual):**
   ```bash
   pip install -r requirements.txt
   playwright install chromium
   ```

4. **Create config:** copy `config.example.json` to `config.json` and set your database connection and FIEL certificate path:

   ```bash
   copy config.example.json config.json
   ```
   Then edit `config.json`: set **db_connection_string** and **fiel_certificate_base_path**. For SAT label/button text and declaration flow, see **Label and flow configuration** below.

5. **(Optional)** Update `form_field_mapping.json` with real SAT selectors after inspecting the portal (see “Finding selectors” below).

### Normal run (production)

Full flow: login (e.firma from DB) → initial form → ISR (Ingresos, Determinación, Pago) → IVA (Determinación, Pago) → check totals → **Enviar declaración** → logout.

From the project folder (with the same venv active if you use one):

```bash
python sat_declaration_filler.py --workbook "C:\path\to\202501_RFC_Hoja de Trabajo.xlsx" --company-id 1 --branch-id 2
```

- **--workbook / -w** — Full path to the Excel workpaper (Impuestos tab). Required.
- **--company-id / -c** — Company ID in Contaayuda (e.firma lookup from DB). Required.
- **--branch-id / -b** — Branch ID in Contaayuda (e.firma lookup from DB). Required.

**Short form:**

```bash
python sat_declaration_filler.py -w "C:\path\to\workpaper.xlsx" -c 1 -b 2
```

**Custom config or mapping:**

```bash
python sat_declaration_filler.py -w "C:\path\to\workpaper.xlsx" -c 1 -b 2 --config C:\other\config.json --mapping C:\other\form_field_mapping.json
```

### Test modes (--test-*)

Test modes use **e.firma from config** (test_cer_path, test_key_path, test_password, test_rfc) — no database. Set these in `config.json` before running.

| Option | Description | Workbook |
|--------|-------------|----------|
| **--test-login** | Login only (e.firma, Enviar). Browser open ~10s. | Not required |
| **--test-initial-form** | Login + fill initial form (Ejercicio, Periodicidad, Período, Tipo). Uses test_year, test_month, test_periodicidad from config. | Not required |
| **--test-full** | Full flow as normal (login → initial form → ISR → IVA) but **no send**, no totals check. Browser open 10s at end, then logout and close. | Required: `-w` or test_workbook_path in config |
| **--test-phase3** | Login + initial form + ISR section only (stops before IVA). Browser open 10s. | Required |
| **--test-iva** | Login + initial form + IVA only (skip ISR). Browser open 10s. | Required |

**Examples:**

```bash
# Login only (no workbook, no DB)
python sat_declaration_filler.py --test-login

# Initial form only (year/month/periodicidad from config)
python sat_declaration_filler.py --test-initial-form

# Full flow without sending (same as normal run but no Enviar declaración)
python sat_declaration_filler.py --test-full -w "C:\path\to\workpaper.xlsx"

# ISR only (through Phase 5 Pago), then stop
python sat_declaration_filler.py --test-phase3 -w "C:\path\to\workpaper.xlsx"

# IVA only (Determinación + Pago tab)
python sat_declaration_filler.py --test-iva -w "C:\path\to\workpaper.xlsx"
```

Workbook for tests can be set in config as **test_workbook_path** (or **test.workbook_path**); then you can omit `-w` for `--test-full`, `--test-phase3`, and `--test-iva`.

**Test config:** Set **test_cer_path**, **test_key_path**, **test_password** (optional: **test_rfc**) in `config.json`. For **--test-initial-form** use **test_year**, **test_month**, **test_periodicidad**. For **--test-full**, **--test-phase3**, **--test-iva** set **test_workbook_path** or pass **-w**.

(Test phase 2 and Test full details are in the table above.)

To test login plus filling the initial form (Ejercicio, Periodicidad, Período, Tipo de declaración) without using an Excel file:

1. In `config.json`, set **test_year**, **test_month**, **test_periodicidad** (e.g. 2026, 1, 1). Optional; defaults: current year, month 1, periodicidad 1.
2. Run:

```bash
python sat_declaration_filler.py --test-initial-form
```

The script will log in (using test_cer_path / test_key_path / test_password), navigate to “Declaración Provisional o Definitiva”, fill the initial form from config, and keep the browser open 15 seconds. No workbook or DB is required.

**Test full (up to initial form only; Excel + config, no DB)**

To test login and the initial “Declaración Provisional o Definitiva” form only (no ISR/IVA fill, no totals check, no send): e.firma from config, initial form from Excel (year/month from filename `YYYYMM_...`, periodicidad from Impuestos sheet).

Set **test_workbook_path** in `config.json` to the full path of your Excel workpaper. Then run:

```bash
python sat_declaration_filler.py --test-full
```

Or pass the workbook on the command line (overrides config):

```bash
python sat_declaration_filler.py --test-full --workbook "C:\path\to\202601_RFC_Hoja de Trabajo.xlsx"
```

The script will login, navigate to the declaration, fill the initial form (Ejercicio, Periodicidad, Período, Tipo) from the Excel, then stop and leave the browser open 15 seconds for inspection.

- **Exit code 0** — success (normal run: declaration sent; test: step completed).
- **Exit code 1** — error or totals mismatch; check console output and `sat_declaration_filler.log`.

## Label and flow configuration (config.json)

When SAT changes button labels, section titles, or question text, update `config.json` instead of changing code. Copy the relevant keys from `config.example.json` and adjust values.

| Key | Purpose |
|-----|--------|
| **sat_ui** | Object of UI text patterns: login (e.firma, Enviar), navigation, initial form labels, ISR (copropiedad, section labels, VER DETALLE, Determinación/Pago tab names, compensaciones/estímulos), IVA, GUARDAR, CERRAR, etc. |
| **declaration_flow** | Array for obligation order, e.g. `["isr", "iva"]`. (Reserved for future use; run order is currently fixed.) |
| **isr_determinacion_labels** | Array of Excel column D labels for ISR Determinación, in form fill order. |
| **iva_determinacion_fields** | Array of `{"excel_label": "...", "form_label": "..."}` for IVA Determinación fields in fill order. |
| **iva_pago_labels** | Array of Excel labels for IVA Pago section, in order. |

Example: set `"sat_ui": { "isr_ingresos_copropiedad": "*¿Los ingresos fueron obtenidos por copropiedad?", ... }` to match SAT wording. Full keys and defaults: **config.example.json** and **docs/PLAN_CONFIG_ISR_IVA_LABELS_AND_SEQUENCE.md**.

## Database configuration (SQL Server on Windows)

**Yes — you must configure the DB.** The script connects to the Contaayuda Microsoft SQL Server database to read e.firma data (certificate filename, key filename, password) via the stored procedure `[GET_AUTOMATICTAXDECLARATION_CUSTOMERDATA]`.

### Where to set it

- **File:** `config.json` in the project folder (`D:\GitHub\sat_declaration_filler\`).
- **Key:** `db_connection_string`.

If `config.json` does not exist, copy it from `config.example.json` and then edit it.

### How to set the connection string

1. **ODBC driver:** On the same Windows PC where you run the script, you need an **ODBC Driver for SQL Server**. Common names:
   - `ODBC Driver 17 for SQL Server`
   - `ODBC Driver 18 for SQL Server`
   - `SQL Server`

   To see installed drivers: **Windows key** → type **ODBC** → open **ODBC Data Sources (64-bit)** → tab **Drivers**. If none of the above is listed, install [Microsoft ODBC Driver for SQL Server](https://learn.microsoft.com/en-us/sql/connect/odbc/download-odbc-driver-for-sql-server).

2. **Build the connection string** using your SQL Server details:

   - **Server:** hostname or IP of the PC where SQL Server runs (e.g. `localhost`, `.\SQLEXPRESS`, `192.168.1.10`, or `SERVERNAME`).
   - **Database:** name of the Contaayuda database.
   - **User / password:** a SQL login that has permission to execute `[GET_AUTOMATICTAXDECLARATION_CUSTOMERDATA]` (usually the same credentials Contaayuda uses).

   **Example (Windows authentication):**

   ```json
   "db_connection_string": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ContaayudaDb;Trusted_Connection=yes;"
   ```

   **Example (SQL login and password):**

   ```json
   "db_connection_string": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=localhost;DATABASE=ContaayudaDb;UID=YourSqlUser;PWD=YourSqlPassword;"
   ```

   Use the exact **DRIVER** name as shown in ODBC Data Sources. In the JSON file, keep the string on one line and escape any backslashes or quotes if needed.

3. **Put it in `config.json`:**

   ```json
   {
     "db_connection_string": "DRIVER={ODBC Driver 17 for SQL Server};SERVER=your_server;DATABASE=your_db;UID=user;PWD=password",
     "fiel_certificate_base_path": "C:\\Path\\To\\FielCertificate",
     "sat_portal_url": "https://ptscdecprov.clouda.sat.gob.mx/",
     "totals_tolerance_pesos": 1,
     "log_file": "sat_declaration_filler.log"
   }
   ```

   Replace `your_server`, `your_db`, `user`, and `password` with your real values. Do **not** commit `config.json` to git (it is in `.gitignore`); it stays only on your PC.

### Where Contaayuda stores e.firma (password and .cer/.key location)

In **Contaayuda** the same data you see under **Empresas → Perfiles → Accesos al SAT y FIEL** (name of .cer file, .key file, and "Contraseña") comes from the **database**:

- **Password:** Stored in the DB column **`FIELTIMBARDOPASSWORD`** (returned by the stored procedure `[GET_AUTOMATICTAXDECLARATION_CUSTOMERDATA]`). When you set or change "Contraseña" in the profile, Contaayuda saves it via the DB (e.g. `[UPDATE_CUSTOMER_FIELCERTIFICATES]` / related SPs). The script reads this column; there is no use of a `Pistas_FIEL_*.txt` file in the current design.
- **.cer and .key file names:** Stored in the DB as **`FIELXMLCERTIFICATE`** and **`FIELXMLKEY`** (only the file names, e.g. `taxpayer.cer`, `taxpayer.key`).
- **.cer and .key file location on disk:** **Not** in the DB. Contaayuda builds the path from:
  - App setting **`FielCertificate`** (in Contaayuda `Web.config`, e.g. `/SysTrack/Fiel/`), and
  - **CompanyId** and **BranchId** and the file name from the DB.  
  Full path on the server = **ApplicationPhysicalPath** + **FielCertificate** + CompanyId + "/" + BranchId + "/" + filename.

If on your Windows PC the FIEL files are under **`C:\Ketan\Fiel\1000\1`** (for company 1000, branch 1), then in **config.json** you must set:

```json
"fiel_certificate_base_path": "C:\\Ketan\\Fiel"
```

The script will then look for the .cer and .key at `C:\Ketan\Fiel\<CompanyId>\<BranchId>\<filename from DB>`.

### Summary

| What            | Where / how                                                                 |
|---------------------------------------------------------------------------------|
| Config file     | `D:\GitHub\sat_declaration_filler\config.json` (copy from `config.example.json`) |
| DB key          | `db_connection_string` inside `config.json`                                  |
| Format          | ODBC connection string for SQL Server (see examples above)                    |
| Same machine?  | Script and SQL Server can be on the same Windows PC or different; use SERVER= accordingly. |
| Password        | In Contaayuda DB column `FIELTIMBARDOPASSWORD` (same as "Contraseña" in Empresas → Perfiles → Accesos al SAT y FIEL). |
| .cer/.key path  | Set `fiel_certificate_base_path` in config.json to the folder that contains `CompanyId\BranchId\` (e.g. `C:\Ketan\Fiel`). |

## Setup (reference)

1. **Dependencies:** `pip install -r requirements.txt` then `playwright install chromium`.
2. **Config:** Copy `config.example.json` to `config.json` and set **db_connection_string** and **fiel_certificate_base_path** (and optionally **sat_portal_url**, **totals_tolerance_pesos**, **log_file**). See **Database configuration** above.
3. **Field mapping:** Update selectors in `form_field_mapping.json` when the SAT form changes (see “Finding selectors” below).

## Usage (CLI options)

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
