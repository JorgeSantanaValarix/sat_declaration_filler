# Plan: Form Fill Automation from Excel Workpaper

**Goal:** Automate the full process: read the Contaayuda-generated Excel workpaper (Impuestos tab), log in to SAT with e.firma (credentials from Contaayuda DB), fill the SAT provisional declaration form with Excel data, check totals, and send the declaration.

**Scope (current phase):** For now we only focus on filling **ISR simplificado de confianza Personas Físicas** and **IVA simplificado de confianza**. **ISR retenciones por salarios** is out of scope for this phase (planned for later).

**Audience:** Team presentation — high-level and clear.

---

## 1. Current vs desired flow

| Current | Desired |
|--------|--------|
| Contaayuda generates Excel workpaper → user opens file → user opens SAT → user logs in with e.firma → user copies data into form → user sends declaration | Contaayuda generates Excel workpaper → **script** reads **Impuestos** tab → **script** opens SAT → **script** logs in with e.firma (**.cer**, **.key**, **password** from Contaayuda DB) → **script** fills form with Excel data → **script** checks totals (Excel vs SAT) → **script** sends declaration (after totals OK) |

---

## 2. Approach

- **Language:** Python (standalone script).
- **Data source:** Contaayuda-generated Excel workbook — **Impuestos** tab only; all declaration data is read from this tab.
- **Credentials:** **e.firma** (**.cer**, **.key**, **password**) stored in **Valarix/Contaayuda DB**; script or app retrieves them to perform SAT (FIEL) login.
- **Excel:** Read Impuestos tab with `openpyxl` (or `pandas`).
- **Browser:** Automate form with **Playwright** (open SAT, log in, find fields, fill, submit).
- **Field mapping:** **JSON config file** — maps logical field names to one or more CSS/Playwright selectors. When the form HTML changes, we update the JSON only; the script stays unchanged.
- **Safety:** **Totals check** (Excel vs SAT) before sending the declaration; do not send if mismatch.
- **Dynamic UI:** The SAT form is **dynamic**: some fields or options (e.g. Tipo de declaración) **only appear after** previous ones are set (e.g. Ejercicio, Periodicidad). The script must fill in order and **wait for each control to appear** before filling the next. In ISR/IVA sections, some actions **open a popup or modal** (e.g. CAPTURAR → Agregar → dialog with Concepto, Importe, GUARDAR). The script must **open the popup, interact inside it** (fill, select, confirm), **wait for it to close**, then continue. The plan and config support this (see **5.3a**, **5.7a**).

---

## 2a. Where the workbook comes from and how to obtain it for sat_declaration_filler

Users normally create the Excel workpaper from **Auxiliar Inteligente → Hojas de trabajo** (or from **Hojas de trabajo** / Generate Workpaper in the app). Once created, the workbook is stored as follows so it can be obtained and passed to **sat_declaration_filler.py**.

**Where the workbook is created**
- **Auxiliar Inteligente:** UI in **ucSmartAssistant.ascx** ("Opens Auxiliar Inteligente"). User generates or uploads the "Hoja de Trabajo"; the file is saved and associated with a workpaper record.
- **Hojas de trabajo:** **ManageWorkPaper.ascx** / **ucGenerateWorkpaper.ascx** and the **AccountGenerateWorksheetService** (background) also generate workpapers. Filename pattern: **YYYYMM_CustomerRfc_Hoja de Trabajo.xlsx** (e.g. `202501_DCM000125IY6_Hoja de Trabajo.xlsx`).

**Where the workbook is stored (disk)**
- **Base path:** App setting **`WorkPaperFile`** (e.g. `SysTrack/WorkPaperFiles/`). The app creates this folder under the application root (e.g. `HttpContext.Current.Server.MapPath("~/")` → **ApplicationPhysicalPath**).
- **Per customer:** Files are under **WorkPaperFile + CustomerId + "/"**. So full directory = **ApplicationPhysicalPath + WorkPaperFile + CustomerId + "/"**. Example: `C:\...\AppRoot\SysTrack\WorkPaperFiles\123\202501_DCM000125IY6_Hoja de Trabajo.xlsx`.

**Where the path is stored (database)**
- **Workpaper master table** (via stored procedures **`[GET_WORKPAPERMASTER_DATA]`** and **`[GET_WORKPAPERMASTERDATA_BYAJEX]`**). When the user saves the workpaper (e.g. from Auxiliar Inteligente), **UpdateWorkPaperMasterFilePath** is called with a pipe-separated **FilePathstr**; the SP stores the relative path(s) (e.g. **WorkPaperFile + CustomerId + "/" + filename**).
- **Returned columns** from **GetWorkPaperMasterData** include **FILEPATH** (relative path), **FILENAME**, **CUSTOMERID**, **DECLARATIONDATE** (year/month), and **WORKPAPERID**. So the script or an API can query by CompanyId, BranchId, and WorkPaperId (or CustomerId + period) and get **FILEPATH** and **FILENAME**.

**How to obtain the workbook for sat_declaration_filler**

**Chosen approach: Option A (caller passes path)**  
Contaayuda (or the process that invokes the script) is responsible for resolving the workbook path. It gets the workpaper record (e.g. **GetWorkPaperMasterData** with CompanyId, BranchId, WorkPaperId), reads **FILEPATH**, and builds **full path = ApplicationPhysicalPath + FILEPATH**. It then invokes **sat_declaration_filler.py** with **exactly one workbook path** (one file per run; no multiple files for this automation). The script receives the path and opens the Excel from disk (same Windows PC). **e.firma** (.cer, .key, password) is **not** passed by the caller: the script **retrieves it from the Contaayuda DB** at runtime using CompanyId/BranchId (and taxpayer id if needed) — see **5.1a**.

*Example invocation:*  
`python sat_declaration_filler.py --workbook "C:\...\SysTrack\WorkPaperFiles\123\202501_DCM000125IY6_Hoja de Trabajo.xlsx" --company-id 1 --branch-id 2`  
(The script uses company-id and branch-id to fetch e.firma from the DB.)

*Alternatives (not chosen for this plan):*  
- **Option B:** Script receives WorkPaperId and connects to the DB to get FILEPATH, then resolves full path using ApplicationPhysicalPath from script config. (Adds DB dependency and config to the script.)  
- **Option C:** UI or scheduler passes the path when it already has it (e.g. user picked a workpaper); same idea as Option A, implemented from the client side.

**Config / app settings to use**
- **WorkPaperFile** — relative base folder for workpaper files (under application root).
- **ApplicationPhysicalPath** — full path to the web app root (needed to turn relative FILEPATH into full path). Often from `ConfigurationManager.AppSettings["ApplicationPhysicalPath"]` or equivalent where the app runs.

---

## 3. JSON config (field mapping)

- **One JSON file** (e.g. `form_field_mapping.json`) maintained by the team.
- **Structure:** Each key = **Excel label** (same text as in the Impuestos tab, e.g. "Base gravable del pago provisional", "IVA a cargo"). Value = list of Playwright selectors (tried in order; first that exists is used). This way the script reads values by Excel label and uses the same label to look up the SAT field selector.
- **Example:**

```json
{
  "Base gravable del pago provisional": ["input[name='ingresos_actividad']", "[data-field='ingresos']"],
  "ISR a cargo": ["input[name='isr_cargo']", "#isr-cargo"],
  "IVA a cargo": ["input[name='iva_cargo']", "#iva-cargo"]
}
```

- When the web form layout changes, we **edit this file** (add/change selectors); no Python changes required for simple field renames or selector updates.

---

## 4. High-level flow (script)

1. **Input:** **Workbook path** (full path to the .xlsx) and company/branch/customer identifiers for e.firma, **passed by the caller** (Contaayuda resolves path and invokes the script with `--workbook "..."` and ids — see **2a**).
2. **Credentials:** Retrieve **e.firma** from Contaayuda DB when the script runs: **.cer**, **.key**, and **password** (script calls DB/API with company-id/branch-id; see **5.1a**).
3. **Load config:** Read `form_field_mapping.json` (keys = Excel labels; see **3**).
4. **Read Excel:** Open the Contaayuda-generated .xlsx and read the **Impuestos** tab (columns D and E: **D4:E29** for ISR, **D33:E58** for IVA). Extract data for **ISR simplificado de confianza** and **IVA simplificado de confianza**, plus period and type for the initial form.
5. **Browser:** Launch Playwright, navigate to SAT portal (`https://ptscdecprov.clouda.sat.gob.mx/`).
6. **Login:** On the SAT page, click the **"e.firma"** button → redirect to "Acceso con e.firma". There, use **"Buscar"** to select the .cer file, **"Buscar"** to select the .key file, enter the password in the text box, and click **"Enviar"** (see **5.1**).
7. **Navigate to declaration:** Click "Nuevo Portal de pagos provisionales" → "Presentar Declaración" → "Iniciar una nueva declaración".
8. **Fill:** Fill initial form (ejercicio, periodicidad, periodo, tipo de declaración) from Excel **in the order the SAT form reveals them** (see **5.3a**). Then fill **only the two obligations in scope**: **ISR simplificado de confianza Personas Físicas** and **IVA simplificado de confianza**, from Impuestos data, tab by tab. Where a button opens a **popup/modal** (e.g. CAPTURAR, Agregar), the script **opens it, fills/selects inside the popup, confirms (GUARDAR)**, waits for close, then continues (see **5.7a**). Use config selectors and conditional logic throughout.
9. **Check totals:** Before sending, compare key totals on the SAT form with the Impuestos tab: **ISR a pagar**, **IVA a pagar**, and **Total a pagar**. Tolerance: **±1 peso** for each (ISR, IVA, and Total a pagar independently). If any comparison is outside tolerance, do **not** send; report mismatch and stop.
10. **Send declaration:** If totals check passes, click "Enviar declaración" and complete the send flow.
11. **Cleanup:** Close browser. **Reporting:** Print errors to console and write a **log file** with all outcomes (success/failed) for the run.

---

## 5. SAT form flow (from step-by-step guide)

The target form is the **SAT provisional payments portal** (`https://ptscdecprov.clouda.sat.gob.mx/`). The script must follow this sequence so parsing and filling match the real process.

**5.1 Login (automated with e.firma from DB)**  
- Script opens SAT portal. The first page shown is **"Acceso por contraseña"** (password access). The script clicks the **"e.firma"** button, which redirects to **"Acceso con e.firma"** (URL includes `id=fiel`).  
- On that page: **Certificado (.cer)** and **Clave privada (.key)** have **"Buscar"** (Browse) buttons; the script uses Playwright to set the file paths (or trigger file chooser with the resolved .cer and .key paths from DB). **Contraseña de clave privada** is a text box; the script fills it with the password from DB. **RFC** is filled if required. Then the script clicks **"Enviar"** to submit.  
- **.cer**, **.key**, and **password** are retrieved from **Contaayuda DB** when the script runs (see **5.1a**); no certificate/signing library is required for this flow — only browser automation of the e.firma form (Buscar + password + Enviar).

**5.1a How we obtain e.firma for filling the SAT form**  
To log in to SAT and fill the form, the script (or a Contaayuda API called by the script) must obtain the FIEL credentials for the taxpayer that owns the declaration. This is done as follows:

- **Source:** Contaayuda database, via stored procedure **`[GET_AUTOMATICTAXDECLARATION_CUSTOMERDATA]`**.  
- **Parameters:** `CompanyId`, `BranchId` (and optionally a taxpayer id such as `PTaxId` if only one row is needed). The script receives these as input (e.g. from the workpaper context or from the app when it triggers the automation).  
- **Returned columns (per taxpayer row):**  
  - **PTAXID**, **TAXID** — identify the taxpayer/declaration.  
  - **FIELXMLCERTIFICATE** — **file name** of the .cer (e.g. `taxpayer.cer`).  
  - **FIELXMLKEY** — **file name** of the .key (e.g. `taxpayer.key`).  
  - **FIELTIMBARDOPASSWORD** — **password** for the private key (stored in DB).  

- **Where the .cer and .key files are:**  
  The DB stores only the **file names**. The actual **.cer** and **.key** files are on **disk** at a path built from:  
  - App setting **`FielCertificate`** (base path), plus  
  - **CompanyId** + `"/"` + **BranchId** + `"/"`, plus  
  - the file name from **FIELXMLCERTIFICATE** or **FIELXMLKEY**.  
  Example: full path to .cer = `FielCertificate` + CompanyId + `/` + BranchId + `/` + value of FIELXMLCERTIFICATE.

- **How the script uses this:**  
  1. Call the Contaayuda DB (directly from Python, or via a small Contaayuda API that exposes this data) with CompanyId and BranchId (and taxpayer id if needed).  
  2. From the result row: get **FIELXMLCERTIFICATE**, **FIELXMLKEY**, **FIELTIMBARDOPASSWORD**.  
  3. Resolve full paths: base path from config (`FielCertificate` + CompanyId + "/" + BranchId + "/") + certificate filename, same base + key filename.  
  4. Pass the two file paths and the password to the FIEL login step (browser automation or signing library) to authenticate on the SAT portal and then fill the form.

- **Note:** If the password is stored encrypted in the DB, the stored procedure or the API that wraps it must return the decrypted value (Contaayuda already does this for the automatic tax declaration and PDF-download flows that use the same SP).

**5.2 Navigation to new declaration**  
1. Click **"Nuevo Portal de pagos provisionales"** → **"Presentar Declaración"**.  
2. Click **"Iniciar una nueva declaración"**.

**5.3 Initial form (period and type)**  
- **Ejercicio:** Year from workpaper (e.g. 2025).  
- **Periodicidad:** 1 mensual, 2 bimestral, 3 trimestre, 5 semestral, 9 sin periodo.  
- **Periodo:** Month from workpaper; sheet name like `202505` = May 2025.  
- **Tipo de declaración:** **Normal** by default; **Normal por corrección fiscal** only if the workpaper (or config) says so.  
- Submit → **"Administración de la declaración"** page.

*Example — workbook named `202501_DCM000125IY6_Hoja de Trabajo.xlsx`:*  
From the filename prefix **202501** we get year and month. The Impuestos tab (or sheet names inside the workbook) may also carry period. Applying the plan logic:

| Field | Value | Source |
|-------|--------|--------|
| **Ejercicio** | **2025** | First 4 digits of period prefix (202501). |
| **Periodo** | **01** (January) | Digits 5–6 of period prefix (202501). |
| **Periodicidad** | **1 mensual** | From Impuestos tab if present; otherwise typical default for a single month (01) is monthly. |
| **Tipo de declaración** | **Normal** (default) or **Normal por corrección fiscal** | Default is Normal; use Normal por corrección fiscal only if the workpaper or config says so. |

So the initial form would be filled with: **Ejercicio = 2025**, **Periodo = 01**, **Periodicidad = 1 mensual** (unless the workpaper says otherwise), and **Tipo de declaración = Normal** (default; use Normal por corrección fiscal only if the workpaper or config says so). The RFC in the filename (e.g. DCM000125IY6) identifies the taxpayer; it is not a field on this initial form but is used to select the correct e.firma (and possibly to validate the declaration).

**5.3a Dynamic form: fields and options appear in sequence**  
Part of the fill process is **dynamic**: the SAT form reveals fields or options only after previous ones are set. For example, the option to fill **Tipo de declaración** may only **appear after** **Ejercicio** and **Periodicidad** are set. The script must therefore: (1) **fill in the correct order** (e.g. Ejercicio → Periodicidad → then wait for and fill Periodo / Tipo de declaración); (2) **wait for the UI to update** after each selection (e.g. wait for the Tipo de declaración control to be visible/clickable) before filling the next; (3) not assume all initial-form fields are visible at once. The same idea applies elsewhere: some options (e.g. "Obligaciones a declarar") depend on "Tipo de declaración". The JSON config and script should support an ordered sequence of steps and explicit waits for elements to appear.

**5.4 Data source: Impuestos tab**  
- All data for the declaration is read from the **Impuestos** tab of the Contaayuda-generated Excel workbook.  
- **Layout:** One sheet named **"Impuestos"**. Labels are in **column D**, values in **column E**.  
  - **ISR (section "ISR General de Ley"):** rows **D4:E29** — read label from D, value from E.  
  - **IVA (section "Impuesto al Valor Agregado"):** rows **D33:E58** — read label from D, value from E.  
- **In the current phase** we only use data that maps to **ISR simplificado de confianza** and **IVA simplificado de confianza**. Data for "Impuestos retenidos a cargo" (ISR retenciones) is not used in this phase.

**5.4a Excel → SAT mapping (where to obtain each value and where it goes)**  
The following mapping defines **which Excel label (column D in the ranges above) to read** and **where it goes on the SAT declaration**. The script uses **Excel labels as JSON keys** in `form_field_mapping.json` to find the SAT field selectors.

**ISR simplificado de confianza — source in Excel (section "ISR General de Ley"):**

| Excel (source — label / section) | SAT declaration (destination) |
|----------------------------------|-------------------------------|
| Ingresos nominales facturados | Used to compare with SAT "Total de ingresos percibidos por la actividad"; drives "ingresos a disminuir" / "ingresos adicionales" logic. |
| Total ingresos nominales / Total de ingresos acumulados | Context for INGRESOS tab. |
| Base gravable del pago provisional | **Total de ingresos percibidos por la actividad** (match/verify); drives Concepto in "ingresos a disminuir" / "ingresos adicionales" when different from SAT. |
| Coeficiente de utilidad, Utilidad fiscal para pago provisional | Context / verification. |
| Impuesto del periodo | **Impuesto mensual** (DETERMINACIÓN). |
| ISR retenido (periodo / total) | **ISR retenido**; differences → **ISR retenido no acreditable** or **ISR retenido a adicionar** (DETERMINACIÓN). |
| ISR a cargo | **IMPUESTO A CARGO** (DETERMINACIÓN); verify before send. |

**IVA simplificado de confianza — source in Excel (section "Impuesto al Valor Agregado"):**

| Excel (source — label / section) | SAT declaration (destination) |
|----------------------------------|-------------------------------|
| Actividades gravadas a la tasa del 16% | **Actividades gravadas a la tasa del 16%** (DETERMINACIÓN). |
| Actividades gravadas a la tasa del 8% | **Actividades gravadas a la tasa del 8%** (if present). |
| Actividades gravadas a la tasa del 0% otros | **Actividades gravadas a la tasa del 0%** (SAT). |
| Actividades exentas | **Actividades exentas**. |
| Actividades no objeto de impuesto | **Actividades no objeto de impuesto**. |
| IVA a cargo a la tasa del 16% y 8% / Total IVA Trasladado | **IVA a cargo a la tasa del 16%** (DETERMINACIÓN). |
| IVA retenido a favor | **IVA Retenido** (SAT). |
| IVA acreditable del periodo | **IVA acreditable del periodo**; also **IVA Acreditable Actividades gravadas a tasa 16% u 8% y tasa 0%** (CAPTURAR). "IVA Acreditable Actividades mixtas" = 0 (fixed). |
| Cantidad a cargo | **Cantidad a cargo** (DETERMINACIÓN). |
| IVA a cargo | **IVA a cargo** (final); verify before send. |
| IVA a favor | **Impuesto a Favor** (verify). |

**Implementation note:** The script reads the **Impuestos** sheet, builds a map **label (from D) → value (from E)** for D4:E29 (ISR) and D33:E58 (IVA). SAT fields are filled using `form_field_mapping.json` where **keys = Excel labels** (same text as in column D); the table above defines which label maps to which SAT concept.

**5.5 Obligations to fill (one by one) — current scope**  
**Fill these two in this order** (matching the order in the Excel: ISR first, then IVA):

| Order | Obligation (click to open) | Data source (Impuestos tab) | Tabs to fill |
|-------|----------------------------|-----------------------------|--------------|
| 1 | **ISR simplificado de confianza Personas Físicas** | D4:E29 ("ISR General de Ley") | INGRESOS → DETERMINACIÓN → PAGO |
| 2 | **IVA simplificado de confianza** | D33:E58 ("Impuesto al Valor Agregado") | DETERMINACIÓN → PAGO |

*Out of scope for this phase:* **ISR retenciones por salarios** (table "Impuestos retenidos a cargo") — planned for a later phase.

**5.6 Field rules and conditions (ISR simplificado and IVA simplificado)**  
- **ISR simplificado:** Worksheet ↔ SAT mapping (e.g. "Base gravable del pago provisional" ↔ "Total de ingresos percibidos por la actividad"). Conditional logic: if SAT total equals worksheet → "Sin ingresos a disminuir" / "Sin ingresos adicionales"; if SAT > worksheet → "ingresos a disminuir" + concept "Ingresos facturados pendientes de cancelación…"; if SAT < worksheet → "ingresos adicionales" + concept "Ingresos no considerados en el prellenado". Fixed values: e.g. "descuentos, devoluciones y bonificaciones" = 0. Verify-and-fill for ISR retenido no acreditable / a adicionar, etc.  
- **IVA simplificado:** Map table "Impuesto al Valor Agregado" to SAT fields (e.g. Actividades gravadas 16%/0%, exentas, IVA a cargo, IVA acreditable, Impuesto a favor, Cantidad a cargo). Fixed: "IVA Acreditable Actividades mixtas" = 0. Verify-and-fill as per guide.

**5.7 Per-tab flow (ISR simplificado and IVA simplificado only)**  
- **ISR simplificado:** INGRESOS tab → fill/capture amounts, "¿Los ingresos fueron obtenidos a través de copropiedad?" → No, CAPTURAR / Agregar / Concepto / Importe / GUARDAR as per guide. DETERMINACIÓN → verify or fill Tasa aplicable, Impuesto mensual, ISR retenido (no acreditable / a adicionar when they differ). PAGO → usually auto; select "No" where indicated; then **"Administración de la declaración"** to return.  
- **IVA simplificado:** DETERMINACIÓN → verify/fill Actividades gravadas 16%/0%, exentas, IVA a cargo, IVA acreditable, etc. as per guide. PAGO → then **"Administración de la declaración"** to return.  
- After both obligations are done, proceed to check totals and send.

**5.7a Popups and modals when filling ISR / IVA**  
When filling data from the Impuestos tab into the ISR or IVA sections, **some steps require clicking a control that opens a small popup or modal**. Inside that popup the user (or script) must interact: fill fields, select options from dropdowns, enter amounts, then confirm (e.g. GUARDAR) to close it and continue. For example, "CAPTURAR" or "Agregar" may open a dialog where we choose "Concepto" and "Importe" and click "GUARDAR". The script must: (1) **click the button** that opens the popup; (2) **wait for the popup to be visible** (e.g. wait for a modal container or a specific heading); (3) **interact inside the popup** (fill inputs, select options, click GUARDAR/Aceptar); (4) **wait for the popup to close**; (5) **continue** with the main form. The JSON config may need selectors that target elements **inside the popup** (e.g. scoped to the modal DOM or using a step label like "popup_concepto_importe"). Playwright's ability to wait for and query within a specific frame or overlay is important here.

**5.8 Check totals before send**  
- After **ISR simplificado** and **IVA simplificado** are filled, the script must **compare** the SAT summary with the Impuestos tab: **ISR a pagar**, **IVA a pagar**, and **Total a pagar**.  
- **Tolerance: ±1 peso** for each of the three: ISR and IVA are checked independently with ±1; Total a pagar is also checked with ±1.  
- If any comparison is outside tolerance, the script must **not** send the declaration; it reports the mismatch, prints the error, and writes the outcome to the log file.  
- Only when all three checks pass should the script proceed to send.

**5.9 Final step: send declaration**  
- Click **"Enviar declaración"** and complete the send flow.  
- Optionally: download the acknowledgment/receipt; later the user may upload it to Contaayuda "Tax Return" or the script may integrate that upload.

**Implications for the script:**  
- The script is **multi-step and stateful**: follow the exact click sequence and wait for each page before filling.  
- **Dynamic flow:** Fields and options **appear in sequence**; fill in order and **wait for each element to be visible** before interacting (e.g. Tipo de declaración only after Ejercicio and Periodicidad — **5.3a**).  
- **Popups/modals:** When filling ISR or IVA, some steps **open a small popup** (e.g. to choose Concepto, Importe, GUARDAR). The script must **click to open → wait for popup → interact inside (fill, select, confirm) → wait for close → continue** (**5.7a**). Config may need selectors scoped to the popup.  
- **Excel data** is read only from the **Impuestos** tab; for the current scope, mapping needs period sheet / "Base gravable del pago provisional" (ISR simplificado) and table **"Impuesto al Valor Agregado"** (IVA simplificado). "Impuestos retenidos a cargo" is not used in this phase.  
- **Credentials** for e.firma (.cer, .key, password) are read from **Contaayuda DB**, not from the Excel file (see **5.1a** for how they are obtained: SP, columns, file paths).  
- **JSON config** can group selectors by step (e.g. `initial_form`, `isr_simplificado_ingresos`, popup steps like `popup_concepto_importe`) and by field; conditions (when to use "ingresos a disminuir" vs "ingresos adicionales") stay in script logic, using values from the Impuestos tab.  
- **Totals check** is a mandatory step before "Enviar declaración".

---

## 6. Resilience to form changes (SAT / config)

- **Selector changes:** Update JSON (add fallback selectors or replace them).
- **New/removed fields:** Add/remove keys in JSON and ensure Excel columns are mapped.
- **New steps (e.g. extra page):** May require a small change in the script flow (e.g. wait for next page, then fill again). Field definitions can still live in JSON.

---

## 7. Deliverables (for implementation)

1. **JSON config** — `form_field_mapping.json` with keys = Excel labels (see **3**).

2. **Python script** — `sat_declaration_filler.py`: read Impuestos tab, get e.firma from DB, log in to SAT, fill form, check totals, send declaration.  
   **Reporting:** Print errors to console; write a **log file** with all success/failed outcomes for the run.

   **How the script works (end-to-end):**
   - **Read workpaper:** Receives **one** workbook path and company/branch id. Uses `openpyxl` to read the **Impuestos** tab, columns D and E: **D4:E29** (ISR), **D33:E58** (IVA). Builds label→value map and period/tipo for the initial form.
   - **Get e.firma:** Retrieves **.cer**, **.key**, and **password** from Contaayuda DB using company-id and branch-id.
   - **Open SAT and log in:** Launches Playwright, goes to SAT portal, clicks **"e.firma"** → on "Acceso con e.firma" uses **Buscar** for .cer and .key, fills password, clicks **Enviar**.
   - **Navigate and fill:** Clicks through to "Iniciar una nueva declaración", fills initial form, then **ISR simplificado** first, then **IVA simplificado**, using JSON selectors keyed by Excel labels.
   - **Check totals:** Compares **ISR a pagar**, **IVA a pagar**, **Total a pagar** with Impuestos; tolerance **±1 peso** each. If any outside tolerance, do not send; print error and log outcome.
   - **Send declaration:** If totals OK, clicks "Enviar declaración" and completes the send.

3. **Requirements and setup** — **Python 3.10+** (3.10 or 3.11 recommended for compatibility; 3.14 is acceptable). **requirements.txt** for easy setup (e.g. `openpyxl`, `playwright`). After `pip install -r requirements.txt`, run `playwright install chromium`.

4. **Docs** — How to run the script (CLI args), how to edit the JSON (and how to find selectors on the page).

5. **Integration** — Contaayuda triggers the script with one workbook path + company-id/branch-id (script fetches e.firma from DB). One file per invocation.

---

## 8. Out of scope (for this plan)

- **ISR retenciones por salarios** — not filled in this phase; planned for a later phase.
- Detailed FIEL/signing implementation (how to use .cer/.key in browser or via library).
- Captcha / 2FA if SAT adds it (manual or separate solution).
- Detailed selector discovery process (can be a short "how to" in docs).
- UI in the main app to trigger the script (can be a later phase).

---

## 9. Resolved decisions (formerly gaps)

The following were open points; they are now **decided** and reflected in the plan body.

| Topic | Decision |
|-------|----------|
| **E.firma** | Script **retrieves** .cer, .key, and password **from Contaayuda DB** when it runs (using company-id and branch-id). Caller does not pass cert/key/password. |
| **Excel layout** | Tab name **"Impuestos"**. Labels in **column D**, values in **column E**. **D4:E29** for ISR ("ISR General de Ley"), **D33:E58** for IVA ("Impuesto al Valor Agregado"). |
| **Totals tolerance** | **±1 peso** for ISR a pagar, IVA a pagar, and Total a pagar (each checked independently). |
| **Order of obligations** | **ISR first**, then **IVA** (as in Excel). |
| **Error/reporting** | **Print** errors to console; **write a log file** with all success/failed outcomes for the run. |
| **Workbook per run** | **One file, one path** per invocation; no multiple files for this automation. |
| **FIEL login** | SAT shows "Acceso por contraseña" → script clicks **"e.firma"** → redirect to "Acceso con e.firma" → **Buscar** for .cer, **Buscar** for .key, fill password, click **Enviar**. No signing library; browser automation only. |
| **Prerequisites** | **Python 3.10+** (3.10 or 3.11 recommended; 3.14 OK). **requirements.txt** for setup (`openpyxl`, `playwright`); then `playwright install chromium`. |
| **JSON keys** | Use **Excel labels** as keys in `form_field_mapping.json`. |

**Remaining for implementation:** Exact Playwright selectors for each SAT field and popup; file-upload handling for .cer/.key (e.g. `set_input_files`); log file path/format; DB connection details for the script (connection string or API endpoint to fetch e.firma).

---

*Document: high-level plan for team. Implementation details (exact selectors, sheet names, form URL) to be defined in a later technical spec.*
