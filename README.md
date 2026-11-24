# PO DB Builder

**PO DB Builder** is a desktop utility (built with Python) that implements a
**controlled Extract–Transform–Load (ETL) ingestion workflow** for pooling-order mineral owner records.

It ingests pooling-order rows from either of two independent, mutually exclusive sources — a shared Google Sheet, or local CSV/XLSX skip-trace export files — cleans and standardizes them, matches each record against the company's Pipedrive deals and BUDB (Bottoms Up Database) mineral-ownership records, and persists the result into a versioned, daily SQLite database for long-term retention and reporting.

---

![Version](https://img.shields.io/badge/version-2.1.0-ffab4c?style=for-the-badge&logo=python&logoColor=white)
![Python](https://img.shields.io/badge/python-3.11%2B-273946?style=for-the-badge&logo=python&logoColor=ffab4c)
![Status](https://img.shields.io/badge/status-active-273946?style=for-the-badge&logo=github&logoColor=ffab4c)

---

## 🚧 Problem Statement / Motivation

The pooling-order dataset is maintained on the **`po_db`** tab of a shared Google Sheet that multiple team members contribute to, with inconsistent formatting, duplicate entries, and no link back to the company's existing systems of record (Pipedrive deals, BUDB ownership data).

Manually cross-referencing each incoming owner against Pipedrive and BUDB to check "do we already have a deal or ownership record for this person" is slow and error-prone at volume, and without a durable, versioned database there is no reliable way to track what changed between imports or recover from a bad run.

To close this gap, a repeatable ingestion pipeline is required — one that standardizes incoming data, automatically links it to existing Pipedrive deals and BUDB records, and stores the result in a daily, auditable database.

A second, independent need is onboarding locally received skip-trace export files (`.csv`/`.xlsx`) into the same database — without letting them delete or overwrite anything sourced from the Sheet, and without a round-trip writeback, since these files aren't the Sheet.

PO DB Builder addresses this by enforcing a controlled ETL pipeline:
1. Extract pooling-order records from the shared Google Sheet's `po_db` tab, **or** from locally selected CSV/XLSX skip-trace files (one source per build, chosen via a radio button).
2. Clean and standardize columns, phone numbers, and dates, flagging any suspicious data loss for audit.
3. Match each record to a Pipedrive deal ID and a BUDB ID using cached reference data.
4. Sync the result into a daily-versioned SQLite database, tracking inserts/updates/deletes (Google Sheet source) or appending new rows only (local-file source).
5. For a Google Sheet build, write matched IDs, serial numbers, and upload dates back to the Sheet; local-file builds have no writeback. Both generate audit/report files for review.

---

## ✨ Features
- **Two independent, mutually exclusive import sources**, picked per build with a radio button on the **Import** tab — selecting one clears and disables the other's inputs, so a build can never mix sources:
  - **Google Sheets ingestion**: reads live data from a shared Google Sheet (tab must be named `po_db`) via a Google service account, with hyperlink extraction (rich-text/chip/link metadata) and a header-capture mode that guards against a link's display text leaking in as a column header; each download is re-read twice (and the Sheet's `modifiedTime` re-checked) to confirm nothing changed mid-capture, and is backed up locally under `input/google_sheet_backups/` (one subfolder per spreadsheet ID, as a timestamped `.xlsx` snapshot plus a JSON snapshot of the raw cell data) for traceability. The Sheet is treated as the single source of truth: rows are inserted, updated, or **deleted** to match it, and matched IDs/serial numbers/upload dates are written back to it.
  - **Local CSV/XLSX ingestion** (`po_local_source.py`): reads locally selected skip-trace export files instead — either multiple files picked directly, or a folder (recursed for `.csv`/`.xlsx`/`.xls`), combined into one build. See **Local file import** below for full details — it is purely additive (no updates/deletes, no Google writeback), with its own required-column, duplicate, and blank-value checks.
- **Data cleaning**: column name sanitization, phone number and date normalization (including numeric Excel date serials), and a NULL-conversion audit for anything that looks like unintended data loss.
- **Stable row identity**: incoming rows are matched back to existing database rows by `po_id` (format `porec_XXXXXX`); new rows are assigned the next sequential `po_id` and serial number (format `PO-XXXX`), with both sequences recovered from the database itself (or its action-log history) if the running counters aren't already cached.
- **Pipedrive deal matching** against a Pipedrive deals cache, downloaded automatically via the Pipedrive API (PO ID, serial number, name + address, and deal-title + address matching, each tracked separately in a per-row audit of which method(s) matched), with a SQLite-backed lookup cache for speed.
- **BUDB ID matching** against a BUDB reference database **you select each session** on the **Bottoms Up** tab (owner name + address matching across both the primary and secondary address fields), with its own signature-based lookup cache that rebuilds only when the selected file changes.
  - **BUDB contact-group auto-resolution**: if a matched owner's BUDB IDs span more than one distinct `contact_group_id`, the app automatically keeps only the IDs belonging to the lowest-numbered group and records the full decision (all groups seen, which IDs were kept/removed) in a `budb_group_selection` audit sheet for review.
- **Combined-county enrichment**: derives a `combined_counties` value from each row's target county and state.
- **Full-database derived-column refresh (local-file builds only)**: after appending new local-file rows, `deal_id`, `budb_id`, `contact_group_id`, and `combined_counties` are recomputed for **every** row currently in `pooling_order` (not just the new ones) and only genuinely changed values are written back — since new local rows can shift which BUDB contact group is "lowest" for owners that already existed in the table.
- **Pre-flight "Check references"**: validates Google Sheet access/permissions and content (headers, data rows), the Pipedrive API token, the BUDB file, and local storage (required folders, a single active database, and that it's readable), before enabling the build button, so a bad credential or file issue is caught before any database changes are made.
- **Tracked, crash-safe database builds** (`po_build_state.py`):
  - Auto-creates/copies forward a new dated DB each day from the most recent prior one (same-day re-runs continue syncing into that day's file).
  - Every build attempt first archives a consistent snapshot of the current database into `database/previous_versions/<build_id>/`, before any changes are made.
  - If a build fails partway through (and no Google writeback has started yet), the app automatically restores the pre-build snapshot so the active database is never left in a half-written state.
  - Syncs rows by `po_id`, reporting inserted / updated / unchanged / deleted counts. Existing `prod_id` and `source_url` values are preserved for matched rows.
  - Build status (`building` / `ready_for_log` / `complete` / `rolled_back` / `recovery_required`) is tracked both in `config/build_state.json` and inside the database itself, so the UI's **Build DB** / **Update Action Log** buttons stay in sync with what actually happened.
- **Google Sheets writeback**: after a successful import, writes `po_id`, serial numbers, and upload dates back to the source sheet, with a pending/completed record so an interrupted writeback can be recovered instead of silently lost.
- **Integrated action log**: after a build completes, clicking **Update Action Log** diffs the before/after database snapshots and records every schema change, added row, deleted row, and modified field into a persistent `pooling_order_action_log` table — no manual file copying required.
- **Baseline snapshot / test-run safety net** (`initialize_po_baseline.py`): a manually-run, standalone script that stamps a known-good, already-tracked database as the official baseline and backs it up to `database/previous_versions/`, so a confirmed-stable version can be restored if a test run needs to be thrown away. It requires the target file to already contain a populated `pooling_order` table with its full `pooling_order_action_log` history — see **Baseline Snapshots & Test Runs** below.
- **Interrupted-writeback recovery** (`po_writeback.py --apply`): inspects an interrupted build's journaled Google writeback, re-downloads the Sheet, confirms the cells it needs to write haven't been changed by anyone else since the original capture, and only then replays or confirms the writeback before marking the build complete.
- **Single-process locking** (`po_run_lock.py`): the app and the baseline initializer both take an exclusive file lock, so two of these processes can't run against the same project folder at once.
- **Audit & reporting output**: a combined NULL-conversion + BUDB group-selection audit workbook, a deal-match audit, and a PO-associated-deals report are generated automatically after each import.
- **Operator-friendly desktop UI** with file/token/sheet selection, reference status indicators, progress bar, and a live activity log.
---

## 🧠 Logic Flow

### Import / build flow
1. User clicks **Check references** — validates the Google Sheet's access/permissions and data, the Pipedrive API token, the selected BUDB file, and local storage, without writing anything. The **Build DB** button stays disabled until this passes.
2. User pastes the Google Sheet URL (its `po_db` tab is read) and clicks **Build DB**.
3. `prepare_build()` (`po_build_state.py`) resolves today's database (`database/YYYY-MM-DD-pooling_order.db`):
   - Archives a consistent snapshot of whatever database currently exists into `database/previous_versions/<build_id>/`, **before** any changes are made.
   - If today's file doesn't exist yet, copies it forward from that snapshot.
   - Records the build as `building` in `config/build_state.json` and inside the database itself.
4. For the Google Sheet:
   - Downloads the `po_db` tab, verifies the sheet hasn't changed mid-download, skips rows with no meaningful data, and saves a timestamped local backup copy to `input/google_sheet_backups/<spreadsheet_id>/`.
   - Cleans and standardizes columns, phone numbers, and dates; records any suspicious NULL conversions for audit.
   - Allocates/confirms each row's `po_id` and `serial_number` against the active database.
   - Matches each row to a **Pipedrive deal ID** using the Pipedrive cache (`cache/pipedrive_local_cache.csv`, refreshed automatically via the Pipedrive API).
   - Matches each row to a **BUDB ID** using the BUDB `.db` file selected on the **Bottoms Up** tab; if the match spans multiple `contact_group_id` values, auto-resolves to the lowest group and logs the decision.
   - Derives `combined_counties` from the row's target county and state.
5. Syncs rows into the daily table keyed by `po_id`, assigning `date_uploaded` as needed, and reports inserted/updated/unchanged/deleted counts.
6. Writes `po_id`, serial numbers, and upload dates back to the sheet, tracking a pending → completed writeback record so a failed writeback can be resumed safely.
7. On success, `finish_build()` marks the build `ready_for_log` and retires the prior active file (its pre-build copy already lives in `database/previous_versions/`). On failure, `fail_build()` automatically restores the pre-build snapshot (unless a Google writeback had already started, in which case it's left as `recovery_required` for manual review) — see **Troubleshooting** below.
8. Saves output artifacts:
   - `audit/` — a combined NULL-conversion + BUDB group-selection audit workbook (only if either kind of issue was found).
   - `existing_deals/` — deal-match audit and the PO-associated-deals report.

### Local file import flow (`po_local_source.py`)

Selected via the **Local Files** radio button on the **Import** tab instead of **Google Sheets**. Rows from local files are always treated as brand-new — there is no `po_id` matching, no update/delete of existing rows, and no Google writeback.

1. **Required columns.** Each file's headers are matched case-insensitively against a set of accepted spellings per field, then renamed to their canonical name; anything left over (the `DEC:`/`ADD:`/`PH:`/`EMAIL:` skip-trace detail columns, etc.) is handled by the normal generic `sanitize_colname()`. A file missing any required column is rejected outright. The canonical fields and their accepted header spellings:
   | Canonical column | Accepted header spelling(s) |
   |---|---|
   | `lessor_owner` | `Lessor`, `Owner` |
   | `contact_type` | `Contact Type` |
   | `target_county` | `Target County` |
   | `target_state` | `Target State` |
   | `first_name` | `Input: First Name`, `First Name` |
   | `middle_name` | `Middle Name`, `Input: Middle Name` |
   | `last_name` | `Input: Last Name`, `Last Name` |
   | `address` | `Input: Address 1`, `Input: Address`, `Address` |
   | `city` | `Input: City`, `City` |
   | `state` | `Input: State`, `State` |
   | `postal_code` | `Input: Zip Code`, `Postal Code`, `Zip Code`, `postal_code`, `zip_code` |
   | `attn` | `ATTN` |
2. **Check references** (per selected file, before any database change):
   - Rejects a file with no data rows.
   - Rejects a file with any blank/NULL value in `lessor_owner`, `target_county`, or `target_state` — the **entire file** is skipped (not just the offending rows), with a popup naming the file, the affected column(s), and the row number(s).
   - If a file's name already matches an existing `source_url` in the database, prompts Yes/No to continue checking it anyway; if continued, the file is auto-skipped unless it still has at least one row that wouldn't be a duplicate.
   - Files that pass become the build's file list; **Build DB** stays disabled until at least one file passes.
3. **Build DB**, for the validated file list:
   - Reads and concatenates all files, applying the same `clean_and_prepare_df()` column/phone/date cleaning used for the Google Sheet path.
   - Backfills `decd_source = "IDI"` wherever `dec_deceased_y_n_u` is present and `decd_source` is otherwise null (local files have no `decd_source` column of their own).
   - Drops duplicate rows — a row matching an existing `pooling_order` row, or an earlier row already queued in this same batch, on the full required-column set (including `target_county`/`target_state`) — and records why each dropped row was skipped, plus a per-file read/kept/skipped summary.
   - Allocates fresh `po_id`/`serial_number` values via the same `allocate()` used by the Google path (still monotonic, never reused).
   - Writes only into columns that **already exist** in `pooling_order` — no new columns are added from local data, with one named exception: `attn` is recognized and added to the schema if it isn't already there (see table above). Any other unrecognized column is dropped and logged.
   - Appends the new rows (insert-only — never updates or deletes existing rows), sets `source_url` to the originating filename, then runs the full-database derived-column refresh described under **Features** above.
   - Saves `audit/po_local_audit_<build_id>.xlsx` (null-conversion audit, skipped-row reasons, per-file summary, BUDB group-selection audit) whenever any of those have entries.

### Action-log flow (`pooling_order_action_log.py`)
This runs **inside `app.py`** — the **Update Action Log** button becomes enabled once a build reaches `ready_for_log`, and clicking it diffs the build's recorded before/after snapshots automatically, with no manual file copying needed.

1. After **Build DB** completes, click **Update Action Log**. The app calls `compare_databases(old_db_path, new_db_path, build_id)` using the paths recorded by the build, then marks the build `complete`. A `pooling_order_action_log_runs` table (keyed by `build_id`) prevents the same build from being logged twice.
2. The comparison:
   - Reads the date from the new DB's filename (expects a `YYYY-MM-DD` pattern, e.g. `2026-11-20-pooling_order.db`).
   - Creates the `pooling_order_action_log` table inside the new DB if it doesn't already exist.
   - If the new DB's log table is still empty, copies forward all historical log rows from the old DB's log table first — this is what carries the full change history along as you roll from one daily DB to the next.
   - Compares the `pooling_order` table between old and new by `po_id`:
     - Logs any columns added or removed between the two schemas.
     - Logs any `po_id` present only in the new DB as `added`, and any present only in the old DB as `deleted` (full row snapshot as JSON).
     - For rows present in both, logs each changed field individually as `modified` (old value → new value). `date_uploaded` is intentionally excluded from field comparison since it changes on every sync.
3. The resulting `pooling_order_action_log` table — now living inside the new DB file — becomes the historical record going forward.

### Matching Criteria

#### BUDB matching criteria
Input key (from the pooling-order row, tried against both the primary and secondary address fields):
- `lessor_owner` + `address` + `city` + `state` (normalized)
- `lessor_owner` + `add_address1` + `add_address1_city` + `add_address1_state` (normalized)

Matched against BUDB combinations:
1. `Owner` + `Input: Address` + `Input: City` + `Input: State`
2. `Owner (Standardized)` + `Input: Address` + `Input: City` + `Input: State`
3. `Owner (Standardized)` + `md_address` + `md_city` + `md_state`
4. `Owner` + `md_address` + `md_city` + `md_state`

Post-processing:
- Direct BUDB ID matches are expanded through `contact_group_id` to include all IDs in the same group, then auto-resolved down to the lowest-numbered group (see **Features** above).

#### Pipedrive matching criteria

Input-side combinations (pooling-order row):

1. **`po_id`** — compared directly to each deal's **Deal - Pooling Order ID**.
2. **Serial number** — compared directly to each deal's **Deal - Serial Number**.
3. `(lessor_owner)` + `address`/`city`/`state` **or** `add_address1`/`add_address1_city`/`add_address1_state`
   compared to **Deal - Contact person + Person - Mailing Address**.
4. `(first_name + last_name)` + the same address sets, compared to **Deal - Contact person + Person - Mailing Address**.
5. **Name found in Deal Title (secondary matching path)** — the system first checks if the **address, city, and state exactly match** the deal's mailing address, then checks if the input name (`lessor_owner` or `first_name + last_name`) is found inside the **Deal - Title**. Only when **both address and name match** is the deal considered a valid match.

Every matched deal ID records which method(s) matched it (`PO ID`, `Serial Number`, `Name + Address`, `Title + Address`), kept in a separate `deal_match_audit` export rather than written to the database.

Notes:
- Pipedrive source fields required in the local cache CSV include `Deal - ID`, `Deal - Title`, `Deal - Contact person`, `Person - Mailing Address`, `Deal - Pooling Order ID`, and `Deal - Serial Number`.
- Output may contain multiple Deal IDs joined by ` | `.
- Deals labeled `Test Deal` are excluded from matching.

### Owner and Address Replacements (Normalization)

#### Owner text replacements
Common abbreviations are expanded during normalization, including:
- `CO` → `COMPANY`
- `CORP` → `CORPORATION`
- `INC` → `INCORPORATED`
- `LLC` → `LIMITED LIABILITY COMPANY`
- `LP` → `LIMITED PARTNERSHIP`
- `LTD` → `LIMITED`
- `TR` / `TRST` / `TRT` / `TST` → `TRUST`
- `TRTEE` / `TSTE` / `TSTEE` / `TTEE` → `TRUSTEE`
- `RLT` → `REVOCABLE LIVING TRUST`
- `IRR` / `IRREV` / `IRRV` / `IRRVCABLE` / `IRV` → `IRREVOCABLE`
- `REV` / `REVC` / `REVOC` → `REVOCABLE`
- `EST` → `ESTATE`
- `FAM` / `FMLY` → `FAMILY`
- `LIV` / `LVG` → `LIVING`

#### Address text replacements
Address components are standardized, including:
- `STATE HWY` / `HIGHWAY` → `HWY`
- `NORTH`/`SOUTH`/`EAST`/`WEST` → `N`/`S`/`E`/`W`
- `NORTHEAST`/`NORTHWEST`/`SOUTHEAST`/`SOUTHWEST` → `NE`/`NW`/`SE`/`SW`
- `AVENUE` → `AVE`
- `BOULEVARD` → `BLVD`
- `CIRCLE` → `CR`
- `COURT` → `CT`
- `DRIVE` → `DR`
- `LANE` → `LN`
- `PARKWAY` → `PKWY`
- `ROAD` → `RD`
- `STREET` → `ST`
- `SUITE` → `STE`
- `APARTMENT` → `APT`
- `TRAIL` → `TRL`
- `FREEWAY` → `FWY`
- `P.O.` variants → `PO`

Additional cleaning behaviors:
- Non-alphanumeric punctuation cleanup.
- Whitespace collapsing.
- Pipedrive address cleaning also strips ZIP codes and trailing `USA`.

---

## 📝 Requirements
- **Python**: 3.11+ recommended.
- **Services**:
  - Google Cloud service account with Sheets (read/write) and Drive (read-only) scopes — required **only** if you'll use the Google Sheets import source. Not needed for local-file builds.
- **Local reference data** (not included in the repo — must be supplied per environment; required regardless of which import source you use):
  - A BUDB SQLite database, selected on the **Bottoms Up** tab at the start of each session (any file location — there is no fixed folder or auto-detection). It must contain a `bottoms_up` table with at least one row and, at minimum, the columns: `id`, `contact_group_id`, `Owner`, `Owner (Standardized)`, `Input: Address`, `Input: City`, `Input: State`, `md_address`, `md_city`, `md_state`.
  - A Pipedrive deals source at `cache/pipedrive_local_cache.csv`. This is downloaded automatically via the Pipedrive API (see below); there's no supported way to keep it fresh with a manually re-exported file instead.
- **Import source** (pick one per build):
  - A Google Sheet whose `po_db` tab includes, at minimum, the headers: `po_id`, `serial_number`, `date_uploaded`. **Or:**
  - One or more local `.csv`/`.xlsx`/`.xls` skip-trace export files (or a folder containing them) with the required columns listed under **Local file import flow** above.
- **Python packages**: see `requirements.txt` (UI: `customtkinter`; data: `pandas`, `openpyxl`, `numpy`; Google auth: `google-auth`, `google-api-python-client`; HTTP: `requests`).

> Install all pinned dependencies from `requirements.txt`.

## 🚀 Installation and Setup
1. **Clone repository**
    ```bash
    git clone https://github.com/dyoliya/po_db_import_manager.git
    cd po_db_import_manager
    ```

2. **Install dependencies**
    ```bash
    pip install -r requirements.txt
    ```

3. **Folder Structure**

    <pre>project/
    │
    ├── app.py                          Main application entry point (GUI)
    ├── po_transform.py                  Column cleaning, phone/date normalization
    ├── po_sqlite.py                     SQLite schema, po_id/serial allocation, row sync helpers
    ├── po_local_source.py               Local CSV/XLSX ingestion: header mapping, required-column/blank/duplicate checks, insert-only build, full-table derived-column refresh
    ├── po_deal_id_matcher.py            Row-level Pipedrive deal ID matching (PO ID, serial, name+address, title+address)
    ├── pooling_order_pipedrive.py       Pipedrive cache loading + lookup maps
    ├── pipedrive_api.py                 Pipedrive API client + deals download/cache refresh
    ├── pooling_order_budb.py            Row-level BUDB ID matching + contact-group auto-resolution + cache
    ├── po_build_state.py                Tracked build lifecycle: prepare/finish/fail-build, pre-build snapshots
    ├── po_existing_deals_report.py      Associated-deals report generation
    ├── po_writeback.py                  Google Sheets writeback reconciliation; also the recovery entry point (--apply)
    ├── pooling_order_action_log.py      Diffs before/after DB snapshots into an action log (run from app.py)
    ├── initialize_po_baseline.py       Standalone: stamps a known-good, already-tracked DB as the baseline (see Baseline Snapshots & Test Runs)
    ├── po_run_lock.py                   Exclusive-run lock shared by app.py and the baseline initializer
    ├── requirements.txt
    ├── version.txt
    ├── config/                         Google service account + Pipedrive settings + build-state tracking + run lock (local only)
    │   ├── google_service_account.json
    │   ├── pipedrive_settings.json
    │   ├── build_state.json
    │   └── po_run.lock
    ├── cache/                          Local lookup caches + Pipedrive deals CSV (local only)
    ├── database/                       Final output: the daily PO database(s) (auto-created)
    │   └── previous_versions/          Pre-build snapshots, archived on every build attempt (and by initialize_po_baseline.py)
    ├── audit/                          Combined NULL-conversion + BUDB group-selection audit exports (auto-created)
    ├── existing_deals/                 Deal-match audit + associated-deals reports (auto-created)
    ├── google_sheet_writeback/         Pending/completed Google writeback records (auto-created)
    └── input/                          Local backup copies of each downloaded Google Sheet (auto-created); also a convenient (not required) place to keep local-file-source skip-trace exports, selected via Browse
        └── google_sheet_backups/       One subfolder per spreadsheet ID, holding timestamped .xlsx + JSON snapshots
    </pre>

    That's everything needed for normal use — the **Update Action Log** button runs the comparison itself using the build's own recorded snapshots, with no file copying needed.

    Everything under `config/`, `cache/`, `database/`, `audit/`, `existing_deals/`, `google_sheet_writeback/`, and `input/` is environment-specific data and is excluded from version control via `.gitignore`.

4. **Set Up Configuration**

    4.1. **Google Service Account** (required for Google Sheets import)
    - In Google Cloud Console, create (or reuse) a service account and download its JSON key.
    - Save it as `config/google_service_account.json`.
    - Share the target Google Sheet (tab must be named `po_db`) with the service account's `client_email` address.
    - No further setup is needed — the app reads this file automatically on each Google Sheets import.

    > Skip this step if a service account already exists for this project — just get `config/google_service_account.json` handed over to you and place it in that path.

    4.2. **BUDB Reference Database**
    - No placement needed — on the **Bottoms Up** tab, click **Browse** and select the current Bottoms Up `.db` file from wherever it's kept.
    - The app validates it on selection: it must have a `bottoms_up` table with at least one row, and include `id`, `contact_group_id`, `Owner`, `Owner (Standardized)`, `Input: Address`, `Input: City`, `Input: State`, `md_address`, `md_city`, and `md_state` columns.
    - This selection is **not remembered** between app launches — re-select it (or confirm it's still selected) each session, via **A. First-time preparation** below.

    4.3. **Pipedrive Deals (API download)**
    - On the app's **Pipedrive** tab, paste in a Pipedrive API token (use the **Open API token page** button to find it in Pipedrive under Settings → API) and click **Save API token**. It's saved to `config/pipedrive_settings.json` so it only needs to be entered once.
    - There is no manual download button — **Check references** (and, if needed, **Build DB** itself) automatically downloads fresh deals whenever the cache is missing or older than 8 hours (hardcoded, not configurable).
    - Downloaded deals are written to `cache/pipedrive_local_cache.csv`, so everything downstream (deal matching, the associated-deals report) works the same either way.
    - A valid token is required to refresh the cache. If the cache file is less than 8 hours old, it's reused without needing a token at all — which is the only way a manually-placed CSV can substitute for the API, and only until it ages out.
    - `base_url`, `deals_filter_id`, and `deal_custom_field_labels` must also be set in `config/pipedrive_settings.json` for a download to succeed.

5. **Run the app**
    ```bash
    python app.py
    ```

## 🖥️ User Guide

### A. First-time preparation (repeat each session)
1. Open the project folder.
2. Confirm `config/google_service_account.json` exists.
3. On the **Bottoms Up** tab, click **Browse** and select the current Bottoms Up `.db` file — this is **not** remembered from the last launch, so it must be re-selected every time the app starts.
4. In the **Pipedrive** tab, confirm an API token is entered and saved (or, if the cache is still less than 8 hours old, confirm `cache/pipedrive_local_cache.csv` exists and is reasonably up to date).

### B. Import PO records

Pick exactly one import source per build with the radio button at the top of the **Import** tab — selecting one clears and disables the other's fields.

**B1. Google Sheets source**
1. Launch the app (`python app.py`, or the compiled executable).
2. On the **Pipedrive** tab, paste your API token and click **Save API token** (skip if already saved from a previous session).
3. On the **Bottoms Up** tab, click **Browse** and select the current Bottoms Up `.db` file — this isn't remembered between launches, so it's needed every session.
4. Select **Google Sheets** and paste the Sheet URL into the **Google Sheets URL** field (its `po_db` tab will be read).
5. Click **Check references** — confirms the sheet is reachable, editable, and correctly formatted, the Pipedrive API token is valid, the BUDB file is valid, and local storage is ready. This must succeed before **Build DB** is enabled.
6. Click **Build DB**.
7. Watch the **Activity Log** for progress: cleaning, Pipedrive matching, BUDB matching, county enrichment, row sync, and Google writeback.
8. On completion, a dialog confirms the build and the action log has already been updated as part of the same run — if the action log step itself fails, keep the updated database and click **Update Action Log** to retry.

**B2. Local Files source**
1. Launch the app, and complete steps 2–3 above (Pipedrive token, BUDB file) — both are still required; there is no Google service account needed for this source.
2. Select **Local Files**, then click **Browse files...** to pick one or more `.csv`/`.xlsx`/`.xls` files directly, or **Browse folder...** to recurse a folder for qualifying files.
3. Click **Check references**. Each selected file is validated independently: a file missing a required column, or with a blank `Lessor/Owner`/`Target County`/`Target State` anywhere in it, is rejected with a popup explaining why and excluded from the build; a file whose name already matches a `source_url` already in the database prompts Yes/No before checking it further. **Build DB** stays disabled until at least one file passes.
4. Click **Build DB**. Rows from every accepted file are combined into a single build: cleaned/standardized, checked for duplicates against the database and each other, assigned fresh `po_id`/`serial_number` values, and inserted (existing rows are never updated or deleted by this path, and there is no Google writeback).
5. Watch the **Activity Log** for progress, including the duplicate-row summary per file and the full-database derived-column refresh (`deal_id`/`budb_id`/`contact_group_id`/`combined_counties`) that runs afterward.
6. On completion, review `audit/po_local_audit_<build_id>.xlsx` if the log mentions null conversions, skipped rows, or multi-group BUDB matches, then click **Update Action Log** same as the Google Sheets path.

### C. Review outputs
- **The final output is the daily database itself**: `database/YYYY-MM-DD-pooling_order.db` — this is what the whole build updates.
- Check `audit/` for the combined NULL-conversion / BUDB group-selection audit workbook (Google Sheets builds) or `po_local_audit_<build_id>.xlsx` (local-file builds, also covering skipped/duplicate rows and the per-file summary) if the log reported anything worth reviewing.
- Check `existing_deals/` for the deal-match audit and the PO-associated-deals report generated after every import.
- For a Google Sheets build, check the Activity Log for the writeback confirmation and record path under `google_sheet_writeback/` — local-file builds have no writeback step.

### D. Review the change log (`pooling_order_action_log.py`)
After a build completes (**B1** step 8 or **B2** step 6 above), review the logged entries in the `pooling_order_action_log` table of the database in `database/` — check that the added/deleted/modified rows make sense for that version.
- **If everything looks correct**: nothing further to do — that build is `complete` and is the official daily version.
- **If something looks wrong**: coordinate on the flagged log entries to correct the input (Google Sheet) data, then:
  1. Restore the pre-build snapshot from `database/previous_versions/` back into `database/`.
  2. Re-run the import in `app.py` against the corrected input to rebuild a new version of the database.
  3. Review the action log again for this rebuilt version before treating it as final.

### E. Important usage notes
- Each calendar day gets its own database file; re-running on the same day continues syncing into that day's file rather than creating a new one.
- Every click of **Build DB** archives a pre-build snapshot into `database/previous_versions/<build_id>/` first, and a failed build is automatically rolled back to that snapshot (unless a Google writeback had already started — see Troubleshooting).
- The **Build DB** button is disabled unless the previous build finished (`complete` or `rolled_back`); an unfinished or `recovery_required` build must be resolved first.
- If a Google Sheets writeback is interrupted, a pending record remains in `google_sheet_writeback/` and must be resolved before the next Google Sheets import can run.
- `pooling_order_action_log.py` runs automatically from `app.py` once a build finishes — see **D** above.
- Only one of `app.py` or `initialize_po_baseline.py` can run against this project folder at a time — a second one will report that another process is running (`po_run_lock.py`).

### F. Baseline snapshots & test runs (`initialize_po_baseline.py`)
`initialize_po_baseline.py` is a **separate, manually-run script** (not launched from `app.py`). It stamps a known-good `database/*.db` file as the officially tracked baseline and records a safety-net backup of it, so you have a confirmed-stable snapshot to fall back on — most useful right before doing a test/experimental run you might want to discard.

Unlike a fresh/empty database, the file you point this at must already be a real, previously-tracked PO database: it must contain a populated `pooling_order` table with its complete `pooling_order_action_log` history, and must not already carry build-state tracking.

1. **Prepare the stable database**: make sure the verified `YYYY-MM-DD-pooling_order.db` file you want to treat as "stable" is the **only** `.db` file sitting directly inside `database/` (move anything else out first).
2. **Run it**:
   ```bash
   python initialize_po_baseline.py
   ```
3. The script:
   - Verifies that file is the only `.db` in `database/`, matches the `YYYY-MM-DD-pooling_order.db` naming pattern, and does **not** already carry build-state tracking (`pooling_order_build_state` table or `config/build_state.json`) — it refuses to run otherwise, so it never silently overwrites existing tracking.
   - Verifies it contains both the `pooling_order` table and its `pooling_order_action_log` history, and that the file passes SQLite's `quick_check`.
   - Verifies no pending Google writeback files exist in `google_sheet_writeback/`.
   - Copies the database, as-is, into `database/previous_versions/baseline-<id>/` — this is your restore point.
   - Stamps the live database (and `config/build_state.json`) with a fresh `baseline-<id>` build, status `complete`, so the app's **Build DB** button is immediately usable again.
4. You can now do your test run(s) through the app as usual. Each build attempt still creates its own `previous_versions/<build_id>/` snapshot automatically, and a failed build auto-restores on its own.
5. **To throw away a test run and go back to the stable baseline**: close the app, replace the file in `database/` with the copy saved in `database/previous_versions/baseline-<id>/`, delete `config/build_state.json`, and re-run `initialize_po_baseline.py` to re-stamp it as a clean, trackable baseline before resuming normal use.

### G. Recovering an interrupted writeback (`po_writeback.py`)
If the app was closed or crashed after a Google writeback may have started (build status `recovery_required`), run this script **before** touching the database or the Sheet further:
```bash
python po_writeback.py --apply
```
- It loads the pending writeback's journaled row mapping, re-downloads the live Sheet, and confirms every cell it needs to write (or already wrote) hasn't been changed by anyone else since the original capture.
- If nothing conflicts, it replays or confirms the writeback, archives the pending record, and marks the build `complete` — open the app afterward and confirm the action log is up to date.
- If the sheet changed in a conflicting way, it stops and leaves the build as `recovery_required` for manual review of that specific cell.

### H. Common troubleshooting
- **"Pipedrive local cache CSV not found"**: `cache/pipedrive_local_cache.csv` is missing. Click **Check references** with a valid API token entered on the **Pipedrive** tab to download it.
- **"Enter and save your Pipedrive API token; a fresh download is required."**: The cache is gone or older than 8 hours and no token is saved. Enter a token on the **Pipedrive** tab and click **Save API token**, then **Check references**.
- **"Cannot write to the Pipedrive cache CSV or its folder."**: Close `cache/pipedrive_local_cache.csv` if it's open in Excel or elsewhere, and confirm you have write permission to the `cache/` folder.
- **Pipedrive download errors (bad token, rate limited, field not found, etc.)**: The error message in the Activity Log / popup names the specific problem (e.g. an unexpected or renamed Pipedrive field) — check `config/pipedrive_settings.json`'s `deal_custom_field_labels` against the actual field names in Pipedrive if a field-not-found error appears.
- **"Select a Bottoms Up .db file first." / "Select an existing .db file."**: Click **Browse** on the **Bottoms Up** tab and select the current Bottoms Up `.db` file — this isn't remembered between app launches.
- **"This file has no bottoms_up table." / "BUDB is missing required columns: ..." / "The bottoms_up table contains no records."**: The selected file isn't a valid BUDB export — confirm it has a populated `bottoms_up` table with the required columns listed under **Requirements** above.
- **"Missing required Sheet headers: ..."**: The `po_db` tab is missing one of `po_id`, `serial_number`, or `date_uploaded` — correct the header row before building.
- **"Unexpected range-ID column header(s): ..."**: A column header pulled through as an internal Google Sheets range name instead of real text — rename that column's header in row 1.
- **"An unresolved Google writeback exists" / "A previous Google writeback needs recovery."**: Resolve the pending file in `google_sheet_writeback/` before starting another Google Sheets import, or run `po_writeback.py --apply` (see **G** above).
- **"Sheet values changed during capture." / "The sheet changed during checking."**: Re-run the import or check after editing of the sheet has stopped.
- **"Browse a file or folder before checking references."**: With **Local Files** selected, click **Browse files...** or **Browse folder...** before clicking **Check references**.
- **"No .csv/.xlsx files found in the selected input."**: The selected file(s)/folder contain no `.csv`/`.xlsx`/`.xls` files (temporary `~$...` lock files are ignored automatically).
- **"missing required column(s): ..."**: A local file is missing one of the required columns — see **Local file import flow** above for the accepted header spellings per field (matching is case-insensitive).
- **"Blank required values" popup / `[LOCAL] Skipped: <file> (blank ...)`**: The named file has a blank `Lessor/Owner`, `Target County`, or `Target State` value in the listed row(s). The whole file is skipped — fix the blanks and re-select it.
- **"'<file>' already appears in the database (matched by filename)." (Yes/No prompt)**: A file with that exact name was already imported before. Choosing **No** skips it; choosing **Yes** re-checks its rows and auto-skips it anyway if every row turns out to already exist.
- **"No files remain after duplicate checks: ..."**: Every selected file was skipped (blank required values, already-imported with no new rows, or no data rows) — review the Activity Log entries just above this error for the reason per file.
- **"No importable rows: every row was a duplicate of an existing pooling_order row or another selected file."**: All rows across the accepted files turned out to be duplicates once checked again at build time (e.g. the database changed between Check references and Build DB) — re-run Check references.
- **"Expected one initialized PO baseline." / "Found N .db files in database/."**: Only one active PO database is allowed directly inside `database/` — resolve the extra files (or run `initialize_po_baseline.py` if none exists yet).
- **"The previous build is not finished."**: A prior build is stuck at `building`, `ready_for_log`, or `recovery_required`. Finish it (click **Update Action Log**) or resolve its recovery state before clicking **Build DB** again.
- **"Recovery required" / "Build tracking disagrees; recovery required." / "Recorded build database is missing; recovery required."**: `config/build_state.json` and the database's own tracking no longer agree (e.g. a `.db` file was moved or replaced by hand). Inspect `database/`, `config/build_state.json`, and the relevant `previous_versions/` snapshot before deleting anything.
- **"Database preserved. Google writeback may have occurred; recovery is required."**: A build failed *after* the Google Sheets writeback had already started, so the sheet may be partially updated. Run `python po_writeback.py --apply` to validate and safely resume or confirm the writeback.
- **"Another PO app, initializer, or recovery process is running."**: Close whichever of `app.py` or `initialize_po_baseline.py` is already running against this project folder, then retry.
- **(From `initialize_po_baseline.py`) "Place exactly one YYYY-MM-DD-pooling_order.db baseline directly in database/."**: Remove any other `.db` files from `database/` so only the intended baseline file remains.
- **(From `initialize_po_baseline.py`) "Build tracking already exists" / "Database already has build tracking; restore its state instead."**: The target database is already tracked — inspect its current state with the app instead of re-initializing it.
- **(From `initialize_po_baseline.py`) "Baseline must contain pooling_order and its pooling_order_action_log history." / "SQLite quick_check failed."**: The file isn't a valid, previously-tracked PO database — point the script at a real exported/verified PO database, not a blank or partial one.
- **(From `initialize_po_baseline.py`) "Unresolved Google writeback exists; do not initialize."**: Resolve the pending file(s) in `google_sheet_writeback/` before initializing a baseline.

---

## 👩‍💻 Credits
- **2026-04-08**: Project created by **Julia** ([@dyoliya](https://github.com/dyoliya))
- 2026–present: Maintained by **Julia** for **Community Minerals II, LLC**
