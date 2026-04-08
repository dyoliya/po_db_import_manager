# Pooling Order DB Import Manager

PO DB Import Manager is a desktop data consolidation tool for pooling-order mineral owner records. It imports data from separately maintained Google-Sheets exports, normalizes owner and address data, enriches records with BUDB / PROD / Pipedrive match IDs, and writes the final dataset into a local SQLite database for fast and consistent lookup.

---

![Version](https://img.shields.io/badge/version-1.0.0-ffab4c?style=for-the-badge&logo=python&logoColor=white)
![Python](https://img.shields.io/badge/python-3.11%2B-273946?style=for-the-badge&logo=python&logoColor=ffab4c)
![Status](https://img.shields.io/badge/status-active-273946?style=for-the-badge&logo=github&logoColor=ffab4c)

---

## 🚧 Problem Statement / Motivation
Pooling-order source data is maintained across separate Google Sheets files. That setup makes it hard to search and reconcile records quickly across teams.

This tool solves that by consolidating those files into a SQLite-backed lookup database that:
- aligns with existing SQLite-based internal workflows,
- makes mineral owner (MO) lookup easier for the Pipedrive team,
- supports consistent cross-system matching (BUDB, .work, Pipedrive), and
- remains read-only from the user perspective (users cannot directly edit source Google Sheets through this tool).

---

## ✨ Features

### Core Import and Consolidation
- Imports pooling-order files and maps headers to a standardized schema.
- Creates/rolls daily SQLite DB files with automatic archiving of older versions.
- Preserves stable row identity using `po_id`.
- Supports optional serial number intake and auto-assignment when missing.

### Matching and Enrichment
- Normalizes owner names, addresses, city/state fields for deterministic matching.
- Matches and populates:
  - `budb_id` from BUDB SQLite,
  - `prod_id` from PROD contact/address data,
  - `deal_id` from local Pipedrive cache CSV.
- Caches lookup maps locally in SQLite for faster repeated runs.
- Refreshes BUDB matches for existing rows using latest lookup data.
- Refreshes Pipedrive matches for existing rows using latest cache data.
- Keeps old match values when no new match is found, based on matching rules defined in each standalone matcher.

### Standalone Companion Utilities
- Includes a separate database comparison / change-tracking utility for auditing differences between old and new PO DB versions.
- Logs added rows, deleted rows, modified fields, and schema-level changes into `pooling_order_action_log`.
- Carries forward prior log history into the new database when available.
- Supports version-to-version traceability without changing the main import workflow.

---

## 🧠 Logic Flow

### A. Main Import Workflow
1. **Input acquisition**
   - User provides pooling-order input files (originating from separate Google Sheets exports).

2. **Schema normalization**
   - Headers are normalized (e.g., `Lessor Owner` → `lessor_owner`, `Postal Code` → `postal_code`).
   - Required columns are validated.
   - Optional fields such as `serial_number` are retained when present.

3. **Database lifecycle**
   - The app creates or reuses a daily SQLite file (`YYYY-MM-DD-pooling_order.db`).
   - If today’s DB does not yet exist, the latest DB is copied forward.
   - Older DB files are moved into `database/previous_versions/`.

4. **Lookup source loading**
   - **BUDB**: loads from `budb/*.db`, with cache invalidation based on file signature changes.
   - **PROD**: loads from PROD source data / lookup cache and builds normalized lookup maps.
   - **Pipedrive**: loads from local Pipedrive cache files and builds normalized lookup maps.

5. **Match refresh for existing database rows**
   - Existing rows already stored in `pooling_order` are re-checked against the latest BUDB and Pipedrive lookup maps.
   - Update behavior follows the implemented rules:
     - blank old → new match = update
     - old match → different new match = update
     - old match → no match now = keep old value

6. **New-row matching and enrichment**
   - Newly imported pooling-order rows are matched independently against BUDB, PROD, and Pipedrive.
   - Matching outputs populate `budb_id`, `prod_id`, and `deal_id`.

7. **Persistence**
   - Final enriched rows are inserted into the daily SQLite DB.
   - Stable row identity is preserved through `po_id`.


### B. Standalone Audit / Change-Tracking Workflow
This workflow is separate from the main importer and is run independently when version-to-version database auditing is needed.

1. **Old/New database selection**
   - The script reads one database from `old/` and one database from `new/`.

2. **Table resolution**
   - The comparison targets the `pooling_order` table.
   - The log destination table is `pooling_order_action_log`.

3. **Historical log carry-forward**
   - If the old database already contains `pooling_order_action_log`, prior log rows are copied into the new database when the new log table is still empty.

4. **Database comparison**
   - The script compares old vs. new databases using `po_id` as the primary key.
   - It detects:
     - added rows
     - deleted rows
     - modified field values
     - schema changes such as added or removed columns

5. **Log writing**
   - All detected differences are written into `pooling_order_action_log` inside the new database.
   - `date_uploaded` is excluded from field-level modification logging.

6. **Audit output**
   - The result is a versioned action log stored directly in the new PO database for downstream review and traceability.

### Matching Criteria

#### BUDB matching criteria
Input key (from pooling order row):
- `lessor_owner` + `address` + `city` + `state` (normalized)

Matched against BUDB combinations:
1. `Owner` + `Input: Address` + `Input: City` + `Input: State`
2. `Owner (Standardized)` + `Input: Address` + `Input: City` + `Input: State`
3. `Owner (Standardized)` + `md_address` + `md_city` + `md_state`
4. `Owner` + `md_address` + `md_city` + `md_state`

Post-processing:
- Direct BUDB ID matches are expanded through `contact_group_id` to include all IDs in the same group.

#### PROD matching criteria
Input-side combinations (pooling order row):
1. `lessor_owner` + `address` + `city` + `state`
2. `first_name + last_name` + `address` + `city` + `state`

Compared against PROD lookup combinations:
- `combo_full_name`: `first_name + middle_name + last_name` + address + city + state
- `combo_no_middle`: `first_name + last_name` + address + city + state

Notes:
- Address is required for match key generation.
- Output may contain multiple PROD IDs joined by `|`.

#### Pipedrive matching criteria

Input-side combinations (pooling order row):

1. `(lessor_owner)` + `address` + `city` + `state`  
   compared to **Deal - Contact person + mailing address**

2. `(first_name + last_name)` + `address` + `city` + `state`  
   compared to **Deal - Contact person + mailing address**

3. **Name found in Deal Title (secondary matching path)**  
   - The system first checks if the **address, city, and state exactly match** the deal’s mailing address  
   - If the address matches, it then checks if the input name (either `lessor_owner` or `first_name + last_name`) is found inside the **Deal - Title**  
   - Only when **both address and name match** → the deal is considered a valid match

**How Deal - Title is used**

- The Deal Title usually contains the name of the mineral owner followed by location details  
  (example: `John Smith Reeves County, TX`)

- The system does **not rely on the title alone**  
  - It first requires an exact match on the mailing address  
  - Then uses the title only to confirm that the name is also present

- The tool does not split or analyze the structure of the title  
  - it simply cleans and standardizes the text  
  - then checks if the input name appears anywhere in the title

- This serves as a **secondary (fallback) matching method** when the contact person field is incomplete or formatted differently

- This approach ensures that matches are **not based on name alone**, helping prevent incorrect matches between different people with similar names

Notes:
- Pipedrive source fields required in CSV:
  - `Deal - ID`
  - `Deal - Title`
  - `Deal - Contact person`
  - `Person - Mailing Address`
- Output may contain multiple Deal IDs joined by ` | `

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
- `P.O.` variants → `PO`

Additional cleaning behaviors:
- Non-alphanumeric punctuation cleanup.
- Whitespace collapsing.
- Pipedrive address cleaning also strips ZIP codes and trailing `USA`.

---

## 📝 Requirements
- Python 3.11+ recommended.
- Local dependencies listed in `requirements.txt`.
- Access to the following local structure (runtime):
  - `budb/` folder containing exactly one BUDB `.db` file.
  - `cache/pipedrive_local_cache.csv` for Pipedrive lookup loading.
- Network access to PROD MySQL when forcing/rebuilding PROD lookup cache.

---

## 🚀 Installation and Setup
1. Clone the repository.
2. Create and activate a virtual environment.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Ensure required folders/files exist:
   - `budb/<your_budb_file>.db`
   - `cache/pipedrive_local_cache.csv`
5. Folder Structure

      <pre>project/
      │
      ├── pooling_order_db_import_manager.py    Main application (GUI) that handles file import, matching, and database updates
      ├── pooling_order_budb.py                 Handles matching logic and lookup preparation for BUDB data
      ├── pooling_order_pipedrive.py            Handles matching logic and lookup preparation for Pipedrive data
      ├── pooling_order_prod.py                 Handles matching logic and lookup preparation for PROD data
      ├── pooling_order_action_log.py           Standalone tool for comparing old vs new databases and tracking changes
      │
      ├── budb/                                 Contains the BUDB source database used for matching
      ├── cache/                                Stores cached lookup data (e.g., Pipedrive, BUDB) for faster processing
      │
      ├── database/                             Local SQLite database storage
      │   ├── yyyy-mm-dd-pooling_order.db       Active daily database used by the tool
      │   └── previous_versions/                Archived older database versions for reference
      │
      ├── new/                                  Holds the newer database version for comparison (used by action log tool)
      └── old/                                  Holds the older database version for comparison (used by action log tool)
      </pre>
6. Run the app:
   ```bash
   python pooling_order_db_import_manager.py
   ```

---

## 🖥️ User Guide
1. Open the application.
2. Select/confirm input pooling-order files.
3. Start import.
4. The tool will:
   - normalize incoming data,
   - load/update BUDB, PROD, and Pipedrive lookup maps,
   - match records and populate `budb_id`, `prod_id`, and `deal_id`,
   - write results to the daily SQLite database.
5. Use the generated SQLite DB for read-only mineral owner lookup and downstream team workflows.

---

## 👩‍💻 Credits
- **2026-04-08**: Project created by **Julia** ([@dyoliya](https://github.com/dyoliya))  
- 2026–present: Maintained by **Julia** for **Community Minerals II, LLC**
