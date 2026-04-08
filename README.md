# PO DB Import Manager

## Brief Description
PO DB Import Manager is a desktop data consolidation tool for pooling-order mineral owner records. It imports data from separately maintained Google-Sheets exports, normalizes owner and address data, enriches records with BUDB / PROD / Pipedrive match IDs, and writes the final dataset into a local SQLite database for fast and consistent lookup.

## Problem Statement / Motivation
Pooling-order source data is maintained across separate Google Sheets files. That setup makes it hard to search and reconcile records quickly across teams.

This tool solves that by consolidating those files into a SQLite-backed lookup database that:
- aligns with existing SQLite-based internal workflows,
- makes mineral owner (MO) lookup easier for the Pipedrive team,
- supports consistent cross-system matching (BUDB, PROD, Pipedrive), and
- remains read-only from the user perspective (users cannot directly edit source Google Sheets through this tool).

## Features
- Imports pooling-order files and maps headers to a standardized schema.
- Creates/rolls daily SQLite DB files with automatic archiving of older versions.
- Normalizes owner names, addresses, city/state fields for deterministic matching.
- Matches and populates:
  - `budb_id` from BUDB SQLite,
  - `prod_id` from PROD contact/address data,
  - `deal_id` from local Pipedrive cache CSV.
- Caches lookup maps locally in SQLite for faster repeated runs.
- Refreshes Pipedrive matches for existing rows using latest cache data.
- Preserves stable row identity using `po_id`.

## Logic Flow
1. **Input acquisition**
   - User provides pooling-order input files (originating from separate Google Sheets exports).
2. **Schema normalization**
   - Headers are normalized (e.g., `Lessor Owner` → `lessor_owner`, `Postal Code` → `postal_code`).
3. **Database lifecycle**
   - The app creates/uses a daily SQLite file (`YYYY-MM-DD-pooling_order.db`) and archives older DB files.
4. **Lookup source loading**
   - **BUDB**: loads from `budb/*.db` (`bottoms_up` table), with cache invalidation on file signature changes.
   - **PROD**: loads from MySQL (or existing local cache), then builds normalized lookup maps.
   - **Pipedrive**: loads from `cache/pipedrive_local_cache.csv`, writes local SQLite cache, then builds lookup maps.
5. **Record matching & enrichment**
   - For each pooling-order row, matching runs independently for BUDB, PROD, and Pipedrive using normalized key criteria.
6. **Persistence**
   - Final enriched records are upserted/saved into the daily SQLite DB for lookup/reporting.

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
1. `(lessor_owner)` + `address` + `city` + `state` compared to **Deal - Contact person + mailing address**
2. `(first_name + last_name)` + `address` + `city` + `state` compared to **Deal - Contact person + mailing address**
3. **Containment path**: if normalized input owner/name is contained in normalized `Deal - Title`, require exact same normalized mailing address/city/state

Notes:
- Pipedrive source fields required in CSV:
  - `Deal - ID`
  - `Deal - Title`
  - `Deal - Contact person`
  - `Person - Mailing Address`
- Output may contain multiple Deal IDs joined by ` | `.

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

## Requirements
- Python 3.11+ recommended.
- Local dependencies listed in `requirements.txt`.
- Access to the following local structure (runtime):
  - `budb/` folder containing exactly one BUDB `.db` file.
  - `cache/pipedrive_local_cache.csv` for Pipedrive lookup loading.
- Network access to PROD MySQL when forcing/rebuilding PROD lookup cache.

## Installation and Setup
1. Clone the repository.
2. Create and activate a virtual environment.
3. Install dependencies:
   ```bash
   pip install -r requirements.txt
   ```
4. Ensure required folders/files exist:
   - `budb/<your_budb_file>.db`
   - `cache/pipedrive_local_cache.csv`
5. Run the app:
   ```bash
   python pooling_order_db_import_manager.py
   ```

## User Guide
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
If needed, this README can be extended with sample input/output screenshots and an operations checklist for daily run procedures.
