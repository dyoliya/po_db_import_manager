"""Local CSV/XLSX ingestion for the pooling_order table.

This is a second, independent source alongside the Google Sheet: every row is
treated as new (no po_id matching), nothing is deleted, and there is no
writeback. See po_sqlite.py for the Google-Sheet-only allocate()/synchronize().
"""

from pathlib import Path

import pandas as pd

from po_deal_id_matcher import populate_po_deal_ids_in_df
from pooling_order_budb import populate_budb_ids_in_df
from po_sqlite import combined_counties
from po_transform import clean_and_prepare_df

FIELD_HEADER_CANDIDATES = {
    "lessor_owner": {"lessor", "owner"},
    "contact_type": {"contact type"},
    "target_county": {"target county"},
    "target_state": {"target state"},
    "first_name": {"input: first name", "first name"},
    "middle_name": {"middle name", "input: middle name"},
    "last_name": {"input: last name", "last name"},
    "address": {"input: address 1", "input: address", "address"},
    "city": {"input: city", "city"},
    "state": {"input: state", "state"},
    "postal_code": {
        "input: zip code", "postal code", "zip code", "postal_code", "zip_code",
    },
    "attn": {"attn"},
}

REQUIRED_COLUMNS = tuple(FIELD_HEADER_CANDIDATES)

# Alternate header spellings for columns that already exist in pooling_order
# but are NOT required — a file missing any of these is still accepted. Unlike
# FIELD_HEADER_CANDIDATES, these never add a file to REQUIRED_COLUMNS; they
# only widen which header text a given existing column is recognized under.
OPTIONAL_FIELD_HEADER_CANDIDATES = {
    "add_address_y_n_u": {"md_address_ynu"},
    "add_address1": {"md_address"},
    "add_address1_city": {"md_city"},
    "add_address1_state": {"md_state"},
    "add_address1_zip": {"md_postalcode"},
    "add_address1_county": {"md_county"},
    "add_address1_first_seen": {"md_first_seen"},
    "add_address1_last_seen": {"md_last_seen"},
    "ph_phone_y_n_u": {"phone_ynu"},
    "ph_phone1": {"phone1"},
    "ph_phone1_first_seen": {"phone1_first_seen"},
    "ph_phone1_last_seen": {"phone1_last_seen"},
    "ph_phone1_type": {"phone1_type"},
    "ph_phone2": {"phone2"},
    "ph_phone2_first_seen": {"phone2_first_seen"},
    "ph_phone2_last_seen": {"phone2_last_seen"},
    "ph_phone2_type": {"phone2_type"},
    "ph_phone3": {"phone3"},
    "ph_phone3_first_seen": {"phone3_first_seen"},
    "ph_phone3_last_seen": {"phone3_last_seen"},
    "ph_phone3_type": {"phone3_type"},
    "ph_phone4": {"phone4"},
    "ph_phone4_first_seen": {"phone4_first_seen"},
    "ph_phone4_last_seen": {"phone4_last_seen"},
    "ph_phone4_type": {"phone4_type"},
    "ph_phone5": {"phone5"},
    "ph_phone5_first_seen": {"phone5_first_seen"},
    "ph_phone5_last_seen": {"phone5_last_seen"},
    "ph_phone5_type": {"phone5_type"},
    "email_hit_y_n_u": {"email_hit_ynu"},
    "email_email1": {"email1"},
    "email_email1_first_seen": {"email1_first_seen"},
    "email_email1_last_seen": {"email1_last_seen"},
    "email_email2": {"email2"},
    "email_email2_first_seen": {"email2_first_seen"},
    "email_email2_last_seen": {"email2_last_seen"},
    "email_email3": {"email3"},
    "email_email3_first_seen": {"email3_first_seen"},
    "email_email3_last_seen": {"email3_last_seen"},
    "email_email4": {"email4"},
    "email_email4_first_seen": {"email4_first_seen"},
    "email_email4_last_seen": {"email4_last_seen"},
    "email_email5": {"email5"},
    "email_email5_first_seen": {"email5_first_seen"},
    "email_email5_last_seen": {"email5_last_seen"},
    "dec_deceased_y_n_u": {"deceased_ynu"},
}

# Headers matched against during renaming: required fields first, then
# optional ones. Keeping them separate dicts means REQUIRED_COLUMNS (derived
# from FIELD_HEADER_CANDIDATES only) is unaffected by the optional additions.
ALL_FIELD_HEADER_CANDIDATES = {**FIELD_HEADER_CANDIDATES, **OPTIONAL_FIELD_HEADER_CANDIDATES}

DERIVED_COLUMNS = ("deal_id", "budb_id", "contact_group_id", "combined_counties")

# Columns that must never be blank/NULL in a local file — a file with any
# blank value in these is rejected entirely rather than imported partially.
NON_BLANK_COLUMNS = ("lessor_owner", "target_county", "target_state")

# Local-file builds otherwise only ever write into columns that already exist
# in the pooling_order table (no new columns are added from local data). This
# is the sole exception: "attn" is now a required column (see
# FIELD_HEADER_CANDIDATES), and is added to the schema if it isn't already
# there.
ALLOWED_NEW_COLUMNS = {"attn"}

_SUPPORTED_EXTENSIONS = {".csv", ".xlsx", ".xls"}


def _normalize_header(name) -> str:
    return str(name).strip().casefold()


def discover_input_files(paths):
    """Expand any directory in `paths` (recursively) and combine with direct files."""
    found = []

    for raw in paths:
        path = Path(raw)
        if path.is_dir():
            found.extend(
                p for p in path.rglob("*")
                if p.is_file()
                and p.suffix.lower() in _SUPPORTED_EXTENSIONS
                and not p.name.startswith("~$")
            )
        elif path.is_file():
            found.append(path)
        else:
            raise FileNotFoundError(f"Local input path not found: {path}")

    seen = set()
    ordered = []
    for path in found:
        resolved = path.resolve()
        if resolved not in seen:
            seen.add(resolved)
            ordered.append(path)

    return sorted(ordered, key=lambda p: str(p).lower())


def read_local_file(path) -> pd.DataFrame:
    path = Path(path)
    ext = path.suffix.lower()

    if ext == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False)

    if ext in {".xlsx", ".xls"}:
        with pd.ExcelFile(path) as xls:
            return pd.read_excel(
                xls,
                sheet_name=xls.sheet_names[0],
                dtype=str,
                keep_default_na=False,
                na_filter=False,
            )

    raise ValueError(f"Unsupported file type: {path.name}. Use .csv or .xlsx.")


def _rename_known_headers(df: pd.DataFrame):
    rename_map = {}
    matched_fields = set()

    for column in df.columns:
        normalized = _normalize_header(column)
        for field, candidates in ALL_FIELD_HEADER_CANDIDATES.items():
            if field in matched_fields or field in rename_map.values():
                continue
            if normalized in candidates:
                rename_map[column] = field
                matched_fields.add(field)
                break

    return df.rename(columns=rename_map), matched_fields


def prepare_local_file(path):
    """Read one local file, map its headers, and validate required columns.

    Returns (business_df, tracking_df) with a shared RangeIndex. `tracking_df`
    carries `source_file`/`row_in_file` out-of-band so it survives
    clean_and_prepare_df without becoming a real pooling_order column.
    """
    path = Path(path)
    df = read_local_file(path)
    df, matched_fields = _rename_known_headers(df)

    missing = [field for field in REQUIRED_COLUMNS if field not in matched_fields]
    if missing:
        raise ValueError(
            f"{path.name}: missing required column(s): {', '.join(missing)}"
        )

    df = df.reset_index(drop=True)
    tracking = pd.DataFrame(
        {"source_file": path.name, "row_in_file": df.index + 2},
        index=df.index,
    )

    return df, tracking


def find_blank_required_values(df: pd.DataFrame, tracking: pd.DataFrame, columns=NON_BLANK_COLUMNS):
    """Locate blank/NULL values in columns that must never be blank.

    Returns a DataFrame with one row per (column, row_in_file) hit; empty if
    none found. A file with any hit here is rejected entirely, not just the
    affected rows.
    """
    records = []
    for col in columns:
        if col not in df.columns:
            continue
        blank_mask = df[col].isna()
        for idx in df.index[blank_mask]:
            records.append({
                "column": col,
                "row_in_file": tracking.at[idx, "row_in_file"],
            })

    return pd.DataFrame(records, columns=["column", "row_in_file"])


def preview_file_rows(path):
    """Prepare and clean a single file the same way the build does, without
    touching the database. Used at Check-references time to decide whether a
    file whose filename already exists in pooling_order still has any new
    (non-duplicate) rows worth building.
    """
    frame, tracking = prepare_local_file(path)
    frame = frame.copy()
    frame.insert(0, "po_id", "")
    cleaned, _originals, _sanitized, _phones, _dates, _null_audit = clean_and_prepare_df(frame)
    cleaned = backfill_decd_source(cleaned)
    return cleaned, tracking


def backfill_decd_source(df: pd.DataFrame) -> pd.DataFrame:
    """Set decd_source="IDI" wherever dec_deceased_y_n_u is present and decd_source is not."""
    if "dec_deceased_y_n_u" not in df.columns:
        return df

    if "decd_source" not in df.columns:
        df["decd_source"] = None

    mask = df["dec_deceased_y_n_u"].notna() & df["decd_source"].isna()
    df.loc[mask, "decd_source"] = "IDI"
    return df


def _normalize_value(value) -> str:
    """str(value).strip().casefold(), treating None/NaN (any dtype) as "".

    Note: `value or ""` is NOT safe here — under pandas' string dtype a
    missing cell comes back as float('nan'), which is truthy, so `or` would
    never fall through to "" and every missing value would normalize to the
    literal string "nan" instead of "".
    """
    if value is None or pd.isna(value):
        return ""
    return str(value).strip().casefold()


def drop_duplicate_rows(conn, df: pd.DataFrame, tracking: pd.DataFrame, columns=REQUIRED_COLUMNS):
    """Flag rows whose required-column values already exist in pooling_order or
    duplicate an earlier row in this same batch.

    Returns (keep_mask, skipped_df). `keep_mask` is a boolean Series aligned to
    df.index; `skipped_df` has one row per skipped record with its reason.
    """
    existing = {}
    cursor = conn.execute(f"SELECT po_id, {', '.join(columns)} FROM pooling_order")
    for record in cursor:
        po_id = record[0]
        key = tuple(_normalize_value(value) for value in record[1:])
        existing.setdefault(key, po_id)

    seen_in_batch = {}
    keep = []
    skipped_records = []

    for idx in df.index:
        key = tuple(_normalize_value(df.at[idx, c]) for c in columns)
        source_file = tracking.at[idx, "source_file"]
        row_in_file = tracking.at[idx, "row_in_file"]

        if key in existing:
            keep.append(False)
            skipped_records.append({
                "source_file": source_file,
                "row_in_file": row_in_file,
                "reason": "matches_existing_po_id",
                "detail": f"Matches existing pooling_order row {existing[key]}.",
            })
            continue

        if key in seen_in_batch:
            dup_file, dup_row = seen_in_batch[key]
            keep.append(False)
            skipped_records.append({
                "source_file": source_file,
                "row_in_file": row_in_file,
                "reason": "duplicate_in_batch",
                "detail": f"Duplicates {dup_file} row {dup_row} in this build.",
            })
            continue

        seen_in_batch[key] = (source_file, row_in_file)
        keep.append(True)

    keep_mask = pd.Series(keep, index=df.index)
    skipped_df = pd.DataFrame(
        skipped_records,
        columns=["source_file", "row_in_file", "reason", "detail"],
    )
    return keep_mask, skipped_df


def summarize_file_results(tracking: pd.DataFrame, keep_mask: pd.Series) -> pd.DataFrame:
    """Per-file rows read/kept/skipped, so a fully-deduplicated file is visible."""
    summary = tracking.groupby("source_file").size().rename("rows_read").to_frame()
    summary["rows_kept"] = tracking[keep_mask].groupby("source_file").size()
    summary["rows_kept"] = summary["rows_kept"].fillna(0).astype(int)
    summary["rows_skipped"] = summary["rows_read"] - summary["rows_kept"]
    return summary.reset_index()


def refresh_derived_columns(conn, maps, base_dir, progress_callback=None):
    """Recompute deal_id/budb_id/contact_group_id/combined_counties for every
    row currently in pooling_order (not just rows from this build), and write
    back only the rows whose computed value actually changed.
    """
    full_df = pd.read_sql_query("SELECT * FROM pooling_order", conn)
    derived = list(DERIVED_COLUMNS)

    before = full_df[derived].astype(object).where(full_df[derived].notna(), None)

    full_df, pd_meta, deal_audit = populate_po_deal_ids_in_df(
        full_df, base_dir=base_dir, progress_callback=progress_callback,
    )
    full_df, group_audit = populate_budb_ids_in_df(
        full_df, maps, progress_callback=progress_callback,
    )
    full_df["combined_counties"] = [
        combined_counties(county, state)
        for county, state in zip(full_df["target_county"], full_df["target_state"])
    ]

    after = full_df[derived].astype(object).where(full_df[derived].notna(), None)
    changed_mask = (before.values != after.values).any(axis=1)

    updates = [
        (row.deal_id, row.budb_id, row.contact_group_id, row.combined_counties, row.po_id)
        for row in full_df.loc[changed_mask].itertuples(index=False)
    ]
    if updates:
        conn.executemany(
            "UPDATE pooling_order SET deal_id=?, budb_id=?, contact_group_id=?, "
            "combined_counties=? WHERE po_id=?",
            updates,
        )

    return {
        "rows_considered": len(full_df),
        "rows_changed": int(changed_mask.sum()),
        "pipedrive_meta": pd_meta,
        "deal_audit": deal_audit,
        "group_audit": group_audit,
    }
