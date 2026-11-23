import re
import pandas as pd

NULL_LIKE_RAW = {
    "", "#N/A", "N/A", "NA", "NULL", "nan", "#REF",
    "#VALUE!", "#REF!", "#DIV/0!", "#NAME?", "#NUM!", "#NULL!", "#ERROR!", "#CALC!", "#SPILL!", "#LOADING!"
}
NULL_LIKE = {x.strip().casefold() for x in NULL_LIKE_RAW}

def sanitize_colname(name: str) -> str:
    """
    DB-friendly header:
    - normalize dashes to space
    - spaces -> underscore
    - remove non-alphanumeric except underscore
    - collapse multiple underscores
    - strip underscores
    - lowercase
    - avoid starting with digit
    """
    if name is None:
        name = ""

    s = str(name).strip().replace("/", " ")

    # Normalize dash types to space (prevents double underscores)
    s = s.replace("–", " ").replace("—", " ").replace("-", " ")
    s = s.replace("\u00A0", " ")

    # Spaces -> underscore
    s = re.sub(r"\s+", "_", s)

    # Keep only alnum + underscore
    s = re.sub(r"[^0-9a-zA-Z_]", "", s)

    # Collapse underscores + trim
    s = re.sub(r"_+", "_", s).strip("_")

    s = s.lower()

    if not s:
        s = "col"

    if s[0].isdigit():
        s = f"c_{s}"

    return {"lessors": "lessor_owner", "owner_standardized": "lessor_owner"}.get(s, s)


def coerce_nulls(x):
    if x is None or pd.isna(x):
        return None
    s = str(x).strip()
    if s.casefold() in NULL_LIKE:
        return None
    return s

def normalize_phone_value(x):
    """Keep phone values as TEXT and avoid float artifacts like 12345.0, preserve NULLs."""
    if x is None:
        return None
    s = str(x).strip()
    if s.casefold() in NULL_LIKE:
        return None

    if re.fullmatch(r"\d+\.0", s):
        s = s[:-2]

    if re.search(r"[eE]", s):
        try:
            s = str(int(float(s)))
        except Exception:
            pass

    return s


def standardize_date_series(series: pd.Series) -> pd.Series:
    """Return YYYY-MM-DD or NULL; stop if a nonblank date is invalid."""
    cleaned = series.map(coerce_nulls)
    converted = {}

    # Parse each distinct value separately so mixed formats work.
    for value in cleaned.dropna().unique():
        if re.fullmatch(r"[+-]?\d+(?:\.\d+)?", value):
            # Numeric values in date columns are spreadsheet serial dates.
            parsed = pd.to_datetime(
                float(value),
                unit="D",
                origin="1899-12-30",
                errors="coerce",
            )
        else:
            parsed = pd.to_datetime(
                value,
                errors="coerce",
                dayfirst=False,
            )

        if pd.isna(parsed):
            converted[value] = None
            continue

        converted[value] = parsed.strftime("%Y-%m-%d")

    return cleaned.map(
        lambda value: None if pd.isna(value) else converted[value]
    )


def clean_and_prepare_df(df_raw: pd.DataFrame):
    original_cols = list(df_raw.columns)
    sanitized = [sanitize_colname(c) for c in original_cols]

    duplicates = sorted({
        c for c in sanitized if sanitized.count(c) > 1
    })
    if duplicates:
        raise ValueError(
            "Headers normalize to duplicate column names: "
            + ", ".join(duplicates)
        )

    df = df_raw.copy(deep=True)
    df.columns = sanitized

    # Keep raw ID text so NULL-like errors cannot turn into new identities.
    raw_ids = df["po_id"].fillna("").astype(str).str.strip()
    df = df.drop(columns=["budb_id", "contact_group_id", "deal_id", "prod_id",
                          "source_url", "combined_counties"], errors="ignore")
    audit_records = []

    def _as_text_series(s: pd.Series) -> pd.Series:
        return s.map(lambda x: "" if pd.isna(x) else str(x))

    def audit_stage(stage: str, before_df: pd.DataFrame, after_df: pd.DataFrame, cols: list[str], reason: str):
        for c in cols:
            if c not in before_df.columns or c not in after_df.columns:
                continue

            before_col = _as_text_series(before_df[c])
            after_col = after_df[c]

            # suspicious: had something (non-empty) then became NULL/None
            after_missing = after_df[c].map(lambda v: v is None or pd.isna(v))
            mask = (before_col.str.strip() != "") & (after_missing)

            if mask.any():
                for idx in after_df.index[mask]:
                    audit_records.append({
                        "row_index": int(idx) + 2,  # assumes header is row 1
                        "column_sanitized": c,
                        "original_value": before_col.loc[idx],
                        "cleaned_value": None,
                        "stage": stage,
                        "reason": reason,
                    })

    # ---------------- Stage 1: NULL-like -> None ----------------
    before1 = df.copy(deep=True)
    for c in df.columns:
        df[c] = df[c].map(coerce_nulls)

    audit_stage(
        stage="coerce_nulls",
        before_df=before1,
        after_df=df,
        cols=list(df.columns),
        reason="Non-empty value became NULL via NULL_LIKE/coerce_nulls",
    )

    # ---------------- Stage 2: phones ----------------
    phone_cols = [c for c in df.columns if re.fullmatch(r"ph_phone[1-5]", c)]
    if phone_cols:
        before2 = df.copy(deep=True)
        for c in phone_cols:
            df[c] = df[c].map(normalize_phone_value)

        audit_stage(
            stage="normalize_phone_value",
            before_df=before2,
            after_df=df,
            cols=phone_cols,
            reason="Non-empty value became NULL during phone normalization",
        )

    # ---------------- Stage 3: dates ----------------
    date_cols = [
        c for c in df.columns
        if c.replace("_", "").endswith(("firstseen", "lastseen"))
    ]
    if date_cols:
        before3 = df.copy(deep=True)
        for c in date_cols:
            df[c] = standardize_date_series(df[c])

        audit_stage(
            stage="standardize_date_series",
            before_df=before3,
            after_df=df,
            cols=date_cols,
            reason="Non-empty value became NULL during date standardization",
        )

    df["po_id"] = raw_ids
    audit_df = pd.DataFrame(audit_records)
    return df, original_cols, sanitized, phone_cols, date_cols, audit_df
