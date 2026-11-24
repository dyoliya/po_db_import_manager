import json
import re
import sqlite3
from collections import Counter
from datetime import datetime
from pathlib import Path

import pandas as pd

ID = re.compile(r"porec_[0-9]{6}")
SERIAL = re.compile(r"PO-([0-9]+)")
SYSTEM = {"po_id", "serial_number", "date_uploaded"}
DERIVED = {"deal_id", "budb_id", "contact_group_id", "combined_counties"}
PROTECTED = {"prod_id", "source_url"}
MATCH_INPUT = {
    "lessor_owner", "first_name", "last_name", "address", "city", "state",
    "add_address1", "add_address1_city", "add_address1_state",
    "target_county", "target_state",
}


def quote(name):
    return '"' + name.replace('"', '""') + '"'


def validate_po_baseline(path):
    with sqlite3.connect(Path(path).resolve().as_uri() + "?mode=ro", uri=True) as conn:
        columns = list(conn.execute("PRAGMA table_info(pooling_order)"))
        types = {r[1]: r[2].upper() for r in columns}
        if [r[1] for r in columns if r[5]] != ["po_id"] or types.get("po_id") != "TEXT":
            raise ValueError("Expected pooling_order.po_id TEXT PRIMARY KEY.")
        missing = (SYSTEM | PROTECTED | {"deal_id", "budb_id"}) - types.keys()
        if missing:
            raise ValueError(f"Baseline columns missing: {sorted(missing)}")
        if not conn.execute("SELECT 1 FROM pooling_order LIMIT 1").fetchone():
            raise ValueError("The PO baseline is empty.")


def allocate(conn, df, tz):
    """Plan identities from this DB before matching, keeping physical Sheet indexes."""
    info = list(conn.execute("PRAGMA table_info(pooling_order)"))
    columns = [r[1] for r in info]
    conn.row_factory = sqlite3.Row
    stored = {r["po_id"]: dict(r) for r in conn.execute("SELECT * FROM pooling_order")}
    if any(not ID.fullmatch(key or "") for key in stored):
        raise ValueError("Malformed po_id in the active SQLite baseline.")
    incoming = df["po_id"].fillna("").astype(str).str.strip()
    counts = Counter(incoming)
    errors = []
    for idx, key in incoming.items():
        if key:
            if counts[key] > 1:
                reason = "duplicate (coordinate with Jennifer Cariño)"
            elif not ID.fullmatch(key):
                reason = "malformed"
            elif key not in stored:
                reason = "absent from SQLite"
            else:
                continue
            errors.append(f"Sheet row {idx + 2}: {key!r} — {reason}")
    if errors:
        raise ValueError("\n".join(errors))

    id_max = max((int(k[6:]) for k in stored), default=0)
    sn_max = max((int(m[1]) for row in stored.values()
                  if (m := SERIAL.fullmatch(row["serial_number"] or ""))), default=2576)
    sn_max = max(sn_max, 2576)
    conn.execute("""CREATE TABLE IF NOT EXISTS pooling_order_sequences (
        singleton INTEGER PRIMARY KEY CHECK(singleton = 1),
        last_id INTEGER NOT NULL, last_serial INTEGER NOT NULL
    )""")
    saved = conn.execute("SELECT last_id, last_serial FROM pooling_order_sequences WHERE singleton=1").fetchone()
    if saved:
        id_max = max(id_max, saved[0])
        sn_max = max(sn_max, saved[1])
    else:
        history = conn.execute("""SELECT po_id, field, old_value, new_value
                                  FROM pooling_order_action_log WHERE po_id IS NOT NULL""")
        for key, field, old, new in history:
            if ID.fullmatch(key or ""):
                id_max = max(id_max, int(key[6:]))
            if field not in {"serial_number", "all_fields"}:
                continue
            for value in (old, new):
                if value is None:
                    continue
                if field == "all_fields":
                    value = json.loads(value).get("serial_number")
                serial = SERIAL.fullmatch(str(value or "").strip())
                if serial:
                    sn_max = max(sn_max, int(serial[1]))
    today = datetime.now(tz).date().isoformat()
    frame = df.astype(object).where(df.notna(), None)
    for c in MATCH_INPUT - set(frame.columns):
        frame[c] = pd.Series([stored.get(key, {}).get(c) for key in incoming], index=df.index, dtype=object)
    for idx, key in incoming.items():
        if key:
            serial, uploaded = stored[key]["serial_number"], stored[key]["date_uploaded"]
        else:
            id_max += 1
            sn_max += 1
            if id_max > 999999:
                raise ValueError("PO ID exceeds the six-digit format.")
            key, serial, uploaded = f"porec_{id_max:06d}", f"PO-{sn_max}", today
        for col, value in zip(("po_id", "serial_number", "date_uploaded"), (key, serial, uploaded)):
            frame.at[idx, col] = value
    conn.execute("INSERT OR REPLACE INTO pooling_order_sequences VALUES (1,?,?)", (id_max, sn_max))
    return frame, stored, columns


def synchronize(conn, df, stored, columns, sheet_business):
    if df.empty:
        raise ValueError("No importable PO rows.")
    writable = sorted(set(sheet_business) | DERIVED)
    for col in writable:
        if col not in columns:
            conn.execute(f"ALTER TABLE pooling_order ADD COLUMN {quote(col)} TEXT")
    fields = sorted(SYSTEM) + writable
    omitted = [c for c in columns if c not in fields]
    names = ",".join(map(quote, fields + omitted))
    marks = ",".join(["?"] * len(fields) + ["NULL"] * len(omitted))
    updates = ",".join(f"{quote(c)}=?" for c in writable)
    result = dict(inserted=0, updated=0, unchanged=0, deleted=0, writeback_rows=[])
    absent = stored.keys() - set(df["po_id"])
    conn.executemany("DELETE FROM pooling_order WHERE po_id=?", ((k,) for k in absent))
    result["deleted"] = len(absent)
    for idx, values in zip(df.index, df[fields].itertuples(index=False, name=None)):
        row = {k: None if pd.isna(v) else str(v) for k, v in zip(fields, values)}
        key = row["po_id"]
        old = stored.get(key)
        if old is None:
            conn.execute(f"INSERT INTO pooling_order ({names}) VALUES ({marks})", tuple(row.values()))
            result["inserted"] += 1
        elif any(row[c] != old.get(c) for c in writable):
            conn.execute(f"UPDATE pooling_order SET {updates} WHERE po_id=?",
                         [row[c] for c in writable] + [key])
            result["updated"] += 1
        else:
            result["unchanged"] += 1
        result["writeback_rows"].append(dict(sheet_row=int(idx) + 2, po_id=key,
            serial_number=row["serial_number"] or "", date_uploaded=row["date_uploaded"] or ""))
    return result


def append_new_rows(conn, df, columns, writable):
    """Insert every row in `df` as a brand-new pooling_order row.

    Unlike synchronize(), this never updates or deletes existing rows — it is
    only safe to call with a dataframe whose po_id values were all freshly
    allocated (see allocate()), e.g. local-file imports.
    """
    if df.empty:
        return 0

    writable = sorted(writable)
    for col in writable:
        if col not in columns:
            conn.execute(f"ALTER TABLE pooling_order ADD COLUMN {quote(col)} TEXT")

    fields = sorted(SYSTEM) + writable
    omitted = [c for c in columns if c not in fields]
    names = ",".join(map(quote, fields + omitted))
    marks = ",".join(["?"] * len(fields) + ["NULL"] * len(omitted))

    rows = (
        tuple(None if pd.isna(v) else str(v) for v in values)
        for values in df[fields].itertuples(index=False, name=None)
    )
    conn.executemany(f"INSERT INTO pooling_order ({names}) VALUES ({marks})", rows)
    return len(df)


def combined_counties(county, state):
    if pd.isna(county) or pd.isna(state):
        return None
    state = str(state).strip().upper()
    if not state:
        return None
    counties = (part.strip().upper() for part in re.split(r"[,|]", str(county)))
    pairs = dict.fromkeys(f"{name}, {state}" for name in counties if name)
    return " | ".join(pairs) or None
