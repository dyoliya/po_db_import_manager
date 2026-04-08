# -------------------------ABOUT --------------------------
#
# Tool: Pooling Order DB Import Manager Tool
# Adapted from the user's NCOP importer use case
#
# ---------------------------------------------------------

import os
import re
import sys
import sqlite3
import shutil
import threading
from pathlib import Path
from datetime import datetime
from zoneinfo import ZoneInfo
from tkinter import messagebox

import pandas as pd
import customtkinter as ctk
from openpyxl import load_workbook
from customtkinter import filedialog

from pooling_order_budb import load_budb_lookup_maps, populate_budb_ids_in_df
from pooling_order_prod import load_prod_lookup_maps, populate_prod_ids_in_df
from pooling_order_pipedrive import load_pipedrive_lookup_maps, populate_deal_ids_in_df

CENTRAL_TZ = ZoneInfo("America/Chicago")

MYSQL_CONFIG = {
    "host": "communitymineralsproduction-instance-1.cwonazo2qluf.us-east-2.rds.amazonaws.com",
    "user": "admin",
    "password": "cmpass2024",
    "database": "prod_community_minerals",
    "port": 3306,
}

# =========================================================
# DB / FILE HELPERS
# =========================================================

def get_app_base_dir() -> Path:
    if getattr(sys, "frozen", False):
        return Path(sys.executable).resolve().parent
    return Path(__file__).resolve().parent


def get_daily_db_path(tool_prefix: str = "pooling_order"):
    """
    Same daily DB behavior as the source tool:
    - Active DB lives in database/: YYYY-MM-DD-<tool_prefix>.db
    - If today's DB does not exist, copy the latest DB from database/ ONLY
    - Archive older DBs into database/previous_versions/
    """
    base_dir = get_app_base_dir()
    db_dir = base_dir / "database"
    prev_dir = db_dir / "previous_versions"
    db_dir.mkdir(parents=True, exist_ok=True)
    prev_dir.mkdir(parents=True, exist_ok=True)

    today_str = datetime.now(CENTRAL_TZ).strftime("%Y-%m-%d")
    today_name = f"{today_str}-{tool_prefix}.db"
    today_path = db_dir / today_name

    if today_path.exists():
        for p in db_dir.glob("*.db"):
            if p.is_file() and p.name != today_name:
                p.rename(prev_dir / p.name)
        return str(today_path), None, "existing"

    pat = re.compile(rf"^(\d{{4}}-\d{{2}}-\d{{2}})-{re.escape(tool_prefix)}\.db$")
    candidates = []
    for p in db_dir.glob("*.db"):
        m = pat.match(p.name)
        if m:
            candidates.append((m.group(1), p))

    latest_path = sorted(candidates, key=lambda x: x[0])[-1][1] if candidates else None

    if latest_path and latest_path.exists():
        shutil.copy2(latest_path, today_path)
        copied_from = latest_path.name

        for p in db_dir.glob("*.db"):
            if p.is_file() and p.name != today_name:
                p.rename(prev_dir / p.name)

        return str(today_path), copied_from, "copied"

    for p in db_dir.glob("*.db"):
        if p.is_file() and p.name != today_name:
            p.rename(prev_dir / p.name)

    return str(today_path), None, "new"


def connect_sqlite(db_path: str) -> sqlite3.Connection:
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA synchronous=NORMAL;")
    return conn


def table_exists(conn: sqlite3.Connection, table_name: str) -> bool:
    cur = conn.cursor()
    cur.execute(
        "SELECT name FROM sqlite_master WHERE type='table' AND name=?",
        (table_name,)
    )
    return cur.fetchone() is not None


def get_existing_columns(conn: sqlite3.Connection, table_name: str) -> list[str]:
    if not table_exists(conn, table_name):
        return []
    cur = conn.cursor()
    cur.execute(f"PRAGMA table_info({table_name})")
    return [row[1] for row in cur.fetchall()]


def ensure_pooling_order_table(conn: sqlite3.Connection, table_name: str):
    conn.execute(f"""
        CREATE TABLE IF NOT EXISTS {table_name} (
            po_id TEXT PRIMARY KEY,
            date_uploaded TEXT,
            source_url TEXT,
            lessor_owner TEXT,
            first_name TEXT,
            middle_name TEXT,
            last_name TEXT,
            address TEXT,
            city TEXT,
            state TEXT,
            postal_code TEXT,
            target_county TEXT,
            target_state TEXT,
            serial_number TEXT,
            budb_id TEXT,
            prod_id TEXT,
            deal_id TEXT
        )
    """)
    conn.commit()


def get_expected_columns_for_import() -> set[str]:
    return {
        "source_url",
        "lessor_owner",
        "first_name",
        "middle_name",
        "last_name",
        "address",
        "city",
        "state",
        "postal_code",
        "target_county",
        "target_state",
    }


def normalize_header(col: str) -> str:
    s = str(col).strip()
    s = s.replace("/", " ")
    s = re.sub(r"[\-\–]+", " ", s)
    s = re.sub(r"[_]+", " ", s)
    s = re.sub(r"\s+", " ", s).strip().lower()

    mapping = {
        "lessor owner": "lessor_owner",
        "lessors": "lessor_owner",
        "owner standardized": "lessor_owner",

        "first name": "first_name",
        "middle name": "middle_name",
        "last name": "last_name",
        "postal code": "postal_code",
        "target county": "target_county",
        "target state": "target_state",
        "source url": "source_url",
        "serial number": "serial_number",
    }

    if s in mapping:
        return mapping[s]

    return s.replace(" ", "_")

def refresh_existing_pipedrive_matches(
    conn: sqlite3.Connection,
    table_name: str,
    lookup_maps_pd,
    progress_callback=None,
):
    """
    Re-check existing pooling_order rows against the latest Pipedrive lookup maps.

    Rules:
    - blank old -> new match = update
    - old match -> different new match = update
    - old match -> no match now = keep old value
    """
    df_existing = pd.read_sql_query(f"""
        SELECT
            po_id,
            lessor_owner,
            first_name,
            middle_name,
            last_name,
            address,
            city,
            state,
            postal_code,
            target_county,
            target_state,
            deal_id
        FROM {table_name}
    """, conn)

    if df_existing.empty:
        return {
            "rows_checked": 0,
            "rows_changed": 0,
            "newly_matched": 0,
            "cleared": 0,
        }

    # Run the same matching logic used for new rows
    df_matched = populate_deal_ids_in_df(
        df_existing.drop(columns=["deal_id"], errors="ignore"),
        lookup_maps_pd,
        progress_callback=progress_callback,
    )

    # keep old + new side by side
    df_compare = df_existing[["po_id", "deal_id"]].rename(columns={"deal_id": "old_deal_id"}).merge(
        df_matched[["po_id", "deal_id"]].rename(columns={"deal_id": "new_deal_id"}),
        on="po_id",
        how="left"
    )

    def norm(v):
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.lower() in {"", "nan", "none", "null"} else s

    df_compare["old_norm"] = df_compare["old_deal_id"].map(norm)
    df_compare["new_norm"] = df_compare["new_deal_id"].map(norm)

    changed = df_compare[
        (df_compare["new_norm"] != "") &
        (df_compare["old_norm"] != df_compare["new_norm"])
    ].copy()

    if changed.empty:
        return {
            "rows_checked": len(df_compare),
            "rows_changed": 0,
            "newly_matched": 0,
            "cleared": 0,
        }

    updates = [
        (row.new_norm if row.new_norm else None, row.po_id)
        for row in changed.itertuples(index=False)
    ]

    conn.executemany(
        f"UPDATE {table_name} SET deal_id = ? WHERE po_id = ?",
        updates
    )
    conn.commit()

    newly_matched = ((changed["old_norm"] == "") & (changed["new_norm"] != "")).sum()
    cleared = ((changed["old_norm"] != "") & (changed["new_norm"] == "")).sum()

    return {
        "rows_checked": len(df_compare),
        "rows_changed": len(changed),
        "newly_matched": int(newly_matched),
        "cleared": int(cleared),
    }

def refresh_existing_budb_matches(
    conn: sqlite3.Connection,
    table_name: str,
    lookup_maps_budb,
    progress_callback=None,
):
    """
    Re-check existing pooling_order rows against the latest BUDB lookup maps.

    Rules:
    - blank old -> new match = update
    - old match -> different new match = update
    - old match -> no match now = keep old value
    """
    df_existing = pd.read_sql_query(f"""
        SELECT
            po_id,
            lessor_owner,
            first_name,
            middle_name,
            last_name,
            address,
            city,
            state,
            postal_code,
            target_county,
            target_state,
            budb_id
        FROM {table_name}
    """, conn)

    if df_existing.empty:
        return {
            "rows_checked": 0,
            "rows_changed": 0,
            "newly_matched": 0,
            "changed_match": 0,
            "kept_old_when_no_new_match": 0,
        }

    # Re-run BUDB matching on existing rows
    df_matched = populate_budb_ids_in_df(
        df_existing.drop(columns=["budb_id"], errors="ignore"),
        lookup_maps_budb,
        progress_callback=progress_callback,
    )

    df_compare = df_existing[["po_id", "budb_id"]].rename(
        columns={"budb_id": "old_budb_id"}
    ).merge(
        df_matched[["po_id", "budb_id"]].rename(columns={"budb_id": "new_budb_id"}),
        on="po_id",
        how="left"
    )

    def norm(v):
        if v is None:
            return ""
        s = str(v).strip()
        return "" if s.lower() in {"", "nan", "none", "null"} else s

    df_compare["old_norm"] = df_compare["old_budb_id"].map(norm)
    df_compare["new_norm"] = df_compare["new_budb_id"].map(norm)

    # Your requested behavior:
    # update only if there is a NEW non-blank match and it differs from old
    changed = df_compare[
        (df_compare["new_norm"] != "") &
        (df_compare["old_norm"] != df_compare["new_norm"])
    ].copy()

    kept_old_when_no_new_match = int(
        ((df_compare["old_norm"] != "") & (df_compare["new_norm"] == "")).sum()
    )

    if changed.empty:
        return {
            "rows_checked": len(df_compare),
            "rows_changed": 0,
            "newly_matched": 0,
            "changed_match": 0,
            "kept_old_when_no_new_match": kept_old_when_no_new_match,
        }

    updates = [
        (row.new_norm, row.po_id)
        for row in changed.itertuples(index=False)
    ]

    conn.executemany(
        f"UPDATE {table_name} SET budb_id = ? WHERE po_id = ?",
        updates
    )
    conn.commit()

    newly_matched = int(((changed["old_norm"] == "") & (changed["new_norm"] != "")).sum())
    changed_match = int(((changed["old_norm"] != "") & (changed["new_norm"] != "")).sum())

    return {
        "rows_checked": len(df_compare),
        "rows_changed": len(changed),
        "newly_matched": newly_matched,
        "changed_match": changed_match,
        "kept_old_when_no_new_match": kept_old_when_no_new_match,
    }

def read_input_file(path: str, sheet_name: str | None = None) -> pd.DataFrame:
    ext = os.path.splitext(path)[1].lower()

    if ext == ".csv":
        return pd.read_csv(path, dtype=str, keep_default_na=False, na_filter=False).fillna("")

    if ext not in {".xlsx", ".xlsm", ".xls"}:
        raise ValueError("Unsupported file type. Please select a .csv or .xlsx file.")

    xls = pd.ExcelFile(path)
    sheet = sheet_name or xls.sheet_names[0]
    df = pd.read_excel(
        xls,
        sheet_name=sheet,
        dtype=str,
        keep_default_na=False,
        na_filter=False,
    ).fillna("")

    return df


def clean_text(value):
    if value is None:
        return ""
    text = str(value).strip()
    if text.lower() in {"nan", "none", "null"}:
        return ""
    return text


def prepare_pooling_order_df(df_raw: pd.DataFrame):
    original_cols = list(df_raw.columns)
    sanitized_cols = [normalize_header(c) for c in original_cols]
    df = df_raw.copy()
    df.columns = sanitized_cols

    # reject duplicate columns after normalization
    dupes = pd.Series(df.columns)
    duplicate_cols = dupes[dupes.duplicated()].tolist()
    if duplicate_cols:
        raise ValueError(
            "Duplicate columns found after header normalization:\n  - "
            + "\n  - ".join(sorted(set(duplicate_cols)))
        )

    expected = get_expected_columns_for_import()
    incoming = set(df.columns)

    optional_cols = {"serial_number"}

    missing = sorted(expected - incoming)
    extra = sorted(incoming - expected - optional_cols)

    # ❌ Still STRICT on missing columns
    if missing:
        mapping_info = "\n".join(
            f"  - {orig} -> {san}"
            for orig, san in zip(original_cols, sanitized_cols)
        )

        msg_lines = ["Schema mismatch: missing required columns."]
        msg_lines.append("\nMissing required columns:")
        msg_lines.extend([f"  - {c}" for c in missing])
        msg_lines.append("\nHeader mapping (original -> normalized):")
        msg_lines.append(mapping_info)

        raise ValueError("\n".join(msg_lines))

    # ⚠️ Log unexpected columns (do NOT stop processing)
    unexpected_cols = extra

    ordered_cols = [
        "source_url",
        "lessor_owner",
        "first_name",
        "middle_name",
        "last_name",
        "address",
        "city",
        "state",
        "postal_code",
        "target_county",
        "target_state",
    ]

    if "serial_number" in df.columns:
        ordered_cols.append("serial_number")

    df = df[ordered_cols].copy()

    for col in ordered_cols:
        df[col] = df[col].map(clean_text)

    if "serial_number" not in df.columns:
        df["serial_number"] = ""
    else:
        df["serial_number"] = df["serial_number"].map(clean_text)

    # Optional row cleanup: drop fully blank rows
    before = len(df)
    df = df.loc[~(df[ordered_cols].apply(lambda row: all(v == "" for v in row), axis=1))].copy()
    dropped_blank = before - len(df)

    return df, original_cols, sanitized_cols, dropped_blank, unexpected_cols


def get_next_po_sequence(conn: sqlite3.Connection, table_name: str, start_at: int = 1) -> int:
    cur = conn.cursor()
    cur.execute(f"""
        SELECT MAX(CAST(SUBSTR(po_id, 7) AS INTEGER))
        FROM {table_name}
        WHERE po_id IS NOT NULL
          AND po_id GLOB 'porec_*'
    """)
    row = cur.fetchone()

    if not row or row[0] is None:
        return start_at

    return max(int(row[0]) + 1, start_at)


def insert_rows_pooling_order(
    conn: sqlite3.Connection,
    table_name: str,
    df: pd.DataFrame,
    progress_callback=None,
    tz=CENTRAL_TZ,
):
    if df.empty:
        return 0

    date_uploaded = datetime.now(tz).strftime("%Y-%m-%d")
    next_seq = get_next_po_sequence(conn, table_name)
    next_serial_seq = get_next_serial_sequence(conn, table_name, start_at=2577)

    records = []
    total = len(df)

    for idx, row in enumerate(df.itertuples(index=False), start=1):
        po_id = f"porec_{next_seq:06d}"
        next_seq += 1

        input_serial = getattr(row, "serial_number", None)
        input_serial = "" if input_serial is None else str(input_serial).strip()

        if input_serial:
            serial_number = input_serial
        else:
            serial_number = f"PO-{next_serial_seq}"
            next_serial_seq += 1

        records.append((
            po_id,
            date_uploaded,
            row.source_url,
            row.lessor_owner,
            row.first_name,
            row.middle_name,
            row.last_name,
            row.address,
            row.city,
            row.state,
            row.postal_code,
            row.target_county,
            row.target_state,
            serial_number,
            getattr(row, "budb_id", None),
            getattr(row, "prod_id", None),
            getattr(row, "deal_id", None),
        ))

        if progress_callback:
            progress_callback(idx / total)

    conn.executemany(f"""
        INSERT INTO {table_name} (
            po_id,
            date_uploaded,
            source_url,
            lessor_owner,
            first_name,
            middle_name,
            last_name,
            address,
            city,
            state,
            postal_code,
            target_county,
            target_state,
            serial_number,
            budb_id,
            prod_id,
            deal_id
        )
        VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
    """, records)
    conn.commit()
    return len(records)

def get_next_serial_sequence(conn: sqlite3.Connection, table_name: str, start_at: int = 2577) -> int:
    cur = conn.cursor()
    cur.execute(f"""
        SELECT MAX(CAST(SUBSTR(serial_number, 4) AS INTEGER))
        FROM {table_name}
        WHERE serial_number IS NOT NULL
          AND TRIM(serial_number) <> ''
          AND serial_number GLOB 'PO-*'
    """)
    row = cur.fetchone()

    if not row or row[0] is None:
        return start_at

    return max(int(row[0]) + 1, start_at)

def count_nonblank(series):
    return series.fillna("").astype(str).str.strip().ne("").sum()
# =========================================================
# UI
# =========================================================

class PoolingOrderImporterApp(ctk.CTk):
    def __init__(self):
        super().__init__()

        self.PANEL_BG = "#273946"
        self.APP_BG = "#fff6de"
        self.ACCENT = "#CB1F47"
        self.ACCENT_HOVER = "#ffab4c"
        self.TEXT_DARK = "#273946"

        self.title("Pooling Orders: DB Import Manager [v1.0.0]")
        self.geometry("430x720")
        self.resizable(False, True)
        self.minsize(430, 650)

        ctk.set_appearance_mode("dark")
        ctk.set_default_color_theme("dark-blue")
        self.configure(fg_color=self.APP_BG)

        self.main_frame = ctk.CTkFrame(self, fg_color=self.APP_BG, corner_radius=12)
        self.main_frame.pack(fill="both", expand=True, padx=10, pady=10)

        self.title_label = ctk.CTkLabel(
            self.main_frame,
            text="Pooling Order DB Import Manager Tool",
            font=ctk.CTkFont(size=20, weight="bold"),
            text_color=self.TEXT_DARK
        )
        self.title_label.pack(pady=(12, 6))

        input_tab = self._create_locked_tab_section(title="I m p o r t", height=200)
        self._setup_import_tab(input_tab)

        self.progress = ctk.CTkProgressBar(
            self.main_frame,
            width=390,
            fg_color=self.PANEL_BG,
            progress_color=self.ACCENT
        )
        self.progress.set(0)
        self.progress.pack(pady=10)

        self.log_container = ctk.CTkFrame(self.main_frame, fg_color="transparent")
        self.log_container.pack(fill="both", expand=True, padx=10, pady=(0, 10))
        self.log_container.grid_rowconfigure(1, weight=1)
        self.log_container.grid_columnconfigure(0, weight=1)

        ctk.CTkLabel(
            self.log_container,
            text="Activity Log",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color=self.TEXT_DARK
        ).grid(row=0, column=0, sticky="w", padx=10, pady=(0, 4))

        self.log_box = ctk.CTkTextbox(self.log_container, fg_color="#ffffff", text_color=self.TEXT_DARK)
        self.log_box.grid(row=1, column=0, sticky="nsew", padx=0, pady=0)
        self.log_box.configure(state="disabled")

        self.input_paths = []

    def _create_locked_tab_section(self, title: str, height: int):
        tab_font = ctk.CTkFont(size=12, weight="bold")
        tv = ctk.CTkTabview(self.main_frame, width=390, height=height)
        tv.configure(
            fg_color=self.PANEL_BG,
            segmented_button_fg_color=self.APP_BG,
            segmented_button_selected_color=self.PANEL_BG,
            segmented_button_selected_hover_color=self.PANEL_BG,
            segmented_button_unselected_color=self.PANEL_BG,
            text_color=self.ACCENT_HOVER,
            text_color_disabled=self.ACCENT_HOVER
        )
        tv.pack(fill="x", padx=10, pady=(10, 8), anchor="w")
        tv.configure(anchor="w")

        tab = tv.add(title)

        try:
            tv._segmented_button.grid_configure(sticky="w")
            btn = tv._segmented_button._buttons_dict[title]
            btn.configure(width=140, height=35)
            tv._segmented_button.configure(state="disabled", font=tab_font)
            for b in tv._segmented_button._buttons_dict.values():
                b.configure(state="disabled")
        except Exception:
            pass

        return tab

    def _setup_import_tab(self, tab):
        label_w = 130

        row1 = ctk.CTkFrame(tab, fg_color="transparent")
        row1.pack(fill="x", padx=10, pady=(10, 6), anchor="w")

        ctk.CTkLabel(
            row1,
            text="Select Files:",
            width=label_w,
            anchor="w",
            text_color=self.APP_BG
        ).pack(side="left")

        self.file_label = ctk.CTkLabel(row1, text="(none)", anchor="w", text_color=self.APP_BG)
        self.file_label.pack(side="left", padx=(0, 8), fill="x", expand=True)

        self.pick_file_btn = ctk.CTkButton(
            row1,
            text="Browse",
            width=80,
            fg_color=self.ACCENT,
            hover_color=self.ACCENT_HOVER,
            command=self.pick_input_file
        )
        self.pick_file_btn.pack(side="right")

        panel = ctk.CTkFrame(tab, fg_color="#1e2b34", corner_radius=10)
        panel.pack(fill="x", expand=False, padx=10, pady=(6, 8))

        panel_header = ctk.CTkFrame(panel, fg_color="transparent")
        panel_header.pack(fill="x", padx=10, pady=(8, 4))

        ctk.CTkLabel(
            panel_header,
            text="Selected files",
            font=ctk.CTkFont(size=12, weight="bold"),
            text_color="#ffffff"
        ).pack(side="left")

        self.clear_btn = ctk.CTkButton(
            panel_header,
            text="Clear",
            width=70,
            fg_color=self.PANEL_BG,
            hover_color="#334957",
            command=self.clear_selected_files
        )
        self.clear_btn.pack(side="right")

        list_wrap = ctk.CTkFrame(panel, fg_color="#1e2b34", height=80, corner_radius=10)
        list_wrap.pack(fill="x", expand=False, padx=10, pady=(0, 10))
        list_wrap.pack_propagate(False)

        self.files_list = ctk.CTkScrollableFrame(
            list_wrap,
            fg_color="#1e2b34",
            scrollbar_fg_color="#273946",
            scrollbar_button_color=self.ACCENT,
            scrollbar_button_hover_color=self.ACCENT_HOVER,
        )
        self.files_list.pack(fill="both", expand=True)

        self.import_btn = ctk.CTkButton(
            tab,
            text="Import & Append to DB",
            fg_color=self.ACCENT,
            hover_color=self.ACCENT_HOVER,
            command=self.start_import
        )
        self.import_btn.pack(pady=(6, 12), padx=10)

    def clear_selected_files(self):
        self.input_paths = []
        self.file_label.configure(text="(none)")
        self._refresh_files_list()
        self._log("[INFO] Cleared selected files.")

    def _refresh_files_list(self):
        for child in self.files_list.winfo_children():
            child.destroy()

        if not self.input_paths:
            ctk.CTkLabel(
                self.files_list,
                text="No files selected",
                text_color="#cbd5e1"
            ).pack(anchor="w", pady=6)
            return

        for p in self.input_paths:
            row = ctk.CTkFrame(self.files_list, fg_color="transparent")
            row.pack(fill="x", pady=3)

            ctk.CTkLabel(
                row,
                text=os.path.basename(p),
                text_color="#ffffff",
                anchor="w"
            ).pack(side="left", fill="x", expand=True)

            ctk.CTkLabel(
                row,
                text=os.path.dirname(p),
                text_color="#94a3b8",
                anchor="e"
            ).pack(side="right")

    def _log(self, text: str):
        def _append():
            self.log_box.configure(state="normal")
            self.log_box.insert("end", text + "\n")
            self.log_box.see("end")
            self.log_box.configure(state="disabled")
        self.after(0, _append)

    def _divider(self):
        self._log("- - - - - - - - - - - - - - - - - - - - - - - - - - -")

    def progress_callback(self, fraction, msg=None):
        self.progress.set(max(0.0, min(1.0, float(fraction))))
        if msg:
            self._log(msg)
        self.update_idletasks()

    def _ui_error(self, title: str, msg: str):
        self.after(0, lambda m=msg: messagebox.showerror(title, m))

    def pick_input_file(self):
        paths = filedialog.askopenfilenames(
            title="Select input file(s)",
            filetypes=[("CSV or Excel", "*.csv *.xlsx *.xlsm *.xls"), ("All files", "*.*")]
        )
        if not paths:
            return

        self.input_paths = list(paths)
        self.file_label.configure(text=f"{len(self.input_paths)} file(s) selected")
        self._refresh_files_list()

        self._log("[INFO] Selected inputs:")
        for p in self.input_paths:
            self._log(f"  - {p}")
        self._log("")

    def start_import(self):
        self._divider()

        if not self.input_paths:
            messagebox.showwarning("Missing input", "Please select one or more CSV/XLSX files.")
            return

        db_path, copied_from, mode = get_daily_db_path("pooling_order")
        self._log(f"[DB] Using daily DB: {db_path}")

        if mode == "copied":
            self._log(f"[DB] Copied base DB: {copied_from}")
        elif mode == "existing":
            self._log("[DB] Today's DB already exists. Continuing append..")
        else:
            self._log("[DB] No base DB found. Starting a NEW DB for today.")

        self.import_btn.configure(state="disabled")
        self.pick_file_btn.configure(state="disabled")
        self.progress_callback(0, "Initializing import workflow...")

        threading.Thread(
            target=self._import_worker,
            args=(db_path, self.input_paths, "pooling_order"),
            daemon=True
        ).start()



    def _import_worker(self, db_path, input_paths, table_name):
        try:
            conn = connect_sqlite(db_path)
            try:
                ensure_pooling_order_table(conn, table_name)

                total_files = len(input_paths)
                total_inserted = 0

                self.progress_callback(0, "[SETUP] Preparing lookup sources...")

                self._log("[SETUP] Loading BUDB lookup maps before processing input files...")
                lookup_maps_budb, budb_meta = load_budb_lookup_maps(get_app_base_dir())
                self._log(f"[BUDB] Using: {budb_meta['budb_path']}")
                self._log(f"[BUDB] Table: {budb_meta['table_name']}")
                self._log(f"[BUDB] Rows loaded: {budb_meta['rows_loaded']:,}")
                self._log(f"[BUDB] Cache mode: {budb_meta.get('cache_mode', 'n/a')}")
                self._log(f"[BUDB] Cache path: {budb_meta.get('cache_path', 'n/a')}")

                self.progress_callback(0, "[SETUP] Refreshing existing DB rows against latest BUDB data.")
                budb_refresh_stats = refresh_existing_budb_matches(
                    conn,
                    table_name,
                    lookup_maps_budb,
                    progress_callback=lambda frac: self.progress_callback(frac)
                )
                self._log(
                    f"[BUDB][EXISTING] Checked: {budb_refresh_stats['rows_checked']:,} | "
                    f"Changed: {budb_refresh_stats['rows_changed']:,} | "
                    f"Newly matched: {budb_refresh_stats['newly_matched']:,} | "
                    f"Changed match: {budb_refresh_stats['changed_match']:,} | "
                    f"Kept old when no new match: {budb_refresh_stats['kept_old_when_no_new_match']:,}"
                )

                self.progress_callback(0, "[SETUP] Loading PROD lookup maps before processing input files...")
                prod_lookup_maps, prod_meta = load_prod_lookup_maps(
                    MYSQL_CONFIG,
                    get_app_base_dir(),
                    force_rebuild=False
                )
                self._log(f"[PROD] Source: {prod_meta.get('source', 'mysql')}")
                self._log(f"[PROD] Rows loaded: {int(prod_meta.get('rows_loaded', 0)):,}")
                self._log(f"[PROD] Cache mode: {prod_meta.get('cache_mode', 'n/a')}")
                self._log(f"[PROD] Cache path: {prod_meta.get('cache_path', 'n/a')}")

                self._log("[SETUP] Loading Pipedrive lookup maps from local CSV cache...")
                lookup_maps_pd, pd_meta = load_pipedrive_lookup_maps(get_app_base_dir())
                self._log(f"[PIPEDRIVE] Source: {pd_meta.get('source', 'n/a')}")
                self._log(f"[PIPEDRIVE] Rows loaded: {int(pd_meta.get('rows_loaded', 0)):,}")
                self._log(f"[PIPEDRIVE] Cache mode: {pd_meta.get('cache_mode', 'n/a')}")
                self._log(f"[PIPEDRIVE] Cache path: {pd_meta.get('cache_path', 'n/a')}")

                self.progress_callback(0, "[SETUP] Refreshing existing DB rows against latest Pipedrive data.")
                refresh_stats = refresh_existing_pipedrive_matches(
                    conn,
                    table_name,
                    lookup_maps_pd,
                    progress_callback=lambda frac: self.progress_callback(frac)
                )
                self._log(
                    f"[PIPEDRIVE][EXISTING] Checked: {refresh_stats['rows_checked']:,} | "
                    f"Changed: {refresh_stats['rows_changed']:,} | "
                    f"Newly matched: {refresh_stats['newly_matched']:,} | "
                    f"Cleared: {refresh_stats['cleared']:,}"
                )

                for idx, input_path in enumerate(input_paths, start=1):
                    filename = os.path.basename(input_path)
                    self.progress_callback(0, f"[{idx}/{total_files}] Reading: {filename}")

                    df_raw = read_input_file(input_path)
                    if df_raw.empty:
                        self._log(f"[SKIP] {filename} has no rows.")
                        continue

                    self.progress_callback(0, f"[{idx}/{total_files}] Validating schema / cleaning...")
                    df, original_cols, sanitized_cols, dropped_blank, unexpected_cols = prepare_pooling_order_df(df_raw)

                    self._log(f"[{idx}/{total_files}] {filename}: {len(df):,} rows ready for BUDB matching")
                    self._log(f"[MAP] Columns: {len(original_cols)} original -> {len(sanitized_cols)} normalized")
                    if dropped_blank:
                        self._log(f"[CLEAN] Dropped fully blank rows: {dropped_blank:,}")
                    if unexpected_cols:
                        self._log(f"[WARN] Unexpected columns (ignored): {', '.join(unexpected_cols)}")

                    self.progress_callback(0, f"[{idx}/{total_files}] Matching BUDB IDs...")
                    df = populate_budb_ids_in_df(
                        df,
                        lookup_maps_budb,
                        progress_callback=lambda frac: self.progress_callback(frac)
                    )

                    budb_matched_count = count_nonblank(df["budb_id"]) if "budb_id" in df.columns else 0
                    self._log(f"[BUDB] Matched rows: {budb_matched_count:,} / {len(df):,}")

                    self.progress_callback(0, f"[{idx}/{total_files}] Matching PROD IDs...")
                    df = populate_prod_ids_in_df(
                        df,
                        prod_lookup_maps,
                        progress_callback=lambda frac: self.progress_callback(frac)
                    )

                    prod_matched_count = count_nonblank(df["prod_id"]) if "prod_id" in df.columns else 0
                    self._log(f"[PROD] Matched rows: {prod_matched_count:,} / {len(df):,}")
                    

                    self.progress_callback(0, f"[{idx}/{total_files}] Matching DEAL IDs...")
                    df = populate_deal_ids_in_df(
                        df,
                        lookup_maps_pd,
                        progress_callback=lambda frac: self.progress_callback(frac)
                    )
                    deal_matched = count_nonblank(df["deal_id"]) if "deal_id" in df.columns else 0
                    self._log(f"[PIPEDRIVE] Matched rows: {deal_matched:,} / {len(df):,}")
                    self._log(f"[{idx}/{total_files}] {filename}: ready for insert")

                    self.progress_callback(0, f"[{idx}/{total_files}] Inserting rows...")
                    inserted = insert_rows_pooling_order(
                        conn,
                        
                        table_name,
                        df,
                        progress_callback=lambda frac: self.progress_callback(frac)
                    )
                    total_inserted += inserted
                    self._log(f"[DONE] Inserted from {filename}: {inserted:,} row(s)")

                self._log(f"[DONE] Daily DB updated: {os.path.basename(db_path)} -> table '{table_name}'")
                self._log(f"[DONE] Total inserted this run: {total_inserted:,} row(s)")
                self.after(
                    0,
                    lambda: messagebox.showinfo(
                        "Success",
                        f"Imported into:\n{db_path}\n\nTable: {table_name}\nRows inserted: {total_inserted:,}"
                    )
                )

            finally:
                conn.close()

        except Exception as e:
            err_msg = str(e)
            self._log(f"[ERROR] {err_msg}")
            self._ui_error("Error", err_msg)

        finally:
            self.after(0, lambda: self.import_btn.configure(state="normal"))
            self.after(0, lambda: self.pick_file_btn.configure(state="normal"))
            self._divider()
            self.progress_callback(0, "Waiting for action...")


if __name__ == "__main__":
    app = PoolingOrderImporterApp()
    app.mainloop()
