"""Reconcile a PO writeback against its snapshot and committed SQLite database."""
import argparse
import json
import sqlite3
from pathlib import Path
from openpyxl.utils import get_column_letter


def sheet_cells(sheet, cell_text):
    cells = {}
    for block in sheet.get("data", []):
        for row_no, row in enumerate(block.get("rowData", []), block.get("startRow", 0) + 1):
            for col, cell in enumerate(row.get("values", []), block.get("startColumn", 0)):
                value = cell_text(cell, cells.get((1, col), ""), is_header=row_no == 1)
                if value != "":
                    cells[row_no, col] = value
    return cells


def reconcile(session, journal, api, before_post=None):
    book = journal["spreadsheet_id"]
    tab = journal["sheet_name"]
    snap = json.loads(Path(journal["snapshot_path"]).read_text(encoding="utf-8"))
    meta = snap["metadata"]
    if (meta["spreadsheet_id"], meta["tab"]) != (book, tab):
        raise ValueError("Snapshot belongs to a different spreadsheet or tab.")
    original = sheet_cells(snap["snapshot"]["sheets"][0], api.google_cell_text)
    headers = [original[1, c] for c in range(sum(r == 1 for r, c in original))]
    columns = api.get_google_writeback_columns([headers])
    targets = {}
    ids = set()
    for record in journal["rows"]:
        row = record["sheet_row"]
        if row < 2 or row > meta["source_row_count"] + 1 or record["po_id"] in ids or (row, columns["po_id"]) in targets:
            raise ValueError("Invalid row or duplicate identity in recovery mapping.")
        ids.add(record["po_id"])
        for field, col in columns.items():
            targets[row, col] = record[field]

    endpoint = f"https://sheets.googleapis.com/v4/spreadsheets/{book}"
    params = {"ranges": [f"'{tab}'"], "fields": (
        "sheets(properties(sheetId,title),data(startRow,startColumn,rowData(values("
        "formattedValue,effectiveValue,hyperlink,textFormatRuns,chipRuns,"
        "userEnteredFormat(textFormat(link)),effectiveFormat(numberFormat,textFormat(link))))))"
    )}

    def differences():
        sheet, = api.google_api_request(session, "GET", endpoint, params=params).json()["sheets"]
        if sheet["properties"]["sheetId"] != meta["sheet_id"] or sheet["properties"]["title"] != tab:
            raise ValueError("The original tab was replaced or renamed.")
        live = sheet_cells(sheet, api.google_cell_text)
        missing = []
        for key in sorted(original.keys() | live.keys() | targets.keys()):
            old, current = original.get(key, ""), live.get(key, "")
            if key in targets and current == targets[key]:
                continue
            row, col = key
            cell = f"'{tab}'!{get_column_letter(col + 1)}{row}"
            if current != old:
                raise ValueError(f"Snapshot conflict at {cell}. Inspect this cell and row before recovery.")
            if key in targets:
                missing.append({"range": cell, "values": [[targets[key]]]})
        return missing

    data = differences()
    print(f"Checked {len(ids):,} mapped rows. Cells still requiring writeback: {len(data):,}.")
    if before_post is None:
        return
    if data:
        before_post()
        response = session.post(endpoint + "/values:batchUpdate",
                                json={"valueInputOption": "RAW", "data": data}, timeout=(15, 120))
        response.raise_for_status()
        if differences():
            raise RuntimeError("Readback found outstanding cells. Keep the pending mapping.")


def main():
    import app
    from po_build_state import read_state, finish_build
    from po_run_lock import exclusive_run
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    base = Path(__file__).resolve().parent
    with exclusive_run(base):
        state = read_state(base)
        if not state or state["status"] != "recovery_required":
            raise ValueError("Expected a build waiting for writeback recovery.")
        pending = Path(state["pending_path"])
        archive = pending.with_name(f"{pending.stem}_{state['build_id']}_verified.json")
        journal = json.loads((pending if pending.is_file() else archive).read_text(encoding="utf-8"))
        db = Path(state["new_db"]).resolve()
        if journal["build_id"] != state["build_id"] or Path(journal["db_path"]).resolve() != db:
            raise ValueError("Mapping and database refer to different builds.")
        if not Path(state["old_db"]).is_file():
            raise ValueError("Pre-build snapshot is missing.")
        conn = sqlite3.connect(db.as_uri() + "?mode=ro", uri=True)
        try:
            actual = set(conn.execute("SELECT po_id, coalesce(serial_number,''), coalesce(date_uploaded,'') FROM pooling_order"))
        finally:
            conn.close()
        wanted = {(r["po_id"], r["serial_number"], r["date_uploaded"]) for r in journal["rows"]}
        if wanted != actual or not wanted:
            raise ValueError("Committed SQLite identities differ from the pending mapping.")
        with app.google_sheets_session() as session:
            reconcile(session, journal, app, (lambda: None) if args.apply else None)
        if args.apply:
            if pending.exists():
                pending.rename(archive)
            finish_build(base)
            print("Writeback verified. Open the builder and click Update Action Log.")


if __name__ == "__main__":
    main()
