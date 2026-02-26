# exploration_ggsheet.py

import json
import gspread
from oauth2client.service_account import ServiceAccountCredentials

SERVICE_ACCOUNT_FILE = "yoluko-frontdesk-3d208271a3c0.json"
SHEET_URL = "https://docs.google.com/spreadsheets/d/1y9zzp1J1Fn60UKYN0RkTsSQcHcMb1mi2cD4NH8OfAF4/edit"

# ── Auth ─────────────────────────────────────────────────────────────────────
scope = [
    "https://spreadsheets.google.com/feeds",
    "https://www.googleapis.com/auth/drive",
]
creds = ServiceAccountCredentials.from_json_keyfile_name(SERVICE_ACCOUNT_FILE, scope)
client = gspread.authorize(creds)

sheet_key = SHEET_URL.split("/d/")[1].split("/")[0]
spreadsheet = client.open_by_key(sheet_key)

# ── Per-tab breakdown ─────────────────────────────────────────────────────────
GOOGLE_SHEETS_CELL_LIMIT = 10_000_000

print(f"\n{'='*65}")
print(f"📊  Spreadsheet: {spreadsheet.title}")
print(f"{'='*65}")
print(f"{'Tab':<35} {'Rows':>8} {'Cols':>6} {'Cells':>12} {'Data Rows':>10}")
print(f"{'-'*65}")

total_allocated = 0
total_data_rows = 0

for ws in spreadsheet.worksheets():
    rows      = ws.row_count
    cols      = ws.col_count
    cells     = rows * cols
    # Actual data rows (non-empty) — one API call per sheet
    data_rows = len(ws.get_all_values()) - 1  # subtract header
    data_rows = max(data_rows, 0)

    total_allocated += cells
    total_data_rows += data_rows

    print(f"{ws.title:<35} {rows:>8,} {cols:>6,} {cells:>12,} {data_rows:>10,}")

# ── Totals ────────────────────────────────────────────────────────────────────
usage_pct = total_allocated / GOOGLE_SHEETS_CELL_LIMIT * 100

print(f"{'='*65}")
print(f"{'TOTAL':<35} {'':>8} {'':>6} {total_allocated:>12,} {total_data_rows:>10,}")
print(f"\n📦  Allocated cells : {total_allocated:>12,}  /  {GOOGLE_SHEETS_CELL_LIMIT:,}")
print(f"📈  Usage           : {usage_pct:>11.2f}%")

if usage_pct > 80:
    print("⚠️   WARNING: Over 80% — consider archiving old tabs")
elif usage_pct > 50:
    print("🟡  Getting full — keep an eye on it")
else:
    print("✅  Plenty of room")

print(f"{'='*65}\n")