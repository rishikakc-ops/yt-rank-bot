import os
import gspread
from datetime import datetime
from oauth2client.service_account import ServiceAccountCredentials

JSON_FILE = "service_account.json"
SHEET_NAME = "YT Final Bot"
RUN_ID = datetime.now().strftime("%Y-%m-%d")


# ------------------ AUTH ------------------
def authenticate_google_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, scope)
    return gspread.authorize(creds)


# ------------------ SUMMARY BUILDER ------------------
def build_daily_keyword_summary(spreadsheet, date_str):
    try:
        sheet = spreadsheet.worksheet("Wakefit_Daily_Ranks")
    except gspread.WorksheetNotFound:
        print("❌ Wakefit_Daily_Ranks not found.")
        return

    rows = sheet.get_all_values()
    if len(rows) <= 1:
        print("❌ No data in Wakefit_Daily_Ranks.")
        return

    header = rows[0]
    col = {h: i for i, h in enumerate(header)}

    required = ["Date", "Keyword", "Type", "Rank", "Title",
                "Channel", "Video URL", "Views", "Likes", "Comments"]
    for r in required:
        if r not in col:
            print(f"❌ Required column missing: {r}")
            return

    summary = []
    for row in rows[1:]:
        if row[col["Date"]] != date_str:
            continue

        summary.append([
            row[col["Keyword"]],
            row[col["Type"]],
            row[col["Channel"]],
            row[col["Title"]],
            row[col["Video URL"]],
            row[col["Rank"]],
            row[col["Views"]],
            row[col["Likes"]],
            row[col["Comments"]],
        ])

    if not summary:
        print("ℹ️ No Wakefit rankings today, no summary tab.")
        return

    sheet_name = f"Summary_{date_str}"
    try:
        summary_sheet = spreadsheet.worksheet(sheet_name)
        summary_sheet.clear()
    except gspread.WorksheetNotFound:
        summary_sheet = spreadsheet.add_worksheet(title=sheet_name, rows=2000, cols=12)

    headers = [
        "Keyword", "Type", "Channel", "Title",
        "Video_URL", "Rank", "Views", "Likes", "Comments"
    ]

    summary_sheet.update("A1:I1", [headers])
    summary_sheet.append_rows(summary)
    print(f"✅ Created {sheet_name} with {len(summary)} rows.")


# ------------------ MOVEMENT BUILDER ------------------
def build_daily_movement_summary(spreadsheet, date_str):
    try:
        sheet = spreadsheet.worksheet("Wakefit_Daily_Ranks")
    except gspread.WorksheetNotFound:
        print("❌ Wakefit_Daily_Ranks not found.")
        return

    rows = sheet.get_all_values()
    if len(rows) <= 1:
        print("❌ No data in Wakefit_Daily_Ranks.")
        return

    header = rows[0]
    col = {h: i for i, h in enumerate(header)}

    dates = sorted(set(r[col["Date"]] for r in rows[1:] if r[col["Date"]]))

    if date_str not in dates:
        print("❌ Today's date missing in Wakefit_Daily_Ranks.")
        return

    idx = dates.index(date_str)
    if idx == 0:
        print("ℹ️ No previous day to compare with.")
        return

    prev_date = dates[idx - 1]

    today_map = {}
    prev_map = {}

    for row in rows[1:]:
        d = row[col["Date"]]
        key = (row[col["Keyword"]], row[col["Type"]], row[col["Video URL"]])

        item = {
            "Rank": row[col["Rank"]],
            "Channel": row[col["Channel"]],
            "Title": row[col["Title"]],
            "Views": row[col["Views"]],
            "Likes": row[col["Likes"]],
            "Comments": row[col["Comments"]],
        }

        if d == date_str:
            today_map[key] = item
        if d == prev_date:
            prev_map[key] = item

    movement = []
    for k, today in today_map.items():
        if k in prev_map:
            prev = prev_map[k]
            try:
                rank_change = int(prev["Rank"]) - int(today["Rank"])
            except:
                rank_change = ""

            movement.append([
                k[0],   # Keyword
                k[1],   # Type
                today["Channel"],
                today["Title"],
                k[2],   # URL
                today["Rank"],
                prev_date,
                prev["Rank"],
                rank_change,
                today["Views"],
                today["Likes"],
                today["Comments"],
                prev["Views"],
                prev["Likes"],
                prev["Comments"],
            ])

    if not movement:
        print("ℹ️ No overlapping videos for movement.")
        return

    sheet_name = f"Movement_{date_str}"
    try:
        movement_sheet = spreadsheet.worksheet(sheet_name)
        movement_sheet.clear()
    except gspread.WorksheetNotFound:
        movement_sheet = spreadsheet.add_worksheet(title=sheet_name, rows=2000, cols=20)

    headers = [
        "Keyword", "Type", "Channel", "Title", "Video_URL",
        "Today_Rank", "Prev_Date", "Prev_Rank", "Rank_Change",
        "Today_Views", "Today_Likes", "Today_Comments",
        "Prev_Views", "Prev_Likes", "Prev_Comments",
    ]

    movement_sheet.update("A1:O1", [headers])
    movement_sheet.append_rows(movement)
    print(f"✅ Created {sheet_name} with {len(movement)} rows.")


# ------------------ MAIN ------------------
def main():
    client = authenticate_google_sheets()
    sheet = client.open(SHEET_NAME)

    print(f"📊 Building summaries for {RUN_ID}")
    build_daily_keyword_summary(sheet, RUN_ID)
    build_daily_movement_summary(sheet, RUN_ID)
    print("🎉 Analysis complete.")


if __name__ == "__main__":
    main()
