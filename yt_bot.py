import os
import requests
import re
import time
import gspread
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from oauth2client.service_account import ServiceAccountCredentials

# ---------- CONFIG ----------
API_KEY = os.environ.get("YT_API_KEY", "").strip()  # from GitHub secret
JSON_FILE = "service_account.json"                  # written by workflow
SHEET_NAME = "YT Final Bot"

# Quota + fetch behaviour
MAX_RESULTS_PER_PAGE = 50   # YouTube max per page
MAX_PAGES = 2               # IMPORTANT: 2 page only (quota-safe)
SLEEP_TIME = 2              # seconds between calls / keywords

# Run ID used in sheet/tab names
RUN_ID = datetime.now().strftime("%Y-%m-%d")   # e.g. 2025-11-21


# ---------- BASIC CHECK ----------
if not os.path.exists(JSON_FILE):
    raise FileNotFoundError(f"JSON file '{JSON_FILE}' not found in working directory.")
else:
    print(f"✅ JSON file found: {JSON_FILE}")


# ---------- AUTH ----------
def authenticate_google_sheets():
    scope = [
        "https://spreadsheets.google.com/feeds",
        "https://www.googleapis.com/auth/drive",
    ]
    creds = ServiceAccountCredentials.from_json_keyfile_name(JSON_FILE, scope)
    return gspread.authorize(creds)


# ---------- HELPERS ----------
def time_ago(published_date):
    """Convert ISO datetime (e.g. 2025-10-16T12:34:56Z) into '2 days ago' style text."""
    try:
        published = datetime.strptime(published_date, "%Y-%m-%dT%H:%M:%SZ")
    except Exception:
        return published_date  # fallback

    now_utc = datetime.now(timezone.utc).replace(tzinfo=None)
    diff = relativedelta(now_utc, published)
    if diff.years:
        return f"{diff.years} year{'s' if diff.years > 1 else ''} ago"
    if diff.months:
        return f"{diff.months} month{'s' if diff.months > 1 else ''} ago"
    if diff.days:
        return f"{diff.days} day{'s' if diff.days > 1 else ''} ago"
    if diff.hours:
        return f"{diff.hours} hour{'s' if diff.hours > 1 else ''} ago"
    if diff.minutes:
        return f"{diff.minutes} minute{'s' if diff.minutes > 1 else ''} ago"
    return "Today"


def is_shorts_by_url(video_id):
    """
    Use requests.head on https://www.youtube.com/shorts/{video_id}.
    If the final URL contains '/shorts/', treat as Short.
    Otherwise treat as regular video and use watch URL.
    """
    probe = f"https://www.youtube.com/shorts/{video_id}"
    try:
        s = requests.Session()
        headers = {"User-Agent": "Mozilla/5.0"}
        r = s.head(probe, allow_redirects=True, timeout=5, headers=headers)
        final_url = r.url.lower()
        if "/shorts/" in final_url:
            return "Short", r.url
    except Exception:
        pass

    return "Video", f"https://www.youtube.com/watch?v={video_id}"


def extract_links(description):
    """Extract all http/https links from description text."""
    raw_links = re.findall(r"(https?://[^\s]+)", description or "")
    return ", ".join(raw_links) if raw_links else "None"


def extract_video_id(url):
    """
    Extracts the video ID from:
    - https://www.youtube.com/watch?v=XXXXX
    - https://youtu.be/XXXXX
    - https://youtube.com/shorts/XXXXX
    (also handles extra query params like ?feature=share)
    """
    if not url:
        return None

    url = url.strip()

    if "shorts/" in url:
        return url.split("shorts/")[1].split("?")[0]

    if "watch?v=" in url:
        return url.split("watch?v=")[1].split("&")[0]

    if "youtu.be/" in url:
        return url.split("youtu.be/")[1].split("?")[0]

    return None


# ---------- CORE: FETCH ONE KEYWORD ----------
def fetch_youtube_results_for_keyword(keyword):
    """
    For a single keyword:
    - Fetch 1 page of search results from YouTube API (region IN)
    - For each video:
        - get snippet + stats
        - classify as Short/Video via URL
    - Try to collect up to 10 Shorts and 10 Videos.
    Returns:
        shorts_rows, video_rows (lists of dicts)
    """
    keyword = keyword.strip()
    if not keyword:
        print("⚠️ Skipping blank keyword.")
        return [], []

    print(f"\n🔍 Fetching results for: {keyword}")

    search_url = "https://www.googleapis.com/youtube/v3/search"
    videos_url = "https://www.googleapis.com/youtube/v3/videos"

    shorts_rows = []
    video_rows = []

    page_token = None
    pages_checked = 0

    while pages_checked < MAX_PAGES and (len(shorts_rows) < 10 or len(video_rows) < 10):
        pages_checked += 1

        search_params = {
            "part": "snippet",
            "q": keyword,
            "type": "video",
            "maxResults": MAX_RESULTS_PER_PAGE,
            "key": API_KEY,
            "regionCode": "IN",
        }
        if page_token:
            search_params["pageToken"] = page_token

        try:
            search_response = requests.get(search_url, params=search_params, timeout=10).json()
        except Exception as e:
            print(f"❌ Error calling search API for '{keyword}' (page {pages_checked}): {e}")
            break

        if "error" in search_response:
            print(f"❌ YouTube search API error for '{keyword}' (page {pages_checked}):")
            print(search_response["error"])
            break

        items = search_response.get("items", [])
        if not items:
            print(f"⚠️ No items returned for '{keyword}' at page {pages_checked}. Full response:")
            print(search_response)
            break

        video_ids = []
        for item in items:
            try:
                vid = item["id"]["videoId"]
                video_ids.append(vid)
            except KeyError:
                continue

        if not video_ids:
            print(f"⚠️ No valid video IDs on page {pages_checked} for '{keyword}'.")
            page_token = search_response.get("nextPageToken")
            if not page_token:
                break
            time.sleep(SLEEP_TIME)
            continue

        video_params = {
            "part": "snippet,statistics",
            "id": ",".join(video_ids),
            "key": API_KEY,
        }

        try:
            video_response = requests.get(videos_url, params=video_params, timeout=10).json()
        except Exception as e:
            print(f"❌ Error calling videos API for '{keyword}' (page {pages_checked}): {e}")
            break

        if "error" in video_response:
            print(f"❌ YouTube videos API error for '{keyword}' (page {pages_checked}):")
            print(video_response["error"])
            break

        video_items = video_response.get("items", [])
        if not video_items:
            print(f"⚠️ No video details returned for '{keyword}' on page {pages_checked}. Full response:")
            print(video_response)
            page_token = search_response.get("nextPageToken")
            if not page_token:
                break
            time.sleep(SLEEP_TIME)
            continue

        id_to_item = {item["id"]: item for item in video_items}

        # Process in the same order as search results
        for vid in video_ids:
            if len(shorts_rows) >= 10 and len(video_rows) >= 10:
                break

            details = id_to_item.get(vid)
            if not details:
                continue

            snip = details.get("snippet", {})
            stats = details.get("statistics", {})

            title = snip.get("title", "Untitled")
            channel = snip.get("channelTitle", "Unknown Channel")
            description = snip.get("description", "")
            views = stats.get("viewCount", "N/A")
            published_at = snip.get("publishedAt", "")
            posted_ago = time_ago(published_at)

            vtype, canonical_url = is_shorts_by_url(vid)
            links_text = extract_links(description)

            row_data = {
                "Title": title,
                "Channel": channel,
                "Views": views,
                "Posted_Ago": posted_ago,
                "Type": vtype,
                "Video_URL": canonical_url,
                "Description_Links": links_text,
            }

            if vtype == "Short":
                if len(shorts_rows) < 10:
                    shorts_rows.append(row_data)
            else:
                if len(video_rows) < 10:
                    video_rows.append(row_data)

        # With MAX_PAGES = 1 we intentionally don't page much, but keep logic:
        page_token = search_response.get("nextPageToken")
        if not page_token:
            break

        time.sleep(SLEEP_TIME)

    # Add Rank
    for idx, row in enumerate(shorts_rows, start=1):
        row["Rank"] = idx
    for idx, row in enumerate(video_rows, start=1):
        row["Rank"] = idx

    print(
        f"✅ '{keyword}': {len(shorts_rows)} shorts, {len(video_rows)} videos "
        f"(aimed for 10 each; limited by actual API results + URL rule)."
    )
    return shorts_rows, video_rows


# ---------- WAKEFIT LIVE LINKS → IDS ----------
def get_wakefit_video_ids(spreadsheet):
    """
    Read 'Live Links' sheet and extract all YTD Live link + YTS live link video IDs.
    """
    try:
        sheet = spreadsheet.worksheet("Live Links")
    except gspread.WorksheetNotFound:
        print("⚠️ 'Live Links' sheet not found. Skipping Wakefit analysis.")
        return set()

    rows = sheet.get_all_values()
    if len(rows) <= 1:
        print("⚠️ 'Live Links' sheet has no data.")
        return set()

    header = rows[0]
    col_index = {name: i for i, name in enumerate(header)}

    ids = set()

    for row in rows[1:]:
        if not row:
            continue

        # Full videos
        if "YTD Live link" in col_index and len(row) > col_index["YTD Live link"]:
            vid_id = extract_video_id(row[col_index["YTD Live link"]])
            if vid_id:
                ids.add(vid_id)

        # Shorts
        if "YTS live link" in col_index and len(row) > col_index["YTS live link"]:
            vid_id = extract_video_id(row[col_index["YTS live link"]])
            if vid_id:
                ids.add(vid_id)

    print(f"🔍 Loaded {len(ids)} Wakefit seeded video IDs from 'Live Links'.")
    return ids


def append_wakefit_daily_ranks(spreadsheet, shorts_sheet, videos_sheet, wakefit_ids):
    """
    Look at today's Shorts_YYYY-MM-DD and Videos_YYYY-MM-DD tabs,
    find all rows whose video ID is in wakefit_ids, and append them
    into 'Wakefit_Daily_Ranks'.
    """
    if not wakefit_ids:
        print("ℹ️ No Wakefit IDs found, skipping Wakefit_Daily_Ranks update.")
        return

    date_str = RUN_ID

    # Ensure summary sheet exists
    try:
        ranks_sheet = spreadsheet.worksheet("Wakefit_Daily_Ranks")
    except gspread.WorksheetNotFound:
        ranks_sheet = spreadsheet.add_worksheet(title="Wakefit_Daily_Ranks", rows="5000", cols="10")
        headers = ["Date", "Type", "Keyword", "Rank", "Title", "Channel", "Video URL"]
        ranks_sheet.update(range_name="A1:G1", values=[headers])
        print("🆕 Created 'Wakefit_Daily_Ranks' sheet with headers.")

    def collect_matches(sheet, type_label):
        values = sheet.get_all_values()
        if len(values) <= 1:
            return []

        header = values[0]
        col = {name: i for i, name in enumerate(header)}

        required = ["Keyword", "Rank", "Title", "Channel", "Video URL"]
        for r in required:
            if r not in col:
                print(f"⚠️ Column '{r}' not found in sheet '{sheet.title}', skipping it for Wakefit analysis.")
                return []

        matches = []
        for row in values[1:]:
            if not row:
                continue

            video_url = row[col["Video URL"]] if len(row) > col["Video URL"] else ""
            vid_id = extract_video_id(video_url)
            if not vid_id or vid_id not in wakefit_ids:
                continue

            keyword = row[col["Keyword"]] if len(row) > col["Keyword"] else ""
            rank = row[col["Rank"]] if len(row) > col["Rank"] else ""
            title = row[col["Title"]] if len(row) > col["Title"] else ""
            channel = row[col["Channel"]] if len(row) > col["Channel"] else ""

            matches.append([
                date_str,
                type_label,
                keyword,
                rank,
                title,
                channel,
                video_url,
            ])

        return matches

    shorts_matches = collect_matches(shorts_sheet, "Short")
    videos_matches = collect_matches(videos_sheet, "Video")

    all_matches = shorts_matches + videos_matches
    if not all_matches:
        print("ℹ️ No Wakefit videos found in today's ranking results.")
        return

    ranks_sheet.append_rows(all_matches, value_input_option="RAW")
    print(f"✅ Appended {len(all_matches)} Wakefit ranking rows to 'Wakefit_Daily_Ranks'.")


# ---------- MAIN ----------
def main():
    if not API_KEY:
        raise RuntimeError("YT_API_KEY environment variable is not set.")

    client = authenticate_google_sheets()
    spreadsheet = client.open(SHEET_NAME)
    keywords_sheet = spreadsheet.worksheet("Keywords")

    # Read keywords
    all_rows = keywords_sheet.get_all_values()
    if len(all_rows) <= 1:
        print("⚠️ No keyword data found (need at least a header + 1 row).")
        return

    raw_keywords = [
        row[0].strip()
        for row in all_rows[1:]
        if row and len(row) > 0 and row[0].strip()
    ]
    total_keywords = len(raw_keywords)
    keywords = list(dict.fromkeys(raw_keywords))
    unique_keywords = len(keywords)

    print(f"📋 Found {total_keywords} keywords in sheet, {unique_keywords} unique keywords to process.\n")

    # Set up today's tabs
    shorts_tab_name = f"Shorts_{RUN_ID}"
    videos_tab_name = f"Videos_{RUN_ID}"

    try:
        shorts_sheet = spreadsheet.worksheet(shorts_tab_name)
        print(f"📄 Using existing '{shorts_tab_name}' sheet (will clear & overwrite).")
    except gspread.WorksheetNotFound:
        shorts_sheet = spreadsheet.add_worksheet(title=shorts_tab_name, rows="2000", cols="10")
        print(f"🆕 Created '{shorts_tab_name}' sheet.")

    try:
        videos_sheet = spreadsheet.worksheet(videos_tab_name)
        print(f"📄 Using existing '{videos_tab_name}' sheet (will clear & overwrite).")
    except gspread.WorksheetNotFound:
        videos_sheet = spreadsheet.add_worksheet(title=videos_tab_name, rows="2000", cols="10")
        print(f"🆕 Created '{videos_tab_name}' sheet.")

    # Clear and write headers
    shorts_sheet.clear()
    videos_sheet.clear()

    headers = [
        "Keyword_Sr_No",
        "Keyword",
        "Rank",
        "Title",
        "Channel",
        "Views",
        "Posted_Ago",
        "Type",
        "Video URL",
        "Description_Links",
    ]

    shorts_sheet.update(range_name="A1:J1", values=[headers])
    videos_sheet.update(range_name="A1:J1", values=[headers])
    print("🪶 Headers written to both Shorts and Videos tabs.")

    shorts_current_row = 1
    videos_current_row = 1

    successful_keywords = []
    failed_keywords = []

    # Process each keyword
    for kw_index, kw in enumerate(keywords, start=1):
        shorts_rows, video_rows = fetch_youtube_results_for_keyword(kw)

        if not shorts_rows and not video_rows:
            failed_keywords.append(kw)
            time.sleep(SLEEP_TIME)
            continue

        successful_keywords.append(kw)

        # Shorts
                # ---- Write Shorts for this keyword ----
        if shorts_rows:
            values = [
                [
                    kw_index,               # Keyword_Sr_No
                    kw,                     # Keyword
                    row["Rank"],
                    row["Title"],
                    row["Channel"],
                    row["Views"],
                    row["Posted_Ago"],
                    row["Type"],
                    row["Video_URL"],
                    row["Description_Links"],
                ]
                for row in shorts_rows
            ]
            shorts_sheet.append_rows(values, value_input_option="RAW")
            shorts_current_row += len(values)

        # Videos
        # ---- Write Videos for this keyword ----
        if video_rows:
            values = [
                [
                    kw_index,
                    kw,
                    row["Rank"],
                    row["Title"],
                    row["Channel"],
                    row["Views"],
                    row["Posted_Ago"],
                    row["Type"],
                    row["Video_URL"],
                    row["Description_Links"],
                ]
                for row in video_rows
            ]
            videos_sheet.append_rows(values, value_input_option="RAW")
            videos_current_row += len(values)

        time.sleep(SLEEP_TIME)

    # Summary
    print("\n========== RUN SUMMARY ==========")
    print(f"RUN_ID (tab date): {RUN_ID}")
    print(f"✅ Successful keywords (had some results): {len(successful_keywords)}")
    for k in successful_keywords:
        print(f"   - {k}")
    print(f"❌ Keywords with no usable results: {len(failed_keywords)}")
    for k in failed_keywords:
        print(f"   - {k}")
    print("=================================\n")

    print(f"🎉 Done. Data saved into '{shorts_tab_name}' and '{videos_tab_name}' tabs.")

    # Wakefit analysis
    wakefit_ids = get_wakefit_video_ids(spreadsheet)
    append_wakefit_daily_ranks(spreadsheet, shorts_sheet, videos_sheet, wakefit_ids)


if __name__ == "__main__":
    main()
