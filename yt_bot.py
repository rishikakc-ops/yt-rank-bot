

import os
import requests
import re
import time
import gspread
from datetime import datetime, timezone
from dateutil.relativedelta import relativedelta
from oauth2client.service_account import ServiceAccountCredentials

# ---------- CONFIG ----------
API_KEY = os.environ["YT_API_KEY"]              # we'll store this as a GitHub secret
JSON_FILE = "service_account.json"              # we'll create this file at runtime from a secret
SHEET_NAME = "YT Final Bot"

MAX_RESULTS_PER_PAGE = 50   # max results per search page (YouTube API limit)
MAX_PAGES = 5               # hard cap: up to 5 pages (~250 results) per keyword
SLEEP_TIME = 2              # seconds between API calls & between keywords

# Run ID used only for tab names (not a column)
RUN_ID = datetime.now().strftime("%Y-%m-%d")   # e.g. '2025-11-20'

# ---------- BASIC CHECK ----------
if not os.path.exists(JSON_FILE):
    raise FileNotFoundError(
        f"JSON file '{JSON_FILE}' not found in working directory."
    )
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
        return published_date  # fallback: just return raw string

    # Use UTC-aware now
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
    Use requests.head on https://www.youtube.com/shorts/{video_id}
    If the final URL contains '/shorts/', treat as Shorts.
    Otherwise, treat as regular watch video.
    Returns: (type_label, canonical_url)
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

    # fallback: regular watch URL
    return "Video", f"https://www.youtube.com/watch?v={video_id}"

def extract_links(description):
    """Extract all http/https links from the description."""
    raw_links = re.findall(r'(https?://[^\s]+)', description or "")
    return ", ".join(raw_links) if raw_links else "None"

# ---------- CORE: FETCH FOR ONE KEYWORD WITH PAGINATION ----------
def fetch_youtube_results_for_keyword(keyword):
    """
    For a single keyword:
    - Keep paging through search results (regionCode=IN) until:
        - We have 10 Shorts AND 10 Videos, OR
        - We reach MAX_PAGES or run out of results
    - For each page:
        - Get video details via videos.list
        - Classify via /shorts/ vs /watch? using requests.head
    Returns:
        shorts_rows, video_rows
        each row: dict with Rank, Title, Channel, Views, Posted_Ago, Type, Video_URL, Description_Links
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
            "regionCode": "IN",  # simulate search from India
        }
        if page_token:
            search_params["pageToken"] = page_token

        try:
            search_response = requests.get(search_url, params=search_params, timeout=10).json()
        except Exception as e:
            print(f"❌ Error calling search API for '{keyword}' (page {pages_checked}): {e}")
            break

        # 🔴 Check for API errors explicitly
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

        # Get details for this page's videos
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

        # 🔴 Check for API errors explicitly
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

        # Process videos in the same order as search results
        for vid in video_ids:
            if len(shorts_rows) >= 10 and len(video_rows) >= 10:
                break  # we have enough of both

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

        # Get next page token (if any)
        page_token = search_response.get("nextPageToken")
        if not page_token:
            break

        time.sleep(SLEEP_TIME)

    # Add Rank within each type for this keyword
    for idx, row in enumerate(shorts_rows, start=1):
        row["Rank"] = idx
    for idx, row in enumerate(video_rows, start=1):
        row["Rank"] = idx

    print(
        f"✅ '{keyword}': {len(shorts_rows)} shorts, {len(video_rows)} videos "
        f"(aimed for 10 each; limited by actual API results + URL rule)."
    )
    return shorts_rows, video_rows

# ---------- MAIN ----------
def main():
    client = authenticate_google_sheets()
    spreadsheet = client.open(SHEET_NAME)
    keywords_sheet = spreadsheet.worksheet("Keywords")

    # Read all keywords from first column, skip header
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

    # Deduplicate while preserving order
    KEYWORDS = list(dict.fromkeys(raw_keywords))
    unique_keywords = len(KEYWORDS)

    print(f"📋 Found {total_keywords} keywords in sheet, {unique_keywords} unique keywords to process.\n")

    # ---- Set up Shorts & Videos tabs for this run ----
    shorts_tab_name = f"Shorts_{RUN_ID}"
    videos_tab_name = f"Videos_{RUN_ID}"

    # Ensure "Shorts_<RUN_ID>" sheet exists, then clear it
    try:
        shorts_sheet = spreadsheet.worksheet(shorts_tab_name)
        print(f"📄 Using existing '{shorts_tab_name}' sheet (will clear & overwrite).")
    except gspread.WorksheetNotFound:
        shorts_sheet = spreadsheet.add_worksheet(title=shorts_tab_name, rows="2000", cols="10")
        print(f"🆕 Created '{shorts_tab_name}' sheet.")

    # Ensure "Videos_<RUN_ID>" sheet exists, then clear it
    try:
        videos_sheet = spreadsheet.worksheet(videos_tab_name)
        print(f"📄 Using existing '{videos_tab_name}' sheet (will clear & overwrite).")
    except gspread.WorksheetNotFound:
        videos_sheet = spreadsheet.add_worksheet(title=videos_tab_name, rows="2000", cols="10")
        print(f"🆕 Created '{videos_tab_name}' sheet.")

    # Clear previous contents for a clean run
    shorts_sheet.clear()
    videos_sheet.clear()

    # Headers (no Run_ID now)
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

    # Write headers to row 1 (using named args to avoid DeprecationWarning)
    shorts_sheet.update(range_name="A1:J1", values=[headers])
    videos_sheet.update(range_name="A1:J1", values=[headers])
    print("🪶 Headers written to both Shorts and Videos tabs.")

    # Track current last row (start at header row = 1)
    shorts_current_row = 1
    videos_current_row = 1

    successful_keywords = []
    failed_keywords = []

    # ---- Process each keyword ----
    for kw_index, kw in enumerate(KEYWORDS, start=1):
        shorts_rows, video_rows = fetch_youtube_results_for_keyword(kw)

        if not shorts_rows and not video_rows:
            failed_keywords.append(kw)
            time.sleep(SLEEP_TIME)
            continue

        successful_keywords.append(kw)

        # ---- Write Shorts for this keyword ----
        if shorts_rows:
            start_row = shorts_current_row + 1
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
            end_row = shorts_current_row + len(values)

            # Merge Keyword_Sr_No and Keyword cells for this block (if multiple rows)
            if len(values) > 1:
                shorts_sheet.merge_cells(start_row, 1, end_row, 1)  # Keyword_Sr_No
                shorts_sheet.merge_cells(start_row, 2, end_row, 2)  # Keyword

            shorts_current_row = end_row

        # ---- Write Videos for this keyword ----
        if video_rows:
            start_row = videos_current_row + 1
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
                for row in video_rows
            ]
            videos_sheet.append_rows(values, value_input_option="RAW")
            end_row = videos_current_row + len(values)

            # Merge Keyword_Sr_No and Keyword cells for this block (if multiple rows)
            if len(values) > 1:
                videos_sheet.merge_cells(start_row, 1, end_row, 1)  # Keyword_Sr_No
                videos_sheet.merge_cells(start_row, 2, end_row, 2)  # Keyword

            videos_current_row = end_row

        # Sleep between keywords to be extra safe with quota
        time.sleep(SLEEP_TIME)

    # ✅ Summary printout
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

# Run it
main()
