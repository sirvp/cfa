# APPSTORE REVIEWS SCRAPER
# Scrapes reviews from the iOS AppStore for the given app and saves them to a file.
# Runs incrementally: loads any existing output file, determines the newest review already
# stored, and only paginates until that date is reached — avoiding a full re-fetch every day.
# Falls back to the full `period`-day window when no existing file is found.

import csv
import glob
import os
import requests
from bs4 import BeautifulSoup
from datetime import datetime, timedelta

# Set the desired parameters
country = "gb"  # Country code for the App Store
app_id = "562202559"
period = 180  # Number of days you need reviews for till today
since_date = datetime.now() - timedelta(days=period)
since_date = since_date.replace(tzinfo=None)

os.makedirs("Reviews", exist_ok=True)

# Load existing reviews for incremental fetch
existing_reviews = []
fetch_since = since_date  # Default: fetch the full window

existing_files = sorted(glob.glob("Reviews/UtilityCompany_AppStore_Reviews_*days.csv"))
if existing_files:
    latest_file = existing_files[-1]
    with open(latest_file, "r", newline="", encoding="utf-8") as f:
        reader = csv.DictReader(f)
        existing_reviews = list(reader)
    if existing_reviews:
        latest_date_str = max(r["date"] for r in existing_reviews)
        fetch_since = datetime.fromisoformat(latest_date_str)
        print(f"Loaded {len(existing_reviews)} existing reviews. Fetching since {fetch_since.date()}")

# Fetch pages of the RSS feed (Apple caps at 10 pages × 50 reviews = 500 max).
# With incremental fetching we typically only need the first page or two on daily runs.
new_reviews = []
for page in range(1, 11):
    url = f"https://itunes.apple.com/{country}/rss/customerreviews/page={page}/id={app_id}/sortBy=mostRecent/xml"
    response = requests.get(url)
    soup = BeautifulSoup(response.text, "html.parser")

    # Skip the first <entry> on page 1 — it is the app metadata, not a review
    entries = soup.find_all("entry")
    if page == 1:
        entries = entries[1:]

    if not entries:
        break  # No more pages

    page_oldest = None
    for entry in entries:
        review_date = entry.find("updated").text
        review_date = datetime.strptime(review_date, "%Y-%m-%dT%H:%M:%S%z")
        review_date = review_date.replace(tzinfo=None)

        if page_oldest is None or review_date < page_oldest:
            page_oldest = review_date

        if review_date >= since_date:
            content_tag = entry.find("content")
            version_tag = entry.find("im:version")
            new_reviews.append({
                "username": entry.find("author").find("name").text,
                "content": content_tag.text if content_tag else "",
                "rating": entry.find("im:rating").text,
                "date": review_date.isoformat(),
                "version": version_tag.text if version_tag else "",
                "source": "AppStore"
            })

    # Stop paginating once we've reached reviews already captured in the existing file
    if page_oldest and page_oldest <= fetch_since:
        break

print(f"Fetched {len(new_reviews)} new reviews")

# Merge new reviews into existing, deduplicate by (username, date), filter to window
existing_keys = {(r["username"], r["date"]) for r in existing_reviews}
for r in new_reviews:
    if (r["username"], r["date"]) not in existing_keys:
        existing_reviews.append(r)
        existing_keys.add((r["username"], r["date"]))

all_reviews = [r for r in existing_reviews if datetime.fromisoformat(r["date"]) >= since_date]
all_reviews.sort(key=lambda r: r["date"], reverse=True)

# Specify the path and filename for the CSV file
since_date_str = since_date.strftime("%Y-%m-%d")
csv_file = f"Reviews/UtilityCompany_AppStore_Reviews_{since_date_str}_{period}days.csv"

with open(csv_file, "w", newline="", encoding="utf-8") as file:
    fieldnames = ["username", "content", "rating", "date", "version", "source"]
    writer = csv.DictWriter(file, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(all_reviews)

print(f"Reviews saved to {csv_file} ({len(all_reviews)} reviews)")
