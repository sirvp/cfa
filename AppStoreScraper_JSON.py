# APPSTORE REVIEWS SCRAPER
# Scrapes reviews from the last x days from iOS AppStore for the given app in given country, and saves it to a file
# Paginates through all available RSS pages (max 10 × 50 = 500 reviews) to maximise coverage.

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta

# Set the desired parameters
country = "gb"  # Country code for the App Store
app_id = "562202559"  # App ID for ScottishPower
period = 180  # Number of days you need reviews for till today
since_date = datetime.now() - timedelta(days=period)
since_date = since_date.replace(tzinfo=None)

# Fetch all pages of the RSS feed (Apple caps at 10 pages × 50 reviews = 500 max)
reviews = []
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
            reviews.append({
                "username": entry.find("author").find("name").text,
                "content": entry.find("content").text,
                "rating": entry.find("im:rating").text,
                "date": review_date.isoformat(),
                "version": entry.find("im:version").text,
                "source": "AppStore"
            })

    # Stop paginating once the oldest review on this page predates our window
    if page_oldest and page_oldest < since_date:
        break

# Specify the path and filename for the JSON file
since_date_str = since_date.strftime("%Y-%m-%d")
json_file = f"Reviews/ScottishPower_AppStore_Reviews_{since_date_str}_{period}days.json"

with open(json_file, "w", encoding="utf-8") as file:
    json.dump(reviews, file, ensure_ascii=False, indent=2)

print(f"Reviews saved to {json_file} ({len(reviews)} reviews)")
