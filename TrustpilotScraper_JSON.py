# TRUSTPILOT REVIEWS SCRAPER
# Scrapes reviews from the last x days from Trustpilot for ScottishPower and saves to a file
# Trustpilot limits unauthenticated access to 10 pages per filter combination. To maximise
# coverage, we paginate each star-rating filter (1–5) separately, giving up to 5×10×20 = 1000
# reviews. Reviews are deduplicated by ID and filtered to the requested date window.

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta

# Set the desired parameters
business_slug = "www.scottishpower.co.uk"  # Trustpilot business identifier
period = 180  # Number of days of history to include
since_date = datetime.now() - timedelta(days=period)
since_date = since_date.replace(tzinfo=None)

HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    )
}

def fetch_page(business_slug, page, stars=None):
    """Fetch one page of reviews. Returns list of raw review dicts, or None on failure."""
    params = {"page": page, "sort": "recency"}
    if stars is not None:
        params["stars"] = stars
    url = f"https://www.trustpilot.com/review/{business_slug}"
    response = requests.get(url, params=params, headers=HEADERS)
    response.raise_for_status()

    soup = BeautifulSoup(response.text, "html.parser")
    next_data_tag = soup.find("script", {"id": "__NEXT_DATA__"})
    if not next_data_tag:
        return None

    data = json.loads(next_data_tag.string)
    return data["props"]["pageProps"].get("reviews")


seen_ids = set()
reviews = []

# Trustpilot caps unauthenticated access at 10 pages. Iterating each star rating
# separately maximises the number of unique reviews retrieved.
for stars in [1, 2, 3, 4, 5]:
    for page in range(1, 11):
        page_reviews = fetch_page(business_slug, page, stars=stars)

        if not page_reviews:
            break  # Login wall hit or no reviews for this filter

        page_oldest = None
        for r in page_reviews:
            date_str = r.get("dates", {}).get("publishedDate", "")
            try:
                review_date = datetime.fromisoformat(date_str.replace("Z", "+00:00")).replace(tzinfo=None)
            except (ValueError, AttributeError):
                continue

            if page_oldest is None or review_date < page_oldest:
                page_oldest = review_date

            review_id = r.get("id")
            if review_id in seen_ids:
                continue
            seen_ids.add(review_id)

            if review_date >= since_date:
                reviews.append({
                    "id": review_id,
                    "username": r.get("consumer", {}).get("displayName", ""),
                    "title": r.get("title", ""),
                    "content": r.get("text", "") or "",
                    "rating": r.get("rating", ""),
                    "date": review_date.isoformat(),
                    "source": "Trustpilot"
                })

        # Stop paginating this star filter once reviews predate our window
        if page_oldest and page_oldest < since_date:
            break

# Sort newest first to match the scraping order intent
reviews.sort(key=lambda r: r["date"], reverse=True)

# Save to JSON
since_date_str = since_date.strftime("%Y-%m-%d")
json_file = f"Reviews/ScottishPower_Trustpilot_Reviews_{since_date_str}_{period}days.json"

with open(json_file, "w", encoding="utf-8") as file:
    json.dump(reviews, file, ensure_ascii=False, indent=2)

print(f"Reviews saved to {json_file} ({len(reviews)} reviews)")
