# APPSTORE REVIEWS SCRAPER
# Scrapes reviews from the last x days grom iOS AppStore for the given app in given country, and saves it to a file

import requests
from bs4 import BeautifulSoup
import json
from datetime import datetime, timedelta

# Set the desired parameters
country = "gb"  # Country code for the App Store
app_id = "562202559"  # App ID for ScottishPower
review_count = 1000  # Number of reviews to scrape
period = 180 # Number of days you need reviews for till today
since_date = datetime.now() - timedelta(days=period)
since_date = since_date.replace(tzinfo=None)

# Construct the API endpoint URL
url = f"https://itunes.apple.com/{country}/rss/customerreviews/id={app_id}/sortBy=mostRecent/xml"

# Send a GET request to the API endpoint
response = requests.get(url)

# Parse the HTML response using Beautiful Soup
soup = BeautifulSoup(response.text, "html.parser")

# Find the review entries
entries = soup.find_all("entry")

# Extract the reviews and relevant information
reviews = []
for entry in entries[:review_count]:
    review_date = entry.find("updated").text
    review_date = datetime.strptime(review_date, "%Y-%m-%dT%H:%M:%S%z")  # Parse review date as datetime object
    review_date = review_date.replace(tzinfo=None)
    if review_date >= since_date:
        review = {
            "username": entry.find("author").find("name").text,
            "content": entry.find("content").text,
            "rating": entry.find("im:rating").text,
            "date": review_date.isoformat(),
            "version": entry.find("im:version").text,
            "source": "AppStore"
        }
        reviews.append(review)


# Specify the path and filename for the JSON file
since_date_str = since_date.strftime("%Y-%m-%d")
json_file = f"Reviews/ScottishPower_AppStore_Reviews_{since_date_str}_{period}days.json"

# Save the reviews into a JSON file
with open(json_file, "w", encoding="utf-8") as file:
    json.dump(reviews, file, ensure_ascii=False, indent=2)

print("Reviews saved to", json_file)
