# PLAYSTORE REVIEWS SCRAPER
# Scrapes reviews from the last x days from Google PlayStore for the given App in given region, and saves it to a file
# Uses continuation tokens to paginate until all reviews within the date window are retrieved.

import os
from google_play_scraper import Sort, reviews as get_reviews
import pandas as pd
from datetime import datetime, timedelta

period = 180  # Number of days before current date you need reviews for
start_date = datetime.now() - timedelta(days=period)
start_date_str = start_date.strftime("%Y-%m-%d")

# Paginate using continuation tokens until the oldest review in a batch
# predates start_date (meaning we've covered the full window) or no more reviews exist.
all_results = []
token = None

while True:
    batch, token = get_reviews(
        'uk.co.scottishpower',
        lang='en',
        country='uk',
        sort=Sort.NEWEST,
        count=200,
        continuation_token=token,
        filter_score_with=None
    )

    if not batch:
        break

    all_results.extend(batch)

    # Stop once the oldest review in this batch is older than our window
    oldest_in_batch = min(r['at'] for r in batch)
    if oldest_in_batch.replace(tzinfo=None) < start_date:
        break

    if token is None:
        break

reviews_df = pd.DataFrame(all_results)
reviews_df['at'] = pd.to_datetime(reviews_df['at']).dt.tz_localize(None)

# Keep ratings without review text — fill missing content with empty string
reviews_df['content'] = reviews_df['content'].fillna('')

new_reviews = reviews_df[reviews_df['at'] > start_date]
new_reviews = new_reviews.drop(
    ['reviewId', 'userImage', 'thumbsUpCount', 'repliedAt', 'replyContent', 'reviewCreatedVersion'],
    axis=1
)
new_reviews['Source'] = 'PlayStore'
new_reviews['at'] = new_reviews['at'].dt.strftime('%Y-%m-%dT%H:%M:%S')

os.makedirs("Reviews", exist_ok=True)
json_file = f"Reviews/UtilityCompany_Playstore_Reviews_{start_date_str}_{period}days.json"
new_reviews.to_json(json_file, orient='records', indent=2)

print(f"Reviews saved to {json_file} ({len(new_reviews)} reviews)")
