# PLAYSTORE REVIEWS SCRAPER
# Scrapes reviews from the last x days grom Google PlayStore for the given App in given region, and saves it to a file

import os
from google_play_scraper import Sort, reviews
import pandas as pd
from datetime import datetime, timedelta

period = 180 #Number of days before current date you need reviews for
start_date = datetime.now() - timedelta(days=period)
start_date_str = start_date.strftime("%Y-%m-%d")
result, continuation_token = reviews(
    'uk.co.scottishpower',
    lang='en', # defaults to 'en'
    country='uk', # defaults to 'us'
    sort=Sort.NEWEST, # defaults to Sort.NEWEST
    count=500, # defaults to 100
    filter_score_with=None # defaults to None(means all score)
)

reviews = pd.DataFrame(result)
reviews['at'] = pd.to_datetime(reviews['at'])

new_reviews = reviews[reviews['at']>start_date]
new_reviews = new_reviews.drop(['reviewId','userImage','thumbsUpCount','repliedAt','replyContent','reviewCreatedVersion'],axis=1)
new_reviews['Source'] = 'PlayStore'
os.makedirs("Reviews", exist_ok=True)
csv_file = f"Reviews/UtilityCompany_Playstore_Reviews_{start_date_str}_{period}days.csv"
new_reviews.to_csv(csv_file, index=False)

print("Reviews saved to ",csv_file)