"""
config.py — Central configuration for the review intelligence pipeline.

All scripts import constants from here; nothing is hardcoded across files.
"""

import os

# ---------------------------------------------------------------------------
# AI model
# ---------------------------------------------------------------------------
MODEL_NAME = "claude-haiku-4-5-20251001"

# ---------------------------------------------------------------------------
# Database
# ---------------------------------------------------------------------------
DB_PATH = "reviews.db"

# ---------------------------------------------------------------------------
# Ingestion: glob patterns to find JSON files produced by the scrapers.
# run_pipeline.py selects the most recently modified file matching each pattern.
# ---------------------------------------------------------------------------
APPSTORE_JSON_GLOB    = "Reviews/UtilityCompany_AppStore_Reviews_*.json"
PLAYSTORE_JSON_GLOB   = "Reviews/UtilityCompany_Playstore_Reviews_*.json"
TRUSTPILOT_JSON_GLOB  = "Reviews/UtilityCompany_Trustpilot_Reviews_*.json"

# Scraper scripts (relative to the project root)
APPSTORE_SCRAPER   = "AppStoreScraper_JSON.py"
PLAYSTORE_SCRAPER  = "PlayStoreScraper_JSON.py"
TRUSTPILOT_SCRAPER = "TrustpilotScraper_JSON.py"

# Path where the dashboard CSV is written and committed for Streamlit Cloud
CSV_EXPORT_PATH = "data/reviews_analysed.csv"

# ---------------------------------------------------------------------------
# Analysis
# ---------------------------------------------------------------------------
BATCH_SIZE = 25

TOPICS = [
    "login",
    "billing",
    "app_crash",
    "smart_meter",
    "customer_service",
    "outage",
    "account",
    "other",
]

# ---------------------------------------------------------------------------
# External integrations (read from environment at import time)
# ---------------------------------------------------------------------------
ANTHROPIC_API_KEY = os.environ.get("ANTHROPIC_API_KEY", "")
SLACK_WEBHOOK_URL = os.environ.get("SLACK_WEBHOOK_URL", "")
