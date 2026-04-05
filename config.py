"""
config.py — Central configuration for the ScottishPower review intelligence pipeline.

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
# Ingestion: paths to the JSON files produced by the scrapers
# ---------------------------------------------------------------------------
APPSTORE_JSON_PATH = "Reviews/ScottishPower_AppStore_Reviews_2025-10-07_180days.json"
PLAYSTORE_JSON_PATH = "Reviews/ScottishPower_Playstore_Reviews_2025-10-07_180days.json"

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
