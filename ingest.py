"""
ingest.py — Incremental ingestion of app review JSON files into SQLite.

Usage:
    python ingest.py <json_file_path> <AppStore|PlayStore|Trustpilot>

Example:
    python ingest.py Reviews/UtilityCompany_AppStore_Reviews_2025-10-07_180days.json AppStore
    python ingest.py Reviews/UtilityCompany_Playstore_Reviews_2025-10-07_180days.json PlayStore
    python ingest.py Reviews/UtilityCompany_Trustpilot_Reviews_2025-10-07_180days.json Trustpilot

On first run all reviews in the file are ingested and a cursor is written.
On subsequent runs only reviews newer than the cursor date are inserted,
and duplicates (matched by source + SHA-256 review_id) are skipped.
"""

import argparse
import hashlib
import json
import logging
import sqlite3
import sys
from datetime import datetime, timezone

from config import DB_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database helpers
# ---------------------------------------------------------------------------


def get_db_connection(db_path: str) -> sqlite3.Connection:
    """Open a SQLite connection with WAL mode and Row factory enabled.

    Args:
        db_path: Filesystem path to the SQLite database file.

    Returns:
        An open sqlite3.Connection instance.
    """
    conn = sqlite3.connect(db_path)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL")
    return conn


def initialise_db(conn: sqlite3.Connection) -> None:
    """Create the raw_reviews and ingestion_cursors tables if they do not exist.

    Safe to call on every run — uses CREATE TABLE IF NOT EXISTS.

    Args:
        conn: An open SQLite connection.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS raw_reviews (
            id           INTEGER PRIMARY KEY AUTOINCREMENT,
            source       TEXT    NOT NULL,
            review_id    TEXT    NOT NULL,
            author       TEXT,
            rating       INTEGER,
            body         TEXT,
            app_version  TEXT,
            date_posted  TEXT    NOT NULL,
            ingested_at  TEXT    NOT NULL,
            is_processed INTEGER NOT NULL DEFAULT 0,
            UNIQUE(source, review_id)
        );

        CREATE TABLE IF NOT EXISTS ingestion_cursors (
            source            TEXT PRIMARY KEY,
            last_fetched_date TEXT NOT NULL
        );
        """
    )
    conn.commit()


# ---------------------------------------------------------------------------
# SHA-256 review ID helpers
# ---------------------------------------------------------------------------


def compute_review_id(source: str, raw: dict) -> str:
    """Compute a stable SHA-256 hash that serves as a synthetic review ID.

    App Store has no native ID, and the Play Store scraper drops reviewId
    before saving. The hash is built from fields that uniquely identify a
    review within each source.

    Fields used:
        AppStore:  username + date + content
        PlayStore: userName + at + content

    Args:
        source: "AppStore" or "PlayStore".
        raw: The raw review dict from the JSON file.

    Returns:
        A 64-character lowercase hex string (SHA-256 digest).
    """
    if source == "AppStore":
        key = (
            (raw.get("username") or "")
            + (raw.get("date") or "")
            + (raw.get("content") or "")
        )
    elif source == "PlayStore":
        key = (
            (raw.get("userName") or "")
            + (raw.get("at") or "")
            + (raw.get("content") or "")
        )
    else:  # Trustpilot — prefer the native platform ID; fall back to content hash
        key = raw.get("id") or (
            (raw.get("username") or "")
            + (raw.get("date") or "")
            + (raw.get("content") or "")
        )
    return hashlib.sha256(key.encode("utf-8")).hexdigest()


# ---------------------------------------------------------------------------
# Normalisation
# ---------------------------------------------------------------------------


def normalise_appstore(raw: dict) -> dict:
    """Normalise a raw App Store review dict into the shared schema.

    Source field mapping:
        username  → author
        content   → body
        rating    → rating  (cast from string to int)
        date      → date_posted
        version   → app_version
        source    → source  (preserved)

    Args:
        raw: A single review dict from the App Store JSON file.

    Returns:
        A normalised dict ready for insertion into raw_reviews.
    """
    rating = None
    try:
        rating = int(raw["rating"])
    except (KeyError, ValueError, TypeError):
        logger.warning("Could not parse App Store rating: %r", raw.get("rating"))

    return {
        "source": "AppStore",
        "author": raw.get("username", ""),
        "rating": rating,
        "body": raw.get("content", ""),
        "app_version": raw.get("version"),
        "date_posted": raw["date"],
    }


def normalise_playstore(raw: dict) -> dict:
    """Normalise a raw Play Store review dict into the shared schema.

    Source field mapping:
        userName   → author
        content    → body
        score      → rating  (already int)
        at         → date_posted
        appVersion → app_version  (may be None/null)
        Source     → source  (preserved)

    Args:
        raw: A single review dict from the Play Store JSON file.

    Returns:
        A normalised dict ready for insertion into raw_reviews.
    """
    return {
        "source": "PlayStore",
        "author": raw.get("userName", ""),
        "rating": raw.get("score"),
        "body": raw.get("content", ""),
        "app_version": raw.get("appVersion"),  # may be None — stored as SQL NULL
        "date_posted": raw["at"],
    }


def normalise_trustpilot(raw: dict) -> dict:
    """Normalise a raw Trustpilot review dict into the shared schema.

    Source field mapping:
        username → author
        content  → body
        rating   → rating  (already int)
        date     → date_posted
        (no app_version — Trustpilot is not an app store)

    Args:
        raw: A single review dict from the Trustpilot JSON file.

    Returns:
        A normalised dict ready for insertion into raw_reviews.
    """
    return {
        "source": "Trustpilot",
        "author": raw.get("username", ""),
        "rating": raw.get("rating"),
        "body": raw.get("content", "") or "",
        "app_version": None,
        "date_posted": raw["date"],
    }


# ---------------------------------------------------------------------------
# Cursor management
# ---------------------------------------------------------------------------


def get_cursor_date(conn: sqlite3.Connection, source: str) -> str | None:
    """Return the last_fetched_date cursor for a given source, or None on first run.

    Args:
        conn: An open SQLite connection.
        source: "AppStore" or "PlayStore".

    Returns:
        ISO 8601 date string if a cursor exists, otherwise None.
    """
    row = conn.execute(
        "SELECT last_fetched_date FROM ingestion_cursors WHERE source = ?", (source,)
    ).fetchone()
    return row["last_fetched_date"] if row else None


def update_cursor(conn: sqlite3.Connection, source: str, max_date: str) -> None:
    """Upsert the ingestion cursor for a source to the most recent date seen.

    Args:
        conn: An open SQLite connection.
        source: "AppStore" or "PlayStore".
        max_date: ISO 8601 string of the most recent date_posted ingested.
    """
    conn.execute(
        "INSERT OR REPLACE INTO ingestion_cursors (source, last_fetched_date) VALUES (?, ?)",
        (source, max_date),
    )


# ---------------------------------------------------------------------------
# Deduplication + insertion
# ---------------------------------------------------------------------------


def review_exists(conn: sqlite3.Connection, source: str, review_id: str) -> bool:
    """Check whether a review already exists in raw_reviews.

    Args:
        conn: An open SQLite connection.
        source: "AppStore" or "PlayStore".
        review_id: SHA-256 hash of the review.

    Returns:
        True if the (source, review_id) pair is already in the table.
    """
    row = conn.execute(
        "SELECT 1 FROM raw_reviews WHERE source = ? AND review_id = ? LIMIT 1",
        (source, review_id),
    ).fetchone()
    return row is not None


def insert_review(
    conn: sqlite3.Connection, normalised: dict, review_id: str
) -> None:
    """Insert a single normalised review into raw_reviews.

    Sets ingested_at to the current UTC time. is_processed defaults to 0.

    Args:
        conn: An open SQLite connection.
        normalised: Output of normalise_appstore() or normalise_playstore().
        review_id: SHA-256 hash to use as the review_id column.
    """
    ingested_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT INTO raw_reviews
            (source, review_id, author, rating, body, app_version, date_posted, ingested_at)
        VALUES (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            normalised["source"],
            review_id,
            normalised["author"],
            normalised["rating"],
            normalised["body"],
            normalised["app_version"],
            normalised["date_posted"],
            ingested_at,
        ),
    )


# ---------------------------------------------------------------------------
# File loading
# ---------------------------------------------------------------------------


def load_json_file(file_path: str) -> list:
    """Load and parse a JSON array from a file.

    Args:
        file_path: Path to the JSON file.

    Returns:
        A list of review dicts.

    Raises:
        FileNotFoundError: If the file does not exist at file_path.
        ValueError: If the file does not contain a JSON array.
    """
    try:
        with open(file_path, encoding="utf-8") as f:
            data = json.load(f)
    except FileNotFoundError:
        raise FileNotFoundError(
            f"Review file not found: {file_path}\n"
            "Run the appropriate scraper first to generate the JSON file."
        )
    if not isinstance(data, list):
        raise ValueError(f"Expected a JSON array in {file_path}, got {type(data)}")
    return data


# ---------------------------------------------------------------------------
# Main ingestion orchestrator
# ---------------------------------------------------------------------------


def ingest_source(file_path: str, source: str, conn: sqlite3.Connection) -> int:
    """Ingest reviews from a JSON file for a given source.

    Steps:
    1. Load the JSON file.
    2. Fetch the cursor date (None on first run — ingest everything).
    3. For each review: compute ID, normalise, apply cursor filter,
       check for duplicates, insert.
    4. Update the cursor to the maximum date_posted seen.
    5. Commit and return the count of newly inserted reviews.

    Args:
        file_path: Path to the source JSON file.
        source: "AppStore" or "PlayStore".
        conn: An open SQLite connection.

    Returns:
        Number of reviews newly inserted in this run.
    """
    logger.info("Loading %s from %s", source, file_path)
    records = load_json_file(file_path)
    logger.info("Loaded %d records from file", len(records))

    cursor_date = get_cursor_date(conn, source)
    if cursor_date:
        logger.info("Cursor date for %s: %s (skipping older reviews)", source, cursor_date)
    else:
        logger.info("No cursor found for %s — ingesting all reviews", source)

    inserted = 0
    skipped_old = 0
    skipped_dup = 0
    max_date: str | None = None

    if source == "AppStore":
        normalise = normalise_appstore
    elif source == "PlayStore":
        normalise = normalise_playstore
    else:
        normalise = normalise_trustpilot

    for raw in records:
        try:
            review_id = compute_review_id(source, raw)
            normalised = normalise(raw)
            date_posted = normalised["date_posted"]

            # Cursor filter: skip reviews that are not newer than the last run
            if cursor_date and date_posted <= cursor_date:
                skipped_old += 1
                continue

            # Deduplication: skip reviews already in the database
            if review_exists(conn, source, review_id):
                skipped_dup += 1
                continue

            insert_review(conn, normalised, review_id)
            inserted += 1

            if max_date is None or date_posted > max_date:
                max_date = date_posted

        except Exception as exc:
            logger.warning("Skipping malformed record: %s — %r", exc, raw)

    if inserted > 0 and max_date:
        update_cursor(conn, source, max_date)
        logger.info("Cursor updated to %s", max_date)

    conn.commit()
    logger.info(
        "%s: inserted=%d  skipped_old=%d  skipped_dup=%d",
        source,
        inserted,
        skipped_old,
        skipped_dup,
    )
    return inserted


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Parse CLI arguments and run ingestion for a single source."""
    parser = argparse.ArgumentParser(
        description="Ingest app review JSON into the reviews.db SQLite database."
    )
    parser.add_argument("file_path", help="Path to the review JSON file")
    parser.add_argument(
        "source",
        choices=["AppStore", "PlayStore", "Trustpilot"],
        help="Source identifier matching the JSON schema",
    )
    args = parser.parse_args()

    conn = get_db_connection(DB_PATH)
    try:
        initialise_db(conn)
        count = ingest_source(args.file_path, args.source, conn)
        logger.info("Done — %d new reviews ingested.", count)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
