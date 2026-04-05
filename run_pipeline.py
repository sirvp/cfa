"""
run_pipeline.py — Orchestrates the full ScottishPower review intelligence pipeline.

Usage:
    python run_pipeline.py

Daily sequence (run automatically via Windows Task Scheduler):
    1. Scrape  — runs both scraper scripts to pull fresh reviews into Reviews/
    2. Ingest  — loads the latest JSON files into reviews.db (cursor-incremental)
    3. Analyse — sends unprocessed reviews to Claude Haiku
    4. Detect  — flags anomalous topic spikes
    5. Publish — exports DB to data/reviews_analysed.csv and git pushes to GitHub
                 so Streamlit Cloud picks up the latest data

Each step is wrapped in a try/except; a failure in one step does not
prevent the remaining steps from running.

Requires environment variable: ANTHROPIC_API_KEY
"""

import glob
import logging
import os
import sqlite3
import subprocess
import sys
from pathlib import Path

import anthropic
import pandas as pd

import analyse
import detect_anomalies
import ingest
from config import (
    ANTHROPIC_API_KEY,
    APPSTORE_JSON_GLOB,
    APPSTORE_SCRAPER,
    CSV_EXPORT_PATH,
    DB_PATH,
    PLAYSTORE_JSON_GLOB,
    PLAYSTORE_SCRAPER,
)

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Helper
# ---------------------------------------------------------------------------


def find_latest_json(pattern: str) -> str:
    """Return the most recently modified JSON file matching a glob pattern.

    Args:
        pattern: A glob pattern, e.g. 'Reviews/ScottishPower_AppStore_*.json'.

    Returns:
        Absolute path of the newest matching file.

    Raises:
        FileNotFoundError: If no files match the pattern.
    """
    matches = glob.glob(pattern)
    if not matches:
        raise FileNotFoundError(
            f"No files found matching '{pattern}'. Run the scraper first."
        )
    return max(matches, key=os.path.getmtime)


# ---------------------------------------------------------------------------
# Step 1 — Scrape
# ---------------------------------------------------------------------------


def run_scrapers() -> None:
    """Run both scraper scripts to pull fresh reviews from each store.

    Each scraper writes a dated JSON file into the Reviews/ directory.
    Failures are logged but do not abort the pipeline — stale files from
    a previous run will still be ingested.

    Args: none
    """
    for script in (APPSTORE_SCRAPER, PLAYSTORE_SCRAPER):
        logger.info("Running scraper: %s", script)
        result = subprocess.run(
            [sys.executable, script],
            capture_output=True,
            text=True,
        )
        if result.returncode == 0:
            logger.info("%s completed. %s", script, result.stdout.strip())
        else:
            logger.error(
                "%s failed (exit %d):\n%s",
                script,
                result.returncode,
                result.stderr.strip(),
            )


# ---------------------------------------------------------------------------
# Step 2 — Ingest
# ---------------------------------------------------------------------------


def run_ingest(db_path: str) -> None:
    """Ingest the latest scraped JSON files into SQLite.

    Finds the most recently modified file for each source, then uses the
    ingestion cursor to insert only reviews newer than the last run.

    Args:
        db_path: Path to the SQLite database file.
    """
    conn = ingest.get_db_connection(db_path)
    try:
        ingest.initialise_db(conn)
        for glob_pattern, source in [
            (APPSTORE_JSON_GLOB, "AppStore"),
            (PLAYSTORE_JSON_GLOB, "PlayStore"),
        ]:
            try:
                file_path = find_latest_json(glob_pattern)
                logger.info("Ingesting %s from %s", source, file_path)
                count = ingest.ingest_source(file_path, source, conn)
                logger.info("Ingested %d new %s reviews.", count, source)
            except Exception as exc:
                logger.error("Ingest failed for %s: %s", source, exc)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Step 3 — Analyse
# ---------------------------------------------------------------------------


def run_analysis(db_path: str) -> None:
    """Send unprocessed reviews to Claude Haiku and write insights to the DB.

    Args:
        db_path: Path to the SQLite database file.
    """
    if not ANTHROPIC_API_KEY:
        logger.error(
            "ANTHROPIC_API_KEY is not set — skipping analysis. "
            "Export it before running: export ANTHROPIC_API_KEY=sk-ant-..."
        )
        return

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    conn = analyse.get_db_connection(db_path)
    try:
        analyse.initialise_db(conn)
        analyse.run_analysis(conn, client)
    except anthropic.AuthenticationError:
        logger.error("Anthropic authentication failed — check ANTHROPIC_API_KEY.")
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Step 4 — Detect anomalies
# ---------------------------------------------------------------------------


def run_anomaly_detection(db_path: str) -> None:
    """Check for unusual topic volume spikes and write to anomaly_log.

    Args:
        db_path: Path to the SQLite database file.
    """
    conn = detect_anomalies.get_db_connection(db_path)
    try:
        detect_anomalies.initialise_db(conn)
        detect_anomalies.run_detection(conn)
    finally:
        conn.close()


# ---------------------------------------------------------------------------
# Step 5 — Publish
# ---------------------------------------------------------------------------


def export_to_csv(db_path: str, csv_path: str) -> int:
    """Export the joined raw_reviews + insights view to a CSV file.

    Only exports rows that have been analysed (is_processed = 1).
    Overwrites the existing CSV so Streamlit Cloud always reads the
    full up-to-date dataset.

    Args:
        db_path: Path to the SQLite database file.
        csv_path: Destination CSV path (e.g. 'data/reviews_analysed.csv').

    Returns:
        Number of rows written.
    """
    conn = sqlite3.connect(db_path)
    try:
        df = pd.read_sql_query(
            """
            SELECT
                r.review_id, r.source, r.author, r.rating, r.body,
                r.app_version, r.date_posted,
                i.topics, i.sentiment, i.insight
            FROM raw_reviews r
            JOIN insights i USING (review_id, source)
            WHERE r.is_processed = 1
            ORDER BY r.date_posted DESC
            """,
            conn,
        )
    finally:
        conn.close()

    Path(csv_path).parent.mkdir(parents=True, exist_ok=True)
    df.to_csv(csv_path, index=False)
    logger.info("Exported %d rows to %s", len(df), csv_path)
    return len(df)


def git_commit_and_push(csv_path: str) -> None:
    """Stage the updated CSV, commit, and push to origin/main.

    This keeps the Streamlit Cloud deployment in sync — it reads the CSV
    since reviews.db is gitignored and never deployed.

    Args:
        csv_path: Path to the CSV file to commit.
    """
    from datetime import date
    today = date.today().isoformat()

    cmds = [
        ["git", "add", csv_path],
        ["git", "commit", "-m", f"chore: daily review data refresh {today}"],
        ["git", "push", "origin", "main"],
    ]
    for cmd in cmds:
        result = subprocess.run(cmd, capture_output=True, text=True)
        if result.returncode == 0:
            logger.info("git %s: OK", cmd[1])
        else:
            # 'nothing to commit' exits non-zero but is not an error
            if "nothing to commit" in result.stdout + result.stderr:
                logger.info("git commit: nothing new to commit.")
            else:
                logger.error(
                    "git %s failed (exit %d): %s",
                    cmd[1],
                    result.returncode,
                    result.stderr.strip(),
                )


def run_publish(db_path: str, csv_path: str) -> None:
    """Export DB to CSV and push to GitHub so Streamlit Cloud updates.

    Args:
        db_path: Path to the SQLite database file.
        csv_path: Destination CSV path.
    """
    rows = export_to_csv(db_path, csv_path)
    if rows > 0:
        git_commit_and_push(csv_path)
    else:
        logger.warning("No analysed rows to export — skipping git push.")


# ---------------------------------------------------------------------------
# Main orchestrator
# ---------------------------------------------------------------------------


def main() -> None:
    """Run the full daily pipeline: scrape → ingest → analyse → detect → publish."""
    logger.info("=== ScottishPower Review Pipeline ===")

    logger.info("Step 1/5: Scraping fresh reviews")
    try:
        run_scrapers()
    except Exception as exc:
        logger.error("Scrape step failed: %s", exc)

    logger.info("Step 2/5: Ingesting reviews")
    try:
        run_ingest(DB_PATH)
    except Exception as exc:
        logger.error("Ingest step failed: %s", exc)

    logger.info("Step 3/5: Analysing reviews")
    try:
        run_analysis(DB_PATH)
    except Exception as exc:
        logger.error("Analysis step failed: %s", exc)

    logger.info("Step 4/5: Detecting anomalies")
    try:
        run_anomaly_detection(DB_PATH)
    except Exception as exc:
        logger.error("Anomaly detection step failed: %s", exc)

    logger.info("Step 5/5: Publishing to GitHub")
    try:
        run_publish(DB_PATH, CSV_EXPORT_PATH)
    except Exception as exc:
        logger.error("Publish step failed: %s", exc)

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
