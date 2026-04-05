"""
run_pipeline.py — Orchestrates the full ScottishPower review intelligence pipeline.

Usage:
    python run_pipeline.py

Runs all three steps in sequence:
    1. Ingest — loads both JSON files into SQLite
    2. Analyse — sends unprocessed reviews to Claude Haiku
    3. Detect anomalies — flags unusual topic volume spikes

Each step is wrapped in a try/except so a failure in one step does not
prevent the remaining steps from running.

Requires environment variable: ANTHROPIC_API_KEY
"""

import logging

import anthropic

import analyse
import detect_anomalies
import ingest
from config import ANTHROPIC_API_KEY, APPSTORE_JSON_PATH, DB_PATH, PLAYSTORE_JSON_PATH

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Pipeline steps
# ---------------------------------------------------------------------------


def run_ingest(db_path: str) -> None:
    """Ingest reviews from both JSON files into SQLite.

    Opens a single DB connection, initialises tables, and ingests App Store
    reviews followed by Play Store reviews. Logs the count of new rows
    inserted for each source.

    Args:
        db_path: Path to the SQLite database file.
    """
    conn = ingest.get_db_connection(db_path)
    try:
        ingest.initialise_db(conn)

        for file_path, source in [
            (APPSTORE_JSON_PATH, "AppStore"),
            (PLAYSTORE_JSON_PATH, "PlayStore"),
        ]:
            try:
                count = ingest.ingest_source(file_path, source, conn)
                logger.info("Ingested %d new %s reviews.", count, source)
            except Exception as exc:
                logger.error("Ingest failed for %s (%s): %s", source, file_path, exc)
    finally:
        conn.close()


def run_analysis(db_path: str) -> None:
    """Run AI analysis on all unprocessed reviews.

    Instantiates the Anthropic client, opens the DB, and processes reviews
    in batches of config.BATCH_SIZE, writing results to the insights table.

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


def run_anomaly_detection(db_path: str) -> None:
    """Run anomaly detection and log any flagged topics.

    Opens the DB, initialises tables, and checks each configured topic for
    unusual volume in the last hour compared to the 7-day rolling average.

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
# Main orchestrator
# ---------------------------------------------------------------------------


def main() -> None:
    """Run all three pipeline steps in sequence.

    Each step is independently wrapped in error handling so a failure in one
    does not abort the subsequent steps.
    """
    logger.info("=== ScottishPower Review Pipeline ===")

    logger.info("Step 1/3: Ingesting reviews")
    try:
        run_ingest(DB_PATH)
    except Exception as exc:
        logger.error("Ingest step failed: %s", exc)

    logger.info("Step 2/3: Analysing reviews")
    try:
        run_analysis(DB_PATH)
    except Exception as exc:
        logger.error("Analysis step failed: %s", exc)

    logger.info("Step 3/3: Detecting anomalies")
    try:
        run_anomaly_detection(DB_PATH)
    except Exception as exc:
        logger.error("Anomaly detection step failed: %s", exc)

    logger.info("=== Pipeline complete ===")


if __name__ == "__main__":
    main()
