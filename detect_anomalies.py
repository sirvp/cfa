"""
detect_anomalies.py — Detect unusual spikes in review topic volume.

Usage:
    python detect_anomalies.py

For each topic in config.TOPICS, computes the 7-day rolling hourly average
from the insights table and flags any topic whose count in the last 1 hour
exceeds twice that average. Anomalies are printed to the console and written
to the anomaly_log table.

SQLite's json_each() function (available since SQLite 3.38) is used to
unnest the topics JSON array stored in the insights table.
"""

import logging
import sqlite3
from datetime import datetime, timezone

import config
from config import DB_PATH, TOPICS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database helpers (duplicated for standalone runnability)
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
    """Create the insights and anomaly_log tables if they do not exist.

    Safe to call on every run — uses CREATE TABLE IF NOT EXISTS.

    Args:
        conn: An open SQLite connection.
    """
    conn.executescript(
        """
        CREATE TABLE IF NOT EXISTS insights (
            review_id    TEXT NOT NULL,
            source       TEXT NOT NULL,
            topics       TEXT NOT NULL,
            sentiment    TEXT NOT NULL,
            insight      TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            PRIMARY KEY (review_id, source)
        );

        CREATE TABLE IF NOT EXISTS anomaly_log (
            id            INTEGER PRIMARY KEY AUTOINCREMENT,
            topic         TEXT    NOT NULL,
            source        TEXT    NOT NULL,
            current_count INTEGER NOT NULL,
            expected_avg  REAL    NOT NULL,
            detected_at   TEXT    NOT NULL
        );
        """
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Volume queries
# ---------------------------------------------------------------------------


def compute_rolling_average(
    conn: sqlite3.Connection, topic: str
) -> dict[str, float]:
    """Compute the 7-day rolling hourly average volume for a topic per source.

    Uses SQLite's json_each() to unnest the topics JSON array in each row,
    then counts occurrences in the last 7 days and divides by 168 (7 × 24).

    Args:
        conn: An open SQLite connection.
        topic: A topic string from config.TOPICS.

    Returns:
        Dict mapping source name to hourly average, e.g.
        {"AppStore": 0.42, "PlayStore": 1.1}
    """
    rows = conn.execute(
        """
        SELECT
            i.source,
            COUNT(*) * 1.0 / (7 * 24) AS hourly_avg
        FROM insights i, json_each(i.topics) je
        WHERE je.value = ?
          AND i.processed_at >= datetime('now', '-7 days')
        GROUP BY i.source
        """,
        (topic,),
    ).fetchall()
    return {row["source"]: row["hourly_avg"] for row in rows}


def get_last_hour_counts(
    conn: sqlite3.Connection, topic: str
) -> dict[str, int]:
    """Count how many insights for a topic were written in the last 1 hour.

    Uses SQLite's json_each() to unnest the topics JSON array.

    Args:
        conn: An open SQLite connection.
        topic: A topic string from config.TOPICS.

    Returns:
        Dict mapping source name to count, e.g.
        {"AppStore": 0, "PlayStore": 3}
    """
    rows = conn.execute(
        """
        SELECT
            i.source,
            COUNT(*) AS cnt
        FROM insights i, json_each(i.topics) je
        WHERE je.value = ?
          AND i.processed_at >= datetime('now', '-1 hour')
        GROUP BY i.source
        """,
        (topic,),
    ).fetchall()
    return {row["source"]: row["cnt"] for row in rows}


# ---------------------------------------------------------------------------
# Anomaly detection logic
# ---------------------------------------------------------------------------


def detect_anomalies_for_topic(
    conn: sqlite3.Connection, topic: str
) -> list[dict]:
    """Detect anomalies for a single topic across all sources.

    An anomaly is raised when:
        current_count (last 1 h) > 2 × expected_avg (7-day rolling hourly)

    The expected_avg guard (> 0) prevents false alarms when there is no
    7-day history baseline for a source.

    Args:
        conn: An open SQLite connection.
        topic: A topic string from config.TOPICS.

    Returns:
        List of anomaly dicts, one per flagged (topic, source) combination.
        Each dict has: topic, source, current_count, expected_avg, detected_at.
    """
    averages = compute_rolling_average(conn, topic)
    counts = get_last_hour_counts(conn, topic)

    detected_at = datetime.now(timezone.utc).isoformat()
    anomalies = []

    # Check all sources that have a recent count
    all_sources = set(averages.keys()) | set(counts.keys())
    for source in all_sources:
        current_count = counts.get(source, 0)
        expected_avg = averages.get(source, 0.0)

        if expected_avg > 0 and current_count > 2 * expected_avg:
            anomalies.append(
                {
                    "topic": topic,
                    "source": source,
                    "current_count": current_count,
                    "expected_avg": expected_avg,
                    "detected_at": detected_at,
                }
            )

    return anomalies


# ---------------------------------------------------------------------------
# Persistence + alerting
# ---------------------------------------------------------------------------


def write_anomaly(conn: sqlite3.Connection, anomaly: dict) -> None:
    """Append an anomaly record to the anomaly_log table.

    The log is append-only — no deduplication is applied, so each detection
    run can record its own snapshot.

    Args:
        conn: An open SQLite connection.
        anomaly: Dict with keys: topic, source, current_count, expected_avg, detected_at.
    """
    conn.execute(
        """
        INSERT INTO anomaly_log
            (topic, source, current_count, expected_avg, detected_at)
        VALUES (?, ?, ?, ?, ?)
        """,
        (
            anomaly["topic"],
            anomaly["source"],
            anomaly["current_count"],
            anomaly["expected_avg"],
            anomaly["detected_at"],
        ),
    )


def send_alert(anomaly: dict) -> None:
    """Placeholder for external alerting (e.g. Slack webhook).

    To activate Slack alerts:
    1. Set the SLACK_WEBHOOK_URL environment variable.
    2. Uncomment the requests block below.

    Args:
        anomaly: Dict with anomaly details (topic, source, counts, detected_at).
    """
    # import requests
    # if config.SLACK_WEBHOOK_URL:
    #     ratio = anomaly["current_count"] / anomaly["expected_avg"]
    #     payload = {
    #         "text": (
    #             f":rotating_light: *Review anomaly detected*\n"
    #             f"Topic: `{anomaly['topic']}`  |  Source: {anomaly['source']}\n"
    #             f"Last 1h: {anomaly['current_count']}  |  "
    #             f"7-day avg: {anomaly['expected_avg']:.2f}/h  |  "
    #             f"Ratio: {ratio:.1f}x\n"
    #             f"Detected at: {anomaly['detected_at']}"
    #         )
    #     }
    #     requests.post(config.SLACK_WEBHOOK_URL, json=payload, timeout=5)
    pass


def print_anomaly(anomaly: dict) -> None:
    """Print a formatted anomaly report to the console.

    Args:
        anomaly: Dict with keys: topic, source, current_count, expected_avg, detected_at.
    """
    ratio = anomaly["current_count"] / anomaly["expected_avg"]
    print(
        f"\nANOMALY DETECTED\n"
        f"  Topic:         {anomaly['topic']}\n"
        f"  Source:        {anomaly['source']}\n"
        f"  Last 1h count: {anomaly['current_count']}\n"
        f"  Expected avg:  {anomaly['expected_avg']:.2f} per hour (7-day rolling)\n"
        f"  Ratio:         {ratio:.1f}x\n"
        f"  Detected at:   {anomaly['detected_at']}"
    )


# ---------------------------------------------------------------------------
# Top-level detection runner
# ---------------------------------------------------------------------------


def run_detection(conn: sqlite3.Connection) -> None:
    """Run anomaly detection across all configured topics.

    For each topic in config.TOPICS: detect anomalies, print them to the
    console, write them to anomaly_log, and call send_alert().

    Args:
        conn: An open SQLite connection.
    """
    total_anomalies = 0

    for topic in TOPICS:
        anomalies = detect_anomalies_for_topic(conn, topic)
        for anomaly in anomalies:
            print_anomaly(anomaly)
            write_anomaly(conn, anomaly)
            send_alert(anomaly)
            total_anomalies += 1

    conn.commit()

    if total_anomalies == 0:
        logger.info("No anomalies detected.")
    else:
        logger.info("Anomaly detection complete — %d anomalies flagged.", total_anomalies)


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Open the database and run anomaly detection."""
    conn = get_db_connection(DB_PATH)
    try:
        initialise_db(conn)
        run_detection(conn)
    finally:
        conn.close()


if __name__ == "__main__":
    main()
