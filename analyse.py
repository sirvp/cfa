"""
analyse.py — AI-powered analysis of raw reviews using Claude Haiku.

Usage:
    python analyse.py

Queries raw_reviews for unprocessed rows, sends each review to Claude Haiku,
and writes structured insights (topics, sentiment, one-line summary) to the
insights table. Processed reviews are marked is_processed = 1.

Failed reviews (parse errors, API errors) stay is_processed = 0 and will be
retried on the next run.

Requires environment variable: ANTHROPIC_API_KEY
"""

import json
import logging
import re
import sqlite3
from datetime import datetime, timezone

import anthropic

from config import ANTHROPIC_API_KEY, BATCH_SIZE, DB_PATH, MODEL_NAME, TOPICS

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s",
)
logger = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Database helpers (duplicated from ingest.py for standalone runnability)
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
    """Create raw_reviews (if needed) and the insights table if they do not exist.

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

        CREATE TABLE IF NOT EXISTS insights (
            review_id    TEXT NOT NULL,
            source       TEXT NOT NULL,
            topics       TEXT NOT NULL,
            sentiment    TEXT NOT NULL,
            insight      TEXT NOT NULL,
            processed_at TEXT NOT NULL,
            PRIMARY KEY (review_id, source)
        );
        """
    )
    conn.commit()


# ---------------------------------------------------------------------------
# Data fetching
# ---------------------------------------------------------------------------


def fetch_unprocessed_reviews(conn: sqlite3.Connection) -> list:
    """Return all reviews not yet processed, ordered oldest-first.

    Args:
        conn: An open SQLite connection.

    Returns:
        List of sqlite3.Row objects with columns:
        id, review_id, source, body, rating, author.
    """
    rows = conn.execute(
        """
        SELECT id, review_id, source, body, rating, author
        FROM raw_reviews
        WHERE is_processed = 0
        ORDER BY date_posted ASC
        """
    ).fetchall()
    return rows


# ---------------------------------------------------------------------------
# Claude prompt + response
# ---------------------------------------------------------------------------


def build_prompt(review_body: str, rating: int | None) -> str:
    """Build the user message sent to Claude Haiku for a single review.

    The prompt is explicit about the required JSON structure to maximise
    reliable, parseable output.

    Args:
        review_body: The full text of the review.
        rating: Star rating (1–5), or None if unavailable.

    Returns:
        A formatted prompt string.
    """
    rating_str = f"{rating}/5" if rating is not None else "not provided"
    topics_str = ", ".join(TOPICS)
    example = (
        '{"topics": ["login", "app_crash"], '
        '"sentiment": "negative", '
        '"insight": "User cannot log in due to a persistent authentication bug."}'
    )
    return (
        f"Analyse this app review for a UK energy utility (ScottishPower).\n\n"
        f'Review text: "{review_body}"\n'
        f"Star rating: {rating_str}\n\n"
        f"Respond with ONLY a valid JSON object. No explanation, no markdown, no code fences.\n"
        f"The JSON must have exactly these three keys:\n"
        f'- "topics": an array containing only values from this list: [{topics_str}]. '
        f"Include all that apply. Use [\"other\"] if none fit.\n"
        f'- "sentiment": one of "positive", "neutral", "negative"\n'
        f'- "insight": a single sentence summarising the core issue or praise\n\n'
        f"Example output:\n{example}"
    )


def call_claude(client: anthropic.Anthropic, prompt: str) -> str:
    """Send a prompt to Claude Haiku and return the raw text response.

    Args:
        client: An initialised Anthropic client.
        prompt: The user message to send.

    Returns:
        The raw text content of Claude's response.

    Raises:
        anthropic.APIError: On any API-level failure.
    """
    message = client.messages.create(
        model=MODEL_NAME,
        max_tokens=256,
        messages=[{"role": "user", "content": prompt}],
    )
    return message.content[0].text


def parse_claude_response(raw_text: str) -> dict:
    """Parse and validate Claude's JSON response.

    Attempts direct JSON parsing first. Falls back to regex extraction of
    the first {...} block if the model wrapped output in markdown fences.
    Validates required keys and filters topic values to the allowed list.

    Args:
        raw_text: The raw string returned by Claude.

    Returns:
        A dict with keys: topics (list), sentiment (str), insight (str).

    Raises:
        ValueError: If the response cannot be parsed or required keys are missing.
    """
    # Stage 1: direct parse
    parsed = None
    try:
        parsed = json.loads(raw_text.strip())
    except json.JSONDecodeError:
        pass

    # Stage 2: find every top-level balanced {...} block and try each in order.
    # This handles the case where the model outputs two JSON objects separated by
    # self-correction prose — the greedy \{.*\} approach would merge them.
    if parsed is None:
        i = 0
        while i < len(raw_text) and parsed is None:
            if raw_text[i] == "{":
                depth = 0
                for j in range(i, len(raw_text)):
                    if raw_text[j] == "{":
                        depth += 1
                    elif raw_text[j] == "}":
                        depth -= 1
                        if depth == 0:
                            try:
                                parsed = json.loads(raw_text[i : j + 1])
                            except json.JSONDecodeError:
                                pass
                            i = j
                            break
            i += 1

    if parsed is None:
        raise ValueError(f"Could not extract JSON from response: {raw_text!r}")

    # Validate required keys
    for key in ("topics", "sentiment", "insight"):
        if key not in parsed:
            raise ValueError(f"Missing key '{key}' in Claude response: {parsed}")

    # Validate sentiment value
    valid_sentiments = {"positive", "neutral", "negative"}
    if parsed["sentiment"] not in valid_sentiments:
        raise ValueError(
            f"Invalid sentiment '{parsed['sentiment']}' — must be one of {valid_sentiments}"
        )

    # Filter topics to the allowed set, silently dropping hallucinated values
    parsed["topics"] = [t for t in parsed["topics"] if t in TOPICS]
    if not parsed["topics"]:
        parsed["topics"] = ["other"]

    return parsed


# ---------------------------------------------------------------------------
# Database writes
# ---------------------------------------------------------------------------


def write_insight(
    conn: sqlite3.Connection,
    review_id: str,
    source: str,
    parsed: dict,
) -> None:
    """Insert an analysis result into the insights table.

    Uses INSERT OR IGNORE so re-running analyse.py never creates duplicates
    (is_processed = 1 normally prevents re-processing, but this is a safety net).

    Args:
        conn: An open SQLite connection.
        review_id: The SHA-256 review identifier.
        source: "AppStore" or "PlayStore".
        parsed: Dict with keys topics, sentiment, insight (from parse_claude_response).
    """
    processed_at = datetime.now(timezone.utc).isoformat()
    conn.execute(
        """
        INSERT OR IGNORE INTO insights
            (review_id, source, topics, sentiment, insight, processed_at)
        VALUES (?, ?, ?, ?, ?, ?)
        """,
        (
            review_id,
            source,
            json.dumps(parsed["topics"]),
            parsed["sentiment"],
            parsed["insight"],
            processed_at,
        ),
    )


def mark_processed(
    conn: sqlite3.Connection, review_id: str, source: str
) -> None:
    """Mark a review as processed so it is not re-sent to Claude.

    Args:
        conn: An open SQLite connection.
        review_id: The SHA-256 review identifier.
        source: "AppStore" or "PlayStore".
    """
    conn.execute(
        "UPDATE raw_reviews SET is_processed = 1 WHERE review_id = ? AND source = ?",
        (review_id, source),
    )


# ---------------------------------------------------------------------------
# Batch processing
# ---------------------------------------------------------------------------


def process_batch(
    client: anthropic.Anthropic,
    conn: sqlite3.Connection,
    batch: list,
) -> tuple[int, int]:
    """Process one batch of unprocessed reviews.

    Each review is handled individually. A failure (API error, parse error)
    is logged and counted but does not abort the rest of the batch.
    Commits to the database after each successful review so that a mid-batch
    crash does not roll back already-processed rows.

    Args:
        client: An initialised Anthropic client.
        conn: An open SQLite connection.
        batch: A list of sqlite3.Row objects from fetch_unprocessed_reviews.

    Returns:
        A (success_count, failure_count) tuple.
    """
    success = 0
    failure = 0

    for row in batch:
        review_id = row["review_id"]
        source = row["source"]
        try:
            prompt = build_prompt(row["body"] or "", row["rating"])
            raw = call_claude(client, prompt)
            parsed = parse_claude_response(raw)
            write_insight(conn, review_id, source, parsed)
            mark_processed(conn, review_id, source)
            conn.commit()
            success += 1
        except Exception as exc:
            logger.error(
                "Failed to process review_id=%s source=%s: %s",
                review_id,
                source,
                exc,
            )
            failure += 1

    return success, failure


# ---------------------------------------------------------------------------
# Top-level analysis runner
# ---------------------------------------------------------------------------


def run_analysis(conn: sqlite3.Connection, client: anthropic.Anthropic) -> None:
    """Fetch all unprocessed reviews and process them in batches.

    Logs total success and failure counts after all batches complete.

    Args:
        conn: An open SQLite connection.
        client: An initialised Anthropic client.
    """
    rows = fetch_unprocessed_reviews(conn)
    total = len(rows)
    logger.info("Found %d unprocessed reviews", total)

    if total == 0:
        logger.info("Nothing to process.")
        return

    total_success = 0
    total_failure = 0

    for batch_start in range(0, total, BATCH_SIZE):
        batch = rows[batch_start : batch_start + BATCH_SIZE]
        batch_num = batch_start // BATCH_SIZE + 1
        logger.info(
            "Processing batch %d (%d reviews)…",
            batch_num,
            len(batch),
        )
        success, failure = process_batch(client, conn, batch)
        total_success += success
        total_failure += failure
        logger.info(
            "Batch %d complete — success: %d  failure: %d",
            batch_num,
            success,
            failure,
        )

    logger.info(
        "Analysis complete — total processed: %d  total failed: %d",
        total_success,
        total_failure,
    )


# ---------------------------------------------------------------------------
# CLI entry point
# ---------------------------------------------------------------------------


def main() -> None:
    """Initialise the Anthropic client and run analysis on unprocessed reviews."""
    if not ANTHROPIC_API_KEY:
        logger.error(
            "ANTHROPIC_API_KEY environment variable is not set. "
            "Export it before running: export ANTHROPIC_API_KEY=sk-ant-..."
        )
        return

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    conn = get_db_connection(DB_PATH)
    try:
        initialise_db(conn)
        run_analysis(conn, client)
    except anthropic.AuthenticationError:
        logger.error(
            "Authentication failed — check that ANTHROPIC_API_KEY is valid."
        )
    finally:
        conn.close()


if __name__ == "__main__":
    main()
