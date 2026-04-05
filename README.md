# ScottishPower Review Intelligence Pipeline

A local Python pipeline that ingests ScottishPower app reviews from JSON files,
analyses them with Claude Haiku, and detects unusual spikes in topic volume.

---

## Prerequisites

- Python 3.10 or newer
- SQLite 3.38 or newer (ships with Python 3.12+; required for `json_each()`)
- An Anthropic API key

---

## Installation

```bash
pip install -r requirements.txt
```

---

## Environment variables

| Variable | Required | Description |
|---|---|---|
| `ANTHROPIC_API_KEY` | Yes | Your Anthropic API key. Used by `analyse.py` and `run_pipeline.py`. |
| `SLACK_WEBHOOK_URL` | No | Slack incoming webhook URL. Activates alerts in `detect_anomalies.py` (see placeholder in `send_alert()`). |

Set them before running:

```bash
export ANTHROPIC_API_KEY=sk-ant-...
export SLACK_WEBHOOK_URL=https://hooks.slack.com/...   # optional
```

---

## Running the full pipeline

```bash
python run_pipeline.py
```

This runs all three steps in order:
1. Ingest both JSON files
2. Analyse unprocessed reviews
3. Detect anomalies

---

## Running each step individually

### 1. Ingest

Load a review JSON file into `reviews.db`:

```bash
python ingest.py Reviews/ScottishPower_AppStore_Reviews_2025-10-07_180days.json AppStore
python ingest.py Reviews/ScottishPower_Playstore_Reviews_2025-10-07_180days.json PlayStore
```

On the first run all reviews in the file are ingested and a cursor is saved.
Subsequent runs only ingest reviews newer than the cursor, and duplicates are skipped.

### 2. Analyse

Send unprocessed reviews to Claude Haiku and write insights:

```bash
python analyse.py
```

Each review is tagged with topics, a sentiment label, and a one-sentence summary.
Failed reviews stay unprocessed and are retried on the next run.

### 3. Detect anomalies

Check for unusual topic volume spikes in the last hour:

```bash
python detect_anomalies.py
```

Flags any topic where the last-hour count exceeds twice the 7-day rolling hourly average.
Results are printed to the console and written to the `anomaly_log` table.

---

## Configuration

All tuneable constants live in `config.py`:

| Constant | Default | Description |
|---|---|---|
| `BATCH_SIZE` | `25` | Reviews per Claude API batch |
| `MODEL_NAME` | `claude-haiku-4-5-20251001` | Claude model used for analysis |
| `DB_PATH` | `reviews.db` | SQLite database file path |
| `APPSTORE_JSON_PATH` | `Reviews/ScottishPower_AppStore_Reviews_2025-10-07_180days.json` | App Store input file |
| `PLAYSTORE_JSON_PATH` | `Reviews/ScottishPower_Playstore_Reviews_2025-10-07_180days.json` | Play Store input file |
| `TOPICS` | `[login, billing, app_crash, ...]` | Fixed topic taxonomy for analysis |

To point the pipeline at a new scraper output, update the `*_JSON_PATH` constants in `config.py`.

---

## Database schema

`reviews.db` contains four tables:

**`raw_reviews`** — normalised reviews from both sources

| Column | Type | Notes |
|---|---|---|
| `id` | INTEGER | Auto-increment primary key |
| `source` | TEXT | `AppStore` or `PlayStore` |
| `review_id` | TEXT | SHA-256 hash of author + date + content |
| `author` | TEXT | Reviewer name |
| `rating` | INTEGER | Star rating 1–5 |
| `body` | TEXT | Review text |
| `app_version` | TEXT | App version reviewed (nullable) |
| `date_posted` | TEXT | ISO 8601 datetime |
| `ingested_at` | TEXT | UTC datetime of ingestion |
| `is_processed` | INTEGER | `0` = unprocessed, `1` = analysed |

**`ingestion_cursors`** — tracks the last ingested date per source

**`insights`** — Claude Haiku analysis results

| Column | Type | Notes |
|---|---|---|
| `review_id` | TEXT | FK to raw_reviews.review_id |
| `source` | TEXT | `AppStore` or `PlayStore` |
| `topics` | TEXT | JSON array e.g. `["login","app_crash"]` |
| `sentiment` | TEXT | `positive`, `neutral`, or `negative` |
| `insight` | TEXT | One-sentence summary |
| `processed_at` | TEXT | UTC datetime of analysis |

**`anomaly_log`** — append-only log of detected anomalies
