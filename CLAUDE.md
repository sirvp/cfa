# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Two pairs of scripts that scrape app reviews for the ScottishPower app and save them to a `Reviews/` output directory. Each scraper has a CSV variant and a JSON variant.

## Running the scripts

```bash
python AppStoreScraper.py       # CSV output
python AppStoreScraper_JSON.py  # JSON output

python PlayStoreScraper.py       # CSV output
python PlayStoreScraper_JSON.py  # JSON output
```

The `Reviews/` directory must exist before running — the scripts do not create it.

## Dependencies

- `requests`, `beautifulsoup4` — for App Store scraping
- `google-play-scraper` — for Play Store scraping
- `pandas` — used in Play Store scripts

## Key parameters (top of each script)

| Parameter | Description |
|-----------|-------------|
| `country` / `lang` / `region` | Store region (App Store uses `"gb"`, Play Store uses `"uk"`) |
| `app_id` | `"562202559"` (App Store) / `"uk.co.scottishpower"` (Play Store) |
| `review_count` / `count` | Max reviews to fetch |
| `period` | Days of history to include (default: 180) |

## Script pairs

The CSV and JSON variants are kept in sync — any change to scraping logic, filtering, or fields should be applied to both variants of the affected script.
