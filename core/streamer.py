"""Continuous streaming pipeline for arXiv papers.

Replaces the file-watcher approach with a polling loop that:
    1. Fetches new papers from the arXiv Atom API (or RSS feed).
    2. Writes them to a temporary JSONL file.
    3. Runs the existing bronze -> silver -> gold pipeline stages.
    4. Tracks the last fetch timestamp in the database so only new papers are ingested.

Usage:
    python -m core.streamer              # Stream from arXiv API (default)
    python -m core.streamer --rss      # Stream from arXiv RSS feed
    python -m core.streamer --query "cat:cs.CL"  # Custom search query
"""

import argparse
import json
import logging
import os
import signal
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path
from typing import Any

from core.arxiv_client import fetch_all_since, fetch_latest_rss_batch
from core.db import check_connection, get_cursor
from pipeline.bronze import ingest_jsonl
from pipeline.gold import process_silver_to_gold
from pipeline.silver import process_bronze_to_silver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)

# --- Configurable via env vars ---
_poll_interval_seconds = int(os.environ.get("STREAM_INTERVAL_SECONDS", "300"))  # 5 min default
STREAM_BATCH_SIZE = int(os.environ.get("STREAM_BATCH_SIZE", "100"))
STREAM_MAX_TOTAL = int(os.environ.get("STREAM_MAX_TOTAL", "1000"))
STREAM_RSS_CATEGORY = os.environ.get("STREAM_RSS_CATEGORY", "cs")
STREAM_RSS_MAX_PAPERS = int(os.environ.get("STREAM_RSS_MAX_PAPERS", "50"))

# Graceful shutdown flag
_shutdown_requested = False


def _signal_handler(_sig: int, _frame: Any) -> None:
    global _shutdown_requested
    log.info("Shutdown requested (Ctrl+C). Finishing current cycle...")
    _shutdown_requested = True


signal.signal(signal.SIGINT, _signal_handler)
signal.signal(signal.SIGTERM, _signal_handler)


def _ensure_state_table() -> None:
    """Create the streaming state table if it doesn't exist."""
    with get_cursor() as cur:
        cur.execute("""
            CREATE TABLE IF NOT EXISTS streaming.state (
                key TEXT PRIMARY KEY,
                value TEXT NOT NULL,
                updated_at TIMESTAMPTZ DEFAULT NOW()
            )
        """)


def _get_state(key: str, default: str | None = None) -> str | None:
    """Read a value from the streaming state table."""
    with get_cursor(commit=False) as cur:
        cur.execute("SELECT value FROM streaming.state WHERE key = %s", (key,))
        row = cur.fetchone()
        return row[0] if row else default


def _set_state(key: str, value: str) -> None:
    """Write a value to the streaming state table."""
    with get_cursor() as cur:
        cur.execute("""
            INSERT INTO streaming.state (key, value, updated_at)
            VALUES (%s, %s, NOW())
            ON CONFLICT (key) DO UPDATE SET value = EXCLUDED.value, updated_at = NOW()
        """, (key, value))


def _get_last_fetch_time() -> datetime:
    """Return the timestamp of the last successful fetch.

    Defaults to 24 hours ago on first run.
    """
    raw = _get_state("last_fetch_time")
    if raw:
        try:
            # Stored as ISO format with timezone
            return datetime.fromisoformat(raw)
        except ValueError:
            log.warning("Invalid last_fetch_time in DB: %s", raw)

    # Default: look back 24 hours on first run
    default = datetime.now(timezone.utc) - timedelta(hours=24)
    log.info("No prior fetch timestamp found; defaulting to 24h ago: %s", default.isoformat())
    return default


def _set_last_fetch_time(dt: datetime) -> None:
    _set_state("last_fetch_time", dt.isoformat())


def _run_pipeline_stages_for_file(filepath: Path) -> tuple[int, int, int]:
    """Execute bronze -> silver -> gold for a single JSONL file.

    This mirrors core.watcher.run_pipeline_stages but does not move the file.
    """
    log.info("=" * 60)
    log.info("Streaming pipeline started for: %s", filepath.name)
    log.info("=" * 60)

    start = time.time()

    # Bronze
    log.info("--- Stage 1: Bronze (raw ingestion) ---")
    bronze_count = ingest_jsonl(filepath)
    log.info("Bronze complete: %d records ingested", bronze_count)

    # Silver
    log.info("--- Stage 2: Silver (cleaning & parsing) ---")
    silver_count = process_bronze_to_silver()
    log.info("Silver complete: %d records processed", silver_count)

    # Gold
    log.info("--- Stage 3: Gold (embedding) ---")
    gold_count = process_silver_to_gold()
    log.info("Gold complete: %d records embedded", gold_count)

    elapsed = time.time() - start
    log.info("=" * 60)
    log.info(
        "Pipeline finished in %.1fs - bronze=%d, silver=%d, gold=%d",
        elapsed,
        bronze_count,
        silver_count,
        gold_count,
    )
    log.info("=" * 60)

    return bronze_count, silver_count, gold_count


def _process_api_batches(
    search_query: str,
    since: datetime,
    batch_size: int,
    max_total: int,
) -> tuple[int, int, int, datetime]:
    """Fetch and process all arXiv API batches since the given datetime.

    Returns cumulative (bronze, silver, gold, newest_published) where
    newest_published is the latest <published> timestamp seen across all
    batches, allowing the caller to advance its checkpoint precisely.
    """
    total_bronze = 0
    total_silver = 0
    total_gold = 0
    overall_newest = since

    for batch, newest_published in fetch_all_since(
        since=since,
        search_query=search_query,
        batch_size=batch_size,
        max_total=max_total,
    ):
        if _shutdown_requested:
            log.info("Shutdown requested; stopping batch processing.")
            break

        if not batch:
            break

        if newest_published > overall_newest:
            overall_newest = newest_published

        with tempfile.NamedTemporaryFile(
            mode="w",
            suffix=".jsonl",
            prefix="arxiv_stream_",
            delete=False,
            encoding="utf-8",
        ) as tmp:
            tmp_path = Path(tmp.name)
            for record in batch:
                tmp.write(json.dumps(record, ensure_ascii=False) + "\n")

        try:
            b, s, g = _run_pipeline_stages_for_file(tmp_path)
            total_bronze += b
            total_silver += s
            total_gold += g
        finally:
            try:
                tmp_path.unlink()
            except OSError as e:
                log.warning("Failed to delete temp file %s: %s", tmp_path, e)

    return total_bronze, total_silver, total_gold, overall_newest


def _process_rss_batch(category: str, max_papers: int) -> tuple[int, int, int]:
    """Fetch and process a single RSS batch.

    Returns (bronze, silver, gold) counts.
    """
    records = fetch_latest_rss_batch(category=category, max_papers=max_papers)
    if not records:
        return 0, 0, 0

    with tempfile.NamedTemporaryFile(
        mode="w",
        suffix=".jsonl",
        prefix="arxiv_rss_",
        delete=False,
        encoding="utf-8",
    ) as tmp:
        tmp_path = Path(tmp.name)
        for record in records:
            tmp.write(json.dumps(record, ensure_ascii=False) + "\n")

    try:
        return _run_pipeline_stages_for_file(tmp_path)
    finally:
        try:
            tmp_path.unlink()
        except OSError as e:
            log.warning("Failed to delete temp file %s: %s", tmp_path, e)


def run_streaming_loop(
    use_rss: bool = False,
    search_query: str = "",
) -> None:
    """Main streaming loop: poll arXiv, ingest, sleep, repeat."""
    log.info("Checking database connection...")
    if not check_connection():
        log.error("Cannot connect to database. Is PostgreSQL running?")
        sys.exit(1)
    log.info("Database connection OK")

    # Ensure streaming schema and state table exist
    with get_cursor() as cur:
        cur.execute("CREATE SCHEMA IF NOT EXISTS streaming")
    _ensure_state_table()

    log.info("Starting arXiv streaming pipeline")
    log.info("Mode: %s", "RSS" if use_rss else "Atom API")
    log.info("Poll interval: %d seconds", _poll_interval_seconds)

    if use_rss:
        log.info("RSS category: %s", STREAM_RSS_CATEGORY)
    else:
        log.info("Search query: %s", search_query or "(all arXiv)")
        log.info("Batch size: %d, max total per cycle: %d", STREAM_BATCH_SIZE, STREAM_MAX_TOTAL)

    cycle = 0
    while not _shutdown_requested:
        cycle += 1
        log.info("--- Cycle %d ---", cycle)

        cycle_start = datetime.now(timezone.utc)

        try:
            if use_rss:
                b, s, g = _process_rss_batch(
                    category=STREAM_RSS_CATEGORY,
                    max_papers=STREAM_RSS_MAX_PAPERS,
                )
                newest_published = cycle_start
            else:
                since = _get_last_fetch_time()
                log.info("Fetching papers submitted since %s", since.isoformat())
                b, s, g, newest_published = _process_api_batches(
                    search_query=search_query,
                    since=since,
                    batch_size=STREAM_BATCH_SIZE,
                    max_total=STREAM_MAX_TOTAL,
                )

            if b == 0 and s == 0 and g == 0:
                log.info("No new papers in this cycle.")
                checkpoint = cycle_start
            else:
                log.info("Cycle complete - bronze=%d, silver=%d, gold=%d", b, s, g)
                checkpoint = newest_published + timedelta(seconds=1)

            _set_last_fetch_time(checkpoint)
            log.info("Checkpoint advanced to %s", checkpoint.isoformat())

        except Exception:
            log.exception("Pipeline cycle %d failed; checkpoint preserved for retry", cycle)

        if _shutdown_requested:
            break

        log.info("Sleeping %d seconds until next poll...", _poll_interval_seconds)
        slept = 0
        while slept < _poll_interval_seconds and not _shutdown_requested:
            time.sleep(1)
            slept += 1

    log.info("Streaming pipeline stopped after %d cycles.", cycle)


def main() -> None:
    parser = argparse.ArgumentParser(description="Continuous arXiv streaming ETL pipeline")
    parser.add_argument(
        "--rss",
        action="store_true",
        help="Use arXiv RSS feed instead of the Atom API",
    )
    parser.add_argument(
        "--query",
        type=str,
        default="",
        help="arXiv search query (e.g. 'cat:cs.CL', 'all:machine+learning')",
    )
    parser.add_argument(
        "--interval",
        type=int,
        default=None,
        help="Poll interval in seconds (overrides STREAM_INTERVAL_SECONDS env var)",
    )
    args = parser.parse_args()

    global _poll_interval_seconds
    if args.interval is not None:
        _poll_interval_seconds = args.interval

    run_streaming_loop(use_rss=args.rss, search_query=args.query)


if __name__ == "__main__":
    main()
