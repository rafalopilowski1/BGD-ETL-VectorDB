"""File watcher and pipeline orchestrator.

Usage:
    python -m core.watcher              # Watch data/incoming/ for new .jsonl files
    python -m core.watcher --file FILE  # Process a single file directly
"""

import argparse
import logging
import shutil
import sys
import time
from pathlib import Path
from typing import override

from watchdog.events import DirCreatedEvent, FileCreatedEvent, FileSystemEventHandler
from watchdog.observers import Observer

from core.config import INCOMING_DIR, PROCESSED_DIR
from core.db import check_connection
from pipeline.bronze import ingest_jsonl
from pipeline.gold import process_silver_to_gold
from pipeline.silver import process_bronze_to_silver

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
log = logging.getLogger(__name__)


def run_pipeline_stages(filepath: Path) -> tuple[int, int, int]:
    """Execute bronze -> silver -> gold stages. Returns (bronze, silver, gold) counts."""
    log.info("=" * 60)
    log.info("Pipeline started for: %s", filepath.name)
    log.info("=" * 60)

    start = time.time()

    # Bronze: raw JSONL -> database
    log.info("--- Stage 1: Bronze (raw ingestion) ---")
    bronze_count = ingest_jsonl(filepath)
    log.info("Bronze complete: %d records ingested", bronze_count)

    # Silver: clean and parse
    log.info("--- Stage 2: Silver (cleaning & parsing) ---")
    silver_count = process_bronze_to_silver()
    log.info("Silver complete: %d records processed", silver_count)

    # Gold: embed
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


def move_to_processed(filepath: Path) -> Path:
    """Move a file to the processed directory. Returns the destination path."""
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
    dest = PROCESSED_DIR / filepath.name
    if dest.exists():
        stem = filepath.stem
        suffix = filepath.suffix
        dest = PROCESSED_DIR / f"{stem}_{int(time.time())}{suffix}"
    shutil.move(str(filepath), str(dest))
    log.info("Moved %s -> %s", filepath.name, dest)
    return dest


def run_pipeline(filepath: Path) -> None:
    """Execute the full pipeline for a single file and move it to processed/."""
    run_pipeline_stages(filepath)
    move_to_processed(filepath)


class JsonlHandler(FileSystemEventHandler):
    """Watchdog handler that triggers the pipeline when a .jsonl file is created."""

    @override
    def on_created(self, event: DirCreatedEvent | FileCreatedEvent) -> None:
        if event.is_directory:
            return
        filepath = Path(str(event.src_path))
        if filepath.suffix != ".jsonl":
            log.debug("Ignoring non-JSONL file: %s", filepath.name)
            return

        # Brief delay to ensure file write is complete
        time.sleep(1)

        try:
            run_pipeline(filepath)
        except Exception:
            log.exception("Pipeline failed for %s", filepath.name)


def watch() -> None:
    """Watch the incoming directory for new .jsonl files."""
    INCOMING_DIR.mkdir(parents=True, exist_ok=True)
    PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

    log.info("Checking database connection...")
    if not check_connection():
        log.error("Cannot connect to database. Is PostgreSQL running?")
        sys.exit(1)
    log.info("Database connection OK")

    log.info("Watching %s for new .jsonl files (Ctrl+C to stop)", INCOMING_DIR)

    observer = Observer()
    observer.schedule(JsonlHandler(), str(INCOMING_DIR), recursive=False)
    observer.start()

    try:
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        log.info("Stopping watcher...")
        observer.stop()
    observer.join()


def main() -> None:
    parser = argparse.ArgumentParser(description="arXiv ETL pipeline")
    parser.add_argument(
        "--file",
        type=Path,
        help="Process a specific JSONL file directly (skip watching)",
    )
    args = parser.parse_args()

    if args.file:
        filepath = args.file.resolve()
        if not filepath.exists():
            log.error("File not found: %s", filepath)
            sys.exit(1)
        if not filepath.suffix == ".jsonl":
            log.error("Expected a .jsonl file, got: %s", filepath.name)
            sys.exit(1)

        log.info("Checking database connection...")
        if not check_connection():
            log.error("Cannot connect to database. Is PostgreSQL running?")
            sys.exit(1)
        log.info("Database connection OK")

        run_pipeline(filepath)
    else:
        watch()


if __name__ == "__main__":
    main()
