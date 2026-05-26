import json
import logging
from pathlib import Path

from psycopg2.extras import execute_values

from core.config import BRONZE_BATCH_SIZE
from core.db import get_cursor

log = logging.getLogger(__name__)


def ingest_jsonl(filepath: Path) -> int:
    """Read a JSONL file and load raw records into bronze.arxiv_raw.

    Returns the number of newly inserted records.
    """
    source_file = filepath.name
    records: list[tuple[str, str, str]] = []
    skipped = 0
    total_inserted = 0

    log.info("Bronze: reading %s", filepath)

    with open(filepath, "r", encoding="utf-8") as f:
        for line_num, line in enumerate(f, start=1):
            line = line.strip()
            if not line:
                continue

            try:
                data = json.loads(line)
            except json.JSONDecodeError as e:
                log.warning("Bronze: skipping line %d (invalid JSON): %s", line_num, e)
                skipped += 1
                continue

            arxiv_id = data.get("id")
            if not arxiv_id:
                log.warning("Bronze: skipping line %d (missing 'id' field)", line_num)
                skipped += 1
                continue

            records.append((arxiv_id, json.dumps(data), source_file))

            if len(records) >= BRONZE_BATCH_SIZE:
                total_inserted += _flush_batch(records)
                records.clear()

    # flush remaining
    if records:
        total_inserted += _flush_batch(records)

    log.info(
        "Bronze: finished %s - inserted=%d, skipped=%d",
        filepath.name,
        total_inserted,
        skipped,
    )
    return total_inserted


def _flush_batch(records: list[tuple[str, str, str]]) -> int:
    """Insert a batch of records into bronze.arxiv_raw. Returns rows inserted."""
    sql = """
        INSERT INTO bronze.arxiv_raw (arxiv_id, raw_data, source_file)
        VALUES %s
        ON CONFLICT (arxiv_id) DO NOTHING
    """
    with get_cursor() as cur:
        execute_values(cur, sql, records, page_size=len(records))
        return cur.rowcount  # type: ignore[return-value]
