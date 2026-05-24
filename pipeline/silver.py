import logging

from psycopg2.extras import execute_values

from core.config import SILVER_BATCH_SIZE
from core.db import get_cursor
from pipeline.cleaning import clean_abstract, clean_title

log = logging.getLogger(__name__)


def process_bronze_to_silver() -> int:
    """Transform raw bronze records into cleaned silver records.

    Reads bronze.arxiv_raw rows that don't yet exist in silver.arxiv_abstracts,
    cleans them, and inserts the results.

    Returns the number of newly inserted silver records.
    """
    total_inserted = 0
    skipped = 0
    offset = 0

    while True:
        rows = _fetch_unprocessed_batch(offset, SILVER_BATCH_SIZE)
        if not rows:
            break

        batch: list[
            tuple[str, str, str, str | None, list[str], str | None, str | None]
        ] = []

        for row in rows:
            arxiv_id = row["arxiv_id"]
            raw = row["raw_data"]

            # Clean abstract
            abstract_clean = clean_abstract(raw.get("abstract"))
            if not abstract_clean:
                log.debug(
                    "Silver: skipping %s (abstract cleaning failed or empty)", arxiv_id
                )
                skipped += 1
                continue

            # Clean title
            title = clean_title(raw.get("title"))
            if not title:
                log.debug(
                    "Silver: skipping %s (title cleaning failed or empty)", arxiv_id
                )
                skipped += 1
                continue

            authors = raw.get("authors")
            categories = raw.get("categories", [])
            if isinstance(categories, str):
                categories = categories.split()
            doi = raw.get("doi")
            journal_ref = raw.get("journal-ref")

            batch.append(
                (arxiv_id, title, abstract_clean, authors, categories, doi, journal_ref)
            )

        if batch:
            total_inserted += _insert_silver_batch(batch)

        offset += SILVER_BATCH_SIZE

    log.info("Silver: finished - inserted=%d, skipped=%d", total_inserted, skipped)
    return total_inserted


def _fetch_unprocessed_batch(offset: int, limit: int) -> list[dict[str, str]]:
    """Fetch bronze rows not yet in silver."""
    sql = """
        SELECT b.arxiv_id, b.raw_data
        FROM bronze.arxiv_raw b
        LEFT JOIN silver.arxiv_abstracts s ON b.arxiv_id = s.arxiv_id
        WHERE s.arxiv_id IS NULL
        ORDER BY b.id
        LIMIT %s OFFSET %s
    """
    with get_cursor(commit=False) as cur:
        cur.execute(sql, (limit, offset))
        return [dict(row) for row in cur.fetchall()]


def _insert_silver_batch(
    batch: list[tuple[str, str, str, str | None, list[str], str | None, str | None]],
) -> int:
    """Insert a batch of cleaned records into silver.arxiv_abstracts."""
    sql = """
        INSERT INTO silver.arxiv_abstracts
            (arxiv_id, title, abstract_clean, authors, categories, doi, journal_ref)
        VALUES %s
        ON CONFLICT (arxiv_id) DO NOTHING
    """
    with get_cursor() as cur:
        execute_values(cur, sql, batch, page_size=len(batch))
        return cur.rowcount
