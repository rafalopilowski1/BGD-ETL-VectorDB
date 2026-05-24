import logging
from typing import Any

from psycopg2.extras import execute_values

from core.config import GOLD_BATCH_SIZE, MODEL_NAME
from core.db import get_cursor
from pipeline.embedding import embed_texts

log = logging.getLogger(__name__)


def process_silver_to_gold() -> int:
    """Generate embeddings for silver abstracts and store in gold.

    Fetches silver rows not yet in gold, embeds them in batches,
    truncates to matryoshka dimensions, and inserts into gold.arxiv_embeddings.

    Returns the total number of newly inserted gold records.
    """
    total_inserted = 0
    offset = 0

    while True:
        rows = _fetch_unembedded_batch(offset, GOLD_BATCH_SIZE)
        if not rows:
            break

        arxiv_ids = [row["arxiv_id"] for row in rows]
        abstracts = [row["abstract_clean"] for row in rows]

        log.info(
            "Gold: embedding batch of %d abstracts (offset=%d)", len(abstracts), offset
        )

        # Embed and get all matryoshka dimensions
        embeddings_by_dim = embed_texts(abstracts)

        # Build rows for insertion:
        # (arxiv_id, embedding_384, embedding_192, embedding_96, embedding_48, model_name)
        batch: list[tuple[Any, ...]] = []
        for i, arxiv_id in enumerate(arxiv_ids):
            row_tuple = (
                arxiv_id,
                embeddings_by_dim[384][i],
                embeddings_by_dim[192][i],
                embeddings_by_dim[96][i],
                embeddings_by_dim[48][i],
                MODEL_NAME,
            )
            batch.append(row_tuple)

        total_inserted += _insert_gold_batch(batch)
        offset += GOLD_BATCH_SIZE

    log.info("Gold: finished - total inserted=%d", total_inserted)
    return total_inserted


def _fetch_unembedded_batch(offset: int, limit: int) -> list[dict[str, str]]:
    """Fetch silver rows that don't yet have gold embeddings."""
    sql = """
        SELECT s.arxiv_id, s.abstract_clean
        FROM silver.arxiv_abstracts s
        LEFT JOIN gold.arxiv_embeddings g ON s.arxiv_id = g.arxiv_id
        WHERE g.arxiv_id IS NULL
        ORDER BY s.id
        LIMIT %s OFFSET %s
    """
    with get_cursor(commit=False) as cur:
        cur.execute(sql, (limit, offset))
        return [dict(row) for row in cur.fetchall()]


def _insert_gold_batch(batch: list[tuple[Any, ...]]) -> int:
    """Insert a batch of embedding records into gold.arxiv_embeddings."""
    sql = """
        INSERT INTO gold.arxiv_embeddings
            (arxiv_id, embedding_384, embedding_192, embedding_96, embedding_48, model_name)
        VALUES %s
        ON CONFLICT (arxiv_id) DO NOTHING
    """
    with get_cursor() as cur:
        execute_values(
            cur,
            sql,
            batch,
            template="(%s, %s::vector, %s::vector, %s::vector, %s::vector, %s)",
            page_size=len(batch),
        )
        return cur.rowcount
