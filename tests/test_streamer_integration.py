"""Integration tests for the arXiv streaming pipeline.

These tests use the real PostgreSQL database but mock HTTP calls to arXiv
and the embedding model to keep tests fast and deterministic.
They verify end-to-end behaviour: fetching → temp file → pipeline → checkpoint.
"""

import json
import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import numpy as np
import psycopg2
import pytest

from core.streamer import (
    _get_last_fetch_time,
    _process_api_batches,
    _run_pipeline_stages_for_file,
    _set_last_fetch_time,
)

PROJECT_ROOT = Path(__file__).parent.parent.resolve()


def get_db_connection():
    url = os.environ.get("DATABASE_URL", "postgresql://bgd:bgd@localhost:5432/bgd")
    return psycopg2.connect(url)


def reset_database():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM bronze.arxiv_raw")
            cur.execute("DELETE FROM silver.arxiv_abstracts")
            cur.execute("DELETE FROM gold.arxiv_embeddings")
            cur.execute("DELETE FROM streaming.state")
            conn.commit()


def get_table_counts():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) FROM bronze.arxiv_raw")
            bronze = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM silver.arxiv_abstracts")
            silver = cur.fetchone()[0]
            cur.execute("SELECT COUNT(*) FROM gold.arxiv_embeddings")
            gold = cur.fetchone()[0]
            return {"bronze": bronze, "silver": silver, "gold": gold}


def get_checkpoint():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT value FROM streaming.state WHERE key = 'last_fetch_time'")
            row = cur.fetchone()
            return row[0] if row else None


@pytest.fixture(autouse=True)
def clean_database():
    reset_database()
    yield
    reset_database()


@pytest.fixture(autouse=True)
def mock_embeddings():
    """Return deterministic fake embeddings so tests don't download models."""
    def fake_embed(texts):
        return {
            384: [np.ones(384, dtype=float).tolist() for _ in texts],
            192: [np.ones(192, dtype=float).tolist() for _ in texts],
            96: [np.ones(96, dtype=float).tolist() for _ in texts],
            48: [np.ones(48, dtype=float).tolist() for _ in texts],
        }

    with patch("pipeline.gold.embed_texts", fake_embed):
        with patch("pipeline.embedding.embed_texts", fake_embed):
            yield


@pytest.fixture
def sample_records():
    return [
        {
            "id": "2301.00001",
            "title": "Test Paper One",
            "abstract": "This is the first test abstract.",
            "authors": ["Alice Author"],
            "categories": ["cs.CL"],
            "published": "2023-01-02T08:00:00Z",
        },
        {
            "id": "2301.00002",
            "title": "Test Paper Two",
            "abstract": "This is the second test abstract.",
            "authors": ["Bob Author"],
            "categories": ["cs.LG"],
            "published": "2023-01-03T08:00:00Z",
        },
    ]


class TestPipelineStagesIntegration:
    def test_full_pipeline_with_real_data(self, sample_records, tmp_path):
        jsonl_path = tmp_path / "test.jsonl"
        with open(jsonl_path, "w") as f:
            for record in sample_records:
                f.write(json.dumps(record) + "\n")

        b, s, g = _run_pipeline_stages_for_file(jsonl_path)

        assert b == 2
        assert s == 2
        assert g == 2

        counts = get_table_counts()
        assert counts["bronze"] == 2
        assert counts["silver"] == 2
        assert counts["gold"] == 2

    def test_pipeline_is_idempotent(self, sample_records, tmp_path):
        jsonl_path = tmp_path / "test.jsonl"
        with open(jsonl_path, "w") as f:
            for record in sample_records:
                f.write(json.dumps(record) + "\n")

        # Run twice with same data
        b1, s1, g1 = _run_pipeline_stages_for_file(jsonl_path)
        b2, s2, g2 = _run_pipeline_stages_for_file(jsonl_path)

        # Second run should have 0 new bronze (ON CONFLICT DO NOTHING)
        assert b2 == 0
        # Silver and gold may still process because they don't check bronze dedup
        # but gold has ON CONFLICT too

        counts = get_table_counts()
        # Bronze should still be exactly 2 (no duplicates)
        assert counts["bronze"] == 2


class TestCheckpointIntegration:
    def test_checkpoint_persists_to_database(self):
        dt = datetime(2023, 1, 15, 12, 0, tzinfo=timezone.utc)
        _set_last_fetch_time(dt)

        raw = get_checkpoint()
        assert raw is not None
        assert datetime.fromisoformat(raw) == dt

    def test_checkpoint_round_trip(self):
        original = datetime(2023, 6, 1, 10, 30, 0, tzinfo=timezone.utc)
        _set_last_fetch_time(original)

        retrieved = _get_last_fetch_time()
        assert retrieved == original

    def test_checkpoint_advances_forward(self):
        old = datetime(2023, 1, 1, tzinfo=timezone.utc)
        new = datetime(2023, 1, 15, tzinfo=timezone.utc)

        _set_last_fetch_time(old)
        assert _get_last_fetch_time() == old

        _set_last_fetch_time(new)
        assert _get_last_fetch_time() == new


class TestProcessAPIBatchesIntegration:
    @patch("core.streamer.fetch_all_since")
    def test_processes_mocked_batches_with_real_db(self, mock_fetch, tmp_path):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "id": "2301.00001",
                "title": "Test Paper",
                "abstract": "Abstract text.",
                "authors": ["Author"],
                "categories": ["cs.CL"],
                "published": "2023-01-02T08:00:00Z",
            },
        ]

        mock_fetch.return_value = iter([
            (records, datetime(2023, 1, 2, 8, 0, tzinfo=timezone.utc)),
        ])

        b, s, g, newest = _process_api_batches(
            search_query="cat:cs.CL",
            since=since,
            batch_size=10,
            max_total=100,
        )

        assert b == 1
        assert s == 1
        assert g == 1
        assert newest == datetime(2023, 1, 2, 8, 0, tzinfo=timezone.utc)

        counts = get_table_counts()
        assert counts["bronze"] == 1
        assert counts["silver"] == 1
        assert counts["gold"] == 1

    @patch("core.streamer.fetch_all_since")
    def test_multiple_batches_all_processed(self, mock_fetch, tmp_path):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        batch1 = [
            {
                "id": "2301.00001",
                "title": "Paper One",
                "abstract": "Abstract one.",
                "authors": ["A"],
                "categories": ["cs.CL"],
                "published": "2023-01-02T08:00:00Z",
            },
        ]
        batch2 = [
            {
                "id": "2301.00002",
                "title": "Paper Two",
                "abstract": "Abstract two.",
                "authors": ["B"],
                "categories": ["cs.LG"],
                "published": "2023-01-03T08:00:00Z",
            },
        ]

        mock_fetch.return_value = iter([
            (batch1, datetime(2023, 1, 2, 8, 0, tzinfo=timezone.utc)),
            (batch2, datetime(2023, 1, 3, 8, 0, tzinfo=timezone.utc)),
        ])

        b, s, g, newest = _process_api_batches(
            search_query="cat:cs.CL",
            since=since,
            batch_size=10,
            max_total=100,
        )

        assert b == 2
        assert s == 2
        assert g == 2
        assert newest == datetime(2023, 1, 3, 8, 0, tzinfo=timezone.utc)

        counts = get_table_counts()
        assert counts["bronze"] == 2
        assert counts["silver"] == 2
        assert counts["gold"] == 2

    @patch("core.streamer.fetch_all_since")
    def test_empty_batch_no_database_changes(self, mock_fetch):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        mock_fetch.return_value = iter([])

        b, s, g, newest = _process_api_batches(
            search_query="cat:cs.CL",
            since=since,
            batch_size=10,
            max_total=100,
        )

        assert b == 0
        assert s == 0
        assert g == 0
        assert newest == since

        counts = get_table_counts()
        assert counts["bronze"] == 0
        assert counts["silver"] == 0
        assert counts["gold"] == 0


class TestNoRefetchIntegration:
    @patch("core.streamer.fetch_all_since")
    def test_same_papers_not_fetched_twice(self, mock_fetch):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        records = [
            {
                "id": "2301.00001",
                "title": "Test Paper",
                "abstract": "Abstract text.",
                "authors": ["Author"],
                "categories": ["cs.CL"],
                "published": "2023-01-02T08:00:00Z",
            },
        ]

        # First fetch
        mock_fetch.return_value = iter([
            (records, datetime(2023, 1, 2, 8, 0, tzinfo=timezone.utc)),
        ])
        b1, s1, g1, newest1 = _process_api_batches(
            search_query="cat:cs.CL",
            since=since,
            batch_size=10,
            max_total=100,
        )
        assert b1 == 1

        # Second fetch with advanced checkpoint should return empty
        mock_fetch.return_value = iter([])
        b2, s2, g2, newest2 = _process_api_batches(
            search_query="cat:cs.CL",
            since=datetime(2023, 1, 2, 8, 1, tzinfo=timezone.utc),  # after published
            batch_size=10,
            max_total=100,
        )
        assert b2 == 0
        assert s2 == 0
        assert g2 == 0

        # Database should still have exactly 1 record
        counts = get_table_counts()
        assert counts["bronze"] == 1
        assert counts["silver"] == 1
        assert counts["gold"] == 1
