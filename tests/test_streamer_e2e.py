"""End-to-end tests for the arXiv streaming ETL pipeline.

These tests hit the real arXiv API and PostgreSQL database.
They reset the database state before each test and clean up after.
"""

import os
import subprocess
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import psycopg2
import pytest

# Project root is two levels up from tests/
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


def get_distinct_counts():
    with get_db_connection() as conn:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT COUNT(DISTINCT arxiv_id), COUNT(*) FROM bronze.arxiv_raw"
            )
            bronze_distinct, bronze_total = cur.fetchone()
            cur.execute(
                "SELECT COUNT(DISTINCT arxiv_id), COUNT(*) FROM silver.arxiv_abstracts"
            )
            silver_distinct, silver_total = cur.fetchone()
            cur.execute(
                "SELECT COUNT(DISTINCT arxiv_id), COUNT(*) FROM gold.arxiv_embeddings"
            )
            gold_distinct, gold_total = cur.fetchone()
            return {
                "bronze": {"distinct": bronze_distinct, "total": bronze_total},
                "silver": {"distinct": silver_distinct, "total": silver_total},
                "gold": {"distinct": gold_distinct, "total": gold_total},
            }


@pytest.fixture(autouse=True)
def clean_database():
    reset_database()
    yield
    reset_database()


@pytest.fixture
def streamer_env():
    env = os.environ.copy()
    env["STREAM_INTERVAL_SECONDS"] = "30"
    env["STREAM_BATCH_SIZE"] = "5"
    env["STREAM_MAX_TOTAL"] = "10"
    env["STREAM_RSS_MAX_PAPERS"] = "10"
    return env


class TestStreamerAPI:
    def _run_streamer(self, env, query="", duration=60):
        cmd = [sys.executable, "-m", "core.streamer"]
        if query:
            cmd += ["--query", query]

        proc = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        try:
            stdout, _ = proc.communicate(timeout=duration)
        except subprocess.TimeoutExpired:
            proc.send_signal(subprocess.signal.SIGINT)
            try:
                stdout, _ = proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, _ = proc.communicate()

        return stdout, proc.returncode

    def test_api_streamer_fetches_and_processes_papers(self, streamer_env):
        stdout, rc = self._run_streamer(
            streamer_env, query="cat:cs.CL", duration=90
        )

        counts = get_table_counts()
        checkpoint = get_checkpoint()

        if "arXiv API returned 0 records" in stdout:
            pytest.skip("No new papers available from arXiv API")

        if "HTTP 429" in stdout:
            pytest.skip("arXiv rate limit hit (429)")

        assert counts["bronze"] > 0, f"Bronze table empty. Output:\n{stdout}"
        assert counts["silver"] > 0, f"Silver table empty. Output:\n{stdout}"
        assert counts["gold"] > 0, f"Gold table empty. Output:\n{stdout}"
        assert checkpoint is not None, "Checkpoint not persisted"

    def test_api_streamer_no_duplicates_on_re_run(self, streamer_env):
        first_stdout, _ = self._run_streamer(
            streamer_env, query="cat:cs.CL", duration=90
        )

        if "HTTP 429" in first_stdout:
            pytest.skip("arXiv rate limit hit on first run")

        counts_before = get_table_counts()
        if counts_before["bronze"] == 0:
            pytest.skip("No papers fetched on first run")

        second_stdout, _ = self._run_streamer(
            streamer_env, query="cat:cs.CL", duration=90
        )

        if "HTTP 429" in second_stdout:
            pytest.skip("arXiv rate limit hit on second run")

        counts_after = get_table_counts()
        distinct = get_distinct_counts()

        assert (
            counts_after["bronze"] == counts_before["bronze"]
        ), f"Bronze count changed on re-run: {counts_before} -> {counts_after}"
        assert (
            distinct["bronze"]["distinct"] == distinct["bronze"]["total"]
        ), f"Duplicate arxiv_ids in bronze: {distinct['bronze']}"
        assert (
            distinct["silver"]["distinct"] == distinct["silver"]["total"]
        ), f"Duplicate arxiv_ids in silver: {distinct['silver']}"
        assert (
            distinct["gold"]["distinct"] == distinct["gold"]["total"]
        ), f"Duplicate arxiv_ids in gold: {distinct['gold']}"

    def test_api_streamer_checkpoint_advances(self, streamer_env):
        reset_database()

        old_checkpoint = datetime.now(timezone.utc) - timedelta(hours=48)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO streaming.state (key, value) VALUES (%s, %s)",
                    ("last_fetch_time", old_checkpoint.isoformat()),
                )
                conn.commit()

        stdout, _ = self._run_streamer(
            streamer_env, query="cat:cs.CL", duration=90
        )

        if "HTTP 429" in stdout:
            pytest.skip("arXiv rate limit hit")

        new_checkpoint_raw = get_checkpoint()
        assert new_checkpoint_raw is not None, "Checkpoint not persisted"

        new_checkpoint = datetime.fromisoformat(new_checkpoint_raw)
        assert (
            new_checkpoint > old_checkpoint
        ), f"Checkpoint did not advance: {old_checkpoint} -> {new_checkpoint}"

    def test_api_streamer_graceful_when_no_papers(self, streamer_env):
        reset_database()

        future = datetime.now(timezone.utc) + timedelta(days=1)
        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute(
                    "INSERT INTO streaming.state (key, value) VALUES (%s, %s)",
                    ("last_fetch_time", future.isoformat()),
                )
                conn.commit()

        stdout, rc = self._run_streamer(
            streamer_env, query="cat:cs.CL", duration=60
        )

        counts = get_table_counts()
        assert counts["bronze"] == 0
        assert counts["silver"] == 0
        assert counts["gold"] == 0
        assert "No new papers in this cycle" in stdout or rc == 0


class TestStreamerRSS:
    def _run_streamer(self, env, duration=60):
        cmd = [sys.executable, "-m", "core.streamer", "--rss"]

        proc = subprocess.Popen(
            cmd,
            cwd=PROJECT_ROOT,
            env=env,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
        )

        try:
            stdout, _ = proc.communicate(timeout=duration)
        except subprocess.TimeoutExpired:
            proc.send_signal(subprocess.signal.SIGINT)
            try:
                stdout, _ = proc.communicate(timeout=10)
            except subprocess.TimeoutExpired:
                proc.kill()
                stdout, _ = proc.communicate()

        return stdout, proc.returncode

    def test_rss_streamer_fetches_and_processes_papers(self, streamer_env):
        stdout, rc = self._run_streamer(streamer_env, duration=60)

        counts = get_table_counts()

        if "arXiv RSS returned 0 records" in stdout:
            pytest.skip("RSS feed returned no papers")

        if "HTTP 429" in stdout:
            pytest.skip("arXiv rate limit hit")

        assert counts["bronze"] > 0, f"Bronze table empty. Output:\n{stdout}"
        assert counts["silver"] > 0, f"Silver table empty. Output:\n{stdout}"
        assert counts["gold"] > 0, f"Gold table empty. Output:\n{stdout}"
