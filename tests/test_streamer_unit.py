"""Unit tests for core.streamer.

These mock all external dependencies (database, arXiv API, pipeline stages)
to test the orchestration logic in isolation.
"""

import json
import tempfile
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

import core.streamer as streamer


class TestCheckpointFunctions:
    @patch("core.streamer._get_state")
    def test_get_last_fetch_time_with_state(self, mock_get_state):
        dt = datetime(2023, 1, 15, 12, 0, tzinfo=timezone.utc)
        mock_get_state.return_value = dt.isoformat()

        result = streamer._get_last_fetch_time()
        assert result == dt

    @patch("core.streamer._get_state")
    def test_get_last_fetch_time_no_state_defaults_to_24h_ago(self, mock_get_state):
        mock_get_state.return_value = None
        result = streamer._get_last_fetch_time()
        now = datetime.now(timezone.utc)
        assert now - timedelta(hours=25) < result < now - timedelta(hours=23)

    @patch("core.streamer._get_state")
    def test_get_last_fetch_time_invalid_state(self, mock_get_state):
        mock_get_state.return_value = "not-a-date"
        result = streamer._get_last_fetch_time()
        now = datetime.now(timezone.utc)
        assert now - timedelta(hours=25) < result < now - timedelta(hours=23)

    @patch("core.streamer._set_state")
    def test_set_last_fetch_time(self, mock_set_state):
        dt = datetime(2023, 1, 15, 12, 0, tzinfo=timezone.utc)
        streamer._set_last_fetch_time(dt)
        mock_set_state.assert_called_once_with("last_fetch_time", dt.isoformat())


class TestStateFunctions:
    @patch("core.streamer.get_cursor")
    def test_get_state_existing(self, mock_get_cursor):
        cursor = MagicMock()
        cursor.fetchone.return_value = ("some-value",)
        mock_get_cursor.return_value.__enter__.return_value = cursor

        result = streamer._get_state("my-key")
        assert result == "some-value"
        cursor.execute.assert_called_once()

    @patch("core.streamer.get_cursor")
    def test_get_state_missing(self, mock_get_cursor):
        cursor = MagicMock()
        cursor.fetchone.return_value = None
        mock_get_cursor.return_value.__enter__.return_value = cursor

        result = streamer._get_state("my-key", default="fallback")
        assert result == "fallback"

    @patch("core.streamer.get_cursor")
    def test_set_state(self, mock_get_cursor):
        cursor = MagicMock()
        mock_get_cursor.return_value.__enter__.return_value = cursor

        streamer._set_state("my-key", "my-value")
        assert "INSERT INTO streaming.state" in cursor.execute.call_args[0][0]
        assert cursor.execute.call_args[0][1] == ("my-key", "my-value")


class TestEnsureStateTable:
    @patch("core.streamer.get_cursor")
    def test_creates_table(self, mock_get_cursor):
        cursor = MagicMock()
        mock_get_cursor.return_value.__enter__.return_value = cursor
        streamer._ensure_state_table()
        assert "CREATE TABLE IF NOT EXISTS streaming.state" in cursor.execute.call_args[0][0]


class TestRunPipelineStagesForFile:
    @patch("core.streamer.ingest_jsonl")
    @patch("core.streamer.process_bronze_to_silver")
    @patch("core.streamer.process_silver_to_gold")
    def test_runs_all_stages(self, mock_gold, mock_silver, mock_bronze):
        mock_bronze.return_value = 10
        mock_silver.return_value = 8
        mock_gold.return_value = 8

        with tempfile.NamedTemporaryFile(mode="w", suffix=".jsonl", delete=False) as tmp:
            tmp.write(json.dumps({"id": "1"}) + "\n")
            tmp_path = Path(tmp.name)

        try:
            b, s, g = streamer._run_pipeline_stages_for_file(tmp_path)
            assert b == 10
            assert s == 8
            assert g == 8
            mock_bronze.assert_called_once_with(tmp_path)
            mock_silver.assert_called_once()
            mock_gold.assert_called_once()
        finally:
            tmp_path.unlink(missing_ok=True)


class TestProcessAPIBatches:
    @patch("core.streamer.fetch_all_since")
    @patch("core.streamer._run_pipeline_stages_for_file")
    def test_processes_batches_and_tracks_newest(self, mock_run, mock_fetch):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        batch1 = [{"id": "1", "published": "2023-01-02T08:00:00Z"}]
        batch2 = [{"id": "2", "published": "2023-01-03T08:00:00Z"}]

        mock_fetch.return_value = iter([
            (batch1, datetime(2023, 1, 2, 8, 0, tzinfo=timezone.utc)),
            (batch2, datetime(2023, 1, 3, 8, 0, tzinfo=timezone.utc)),
        ])
        mock_run.return_value = (5, 5, 5)

        b, s, g, newest = streamer._process_api_batches(
            search_query="cat:cs.CL",
            since=since,
            batch_size=10,
            max_total=100,
        )

        assert b == 10
        assert s == 10
        assert g == 10
        assert newest == datetime(2023, 1, 3, 8, 0, tzinfo=timezone.utc)
        assert mock_run.call_count == 2

    @patch("core.streamer.fetch_all_since")
    @patch("core.streamer._run_pipeline_stages_for_file")
    def test_empty_batches_returns_since(self, mock_run, mock_fetch):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        mock_fetch.return_value = iter([])

        b, s, g, newest = streamer._process_api_batches(
            search_query="cat:cs.CL",
            since=since,
            batch_size=10,
            max_total=100,
        )

        assert b == 0
        assert s == 0
        assert g == 0
        assert newest == since
        mock_run.assert_not_called()

    @patch("core.streamer._shutdown_requested", True)
    @patch("core.streamer.fetch_all_since")
    @patch("core.streamer._run_pipeline_stages_for_file")
    def test_respects_shutdown(self, mock_run, mock_fetch):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        batch1 = [{"id": "1", "published": "2023-01-02T08:00:00Z"}]
        mock_fetch.return_value = iter([
            (batch1, datetime(2023, 1, 2, 8, 0, tzinfo=timezone.utc)),
        ])

        b, s, g, newest = streamer._process_api_batches(
            search_query="cat:cs.CL",
            since=since,
            batch_size=10,
            max_total=100,
        )

        assert b == 0
        assert s == 0
        assert g == 0
        mock_run.assert_not_called()


class TestProcessRSSBatch:
    @patch("core.streamer.fetch_latest_rss_batch")
    @patch("core.streamer._run_pipeline_stages_for_file")
    def test_processes_rss_records(self, mock_run, mock_fetch):
        mock_fetch.return_value = [
            {"id": "1", "title": "Paper 1", "abstract": "Abstract 1"},
        ]
        mock_run.return_value = (1, 1, 1)

        b, s, g = streamer._process_rss_batch(category="cs", max_papers=10)
        assert b == 1
        assert s == 1
        assert g == 1
        mock_run.assert_called_once()
        # Verify temp file was passed to pipeline (file is deleted after)
        tmp_path = mock_run.call_args[0][0]
        assert isinstance(tmp_path, Path)
        assert "arxiv_rss_" in tmp_path.name

    @patch("core.streamer.fetch_latest_rss_batch")
    @patch("core.streamer._run_pipeline_stages_for_file")
    def test_empty_rss_returns_zero(self, mock_run, mock_fetch):
        mock_fetch.return_value = []
        b, s, g = streamer._process_rss_batch(category="cs", max_papers=10)
        assert b == 0
        assert s == 0
        assert g == 0
        mock_run.assert_not_called()


class TestSignalHandler:
    def test_sets_shutdown_flag(self):
        # Reset flag before test
        streamer._shutdown_requested = False
        streamer._signal_handler(2, None)
        assert streamer._shutdown_requested is True
        # Reset after test
        streamer._shutdown_requested = False
