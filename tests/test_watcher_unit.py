"""Unit tests for core.watcher.

These test the file watcher handler and pipeline orchestration in isolation.
"""

import json
import time
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest
from watchdog.events import DirCreatedEvent, FileCreatedEvent

import core.watcher as watcher


class TestRunPipelineStages:
    @patch("core.watcher.ingest_jsonl")
    @patch("core.watcher.process_bronze_to_silver")
    @patch("core.watcher.process_silver_to_gold")
    def test_runs_all_stages(self, mock_gold, mock_silver, mock_bronze):
        mock_bronze.return_value = 5
        mock_silver.return_value = 4
        mock_gold.return_value = 4

        result = watcher.run_pipeline_stages(Path("/fake/file.jsonl"))
        assert result == (5, 4, 4)
        mock_bronze.assert_called_once_with(Path("/fake/file.jsonl"))
        mock_silver.assert_called_once()
        mock_gold.assert_called_once()


class TestMoveToProcessed:
    def test_moves_file(self, tmp_path):
        incoming = tmp_path / "incoming"
        processed = tmp_path / "processed"
        incoming.mkdir()
        processed.mkdir()

        src = incoming / "test.jsonl"
        src.write_text("data")

        with patch("core.watcher.PROCESSED_DIR", processed):
            dest = watcher.move_to_processed(src)

        assert not src.exists()
        assert dest.exists()
        assert dest.name == "test.jsonl"
        assert dest.parent == processed

    def test_renames_on_collision(self, tmp_path):
        incoming = tmp_path / "incoming"
        processed = tmp_path / "processed"
        incoming.mkdir()
        processed.mkdir()

        src = incoming / "test.jsonl"
        src.write_text("data")
        (processed / "test.jsonl").write_text("existing")

        with patch("core.watcher.PROCESSED_DIR", processed):
            dest = watcher.move_to_processed(src)

        assert not src.exists()
        assert dest.exists()
        assert dest.name != "test.jsonl"  # should have timestamp suffix
        assert dest.parent == processed


class TestRunPipeline:
    @patch("core.watcher.run_pipeline_stages")
    @patch("core.watcher.move_to_processed")
    def test_executes_and_moves(self, mock_move, mock_run):
        mock_run.return_value = (1, 1, 1)
        filepath = Path("/fake/file.jsonl")
        watcher.run_pipeline(filepath)
        mock_run.assert_called_once_with(filepath)
        mock_move.assert_called_once_with(filepath)


class TestJsonlHandler:
    def test_ignores_directories(self):
        handler = watcher.JsonlHandler()
        event = DirCreatedEvent("/some/dir")
        handler.on_created(event)  # should not raise

    def test_ignores_non_jsonl_files(self):
        handler = watcher.JsonlHandler()
        event = FileCreatedEvent("/some/file.txt")
        with patch("core.watcher.run_pipeline") as mock_run:
            handler.on_created(event)
            mock_run.assert_not_called()

    @patch("core.watcher.run_pipeline")
    @patch("core.watcher.time.sleep")
    def test_triggers_pipeline_for_jsonl(self, mock_sleep, mock_run):
        handler = watcher.JsonlHandler()
        event = FileCreatedEvent("/some/file.jsonl")
        handler.on_created(event)
        mock_sleep.assert_called_once_with(1)
        mock_run.assert_called_once_with(Path("/some/file.jsonl"))

    @patch("core.watcher.run_pipeline")
    @patch("core.watcher.time.sleep")
    def test_logs_exception_on_failure(self, mock_sleep, mock_run):
        handler = watcher.JsonlHandler()
        mock_run.side_effect = RuntimeError("boom")
        event = FileCreatedEvent("/some/file.jsonl")
        handler.on_created(event)  # should not raise
        mock_run.assert_called_once()
