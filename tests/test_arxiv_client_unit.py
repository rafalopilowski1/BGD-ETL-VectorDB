"""Unit tests for core.arxiv_client.

These tests exercise pure functions and retry logic without making
any real HTTP calls or touching the database.
"""

import time
import xml.etree.ElementTree as ET
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

import pytest
import requests

from core.arxiv_client import (
    ARXIV_API_BASE,
    NS_ATOM,
    NS_ARXIV,
    _extract_arxiv_id,
    _is_retryable_status,
    _parse_atom_entry,
    _parse_retry_after,
    _retrying_get,
    fetch_all_since,
    fetch_arxiv_api,
)


class TestExtractArxivId:
    def test_standard_id(self):
        assert _extract_arxiv_id("http://arxiv.org/abs/2301.12345") == "2301.12345"

    def test_old_style_id(self):
        assert _extract_arxiv_id("http://arxiv.org/abs/quant-ph/0202022") == "quant-ph/0202022"

    def test_no_match(self):
        assert _extract_arxiv_id("http://example.com/other") is None

    def test_empty(self):
        assert _extract_arxiv_id("") is None


class TestParseAtomEntry:
    def _make_entry(self, arxiv_id="2301.12345", title="Test Title", summary="Test abstract."):
        xml = f"""<?xml version="1.0"?>
        <feed xmlns="http://www.w3.org/2005/Atom"
              xmlns:arxiv="http://arxiv.org/schemas/atom">
            <entry>
                <id>http://arxiv.org/abs/{arxiv_id}</id>
                <title>{title}</title>
                <summary>{summary}</summary>
                <published>2023-01-15T08:00:00Z</published>
                <author><name>Alice Author</name></author>
                <author><name>Bob Author</name></author>
                <category term="cs.CL"/>
                <category term="cs.LG"/>
                <arxiv:doi>10.1234/example</arxiv:doi>
                <arxiv:journal_ref>Journal of Testing, 2023</arxiv:journal_ref>
            </entry>
        </feed>"""
        root = ET.fromstring(xml)
        return root.find(f"{NS_ATOM}entry")

    def test_full_entry(self):
        entry = self._make_entry()
        record = _parse_atom_entry(entry)
        assert record is not None
        assert record["id"] == "2301.12345"
        assert record["title"] == "Test Title"
        assert record["abstract"] == "Test abstract."
        assert record["authors"] == ["Alice Author", "Bob Author"]
        assert record["categories"] == ["cs.CL", "cs.LG"]
        assert record["doi"] == "10.1234/example"
        assert record["journal-ref"] == "Journal of Testing, 2023"
        assert record["published"] == "2023-01-15T08:00:00Z"

    def test_missing_optional_fields(self):
        xml = """<?xml version="1.0"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
            <entry>
                <id>http://arxiv.org/abs/2301.12345</id>
                <title>Minimal</title>
                <summary>Abstract.</summary>
                <published>2023-01-15T08:00:00Z</published>
            </entry>
        </feed>"""
        root = ET.fromstring(xml)
        entry = root.find(f"{NS_ATOM}entry")
        record = _parse_atom_entry(entry)
        assert record["doi"] is None
        assert record["journal-ref"] is None
        assert record["authors"] == []
        assert record["categories"] == []

    def test_no_id_returns_none(self):
        xml = """<?xml version="1.0"?>
        <feed xmlns="http://www.w3.org/2005/Atom">
            <entry><title>No ID</title></entry>
        </feed>"""
        root = ET.fromstring(xml)
        entry = root.find(f"{NS_ATOM}entry")
        assert _parse_atom_entry(entry) is None


class TestRetryableStatus:
    def test_retryable(self):
        assert _is_retryable_status(429) is True
        assert _is_retryable_status(500) is True
        assert _is_retryable_status(502) is True
        assert _is_retryable_status(503) is True
        assert _is_retryable_status(504) is True

    def test_non_retryable(self):
        assert _is_retryable_status(400) is False
        assert _is_retryable_status(401) is False
        assert _is_retryable_status(403) is False
        assert _is_retryable_status(404) is False
        assert _is_retryable_status(200) is False


class TestParseRetryAfter:
    def test_integer_seconds(self):
        resp = MagicMock()
        resp.headers = {"Retry-After": "10"}
        assert _parse_retry_after(resp) == 10.0

    def test_missing_header(self):
        resp = MagicMock()
        resp.headers = {}
        assert _parse_retry_after(resp) is None

    def test_http_date_unsupported(self):
        resp = MagicMock()
        resp.headers = {"Retry-After": "Wed, 21 Oct 2015 07:28:00 GMT"}
        assert _parse_retry_after(resp) is None


class TestRetryingGet:
    @patch("core.arxiv_client.requests.get")
    @patch("core.arxiv_client.time.sleep")
    def test_success_no_retry(self, mock_sleep, mock_get):
        mock_get.return_value = MagicMock()
        mock_get.return_value.raise_for_status.return_value = None

        resp = _retrying_get("http://example.com")
        assert resp is mock_get.return_value
        mock_get.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("core.arxiv_client.requests.get")
    @patch("core.arxiv_client.time.sleep")
    def test_429_retry_then_success(self, mock_sleep, mock_get):
        fail = MagicMock()
        fail.raise_for_status.side_effect = requests.HTTPError(
            "429", response=MagicMock(status_code=429, headers={})
        )
        success = MagicMock()
        success.raise_for_status.return_value = None

        mock_get.side_effect = [fail, success]

        resp = _retrying_get("http://example.com")
        assert resp is success
        assert mock_get.call_count == 2
        assert mock_sleep.call_count == 1
        # First retry delay should be ~3s (base)
        assert 2.5 <= mock_sleep.call_args[0][0] <= 4.0

    @patch("core.arxiv_client.requests.get")
    @patch("core.arxiv_client.time.sleep")
    def test_429_with_retry_after(self, mock_sleep, mock_get):
        fail = MagicMock()
        fail.raise_for_status.side_effect = requests.HTTPError(
            "429", response=MagicMock(status_code=429, headers={"Retry-After": "15"})
        )
        success = MagicMock()
        success.raise_for_status.return_value = None

        mock_get.side_effect = [fail, success]

        _retrying_get("http://example.com")
        mock_sleep.assert_called_once_with(15.0)

    @patch("core.arxiv_client.requests.get")
    @patch("core.arxiv_client.time.sleep")
    def test_non_retryable_fails_fast(self, mock_sleep, mock_get):
        fail = MagicMock()
        fail.raise_for_status.side_effect = requests.HTTPError(
            "404", response=MagicMock(status_code=404, headers={})
        )
        mock_get.return_value = fail

        with pytest.raises(requests.HTTPError):
            _retrying_get("http://example.com")
        mock_get.assert_called_once()
        mock_sleep.assert_not_called()

    @patch("core.arxiv_client.requests.get")
    @patch("core.arxiv_client.time.sleep")
    def test_all_retries_exhausted(self, mock_sleep, mock_get):
        fail = MagicMock()
        fail.raise_for_status.side_effect = requests.HTTPError(
            "503", response=MagicMock(status_code=503, headers={})
        )
        mock_get.return_value = fail

        with pytest.raises(requests.HTTPError):
            _retrying_get("http://example.com")
        assert mock_get.call_count == 3  # _MAX_RETRIES
        assert mock_sleep.call_count == 2  # 2 retries before final failure


class TestFetchArxivAPI:
    @patch("core.arxiv_client._retrying_get")
    def test_empty_response(self, mock_get):
        mock_get.return_value = MagicMock(content=b"""<?xml version="1.0"?>
        <feed xmlns="http://www.w3.org/2005/Atom"></feed>""")
        records = fetch_arxiv_api(search_query="cat:cs.CL")
        assert records == []

    @patch("core.arxiv_client._retrying_get")
    def test_date_filter_appended(self, mock_get):
        mock_get.return_value = MagicMock(content=b"""<?xml version="1.0"?>
        <feed xmlns="http://www.w3.org/2005/Atom"></feed>""")
        fetch_arxiv_api(search_query="cat:cs.CL", submitted_date_from="20240101")
        call = mock_get.call_args
        params = call.args[1]
        assert "submittedDate:[20240101 TO 99991231]" in params["search_query"]


class TestFetchAllSince:
    @patch("core.arxiv_client.fetch_arxiv_api")
    def test_yields_batches_with_newest_published(self, mock_fetch):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        mock_fetch.side_effect = [
            [
                {"id": "1", "published": "2023-01-02T08:00:00Z"},
                {"id": "2", "published": "2023-01-03T08:00:00Z"},
            ],
            [
                {"id": "3", "published": "2023-01-04T08:00:00Z"},
            ],
            [],  # end
        ]

        batches = list(fetch_all_since(since=since, batch_size=2, max_total=10))
        assert len(batches) == 2

        batch1, newest1 = batches[0]
        assert len(batch1) == 2
        assert newest1 == datetime(2023, 1, 3, 8, 0, tzinfo=timezone.utc)

        batch2, newest2 = batches[1]
        assert len(batch2) == 1
        assert newest2 == datetime(2023, 1, 4, 8, 0, tzinfo=timezone.utc)

    @patch("core.arxiv_client.fetch_arxiv_api")
    def test_empty_response_stops_iteration(self, mock_fetch):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        mock_fetch.return_value = []
        batches = list(fetch_all_since(since=since))
        assert batches == []

    @patch("core.arxiv_client.fetch_arxiv_api")
    def test_respects_max_total(self, mock_fetch):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        mock_fetch.side_effect = [
            [{"id": "1", "published": "2023-01-02T08:00:00Z"}],
            [{"id": "2", "published": "2023-01-03T08:00:00Z"}],
        ]
        batches = list(fetch_all_since(since=since, batch_size=1, max_total=1))
        assert len(batches) == 1
        assert mock_fetch.call_count == 1

    @patch("core.arxiv_client.fetch_arxiv_api")
    @patch("core.arxiv_client.time.sleep")
    def test_sleeps_between_batches(self, mock_sleep, mock_fetch):
        since = datetime(2023, 1, 1, tzinfo=timezone.utc)
        mock_fetch.side_effect = [
            [{"id": "1", "published": "2023-01-02T08:00:00Z"}],
            [{"id": "2", "published": "2023-01-03T08:00:00Z"}],
            [],
        ]
        list(fetch_all_since(since=since, batch_size=1, max_total=10))
        assert mock_sleep.call_count == 2
        # Should sleep ~3s between requests
        assert mock_sleep.call_args[0][0] == pytest.approx(3.0, abs=0.1)
