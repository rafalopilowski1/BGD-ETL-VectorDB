"""Client for fetching arXiv papers via the Atom API and RSS feeds.

Supports two modes:
    1. arXiv Atom API -- full metadata, search queries, date filtering.
    2. arXiv RSS feeds -- lightweight, category-based, latest papers.

Both return records that match the JSONL schema expected by the pipeline.
"""

import logging
import random
import re
import time
import xml.etree.ElementTree as ET
from collections.abc import Generator
from datetime import datetime
from pathlib import Path

import requests

log = logging.getLogger(__name__)

ARXIV_API_BASE = "http://export.arxiv.org/api/query"
ARXIV_RSS_BASE = "http://export.arxiv.org/rss"

# Atom namespaces
NS_ATOM = "{http://www.w3.org/2005/Atom}"
NS_ARXIV = "{http://arxiv.org/schemas/atom}"

_REQUEST_TIMEOUT = 30

# arXiv asks for ~3 s between requests; we honour that as the base delay.
_BASE_DELAY = 3.0
_MAX_RETRIES = 3


def _is_retryable_status(status_code: int) -> bool:
    """Return True for status codes that warrant a retry."""
    return status_code in {429, 500, 502, 503, 504}


def _parse_retry_after(response: requests.Response) -> float | None:
    """Extract retry delay (seconds) from Retry-After header if present."""
    header = response.headers.get("Retry-After")
    if not header:
        return None
    try:
        return float(header)
    except ValueError:
        return None


def _retrying_get(url: str, params: dict[str, str] | None = None) -> requests.Response:
    """Perform a GET with polite exponential-backoff retries.

    - Base delay of 3 s between requests, matching arXiv's etiquette.
    - Up to 3 retries on transient errors (same default as arxiv.py).
    - Respects Retry-After when arXiv provides it.
    - Uses GET only (POST bypasses Fastly cache and triggers 429s faster).
    """
    headers = {"User-Agent": "bgd-etl-vectordb/1.0"}
    for attempt in range(1, _MAX_RETRIES + 1):
        try:
            resp = requests.get(url, params=params, timeout=_REQUEST_TIMEOUT, headers=headers)
            resp.raise_for_status()
            return resp
        except requests.HTTPError as e:
            response = e.response
            status_code = response.status_code if response is not None else 0

            if not _is_retryable_status(status_code):
                raise

            retry_after = _parse_retry_after(response) if response is not None else None
            if retry_after is not None:
                delay = retry_after
            else:
                delay = _BASE_DELAY * (2 ** (attempt - 1))

            log.warning(
                "Request failed (attempt %d/%d): HTTP %d – retrying in %.1fs",
                attempt, _MAX_RETRIES, status_code, delay,
            )

            if attempt == _MAX_RETRIES:
                raise
            time.sleep(delay)

        except (requests.Timeout, requests.ConnectionError) as e:
            delay = _BASE_DELAY * (2 ** (attempt - 1))

            log.warning(
                "Request failed (attempt %d/%d): %s – retrying in %.1fs",
                attempt, _MAX_RETRIES, e, delay,
            )

            if attempt == _MAX_RETRIES:
                raise
            time.sleep(delay)

        except requests.RequestException as e:
            log.warning("Request failed (attempt %d/%d): %s", attempt, _MAX_RETRIES, e)
            if attempt == _MAX_RETRIES:
                raise
            time.sleep(_BASE_DELAY)

    raise RuntimeError("Unexpected exit from retry loop")


def _extract_arxiv_id(atom_id: str) -> str | None:
    """Extract the raw arXiv ID from an Atom <id> URL.

    Examples:
        http://arxiv.org/abs/2301.12345   -> 2301.12345
        http://arxiv.org/abs/quant-ph/0202022 -> quant-ph/0202022
    """
    match = re.search(r"/abs/(.+)$", atom_id)
    return match.group(1) if match else None


def _parse_atom_entry(entry: ET.Element) -> dict[str, object] | None:
    """Parse a single Atom <entry> into a record dict."""
    atom_id_elem = entry.find(f"{NS_ATOM}id")
    if atom_id_elem is None or atom_id_elem.text is None:
        return None

    arxiv_id = _extract_arxiv_id(atom_id_elem.text)
    if not arxiv_id:
        return None

    title_elem = entry.find(f"{NS_ATOM}title")
    title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

    summary_elem = entry.find(f"{NS_ATOM}summary")
    summary = summary_elem.text.strip() if summary_elem is not None and summary_elem.text else ""

    authors: list[str] = []
    for author in entry.findall(f"{NS_ATOM}author"):
        name_elem = author.find(f"{NS_ATOM}name")
        if name_elem is not None and name_elem.text:
            authors.append(name_elem.text.strip())

    categories: list[str] = []
    for cat in entry.findall(f"{NS_ATOM}category"):
        term = cat.get("term")
        if term:
            categories.append(term)

    doi_elem = entry.find(f"{NS_ARXIV}doi")
    doi = doi_elem.text.strip() if doi_elem is not None and doi_elem.text else None

    journal_ref_elem = entry.find(f"{NS_ARXIV}journal_ref")
    journal_ref = (
        journal_ref_elem.text.strip()
        if journal_ref_elem is not None and journal_ref_elem.text
        else None
    )

    published_elem = entry.find(f"{NS_ATOM}published")
    published = published_elem.text.strip() if published_elem is not None and published_elem.text else None

    return {
        "id": arxiv_id,
        "title": title,
        "abstract": summary,
        "authors": authors,
        "categories": categories,
        "doi": doi,
        "journal-ref": journal_ref,
        "published": published,
    }


def fetch_arxiv_api(
    search_query: str = "",
    start: int = 0,
    max_results: int = 100,
    sort_by: str = "submittedDate",
    sort_order: str = "descending",
    submitted_date_from: str | None = None,
    submitted_date_to: str | None = None,
) -> list[dict[str, object]]:
    """Fetch papers from the arXiv Atom API.

    Args:
        search_query: arXiv search query (e.g. "cat:cs.CL").
        start: Offset for pagination.
        max_results: Page size (arXiv caps at ~30k total, 2k per query recommended).
        sort_by: "relevance", "lastUpdatedDate", or "submittedDate".
        sort_order: "ascending" or "descending".
        submitted_date_from: Inclusive start date (YYYYMMDD compact format).
        submitted_date_to: Inclusive end date (same format).

    Returns:
        List of record dicts matching the pipeline JSONL schema.
    """
    params: dict[str, str] = {
        "search_query": search_query,
        "start": str(start),
        "max_results": str(max_results),
        "sortBy": sort_by,
        "sortOrder": sort_order,
    }

    # arXiv date range syntax: submittedDate:[YYYYMMDD TO YYYYMMDD]
    # Use sentinel dates for open-ended ranges (* wildcard causes HTTP 500).
    date_filter = ""
    if submitted_date_from:
        lo = submitted_date_from
        hi = submitted_date_to if submitted_date_to else "99991231"
        date_filter = f"submittedDate:[{lo} TO {hi}]"
    elif submitted_date_to:
        date_filter = f"submittedDate:[19000101 TO {submitted_date_to}]"

    if date_filter:
        # Append to search_query if one exists, otherwise use the date filter alone
        if search_query:
            params["search_query"] = f"{search_query} AND {date_filter}"
        else:
            params["search_query"] = date_filter

    log.info("Fetching arXiv API: %s (start=%s, max=%s)", params.get("search_query"), start, max_results)
    resp = _retrying_get(ARXIV_API_BASE, params)

    root = ET.fromstring(resp.content)
    records: list[dict[str, object]] = []
    for entry in root.findall(f"{NS_ATOM}entry"):
        record = _parse_atom_entry(entry)
        if record:
            records.append(record)

    log.info("arXiv API returned %d records", len(records))
    return records


def fetch_arxiv_rss(
    category: str = "cs",
    max_papers: int = 50,
) -> list[dict[str, object]]:
    """Fetch latest papers from an arXiv RSS feed.

    Args:
        category: arXiv category slug (e.g. "cs", "cs.CL", "math", "physics").
        max_papers: Maximum number of papers to return.

    Returns:
        List of record dicts matching the pipeline JSONL schema.
        Note: RSS feeds provide less metadata than the Atom API (no DOI/journal-ref).
    """
    url = f"{ARXIV_RSS_BASE}/{category}"
    log.info("Fetching arXiv RSS: %s", url)
    resp = _retrying_get(url)

    root = ET.fromstring(resp.content)

    # RSS namespace may or may not be present
    ns_rss = "{http://purl.org/rss/1.0/}"
    if root.tag.startswith(ns_rss):
        item_tag = f"{ns_rss}item"
        title_tag = f"{ns_rss}title"
        link_tag = f"{ns_rss}link"
        desc_tag = f"{ns_rss}description"
    else:
        item_tag = "item"
        title_tag = "title"
        link_tag = "link"
        desc_tag = "description"

    # Dublin Core namespace for creators
    ns_dc = "{http://purl.org/dc/elements/1.1/}"
    creator_tag = f"{ns_dc}creator"

    records: list[dict[str, object]] = []
    for item in root.findall(f".//{item_tag}"):
        title_elem = item.find(title_tag)
        title = title_elem.text.strip() if title_elem is not None and title_elem.text else ""

        link_elem = item.find(link_tag)
        link = link_elem.text.strip() if link_elem is not None and link_elem.text else ""

        # Extract arxiv_id from the link
        arxiv_id = _extract_arxiv_id(link)
        if not arxiv_id:
            continue

        desc_elem = item.find(desc_tag)
        # arXiv RSS <description> often wraps the abstract in HTML; strip tags roughly
        abstract = ""
        if desc_elem is not None and desc_elem.text:
            abstract = _strip_html_tags(desc_elem.text.strip())

        authors: list[str] = []
        creator = item.find(creator_tag)
        if creator is not None and creator.text:
            # Often comma-separated
            authors = [a.strip() for a in creator.text.split(",") if a.strip()]

        records.append({
            "id": arxiv_id,
            "title": title,
            "abstract": abstract,
            "authors": authors,
            "categories": [category],
            "doi": None,
            "journal-ref": None,
        })

        if len(records) >= max_papers:
            break

    log.info("arXiv RSS returned %d records", len(records))
    return records


def _strip_html_tags(html: str) -> str:
    """Remove simple HTML tags from a string."""
    return re.sub(r"<[^>]+>", "", html)


def write_records_to_jsonl(records: list[dict[str, object]], filepath: Path) -> int:
    """Write a list of record dicts to a JSONL file.

    Returns the number of records written.
    """
    import json

    filepath.parent.mkdir(parents=True, exist_ok=True)
    with open(filepath, "w", encoding="utf-8") as f:
        for record in records:
            f.write(json.dumps(record, ensure_ascii=False) + "\n")

    log.info("Wrote %d records to %s", len(records), filepath)
    return len(records)


def fetch_all_since(
    since: datetime,
    search_query: str = "",
    batch_size: int = 100,
    max_total: int = 1000,
) -> Generator[tuple[list[dict[str, object]], datetime], None, None]:
    """Generator that yields batches of arXiv papers submitted since a given datetime.

    Args:
        since: Only fetch papers with submittedDate >= this datetime.
        search_query: Optional arXiv search query.
        batch_size: Records per API request.
        max_total: Hard cap on total records to fetch across all batches.

    Yields:
        Tuples of (records, newest_published) where newest_published is the
        maximum <published> timestamp in the batch, allowing the caller to
        advance its checkpoint precisely instead of relying on wall-clock time.
    """
    since_str = since.strftime("%Y%m%d")
    total_fetched = 0
    start = 0

    while total_fetched < max_total:
        records = fetch_arxiv_api(
            search_query=search_query,
            start=start,
            max_results=batch_size,
            sort_by="submittedDate",
            sort_order="ascending",
            submitted_date_from=since_str,
        )

        if not records:
            break

        newest_published = since
        for record in records:
            pub_raw = record.get("published")
            if pub_raw:
                try:
                    pub_dt = datetime.fromisoformat(pub_raw.replace("Z", "+00:00"))
                    if pub_dt > newest_published:
                        newest_published = pub_dt
                except ValueError:
                    continue

        yield records, newest_published

        total_fetched += len(records)
        start += len(records)

        time.sleep(3)


def fetch_latest_rss_batch(
    category: str = "cs",
    max_papers: int = 50,
) -> list[dict[str, object]]:
    """Fetch the latest papers from an arXiv RSS feed.

    This is a thin wrapper around fetch_arxiv_rss for the streaming pipeline.
    """
    return fetch_arxiv_rss(category=category, max_papers=max_papers)
