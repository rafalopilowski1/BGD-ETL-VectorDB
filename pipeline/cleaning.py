import logging
import re

from pylatexenc.latex2text import LatexNodes2Text

log = logging.getLogger(__name__)

_latex_converter = LatexNodes2Text()


def clean_abstract(raw_abstract: str | None) -> str | None:
    """Clean an arXiv abstract for embedding.

    Steps:
        1. Convert LaTeX markup to readable unicode text.
        2. Collapse whitespace (newlines, tabs, multiple spaces) into single spaces.
        3. Strip leading/trailing whitespace.

    Returns None if the result is empty or conversion fails entirely.
    """
    if not raw_abstract or not raw_abstract.strip():
        return None

    try:
        text = _latex_converter.latex_to_text(raw_abstract)
    except Exception as e:
        log.debug("LaTeX conversion failed, falling back to raw text: %s", e)
        text = raw_abstract

    # Collapse all whitespace runs into a single space
    text = re.sub(r"\s+", " ", text).strip()

    if not text:
        return None

    return text


def clean_title(raw_title: str | None) -> str | None:
    """Clean a paper title. Same light cleaning as abstracts."""
    if not raw_title or not raw_title.strip():
        return None

    try:
        text = _latex_converter.latex_to_text(raw_title)
    except Exception:
        text = raw_title

    text = re.sub(r"\s+", " ", text).strip()
    return text if text else None
