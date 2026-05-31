"""Unit tests for pipeline.cleaning.

These test LaTeX conversion and whitespace normalization in isolation.
"""

import pytest

from pipeline.cleaning import clean_abstract, clean_title


class TestCleanAbstract:
    def test_basic_text(self):
        assert clean_abstract("Hello world") == "Hello world"

    def test_latex_to_text(self):
        result = clean_abstract(r"$\\alpha$ decay")
        assert "α" in result or "alpha" in result.lower()
        assert "decay" in result

    def test_whitespace_collapse(self):
        assert clean_abstract("Hello   world\n\ttab") == "Hello world tab"

    def test_empty_string(self):
        assert clean_abstract("") is None

    def test_none_input(self):
        assert clean_abstract(None) is None

    def test_only_whitespace(self):
        assert clean_abstract("   \\n\\t  ") is None

    def test_latex_conversion_failure_fallback(self):
        bad_input = "\\begin{invalid} some text"
        result = clean_abstract(bad_input)
        assert "some text" in result


class TestCleanTitle:
    def test_basic_title(self):
        assert clean_title("Machine Learning") == "Machine Learning"

    def test_latex_in_title(self):
        result = clean_title(r"$H_2O$ Chemistry")
        assert "Chemistry" in result

    def test_empty_title(self):
        assert clean_title("") is None

    def test_none_title(self):
        assert clean_title(None) is None
