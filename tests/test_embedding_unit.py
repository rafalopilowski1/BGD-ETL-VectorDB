"""Unit tests for pipeline.embedding.

These test the embedding generation logic with a mocked SentenceTransformer
to avoid downloading models during unit tests.
"""

import numpy as np
import pytest
from unittest.mock import MagicMock, patch

import pipeline.embedding as embedding_module
from pipeline.embedding import embed_texts, get_model


@pytest.fixture(autouse=True)
def reset_model_singleton():
    """Reset the lazy-loaded model so each test gets a fresh mock."""
    embedding_module._model = None
    yield
    embedding_module._model = None


class TestEmbedTexts:
    @patch("pipeline.embedding.SentenceTransformer")
    def test_returns_all_dimensions(self, mock_st):
        mock_model = MagicMock()
        mock_model.encode.return_value = np.ones((2, 384), dtype=float)
        mock_st.return_value = mock_model

        result = embed_texts(["hello", "world"])

        assert 384 in result
        assert 192 in result
        assert 96 in result
        assert 48 in result

        assert len(result[384]) == 2
        assert len(result[192]) == 2
        assert len(result[96]) == 2
        assert len(result[48]) == 2

    @patch("pipeline.embedding.SentenceTransformer")
    def test_384_is_full_vector(self, mock_st):
        mock_model = MagicMock()
        full = np.ones((1, 384), dtype=float)
        mock_model.encode.return_value = full
        mock_st.return_value = mock_model

        result = embed_texts(["test"])
        np.testing.assert_allclose(result[384][0], full[0], rtol=1e-5)

    @patch("pipeline.embedding.SentenceTransformer")
    def test_lower_dims_are_truncated(self, mock_st):
        mock_model = MagicMock()
        full = np.ones((1, 384), dtype=float)
        mock_model.encode.return_value = full
        mock_st.return_value = mock_model

        result = embed_texts(["test"])

        # 192 should be first 192 dims
        assert len(result[192][0]) == 192
        # 48 should be first 48 dims
        assert len(result[48][0]) == 48

    @patch("pipeline.embedding.SentenceTransformer")
    def test_lower_dims_are_normalized(self, mock_st):
        mock_model = MagicMock()
        full = np.array([[3.0, 4.0] + [0.0] * 382], dtype=float)
        mock_model.encode.return_value = full
        mock_st.return_value = mock_model

        result = embed_texts(["test"])

        # First 2 dims truncated -> [3, 4], norm = 5
        # Normalized -> [3/5, 4/5] = [0.6, 0.8]
        vec_2 = np.array(result[48][0][:2])
        np.testing.assert_allclose(vec_2, [0.6, 0.8], rtol=1e-5)

    @patch("pipeline.embedding.SentenceTransformer")
    def test_empty_list(self, mock_st):
        mock_model = MagicMock()
        mock_model.encode.return_value = np.zeros((0, 384), dtype=float)
        mock_st.return_value = mock_model

        result = embed_texts([])
        assert result[384] == []
        assert result[192] == []
        assert result[96] == []
        assert result[48] == []


class TestGetModel:
    @patch("pipeline.embedding.SentenceTransformer")
    def test_singleton(self, mock_st):
        mock_st.return_value = MagicMock()

        m1 = get_model()
        m2 = get_model()
        assert m1 is m2
        mock_st.assert_called_once()
