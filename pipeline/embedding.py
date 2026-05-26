import logging

import numpy as np
from sentence_transformers import SentenceTransformer

from core.config import (
    EMBEDDING_BATCH_SIZE,
    EMBEDDING_MAX_DIM,
    MATRYOSHKA_DIMS,
    MODEL_NAME,
)

log = logging.getLogger(__name__)

_model: SentenceTransformer | None = None


def get_model() -> SentenceTransformer:
    """Lazy-load the sentence transformer model (singleton)."""
    global _model
    if _model is None:
        log.info("Loading embedding model: %s", MODEL_NAME)
        _model = SentenceTransformer(MODEL_NAME)
        log.info("Model loaded (max dim=%d)", EMBEDDING_MAX_DIM)
    return _model


def embed_texts(texts: list[str]) -> dict[int, list[list[float]]]:
    """Embed a list of texts and return truncated + normalized vectors at all matryoshka dims.

    Returns:
        A dict mapping dimension -> list of embedding vectors (one per input text).
        e.g. {384: [[...], [...]], 192: [[...], [...]], ...}
    """
    model = get_model()

    # Encode at full dimensionality
    full_embeddings: np.ndarray = model.encode(
        texts,
        batch_size=EMBEDDING_BATCH_SIZE,
        show_progress_bar=len(texts) > EMBEDDING_BATCH_SIZE,
        normalize_embeddings=True,  # L2-normalize the full vectors
    )

    result: dict[int, list[list[float]]] = {}

    for dim in MATRYOSHKA_DIMS:
        if dim == EMBEDDING_MAX_DIM:
            # Full dimension - already normalized
            result[dim] = full_embeddings.tolist()
        else:
            # Truncate and re-normalize
            truncated = full_embeddings[:, :dim]
            norms = np.linalg.norm(truncated, axis=1, keepdims=True)
            # Avoid division by zero (shouldn't happen with real text)
            norms = np.maximum(norms, 1e-12)
            normalized = truncated / norms
            result[dim] = normalized.tolist()

    return result
