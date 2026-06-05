#!/usr/bin/env python3
"""Simple benchmark for Matryoshka embedding retrieval across dimensions."""

import json
import sys
from pathlib import Path
from typing import Any, TypedDict

import numpy as np

from core.config import MATRYOSHKA_DIMS, MODEL_NAME
from core.db import get_cursor
from pipeline.embedding import embed_texts

TOP_K = 10  # How many results to retrieve per query
MATCH_THRESHOLD = 1  # Position (1-indexed) the correct arxiv_id must be within

if MATCH_THRESHOLD > TOP_K:
    raise ValueError(
        f"MATCH_THRESHOLD ({MATCH_THRESHOLD}) cannot exceed TOP_K ({TOP_K})"
    )


class DimStats(TypedDict):
    hits: int
    ranks: list[int]


def _validate_ids_in_gold(ground_truth_ids: list[str]) -> None:
    """Abort if any arxiv_ids are missing from gold.arxiv_embeddings."""
    sql = """
        SELECT arxiv_id FROM gold.arxiv_embeddings
        WHERE arxiv_id = ANY(%s)
    """
    with get_cursor(commit=False) as cur:
        cur.execute(sql, (ground_truth_ids,))
        found = {row["arxiv_id"] for row in cur.fetchall()}

    missing = [aid for aid in ground_truth_ids if aid not in found]
    if missing:
        print(
            f"Error: {len(missing)} arxiv_id(s) from the benchmark test set are missing from gold.arxiv_embeddings:"
        )
        for aid in missing:
            print(f"  - {aid}")
        print("Please run the pipeline so the gold table includes all benchmark IDs.")
        sys.exit(1)


def load_test_set(path: Path) -> list[dict[str, Any]]:
    with open(path, "r", encoding="utf-8") as f:
        return json.load(f)


def search_dimension(dim: int, query_vector: list[float]) -> list[str]:
    """Return top K arxiv_ids for a given dimension using cosine similarity."""
    col_name = f"embedding_{dim}"
    sql = f"""
        SELECT arxiv_id
        FROM gold.arxiv_embeddings
        ORDER BY {col_name} <=> %s::vector
        LIMIT %s
    """
    with get_cursor(commit=False) as cur:
        cur.execute(sql, (query_vector, TOP_K))
        return [row["arxiv_id"] for row in cur.fetchall()]


def run_benchmark(test_set: list[dict[str, Any]]) -> None:
    queries = [item["query"] for item in test_set]
    ground_truth = [item["arxiv_id"] for item in test_set]
    n = len(test_set)

    _validate_ids_in_gold(ground_truth)
    print(f"Benchmarking {n} queries")
    print(f"Model: {MODEL_NAME}")
    print(f"Top-K retrieved: {TOP_K}  |  Match threshold: <= {MATCH_THRESHOLD}")
    print("-" * 65)

    embeddings_by_dim = embed_texts(queries)

    stats: dict[int, DimStats] = {
        dim: {"hits": 0, "ranks": []} for dim in MATRYOSHKA_DIMS
    }

    for i, truth_id in enumerate(ground_truth):
        for dim in MATRYOSHKA_DIMS:
            top_ids = search_dimension(dim, embeddings_by_dim[dim][i])

            try:
                rank = top_ids.index(truth_id) + 1  # 1-indexed
            except ValueError:
                rank = None

            if rank is not None and rank <= MATCH_THRESHOLD:
                stats[dim]["hits"] += 1

            # Use TOP_K + 1 as penalty when not found, for avg-rank calculation
            stats[dim]["ranks"].append(rank if rank is not None else TOP_K + 1)

    print(f"{'Dimension':<12} {'Accuracy':<12} {'Avg Rank':<12} {'MRR':<12}")
    print("-" * 65)

    for dim in MATRYOSHKA_DIMS:
        hits: int = stats[dim]["hits"]
        ranks: list[int] = stats[dim]["ranks"]
        accuracy = hits / n
        avg_rank = float(np.mean(ranks))
        mrr = float(np.mean([1.0 / r for r in ranks]))

        print(f"{dim:<12} {accuracy:<12.2%} {avg_rank:<12.2f} {mrr:<12.4f}")

    print("-" * 65)
    print(f"Accuracy = % of queries where ground-truth is in top {MATCH_THRESHOLD}")
    print(f"MRR      = Mean Reciprocal Rank (higher is better, max = 1.0)")


if __name__ == "__main__":
    test_path = Path(__file__).parent / "test_set.json"
    test_set = load_test_set(test_path)
    run_benchmark(test_set)
