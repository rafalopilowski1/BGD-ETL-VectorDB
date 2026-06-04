from pathlib import Path
import sys

import psycopg2
import streamlit as st

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from core.db import get_cursor
from pipeline.embedding import embed_texts


st.set_page_config(
    page_title="arXiv Semantic Search",
    page_icon="🔎",
    layout="wide",
)


@st.cache_data(ttl=30)
def fetch_table_counts() -> dict[str, int]:
    sql = """
        SELECT 'bronze' AS layer, COUNT(*) AS count FROM bronze.arxiv_raw
        UNION ALL
        SELECT 'silver' AS layer, COUNT(*) AS count FROM silver.arxiv_abstracts
        UNION ALL
        SELECT 'gold' AS layer, COUNT(*) AS count FROM gold.arxiv_embeddings
    """
    with get_cursor(commit=False) as cursor:
        cursor.execute(sql)
        return {row["layer"]: row["count"] for row in cursor.fetchall()}


@st.cache_data(ttl=30)
def fetch_joined_articles(limit: int) -> list[dict[str, object]]:
    sql = """
        SELECT
            row_number() OVER (ORDER BY g.embedded_at DESC, s.arxiv_id) AS article_no,
            s.arxiv_id,
            s.title,
            s.authors,
            s.categories,
            s.abstract_clean,
            left(g.embedding_384::text, 96) || '...' AS embedding_384_preview,
            'https://arxiv.org/abs/' || s.arxiv_id AS arxiv_url,
            'https://arxiv.org/pdf/' || s.arxiv_id AS pdf_url
        FROM gold.arxiv_embeddings g
        JOIN silver.arxiv_abstracts s ON s.arxiv_id = g.arxiv_id
        ORDER BY g.embedded_at DESC, s.arxiv_id
        LIMIT %s
    """
    with get_cursor(commit=False) as cursor:
        cursor.execute(sql, (limit,))
        return [dict(row) for row in cursor.fetchall()]


def to_pgvector(vector: list[float]) -> str:
    return "[" + ",".join(str(value) for value in vector) + "]"


@st.cache_data(ttl=300)
def search_articles(query: str, top_k: int) -> list[dict[str, object]]:
    query_embedding = embed_texts([query])[384][0]
    query_vector = to_pgvector(query_embedding)

    sql = """
        SELECT
            row_number() OVER (ORDER BY g.embedding_384 <=> %s::vector) AS rank,
            s.arxiv_id,
            s.title,
            s.authors,
            s.categories,
            s.abstract_clean,
            round((1 - (g.embedding_384 <=> %s::vector))::numeric, 4) AS score,
            'https://arxiv.org/abs/' || s.arxiv_id AS arxiv_url,
            'https://arxiv.org/pdf/' || s.arxiv_id AS pdf_url
        FROM gold.arxiv_embeddings g
        JOIN silver.arxiv_abstracts s ON s.arxiv_id = g.arxiv_id
        ORDER BY g.embedding_384 <=> %s::vector
        LIMIT %s
    """
    with get_cursor(commit=False) as cursor:
        cursor.execute(sql, (query_vector, query_vector, query_vector, top_k))
        return [dict(row) for row in cursor.fetchall()]


st.title("arXiv Semantic Search")
st.caption("A basic WebUI for arXiv articles processed by the ETL pipeline.")

with st.sidebar:
    st.header("Configuration")
    top_k = st.slider("Top-k search results", 5, 50, 10, step=5)
    refresh_clicked = st.button("Refresh data")
    if refresh_clicked:
        st.cache_data.clear()

search_query = st.text_input(
    "Search articles",
    placeholder="e.g. transformer models for natural language processing",
)

try:
    counts = fetch_table_counts()
    metric_columns = st.columns(3)
    metric_columns[0].metric("Bronze", counts.get("bronze", 0))
    metric_columns[1].metric("Silver", counts.get("silver", 0))
    metric_columns[2].metric("Gold", counts.get("gold", 0))

    if search_query:
        st.subheader("Semantic search results")
        with st.spinner("Generating query embedding and searching nearest articles..."):
            results = search_articles(search_query, top_k)

        if not results:
            st.warning("No results found for this query.")
        else:
            st.dataframe(
                results,
                width="stretch",
                hide_index=True,
                column_config={
                    "rank": st.column_config.NumberColumn("Rank"),
                    "score": st.column_config.NumberColumn("score", format="%.4f"),
                    "arxiv_url": st.column_config.LinkColumn("arXiv"),
                    "pdf_url": st.column_config.LinkColumn("PDF"),
                    "abstract_clean": st.column_config.TextColumn(
                        "abstract_clean",
                        width="large",
                    ),
                },
            )

    st.subheader("25 recently processed articles with embeddings")
    articles = fetch_joined_articles(25)

    if not articles:
        st.warning(
            "No records to display. Run the ETL pipeline and make sure "
            "gold.arxiv_embeddings contains data."
        )
    else:
        st.dataframe(
            articles,
            width="stretch",
            hide_index=True,
            column_config={
                "article_no": st.column_config.NumberColumn("No."),
                "arxiv_url": st.column_config.LinkColumn("arXiv"),
                "pdf_url": st.column_config.LinkColumn("PDF"),
                "abstract_clean": st.column_config.TextColumn(
                    "abstract_clean",
                    width="large",
                ),
                "embedding_384_preview": st.column_config.TextColumn(
                    "embedding_384_preview",
                    width="large",
                ),
            },
        )

except psycopg2.Error as error:
    st.error("Failed to fetch data from PostgreSQL.")
    st.code(str(error))
