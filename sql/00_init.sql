-- Initialize database: extensions, schemas, and tables

CREATE EXTENSION IF NOT EXISTS vector;

-- Create medallion architecture schemas
DO $$
BEGIN
    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'bronze') THEN
        CREATE SCHEMA bronze;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'silver') THEN
        CREATE SCHEMA silver;
    END IF;

    IF NOT EXISTS (SELECT 1 FROM information_schema.schemata WHERE schema_name = 'gold') THEN
        CREATE SCHEMA gold;
    END IF;
END
$$;

-- Bronze: raw landing table for JSONL records
CREATE TABLE IF NOT EXISTS bronze.arxiv_raw (
    id          SERIAL PRIMARY KEY,
    arxiv_id    TEXT UNIQUE NOT NULL,
    raw_data    JSONB NOT NULL,
    source_file TEXT,
    ingested_at TIMESTAMPTZ DEFAULT NOW()
);

-- Silver: cleaned and parsed abstracts
CREATE TABLE IF NOT EXISTS silver.arxiv_abstracts (
    id              SERIAL PRIMARY KEY,
    arxiv_id        TEXT UNIQUE NOT NULL,
    title           TEXT NOT NULL,
    abstract_clean  TEXT NOT NULL,
    authors         TEXT,
    categories      TEXT[],
    doi             TEXT,
    journal_ref     TEXT,
    processed_at    TIMESTAMPTZ DEFAULT NOW()
);

-- Gold: embeddings at multiple matryoshka dimensions
CREATE TABLE IF NOT EXISTS gold.arxiv_embeddings (
    id              SERIAL PRIMARY KEY,
    arxiv_id        TEXT UNIQUE NOT NULL,
    embedding_384   vector(384) NOT NULL,
    embedding_192   vector(192) NOT NULL,
    embedding_96    vector(96) NOT NULL,
    embedding_48    vector(48) NOT NULL,
    model_name      TEXT NOT NULL DEFAULT 'mxbai-embed-xsmall-v1',
    embedded_at     TIMESTAMPTZ DEFAULT NOW()
);

-- Indexes for lookup by arxiv_id across layers
CREATE INDEX IF NOT EXISTS idx_bronze_arxiv_id ON bronze.arxiv_raw (arxiv_id);
CREATE INDEX IF NOT EXISTS idx_silver_arxiv_id ON silver.arxiv_abstracts (arxiv_id);
CREATE INDEX IF NOT EXISTS idx_gold_arxiv_id   ON gold.arxiv_embeddings (arxiv_id);
CREATE INDEX IF NOT EXISTS idx_gold_embedding_384_hnsw
    ON gold.arxiv_embeddings
    USING hnsw (embedding_384 vector_cosine_ops);
