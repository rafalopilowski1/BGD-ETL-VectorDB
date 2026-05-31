
default:
  @just --list

split-data:
  uv run python -m scripts.split_jsonl

up:
  docker compose up -d
  @echo "Waiting for PostgreSQL to be ready..."
  @sleep 3

build:
  docker compose build etl

etl-logs:
  docker compose logs -f etl

etl-stream query="":
  docker compose run --rm etl python -m core.streamer --query "{{query}}"

etl-stream-rss:
  docker compose run --rm etl python -m core.streamer --rss

etl-watch:
  docker compose run --rm etl python -m core.watcher

etl-run file:
  docker compose run --rm etl python -m core.watcher --file /app/data/incoming/{{file}}

etl-kafka-producer:
  docker compose run --rm etl python -m core.producer

etl-kafka-consumer:
  docker compose run --rm etl python -m core.consumer

db-init:
  @echo "Initializing database schemas and tables..."
  docker compose exec -T postgres psql -U bgd -d bgd -f /sql/00_init.sql
  docker compose exec -T postgres psql -U bgd -d bgd -f /sql/01_streaming_state.sql
  @echo "Database initialized."

db-reset:
  @echo "Dropping and recreating database objects..."
  docker compose exec -T postgres psql -U bgd -d bgd -c "DROP SCHEMA IF EXISTS gold CASCADE; DROP SCHEMA IF EXISTS silver CASCADE; DROP SCHEMA IF EXISTS bronze CASCADE;"
  just db-init

db-shell:
  docker compose exec postgres psql -U bgd -d bgd

watch:
  uv run python -m core.watcher

run file:
  uv run python -m core.watcher --file {{file}}

kafka-producer:
  uv run python -m core.producer

kafka-consumer:
  uv run python -m core.consumer

stream-arxiv query="":
  uv run python -m core.streamer --query "{{query}}"

stream-arxiv-rss:
  uv run python -m core.streamer --rss

test:
  uv run pytest tests/ -x

dev-sample:
  @echo "Extracting 200-line dev sample..."
  head -n 200 data/arxiv-abstracts.jsonl > data/incoming/dev_sample.jsonl
  @echo "Created data/incoming/dev_sample.jsonl (200 records)"

db-counts:
  @docker compose exec -T postgres psql -U bgd -d bgd --no-align --tuples-only -c " \
    SELECT 'bronze.arxiv_raw', COUNT(*) FROM bronze.arxiv_raw \
    UNION ALL \
    SELECT 'silver.arxiv_abstracts', COUNT(*) FROM silver.arxiv_abstracts \
    UNION ALL \
    SELECT 'gold.arxiv_embeddings', COUNT(*) FROM gold.arxiv_embeddings;" \
    | column -t -s '|'

db-peek:
  @echo "=== bronze.arxiv_raw (first 2) ==="
  @docker compose exec -T postgres psql -U bgd -d bgd -c " \
    SELECT id, arxiv_id, source_file, ingested_at, \
           left(raw_data::text, 120) || '...' AS raw_data_preview \
    FROM bronze.arxiv_raw ORDER BY id LIMIT 2;"
  @echo ""
  @echo "=== silver.arxiv_abstracts (first 2) ==="
  @docker compose exec -T postgres psql -U bgd -d bgd -c " \
    SELECT id, arxiv_id, \
           left(title, 60) AS title, \
           left(abstract_clean, 80) || '...' AS abstract_preview, \
           categories, processed_at \
    FROM silver.arxiv_abstracts ORDER BY id LIMIT 2;"
  @echo ""
  @echo "=== gold.arxiv_embeddings (first 2) ==="
  @docker compose exec -T postgres psql -U bgd -d bgd -c " \
    SELECT id, arxiv_id, model_name, embedded_at, \
           left(embedding_384::text, 40) || '...]' AS emb_384_preview, \
           left(embedding_192::text, 40) || '...]' AS emb_192_preview, \
           left(embedding_96::text, 40) || '...]' AS emb_96_preview, \
           left(embedding_48::text, 40) || '...]' AS emb_48_preview \
    FROM gold.arxiv_embeddings ORDER BY id LIMIT 2;"
