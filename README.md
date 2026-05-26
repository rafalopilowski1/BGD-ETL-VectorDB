# BGD-ETL-VectorDB

ETL pipeline that ingests arXiv paper abstracts, cleans them, and generates
embedding vectors at multiple dimensions using Matryoshka Representation
Learning (MRL). Stores results in PostgreSQL with pgvector.

## Architecture

The pipeline follows a medallion (bronze/silver/gold) architecture:

```
.jsonl file arrives in data/incoming/
  -> Bronze: raw JSON records loaded into PostgreSQL as JSONB
  -> Silver: abstracts cleaned (LaTeX to text, whitespace normalized), fields parsed
  -> Gold:   embeddings generated at 384, 192, 96, 48 dimensions
  -> File moved to data/processed/
```

Three ingestion modes are available:
- **Direct watcher** -- watchdog monitors `data/incoming/` and runs the pipeline directly
- **Kafka** -- a producer watches `data/incoming/` and publishes file paths to a Kafka
  topic; a separate consumer reads the topic and runs the pipeline
- **Streaming** -- continuously polls the arXiv Atom API or RSS feed, fetches new
  papers, and runs the pipeline in a loop. State is persisted in PostgreSQL so
  restarts resume from the last successful checkpoint.

### Project Structure

```
core/
  config.py         # All configuration (DB, model, Kafka, paths)
  db.py             # PostgreSQL connection helpers
  watcher.py        # Direct file watcher + pipeline orchestration
  producer.py       # Kafka producer (file watcher -> topic)
  consumer.py       # Kafka consumer (topic -> pipeline)
  arxiv_client.py   # arXiv Atom API & RSS client
  streamer.py       # Continuous streaming pipeline
pipeline/
  bronze.py         # Raw JSONL -> bronze.arxiv_raw
  cleaning.py       # LaTeX-to-text, whitespace normalization
  silver.py         # bronze -> silver.arxiv_abstracts
  embedding.py      # SentenceTransformer wrapper, MRL truncation
  gold.py           # silver -> gold.arxiv_embeddings
scripts/
  split_jsonl.py    # Utility to split large JSONL files
sql/
  00_init.sql       # Schema and table definitions
  01_streaming_state.sql  # Streaming checkpoint state table
data/
  incoming/         # Drop .jsonl files here to trigger pipeline
  processed/        # Files moved here after successful processing
```

### Database Schema

| Table | Schema | Purpose |
|---|---|---|
| `bronze.arxiv_raw` | `arxiv_id`, `raw_data` (JSONB), `source_file`, `ingested_at` | Raw landing |
| `silver.arxiv_abstracts` | `arxiv_id`, `title`, `abstract_clean`, `authors`, `categories`, `doi`, `journal_ref` | Cleaned records |
| `gold.arxiv_embeddings` | `arxiv_id`, `embedding_384`, `embedding_192`, `embedding_96`, `embedding_48`, `model_name` | Matryoshka embeddings |

### Embedding Model

Uses [mixedbread-ai/mxbai-embed-xsmall-v1](https://huggingface.co/mixedbread-ai/mxbai-embed-xsmall-v1)
via `sentence-transformers`. The model produces 384-dimensional embeddings. Smaller
dimensions (192, 96, 48) are obtained by truncating the full vector and L2-normalizing
the result -- this is how MRL is designed to work and avoids redundant model calls.

## Prerequisites

- Python 3.13
- [uv](https://docs.astral.sh/uv/) (package manager)
- Docker / Podman with Compose
- [just](https://github.com/casey/just) (command runner)

## Setup

### Local (uv + Docker services)

```bash
# Install Python dependencies
uv sync

# Start all services (PostgreSQL + pgvector, Kafka, Redpanda Console, ETL)
just up

# Initialize database (create schemas, tables, indexes, streaming state)
just db-init
```

### Fully Dockerized (no local Python needed)

```bash
# Start all services -- the ETL streamer runs automatically in its container
just up

# Initialize the database from inside Docker
just db-init

# View ETL logs
just etl-logs
```

## Usage

### Option A: Direct Watcher (no Kafka)

```bash
# Watch data/incoming/ for new .jsonl files (Ctrl+C to stop)
just watch

# Or process a specific file directly
just run data/incoming/some_file.jsonl
```

### Option B: Kafka Pipeline

Run the producer and consumer in separate terminals:

```bash
# Terminal 1: watch for files and publish to Kafka
just kafka-producer

# Terminal 2: consume from Kafka and run pipeline
just kafka-consumer
```

Then drop a `.jsonl` file into `data/incoming/`.

### Option C: Streaming Pipeline (no files, no Kafka)

Continuously poll arXiv for new papers and process them automatically:

```bash
# Stream all recent arXiv papers (default: last 24h, then every 5 min)
just stream-arxiv

# Stream a specific category or query
just stream-arxiv "cat:cs.CL"
just stream-arxiv "all:machine+learning"

# Stream from arXiv RSS feed instead of the Atom API
just stream-arxiv-rss
```

Environment variables for tuning the streamer:

| Variable | Default | Description |
|---|---|---|
| `STREAM_INTERVAL_SECONDS` | `300` | Seconds between polling cycles |
| `STREAM_BATCH_SIZE` | `100` | Papers fetched per API request |
| `STREAM_MAX_TOTAL` | `1000` | Max papers per cycle |
| `STREAM_RSS_CATEGORY` | `cs` | RSS category slug |
| `STREAM_RSS_MAX_PAPERS` | `50` | Max papers from RSS per cycle |

### Option D: Docker-Compose (everything in containers)

All services including the ETL pipeline can run inside Docker. The `etl` service is built from the included `Dockerfile` and starts the streamer by default.

```bash
# Start all services (postgres, kafka, redpanda, etl streamer)
just up

# The ETL container starts automatically and begins streaming.
# View its logs:
just etl-logs

# Run the watcher inside Docker instead (process files from data/incoming/)
just etl-watch

# Process a single file inside Docker
just etl-run dev_sample.jsonl

# Run the streaming pipeline inside Docker with a custom query
just etl-stream "cat:cs.CL"

# Run the RSS streamer inside Docker
just etl-stream-rss

# Run Kafka producer / consumer inside Docker
just etl-kafka-producer
just etl-kafka-consumer
```

Volumes mounted by the `etl` service:

| Host path | Container path | Purpose |
|---|---|---|
| `./data` | `/app/data` | Shared data directory (incoming / processed / split) |
| `model_cache` (named volume) | `/root/.cache` | Persisted embedding model downloads |

### Dev Sample

Extract a small 200-record sample from the full dataset for testing:

```bash
just dev-sample    # creates data/incoming/dev_sample.jsonl
```

## Testing

The project includes a comprehensive test suite covering unit, integration, and e2e tests:

```bash
# Run all tests
just test

# Or run directly with pytest
uv run pytest tests/ -x
```

| Test File | Coverage |
|---|---|
| `tests/test_arxiv_client_unit.py` | arXiv API client (Atom API & RSS parsing, retries) |
| `tests/test_streamer_unit.py` | Streaming pipeline orchestration logic |
| `tests/test_streamer_integration.py` | Database state management integration |
| `tests/test_streamer_e2e.py` | End-to-end streaming with mocked arXiv API |
| `tests/test_watcher_unit.py` | File watcher and pipeline stage orchestration |
| `tests/test_cleaning_unit.py` | LaTeX-to-text cleaning logic |
| `tests/test_embedding_unit.py` | Embedding generation and MRL truncation |

## Data

The pipeline expects JSONL files where each line is a JSON object with at least
an `id` and `abstract` field. The arXiv abstracts dataset (`arxiv-abstracts.jsonl`,
~2M records) is the primary data source. It is not included in the repository.

To split the full dataset into smaller parts:

```bash
just split-data
```

## Justfile Commands

| Command | Description |
|---|---|
| `just up` | Start Docker services |
| `just db-init` | Initialize database schemas and tables |
| `just db-reset` | Drop all schemas and reinitialize |
| `just db-shell` | Open interactive psql session |
| `just db-counts` | Show row counts for all tables |
| `just db-peek` | Show first 2 rows from each table |
| `just watch` | Start direct file watcher |
| `just run <file>` | Process a single file |
| `just kafka-producer` | Start Kafka producer (file watcher) |
| `just kafka-consumer` | Start Kafka consumer (pipeline runner) |
| `just stream-arxiv <query>` | Start continuous arXiv API streaming pipeline |
| `just stream-arxiv-rss` | Start continuous arXiv RSS streaming pipeline |
| `just build` | Build the ETL Docker image |
| `just etl-logs` | Tail logs from the running ETL container |
| `just etl-stream <query>` | Run streamer inside Docker |
| `just etl-stream-rss` | Run RSS streamer inside Docker |
| `just etl-watch` | Run file watcher inside Docker |
| `just etl-run <file>` | Process a single file inside Docker |
| `just etl-kafka-producer` | Run Kafka producer inside Docker |
| `just etl-kafka-consumer` | Run Kafka consumer inside Docker |
| `just test` | Run the full test suite |
| `just dev-sample` | Extract 200-record test sample |
| `just split-data` | Split full dataset into parts |

## Configuration

All config is in `core/config.py` with environment variable overrides:

| Variable | Default | Description |
|---|---|---|
| `DATABASE_URL` | `postgresql://bgd:bgd@localhost:5432/bgd` | PostgreSQL connection string |
| `DB_HOST`, `DB_PORT`, `DB_NAME`, `DB_USER`, `DB_PASSWORD` | `localhost`, `5432`, `bgd`, `bgd`, `bgd` | Individual DB connection params (used if `DATABASE_URL` not set) |
| `KAFKA_BOOTSTRAP` | `localhost:9092` | Kafka broker address |
| `KAFKA_TOPIC` | `bgd-incoming-files` | Topic for file notifications |
| `KAFKA_GROUP_ID` | `bgd-pipeline` | Consumer group ID |

## Services

Started via `docker compose up -d`:

| Service | Port | Description |
|---|---|---|
| PostgreSQL + pgvector | 5432 | Vector database |
| Kafka (KRaft) | 9092 | Message broker |
| Redpanda Console | 8080 | Kafka UI at http://localhost:8080 |
| ETL (Streamer) | — | Continuous arXiv ingestion pipeline |
